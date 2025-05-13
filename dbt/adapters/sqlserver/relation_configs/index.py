from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import FrozenSet, Optional, Set, Tuple

import agate
from dbt.adapters.exceptions import IndexConfigError, IndexConfigNotDictError
from dbt.adapters.relation_configs import (
    RelationConfigBase,
    RelationConfigChange,
    RelationConfigChangeAction,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt_common.dataclass_schema import StrEnum, ValidationError, dbtClassMixin
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.utils import encoding as dbt_encoding


# Handle datetime now for testing.
def datetime_now(tz: Optional[timezone] = timezone.utc) -> datetime:
    return datetime.now(tz)


# ALTERED FROM:
# github.com/dbt-labs/dbt-postgres/blob/main/dbt/adapters/postgres/relation_configs/index.py
class SQLServerIndexType(StrEnum):
    # btree = "btree" #All SQL Server common indexes are B-tree indexes
    # hash = "hash" #A hash index can exist only on a memory-optimized table.
    # TODO Implement memory optimized table materialization.
    clustered = "clustered"  # Cant't have included columns
    nonclustered = "nonclustered"
    columnstore = "columnstore"  # Cant't have included columns or unique config

    @classmethod
    def default(cls) -> "SQLServerIndexType":
        return cls("nonclustered")

    @classmethod
    def valid_types(cls):
        return tuple(cls)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SQLServerIndexConfig(RelationConfigBase, RelationConfigValidationMixin, dbtClassMixin):
    """
    This config follows the specs found here:

    https://learn.microsoft.com/en-us/sql/t-sql/statements/create-index-transact-sql

    The following parameters are configurable by dbt:
    - name: the name of the index in the database, isn't predictable since we apply a timestamp
    - unique: checks for duplicate values when the index is created and on data updates
    - type: the index type method to be used
    - columns: the columns names in the index
    - included_columns: the extra included columns names in the index

    """

    name: str = field(default="", hash=False, compare=False)
    columns: Tuple[str, ...] = field(
        default_factory=tuple, hash=True
    )  # Keeping order is important
    unique: bool = field(
        default=False, hash=True
    )  # Uniqueness can be a property of both clustered and nonclustered indexes.
    type: SQLServerIndexType = field(default=SQLServerIndexType.default(), hash=True)
    included_columns: FrozenSet[str] = field(
        default_factory=frozenset, hash=True
    )  # Keeping order is not important

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return {
            RelationConfigValidationRule(
                validation_check=True if self.columns else False,
                validation_error=DbtRuntimeError("'columns' is a required property"),
            ),
            RelationConfigValidationRule(
                validation_check=(
                    True
                    if not self.included_columns
                    else self.type == SQLServerIndexType.nonclustered
                ),
                validation_error=DbtRuntimeError(
                    "Non-clustered indexes are the only index types that can include extra columns"
                ),
            ),
            RelationConfigValidationRule(
                validation_check=(
                    True
                    if not self.unique
                    else self.type
                    in (SQLServerIndexType.clustered, SQLServerIndexType.nonclustered)
                ),
                validation_error=DbtRuntimeError(
                    "Clustered and nonclustered indexes are the only types that can be unique"
                ),
            ),
            RelationConfigValidationRule(
                validation_check=True if self.type in SQLServerIndexType.valid_types() else False,
                validation_error=DbtRuntimeError(
                    f"Invalid index type: {self.type}, valid types:"
                    + f"{SQLServerIndexType.valid_types()}"
                ),
            ),
        }

    @classmethod
    def from_dict(cls, config_dict) -> "SQLServerIndexConfig":
        kwargs_dict = {
            "name": config_dict.get("name"),
            "columns": tuple(column for column in config_dict.get("columns", tuple())),
            "unique": config_dict.get("unique"),
            "type": config_dict.get("type"),
            "included_columns": frozenset(
                column for column in config_dict.get("included_columns", set())
            ),
        }
        index: "SQLServerIndexConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return index

    @classmethod
    def parse_model_node(cls, model_node_entry: dict) -> dict:
        config_dict = {
            "columns": tuple(model_node_entry.get("columns", tuple())),
            "unique": model_node_entry.get("unique"),
            "type": model_node_entry.get("type"),
            "included_columns": frozenset(model_node_entry.get("included_columns", set())),
        }
        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results_entry: agate.Row) -> dict:
        config_dict = {
            "name": relation_results_entry.get("name"),
            "columns": tuple(relation_results_entry.get("columns", "").split(",")),
            "unique": relation_results_entry.get("unique"),
            "type": relation_results_entry.get("type"),
            "included_columns": set(relation_results_entry.get("included_columns", "").split(",")),
        }
        return config_dict

    @property
    def as_node_config(self) -> dict:
        """
        Returns: a dictionary that can be passed into `get_create_index_sql()`
        """
        node_config = {
            "columns": tuple(self.columns),
            "unique": self.unique,
            "type": self.type.value,
            "included_columns": frozenset(self.included_columns),
        }
        return node_config

    def render(self, relation):
        # We append the current timestamp to the index name because otherwise
        # the index will only be created on every other run. See
        # https://github.com/dbt-labs/dbt-core/issues/1945#issuecomment-576714925
        # for an explanation.

        now = datetime_now(tz=timezone.utc).isoformat()
        inputs = self.columns + tuple((relation.render(), str(self.unique), str(self.type), now))
        string = "_".join(inputs)
        print(f"Actual string before MD5: {string}")
        return dbt_encoding.md5(string)

    @classmethod
    def parse(cls, raw_index) -> Optional["SQLServerIndexConfig"]:
        if raw_index is None:
            return None
        try:
            if not isinstance(raw_index, dict):
                raise IndexConfigNotDictError(raw_index)
            cls.validate(raw_index)
            return cls.from_dict(raw_index)
        except ValidationError as exc:
            raise IndexConfigError(exc)
        except TypeError:
            raise IndexConfigNotDictError(raw_index)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SQLServerIndexConfigChange(RelationConfigChange, RelationConfigValidationMixin):
    """
    Example of an index change:
    {
        "action": "create",
        "context": {
            "name": "",  # we don't know the name since it gets created as a hash at runtime
            "columns": ["column_1", "column_3"],
            "type": "clustered",
            "unique": True
        }
    },
    {
        "action": "drop",
        "context": {
            "name": "index_abc",  # we only need this to drop, but we need the rest to compare
            "columns": ["column_1"],
            "type": "nonclustered",
            "unique": True
        }
    }
    """

    # TODO: Implement the change actions on the adapter
    context: SQLServerIndexConfig

    @property
    def requires_full_refresh(self) -> bool:
        return False

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return {
            RelationConfigValidationRule(
                validation_check=self.action
                in {RelationConfigChangeAction.create, RelationConfigChangeAction.drop},
                validation_error=DbtRuntimeError(
                    "Invalid operation, only `drop` and `create` are supported for indexes."
                ),
            ),
            RelationConfigValidationRule(
                validation_check=not (
                    self.action == RelationConfigChangeAction.drop and self.context.name is None
                ),
                validation_error=DbtRuntimeError(
                    "Invalid operation, attempting to drop an index with no name."
                ),
            ),
            RelationConfigValidationRule(
                validation_check=not (
                    self.action == RelationConfigChangeAction.create
                    and self.context.columns == set()
                ),
                validation_error=DbtRuntimeError(
                    "Invalid operations, attempting to create an index with no columns."
                ),
            ),
        }
