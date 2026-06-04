from dataclasses import dataclass, field
from typing import FrozenSet, Optional, Set, Tuple

import agate
from dbt_common.dataclass_schema import StrEnum, ValidationError, dbtClassMixin
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.utils import encoding as dbt_encoding

from dbt.adapters.exceptions import IndexConfigError, IndexConfigNotDictError
from dbt.adapters.relation_configs import (
    RelationConfigBase,
    RelationConfigChange,
    RelationConfigChangeAction,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)

# Prefix identifying indexes whose lifecycle is managed by the adapter via the
# `indexes` model config. Reconciliation only ever drops indexes carrying it.
SQLSERVER_MANAGED_INDEX_PREFIX = "dbt_idx_"


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
    - name: the name of the index in the database; deterministic hash of the full
      definition (so a definition change always produces a new name)
    - unique: checks for duplicate values when the index is created and on data updates
    - type: the index type method to be used
    - columns: the columns names in the index
    - included_columns: the extra included columns names in the index
    - data_compression: none | row | page (rowstore indexes only)
    - sort_in_tempdb: build-time option for the create statement; deliberately
      excluded from identity (not introspectable and doesn't change the
      resulting index, so it must not trigger drop/recreate)

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
    data_compression: Optional[str] = field(default=None, hash=True)
    sort_in_tempdb: bool = field(default=False, hash=False, compare=False)

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
            RelationConfigValidationRule(
                validation_check=self.data_compression in (None, "none", "row", "page"),
                validation_error=DbtRuntimeError(
                    f"Invalid data_compression: {self.data_compression}, "
                    "valid values: none, row, page"
                ),
            ),
            RelationConfigValidationRule(
                validation_check=not (
                    self.type == SQLServerIndexType.columnstore and self.data_compression
                ),
                validation_error=DbtRuntimeError(
                    "data_compression is not configurable for columnstore indexes here; "
                    "columnstore compression is managed by the engine "
                    "(COLUMNSTORE / COLUMNSTORE_ARCHIVE via maintenance, not CREATE INDEX)"
                ),
            ),
            RelationConfigValidationRule(
                validation_check=not (
                    self.type == SQLServerIndexType.columnstore and self.sort_in_tempdb
                ),
                validation_error=DbtRuntimeError(
                    "sort_in_tempdb is not valid for columnstore indexes"
                ),
            ),
        }

    @staticmethod
    def _normalize_data_compression(value):
        if isinstance(value, str):
            value = value.lower()
        # "none" is the engine default: normalize so it hashes/compares the
        # same as omitting the key entirely.
        return None if value in (None, "none") else value

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
            "data_compression": cls._normalize_data_compression(
                config_dict.get("data_compression")
            ),
            "sort_in_tempdb": bool(config_dict.get("sort_in_tempdb") or False),
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
            "data_compression": cls._normalize_data_compression(
                model_node_entry.get("data_compression")
            ),
            "sort_in_tempdb": bool(model_node_entry.get("sort_in_tempdb") or False),
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
            "data_compression": self.data_compression,
            "sort_in_tempdb": self.sort_in_tempdb,
        }
        return node_config

    def render(self, relation):
        # Deterministic full-definition hash. Unlike Postgres (dbt-core#1945),
        # SQL Server index names are scoped per table, so a renamed backup
        # relation keeping the same index names cannot collide with the new
        # target's indexes — no timestamp salt is needed. Determinism is what
        # makes name equality <=> definition equality, which reconciliation
        # relies on; create idempotency comes from the IF NOT EXISTS guard in
        # sqlserver__get_create_index_sql.
        # sort_in_tempdb is deliberately NOT hashed: it's a build-time option
        # that doesn't change the resulting index, so toggling it must not
        # produce a new name (which would drop/recreate on reconcile).
        inputs = (
            self.columns
            + tuple(sorted(self.included_columns))
            + (relation.render(), str(self.unique), str(self.type))
            + ((str(self.data_compression),) if self.data_compression else ())
        )
        return SQLSERVER_MANAGED_INDEX_PREFIX + dbt_encoding.md5("_".join(inputs))

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
