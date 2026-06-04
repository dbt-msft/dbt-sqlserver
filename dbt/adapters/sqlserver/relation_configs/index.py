from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, Optional, Set, Tuple

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

# Names created by the legacy post-hook macros (create_clustered_index /
# create_nonclustered_index). Existing users rely on those post-hooks, so
# reconciliation must never drop them — not even under
# drop_unmanaged_indexes: true.
LEGACY_INDEX_PREFIXES = ("clustered_", "nonclustered_")

VALID_DROP_UNMANAGED_MODES = ("false", "warn", "true")

# CREATE INDEX WITH(...) options that only affect HOW the index is built, not
# WHAT it is. They are excluded from the name hash and from equality, so
# changing them never triggers a drop/recreate on reconcile.
VALID_BUILD_OPTIONS = (
    "online",
    "maxdop",
    "resumable",
    "max_duration",
    "allow_row_locks",
    "allow_page_locks",
    "statistics_norecompute",
    "statistics_incremental",
    "compression_delay",  # columnstore only
)


def _split_column_list(raw) -> Tuple[str, ...]:
    """Split a 'col1, col2' aggregate from sys introspection into clean parts."""
    if not raw:
        return ()
    return tuple(part.strip() for part in raw.split(",") if part.strip())


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
    - columns: the column names in the index; entries are either a plain name
      (ascending) or {'column': name, 'desc': true}
    - included_columns: the extra included columns names in the index
    - data_compression: none | row | page (rowstore) or
      columnstore | columnstore_archive (columnstore)
    - where: filter predicate for a filtered index (nonclustered / columnstore)
    - fillfactor (1-100) / pad_index: leaf/intermediate page density (rowstore)
    - ignore_dup_key: discard duplicate rows instead of erroring (unique rowstore)
    - optimize_for_sequential_key: last-page contention optimization
      (rowstore, SQL Server 2019+)
    - sort_in_tempdb: build-time option for the create statement; deliberately
      excluded from identity (not introspectable and doesn't change the
      resulting index, so it must not trigger drop/recreate)

    All definition-affecting fields participate in the rendered name hash, but
    only when set - so names of pre-existing managed indexes stay stable when
    the adapter adds new optional fields.
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
    descending_columns: FrozenSet[str] = field(default_factory=frozenset, hash=True)
    where: Optional[str] = field(default=None, hash=True)
    fillfactor: Optional[int] = field(default=None, hash=True)
    pad_index: bool = field(default=False, hash=True)
    ignore_dup_key: bool = field(default=False, hash=True)
    optimize_for_sequential_key: bool = field(default=False, hash=True)
    build_options: Optional[Dict[str, Any]] = field(default=None, hash=False, compare=False)

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
                validation_check=self.data_compression
                in (
                    (None, "columnstore", "columnstore_archive")
                    if self.type == SQLServerIndexType.columnstore
                    else (None, "none", "row", "page")
                ),
                validation_error=DbtRuntimeError(
                    f"Invalid data_compression: {self.data_compression}. "
                    "Valid values: none, row, page (rowstore) or "
                    "columnstore, columnstore_archive (columnstore)"
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
            RelationConfigValidationRule(
                validation_check=frozenset(self.descending_columns) <= frozenset(self.columns),
                validation_error=DbtRuntimeError(
                    "descending columns must be a subset of the index key columns: "
                    f"{sorted(set(self.descending_columns) - set(self.columns))}"
                ),
            ),
            RelationConfigValidationRule(
                validation_check=not (self.where and self.type == SQLServerIndexType.clustered),
                validation_error=DbtRuntimeError(
                    "'where' (filtered index) is not valid for clustered indexes"
                ),
            ),
            RelationConfigValidationRule(
                validation_check=(
                    self.fillfactor is None
                    or (
                        1 <= self.fillfactor <= 100 and self.type != SQLServerIndexType.columnstore
                    )
                )
                and not (self.pad_index and self.type == SQLServerIndexType.columnstore),
                validation_error=DbtRuntimeError(
                    f"Invalid fillfactor/pad_index: fillfactor={self.fillfactor} "
                    "(must be 1-100, rowstore indexes only)"
                ),
            ),
            RelationConfigValidationRule(
                validation_check=not self.ignore_dup_key
                or (
                    self.unique and self.type != SQLServerIndexType.columnstore and not self.where
                ),
                validation_error=DbtRuntimeError(
                    "ignore_dup_key is only valid for unique rowstore indexes, "
                    "and cannot be combined with 'where' (the engine forbids "
                    "IGNORE_DUP_KEY on filtered indexes)"
                ),
            ),
            RelationConfigValidationRule(
                validation_check=not self.build_options
                or all(key in VALID_BUILD_OPTIONS for key in self.build_options),
                validation_error=DbtRuntimeError(
                    "Invalid build_options key(s): "
                    f"{sorted(set(self.build_options or {}) - set(VALID_BUILD_OPTIONS))}. "
                    f"Valid keys: {', '.join(VALID_BUILD_OPTIONS)}"
                ),
            ),
            RelationConfigValidationRule(
                validation_check=not self.optimize_for_sequential_key
                or self.type in (SQLServerIndexType.clustered, SQLServerIndexType.nonclustered),
                validation_error=DbtRuntimeError(
                    "optimize_for_sequential_key is only valid for rowstore indexes "
                    "(SQL Server 2019+)"
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

    @staticmethod
    def _normalize_columns(raw_columns):
        """Split mixed column entries - plain names or {'column': n, 'desc': bool}
        dicts - into (names, descending_names). Non-conforming entries are left
        as-is for jsonschema validation to reject with a useful message."""
        names, descending = [], []
        for entry in raw_columns:
            if isinstance(entry, dict) and isinstance(entry.get("column"), str):
                names.append(entry["column"])
                if entry.get("desc"):
                    descending.append(entry["column"])
            else:
                names.append(entry)
        return names, descending

    # NOTE: no custom from_dict override here - dbtClassMixin (mashumaro)
    # generates from_dict during class creation and silently replaces any
    # method defined in the class body, so an override would be dead code.
    # Raw-input normalization therefore lives in parse() below.

    @classmethod
    def parse_model_node(cls, model_node_entry: dict) -> dict:
        raw_columns = model_node_entry.get("columns", [])
        if isinstance(raw_columns, list):
            names, descending = cls._normalize_columns(raw_columns)
        else:
            names, descending = raw_columns, []
        config_dict = {
            "columns": tuple(names),
            "unique": model_node_entry.get("unique"),
            "type": model_node_entry.get("type"),
            "included_columns": frozenset(model_node_entry.get("included_columns", set())),
            "data_compression": cls._normalize_data_compression(
                model_node_entry.get("data_compression")
            ),
            "sort_in_tempdb": bool(model_node_entry.get("sort_in_tempdb") or False),
            "descending_columns": frozenset(
                set(descending) | set(model_node_entry.get("descending_columns") or [])
            ),
            "where": model_node_entry.get("where"),
            "fillfactor": model_node_entry.get("fillfactor"),
            "pad_index": bool(model_node_entry.get("pad_index") or False),
            "ignore_dup_key": bool(model_node_entry.get("ignore_dup_key") or False),
            "optimize_for_sequential_key": bool(
                model_node_entry.get("optimize_for_sequential_key") or False
            ),
        }
        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results_entry: agate.Row) -> dict:
        config_dict = {
            "name": relation_results_entry.get("name"),
            "columns": _split_column_list(relation_results_entry.get("columns")),
            "unique": bool(relation_results_entry.get("unique")),
            "type": relation_results_entry.get("type"),
            "included_columns": set(
                _split_column_list(relation_results_entry.get("included_columns"))
            ),
            "data_compression": cls._normalize_data_compression(
                relation_results_entry.get("data_compression")
            ),
            "descending_columns": set(
                _split_column_list(relation_results_entry.get("descending_columns"))
            ),
            "where": relation_results_entry.get("where") or None,
            # sys.indexes.fill_factor reports 0 for the server default
            "fillfactor": relation_results_entry.get("fillfactor") or None,
            "pad_index": bool(relation_results_entry.get("pad_index")),
            "ignore_dup_key": bool(relation_results_entry.get("ignore_dup_key")),
            "optimize_for_sequential_key": bool(
                relation_results_entry.get("optimize_for_sequential_key")
            ),
        }
        return config_dict

    @property
    def as_node_config(self) -> dict:
        """
        Returns: a dictionary that can be passed into `get_create_index_sql()`
        """
        # JSON-compatible types only: this dict is fed back through
        # adapter.parse_index, whose jsonschema validation requires arrays.
        node_config = {
            "columns": list(self.columns),
            "unique": self.unique,
            "type": self.type.value,
            "included_columns": sorted(self.included_columns),
            "data_compression": self.data_compression,
            "sort_in_tempdb": self.sort_in_tempdb,
            "descending_columns": sorted(self.descending_columns),
            "where": self.where,
            "fillfactor": self.fillfactor,
            "pad_index": self.pad_index,
            "ignore_dup_key": self.ignore_dup_key,
            "optimize_for_sequential_key": self.optimize_for_sequential_key,
            "build_options": dict(self.build_options) if self.build_options else None,
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
        # Build-time options (sort_in_tempdb, build_options) are deliberately
        # NOT hashed: they don't change the resulting index, so toggling them
        # must not produce a new name (which would drop/recreate on reconcile).
        # Optional definition fields are appended ONLY when set, with a field
        # prefix, so pre-existing managed index names stay stable as the
        # adapter grows new options.
        inputs = (
            self.columns
            + tuple(sorted(self.included_columns))
            + (relation.render(), str(self.unique), str(self.type))
            + ((str(self.data_compression),) if self.data_compression else ())
            + (
                ("desc:" + ",".join(sorted(self.descending_columns)),)
                if self.descending_columns
                else ()
            )
            + (("where:" + self.where,) if self.where else ())
            + ((f"fillfactor:{self.fillfactor}",) if self.fillfactor else ())
            + (("pad_index",) if self.pad_index else ())
            + (("ignore_dup_key",) if self.ignore_dup_key else ())
            + (("optimize_for_sequential_key",) if self.optimize_for_sequential_key else ())
        )
        return SQLSERVER_MANAGED_INDEX_PREFIX + dbt_encoding.md5("_".join(inputs))

    @classmethod
    def parse(cls, raw_index) -> Optional["SQLServerIndexConfig"]:
        if raw_index is None:
            return None
        try:
            if not isinstance(raw_index, dict):
                raise IndexConfigNotDictError(raw_index)
            # Normalize BEFORE jsonschema validation: {'column','desc'} entries
            # become plain names + descending_columns, and data_compression is
            # case/'none'-normalized. mashumaro replaces from_dict overrides,
            # so this is the only place raw input can be massaged.
            normalized = dict(raw_index)
            if isinstance(normalized.get("columns"), list):
                names, descending = cls._normalize_columns(normalized["columns"])
                normalized["columns"] = names
                if descending or normalized.get("descending_columns"):
                    merged = set(descending) | set(normalized.get("descending_columns") or [])
                    normalized["descending_columns"] = sorted(merged)
            if "data_compression" in normalized:
                normalized["data_compression"] = cls._normalize_data_compression(
                    normalized.get("data_compression")
                )
            cls.validate(normalized)
            return cls.from_dict(normalized)
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


def index_config_changes(
    existing_rows,
    expected_configs,
    relation,
    drop_unmanaged="false",
):
    """Compute the index changes needed to converge an existing relation on its
    configured index set.

    Managed indexes (dbt_idx_ prefix) carry a deterministic full-definition
    hash in their name, so name equality <=> definition equality: drops are
    "managed names not expected", creates are "expected names not present".

    Args:
        existing_rows: rows from sqlserver__describe_indexes (mappings with
            name/type/unique/columns/included_columns/data_compression plus
            is_primary_key/is_unique_constraint flags)
        expected_configs: list of SQLServerIndexConfig from the model config
        relation: the target relation (for rendering expected names)
        drop_unmanaged: "false" (default) | "warn" | "true" - how to treat
            droppable indexes dbt didn't create. Constraint-backing indexes
            and legacy post-hook (clustered_/nonclustered_) indexes are never
            dropped in any mode.

    Returns:
        (changes, warnings): changes is a list of SQLServerIndexConfigChange
        with all drops ordered before all creates; warnings is a list of
        messages about unmanaged indexes (populated in "warn" mode).

    Raises:
        DbtRuntimeError: for an invalid drop_unmanaged value, or when an
            expected clustered index is blocked by an existing clustered index
            that will not be dropped (a table can only have one).
    """
    if isinstance(drop_unmanaged, bool):
        drop_unmanaged = "true" if drop_unmanaged else "false"
    if drop_unmanaged not in VALID_DROP_UNMANAGED_MODES:
        raise DbtRuntimeError(
            f"Invalid drop_unmanaged_indexes value: {drop_unmanaged!r}. "
            f"Valid values: {', '.join(VALID_DROP_UNMANAGED_MODES)}"
        )

    expected_by_name = {config.render(relation): config for config in expected_configs}
    existing_names = set()
    drop_names = []
    warnings = []

    for row in existing_rows:
        name = row.get("name")
        existing_names.add(name)
        if name in expected_by_name:
            continue

        protected = (
            bool(row.get("is_primary_key"))
            or bool(row.get("is_unique_constraint"))
            # The adapter's own as_columnstore CCI lives outside the indexes
            # config; dropping it would silently convert the table to a heap.
            or str(row.get("type") or "") == "clustered columnstore"
        )
        if protected:
            continue
        if name.startswith(SQLSERVER_MANAGED_INDEX_PREFIX):
            drop_names.append(name)
            continue
        if name.startswith(LEGACY_INDEX_PREFIXES):
            # Legacy post-hook indexes: the owning post-hook would recreate
            # them right after we dropped them. Never touch.
            continue
        if drop_unmanaged == "true":
            drop_names.append(name)
        elif drop_unmanaged == "warn":
            warnings.append(
                f"Unmanaged index not in the expected set "
                f"(kept; set drop_unmanaged_indexes: true to drop): "
                f"{name} on ({row.get('columns')})"
            )

    creating_clustered = any(
        config.type == SQLServerIndexType.clustered
        for name, config in expected_by_name.items()
        if name not in existing_names
    )
    if creating_clustered:
        blocking = [
            row.get("name")
            for row in existing_rows
            # 'clustered' and 'clustered columnstore' both occupy the one
            # clustered slot a table has.
            if str(row.get("type") or "").startswith("clustered")
            and row.get("name") not in expected_by_name
            and row.get("name") not in drop_names
        ]
        if blocking:
            raise DbtRuntimeError(
                f"Cannot create the configured clustered index on "
                f"{relation.render()}: existing clustered index "
                f"{', '.join(blocking)} will not be dropped (constraint-backing "
                "and legacy post-hook indexes are never dropped; other unmanaged "
                "indexes require drop_unmanaged_indexes: true). Drop or migrate "
                "the conflicting index, or remove the clustered entry from the "
                "indexes config."
            )

    rows_by_name = {row.get("name"): row for row in existing_rows}
    changes = []
    for name in drop_names:
        context = SQLServerIndexConfig.from_dict(
            SQLServerIndexConfig.parse_relation_results(rows_by_name[name])
        )
        changes.append(
            SQLServerIndexConfigChange(action=RelationConfigChangeAction.drop, context=context)
        )
    for name, config in expected_by_name.items():
        if name not in existing_names:
            changes.append(
                SQLServerIndexConfigChange(
                    action=RelationConfigChangeAction.create, context=config
                )
            )
    return changes, warnings
