import datetime as _dt
import re
from typing import Any, Dict, List, Optional, Tuple, Type, Union

import agate
import dbt_common.exceptions
from dbt_common.behavior_flags import BehaviorFlag
from dbt_common.contracts.constraints import (
    ColumnLevelConstraint,
    ConstraintType,
    ModelLevelConstraint,
)
from dbt_common.events.functions import fire_event

from dbt.adapters.base.column import Column as BaseColumn
from dbt.adapters.base.impl import ConstraintSupport
from dbt.adapters.base.meta import available
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.capability import Capability, CapabilityDict, CapabilitySupport, Support
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.events.types import ColTypeChange, SchemaCreation
from dbt.adapters.reference_keys import _make_ref_key_dict
from dbt.adapters.relation_configs import RelationConfigChangeAction
from dbt.adapters.sql.impl import CREATE_SCHEMA_MACRO_NAME, SQLAdapter
from dbt.adapters.sqlserver.relation_configs import SQLServerIndexConfig, SQLServerIndexType
from dbt.adapters.sqlserver.relation_configs.index import (
    create_needs_own_batch,
    index_config_changes,
    normalize_drop_unmanaged,
)
from dbt.adapters.sqlserver.sqlserver_auth import is_adbc_backend, is_mssql_python_backend
from dbt.adapters.sqlserver.sqlserver_column import SQLServerColumn, SQLServerColumnNative
from dbt.adapters.sqlserver.sqlserver_configs import SQLServerConfigs
from dbt.adapters.sqlserver.sqlserver_connections import (
    SQLServerConnectionManager,
    _discard_pending_results,
)
from dbt.adapters.sqlserver.sqlserver_deny import deny_changes as _deny_changes
from dbt.adapters.sqlserver.sqlserver_deny import resolve_denies as _resolve_denies
from dbt.adapters.sqlserver.sqlserver_mask import ColumnMask
from dbt.adapters.sqlserver.sqlserver_mask import mask_changes as _mask_changes
from dbt.adapters.sqlserver.sqlserver_mask import resolve_masks as _resolve_masks
from dbt.adapters.sqlserver.sqlserver_relation import SQLServerRelation
from dbt.adapters.sqlserver.sqlserver_runtime import _get_pyodbc

logger = AdapterLogger("SQLServer")

# Mirrors sqlserver__select_starts_with_cte
# (dbt/include/sqlserver/macros/adapters/columns.sql): a query opening with a
# CTE cannot be neutered as ``select * from (...) where 1 = 0``, so it reaches
# get_column_schema_from_query unwrapped and would otherwise be executed in
# full just to read its column names.
_SQL_COMMENT = re.compile(r"(?s)/\*.*?\*/|--[^\n]*\n")

# sp_describe_first_result_set reports true SQL Server types; reading
# ``cursor.description`` reports Python classes, which collapse whole families
# (every integer width arrives as ``int``, every string type as ``varchar``).
# Contract comparison comes through this method either way, so the describe
# path is mapped back onto exactly the names the execute path yields via
# ``data_type_code_to_name``. TestCteProbeAvoidsExecution pins the two
# together; a type missing from this map falls back to executing rather than
# guessing.
#
# The names below are pyodbc's. The class a driver picks for a column is its
# own choice, not SQL Server's, and mssql-python decodes three of these
# differently -- see ``_MSSQL_PYTHON_TYPE_OVERRIDES``.
_SYSTEM_TYPE_TO_EXECUTED_NAME = {
    "bigint": "int",
    "int": "int",
    "smallint": "int",
    "tinyint": "int",
    "bit": "bit",
    "decimal": "decimal",
    "numeric": "decimal",
    "money": "decimal",
    "smallmoney": "decimal",
    "float": "float",
    "real": "float",
    "date": "date",
    "time": "time",
    "datetime": "datetime2(6)",
    "smalldatetime": "datetime2(6)",
    "datetime2": "datetime2(6)",
    "char": "varchar",
    "nchar": "varchar",
    "varchar": "varchar",
    "nvarchar": "varchar",
    "text": "varchar",
    "ntext": "varchar",
    "xml": "varchar",
    "uniqueidentifier": "varchar",
    "binary": "varbinary",
    "varbinary": "varbinary",
    "image": "varbinary",
    "timestamp": "varbinary",
    "rowversion": "varbinary",
    # datetimeoffset is deliberately absent *from this map*: add_query
    # registers the -155 output converter after the execute that needed it,
    # so pyodbc reports the column as bytearray on a connection's first query
    # and as str on every one after. No fixed name mirrors that, so on pyodbc
    # describing gives up and the query is executed -- which agrees with
    # itself by construction. mssql-python needs no converter and is pinned
    # in _MSSQL_PYTHON_TYPE_OVERRIDES.
    "sql_variant": "varbinary",
    "hierarchyid": "varbinary",
    "geography": "varbinary",
    "geometry": "varbinary",
}

# mssql-python decodes three types into richer Python objects than pyodbc
# does: uniqueidentifier as ``uuid.UUID`` (pyodbc: ``str``), datetimeoffset
# as ``datetime`` and sql_variant as ``str`` (pyodbc: ``bytearray`` for
# both). Executing reports those classes, so describing has to agree.
_MSSQL_PYTHON_TYPE_OVERRIDES = {
    "uniqueidentifier": "uniqueidentifier",
    "datetimeoffset": "datetime2(6)",
    "sql_variant": "varchar",
}


def _executed_name_for_system_type(base_type: str, backend: Any) -> Optional[str]:
    """The name the executed probe would report for a described column type.

    None means "no confident answer" -- the caller then executes the query,
    which is slower but cannot disagree with itself.
    """
    if is_mssql_python_backend(backend):
        overridden = _MSSQL_PYTHON_TYPE_OVERRIDES.get(base_type)
        if overridden is not None:
            return overridden

    elif base_type == "uniqueidentifier":
        # pyodbc yields uuid.UUID or str for a GUID depending on its
        # module-level ``native_uuid`` flag -- process-global state anything
        # in the process can flip, so read it rather than assume a default.
        try:
            native_uuid = bool(_get_pyodbc().native_uuid)
        except Exception as e:  # pragma: no cover - pyodbc is present if in use
            logger.debug(f"Could not read pyodbc.native_uuid, executing the query: {e}")
            return None
        return "uniqueidentifier" if native_uuid else "varchar"

    return _SYSTEM_TYPE_TO_EXECUTED_NAME.get(base_type)


def _normalize_result_datetimes(
    result: Union[Tuple, List[Tuple], None],
) -> Union[Tuple, List[Tuple], None]:
    """Strip spurious ``tzinfo=UTC`` that ADBC attaches to SQL Server
    DATETIME2 / DATETIME values.

    SQL Server does **not** store timezone offsets for these types, so the
    correct Python representation is a naive ``datetime``.  ADBC's Arrow
    backend wraps every ``timestamp`` column as ``timestamp[us, tz=UTC]``
    regardless of the source semantics, producing ``datetime(…, tzinfo=UTC)``.
    We revert that to match SQL Server semantics and to stay compatible with
    the existing pyodbc / mssql-python backends.
    """
    if result is None:
        return None

    if isinstance(result, tuple):
        return tuple(
            (v.replace(tzinfo=None) if isinstance(v, _dt.datetime) and v.tzinfo is not None else v)
            for v in result
        )

    if isinstance(result, list):
        return [
            tuple(
                (
                    v.replace(tzinfo=None)
                    if isinstance(v, _dt.datetime) and v.tzinfo is not None
                    else v
                )
                for v in row
            )
            for row in result
        ]

    return result


class SQLServerAdapter(SQLAdapter):
    """
    Controls actual implementation of adapter, and ability to override certain methods.
    """

    ConnectionManager = SQLServerConnectionManager
    # Annotated because __init__ swaps in the SQLServerColumnNative subclass.
    Column: Type[SQLServerColumn] = SQLServerColumn
    AdapterSpecificConfigs = SQLServerConfigs
    Relation = SQLServerRelation

    _capabilities: CapabilityDict = CapabilityDict(
        {
            Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Full),
            Capability.TableLastModifiedMetadata: CapabilitySupport(support=Support.Full),
        }
    )
    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.ENFORCED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.ENFORCED,
    }

    def __init__(self, config, mp_context=None):
        super().__init__(config, mp_context)
        SQLServerRelation.disable_empty_relation_aliases = (
            self.behavior.dbt_sqlserver_disable_empty_relation_aliases
        )
        if self.behavior.dbt_sqlserver_use_native_string_types:
            self.Column = SQLServerColumnNative
        # add_begin_query/add_commit_query read the instance flag, while dbt-core
        # rollback handling is classmethod-based and reads the class flag.
        use_dbt_transactions = bool(self.behavior.dbt_sqlserver_use_dbt_transactions)
        SQLServerConnectionManager._dbt_sqlserver_use_dbt_transactions = use_dbt_transactions
        self.connections._dbt_sqlserver_use_dbt_transactions = use_dbt_transactions

    @property
    def _behavior_flags(self) -> List[BehaviorFlag]:
        # dbt-common declares BehaviorFlag's optional keys with a NotRequired
        # that falls back to Optional under a try/except ImportError shim, so a
        # type checker reads `source` and `docs_url` as required and rejects
        # every flag below. The suppressions go stale once that shim is dropped.
        return [  # ty: ignore[invalid-return-type]
            {  # ty: ignore[missing-typed-dict-key]
                "name": "dbt_sqlserver_use_default_schema_concat",
                "default": True,
                "description": (
                    "When True (default), uses dbt-core's standard schema concatenation "
                    "(`target.schema` + `_` + `custom_schema_name`). "
                    "When False, uses the legacy adapter behaviour: "
                    "`custom_schema_name` is used directly without prefixing `target.schema`. "
                    "The legacy behaviour is deprecated and this override will be removed in a "
                    "future release. For a permanent solution, override the "
                    "`sqlserver__generate_schema_name` macro in your project instead."
                ),
            },
            {  # ty: ignore[missing-typed-dict-key]
                "name": "dbt_sqlserver_disable_empty_relation_aliases",
                "default": True,
                "description": (
                    "When True, SQL Server limited relations used by --empty and sample mode "
                    "do not automatically receive dbt-generated aliases. Set this false to opt "
                    "out of alias generation temporarily for testing."
                ),
            },
            {  # ty: ignore[missing-typed-dict-key]
                "name": "dbt_sqlserver_use_native_string_types",
                "default": True,
                "description": (
                    "When True (default), uses SQL Server-native string type mappings: "
                    "STRING -> VARCHAR(MAX), NCHAR -> NCHAR(1), NVARCHAR -> NVARCHAR(4000). "
                    "When False, preserves deprecated legacy mappings: "
                    "STRING and NVARCHAR -> VARCHAR(8000), NCHAR -> CHAR(1). "
                    "The legacy False behavior is deprecated "
                    "and will be removed in a future release."
                ),
            },
            {  # ty: ignore[missing-typed-dict-key]
                "name": "dbt_sqlserver_enable_safe_type_expansion",
                "default": False,
                "description": (
                    "Allow the SQL Server adapter to widen column types during schema expansion. "
                    "This enables promotions like varchar -> nvarchar, "
                    "bit -> tinyint -> smallint -> int -> bigint, "
                    "and numeric(p,s) -> numeric(p2,s2) using alter column."
                ),
            },
            {  # ty: ignore[missing-typed-dict-key]
                "name": "dbt_sqlserver_use_dbt_transactions",
                "default": True,
                "description": (
                    "When True (default), dbt transaction hooks (begin/commit) emit real T-SQL "
                    "BEGIN TRANSACTION / COMMIT TRANSACTION statements. "
                    "When False, begin/commit are no-ops and each statement "
                    "is auto-committed by the driver, meaning earlier successful statements "
                    "are not rolled back if a later statement fails. "
                    "The legacy False behavior is deprecated "
                    "and will be removed in a future release."
                ),
            },
        ]

    @available.parse(lambda *a, **k: [])
    def get_column_schema_from_query(self, sql: str) -> List[BaseColumn]:
        """Get a list of the Columns with names and data types from the given sql.

        Only the result *shape* is wanted, but the query still runs, so the
        cursor comes back holding the whole result set. Usually that set is
        empty: dbt-core's ``get_column_schema_from_query`` macro wraps the
        query first, and ``sqlserver__get_empty_subquery_sql`` renders that as
        ``select * from (...) where 1 = 0``. A query that opens with a CTE
        cannot be wrapped that way, though, and is passed through untouched
        (dbt/include/sqlserver/macros/adapters/columns.sql), so snapshot
        staging queries and CTE-headed contract models arrive here in full.

        Either way the cursor must not be abandoned holding rows -- see
        ``_discard_pending_results`` for what that costs.
        """
        if _SQL_COMMENT.sub("", sql).strip().lower().startswith("with"):
            described = self._describe_result_set(sql)
            if described is not None:
                return described

        _, cursor = self.connections.add_select_query(sql)

        try:
            columns = [
                self.Column.create(
                    column_name, self.connections.data_type_code_to_name(column_type_code)
                )
                # https://peps.python.org/pep-0249/#description
                for column_name, column_type_code, *_ in cursor.description
            ]
        finally:
            _discard_pending_results(cursor)

        return columns

    def _describe_result_set(self, sql: str) -> Optional[List[BaseColumn]]:
        """Read a query's column shape without running it, or None to fall back.

        ``sp_describe_first_result_set`` compiles the query and reports its
        result shape, which is all this method ever wanted. It is already how
        ``sqlserver__get_columns_in_query`` handles CTEs (#698).

        Returns None -- deliberately, rather than raising -- whenever the
        describe cannot be trusted to match what executing would have reported:
        an unsupported backend, a query it refuses to describe (it cannot see
        through ``#temp`` tables, where executing works), or a type this
        backend's driver has no known executed name for. The caller then
        executes as before, which is slower but never disagrees with itself.
        """
        credentials = self.connections.profile.credentials
        if is_adbc_backend(credentials.backend):
            # ADBC derives its type names from Arrow codes (int64 -> bigint,
            # large_string -> varchar(max)), so the map above -- built for the
            # pyodbc/mssql-python collapse -- would make the two branches
            # disagree on that backend.
            return None

        # Inline rather than bound: mssql-python binds str as varchar and the
        # procedure demands nvarchar(max). columns.sql:24 escapes it the same
        # way for the same reason.
        describe_sql = "exec sp_describe_first_result_set @tsql = N'{}'".format(
            sql.replace("'", "''")
        )

        try:
            _, cursor = self.connections.add_select_query(describe_sql)
        except Exception as e:
            logger.debug(f"Could not describe a CTE query, falling back to executing it: {e}")
            return None

        try:
            fields = [description[0].lower() for description in cursor.description]
            rows = cursor.fetchall()
        except Exception as e:
            logger.debug(f"Could not read a described result set, executing the query: {e}")
            return None
        finally:
            _discard_pending_results(cursor)

        try:
            hidden, name, type_name = (
                fields.index("is_hidden"),
                fields.index("name"),
                fields.index("system_type_name"),
            )
        except ValueError:  # pragma: no cover - shape is fixed by SQL Server
            return None

        columns = []
        for row in rows:
            if row[hidden]:
                continue
            # "varchar(10)" / "decimal(10,2)" -> "varchar" / "decimal"
            base_type = str(row[type_name]).split("(")[0].strip().lower()
            executed_name = _executed_name_for_system_type(base_type, credentials.backend)
            if executed_name is None or row[name] is None:
                logger.debug(
                    f"Describing a CTE query reported {base_type!r}, which has no "
                    "equivalent in the executed path; executing it instead"
                )
                return None
            columns.append(self.Column.create(row[name], executed_name))

        # Every select has at least one column, so nothing described means
        # sp_describe_first_result_set could not work the shape out. Returning
        # an empty list would read as "this query has no columns" and surface
        # as a baffling contract mismatch; execute instead.
        return columns or None

    @classmethod
    def quote(cls, identifier: str) -> str:
        """Double-quote an identifier, doubling any embedded double quote.

        ``SQLAdapter.quote`` interpolates the identifier verbatim, so a name
        containing a ``"`` would close the quoted identifier early and the
        remainder would parse as SQL. T-SQL escapes a delimiter by doubling
        it -- the same rule ``QUOTENAME()`` applies to brackets -- so
        ``ab"cd`` must render as ``"ab""cd"``.

        This is the quoting used by every macro that formats an identifier
        (see #785). Relation rendering escapes nothing, since it goes through
        ``BaseRelation.quote_character`` upstream rather than this method.
        """
        return '"{}"'.format(str(identifier).replace('"', '""'))

    @classmethod
    def convert_boolean_type(cls, agate_table, col_idx):
        return "bit"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "datetime2(6)"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "float" if decimals else "int"

    def create_schema(self, relation: BaseRelation) -> None:
        relation = relation.without_identifier()
        fire_event(SchemaCreation(relation=_make_ref_key_dict(relation)))
        macro_name = CREATE_SCHEMA_MACRO_NAME
        kwargs = {
            "relation": relation,
        }

        if self.config.credentials.schema_authorization:
            kwargs["schema_authorization"] = self.config.credentials.schema_authorization
            macro_name = "sqlserver__create_schema_with_authorization"

        self.execute_macro(macro_name, kwargs=kwargs)
        self.commit_if_has_connection()

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        column = agate_table.columns[col_idx]
        # see https://github.com/fishtown-analytics/dbt/pull/2255
        lens = [len(d.encode("utf-8")) for d in column.values_without_nulls()]
        max_len = max(lens) if lens else 64
        length = max_len if max_len > 16 else 16
        return "varchar({})".format(length)

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        return "time(6)"

    @classmethod
    def date_function(cls):
        return "getdate()"

    # Methods used in adapter tests
    def timestamp_add_sql(self, add_to: str, number: int = 1, interval: str = "hour") -> str:
        # note: 'interval' is not supported for T-SQL
        # for backwards compatibility, we're compelled to set some sort of
        # default. A lot of searching has lead me to believe that the
        # '+ interval' syntax used in postgres/redshift is relatively common
        # and might even be the SQL standard's intention.
        return f"DATEADD({interval},{number},{add_to})"

    def string_add_sql(
        self,
        add_to: str,
        value: str,
        location="append",
    ) -> str:
        """
        `+` is T-SQL's string concatenation operator
        """
        if location == "append":
            return f"{add_to} + '{value}'"
        elif location == "prepend":
            return f"'{value}' + {add_to}"
        else:
            raise ValueError(f'Got an unexpected location value of "{location}"')

    def get_rows_different_sql(
        self,
        relation_a: BaseRelation,
        relation_b: BaseRelation,
        column_names: Optional[List[str]] = None,
        except_operator: str = "EXCEPT",
    ) -> str:
        """
        note: using is not supported on Synapse so COLUMNS_EQUAL_SQL is adjusted
        Generate SQL for a query that returns a single row with a two
        columns: the number of rows that are different between the two
        relations and the number of mismatched rows.
        """
        # This method only really exists for test reasons.
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))
        columns_csv = ", ".join(names)

        if columns_csv == "":
            columns_csv = "*"

        sql = COLUMNS_EQUAL_SQL.format(
            columns=columns_csv,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
            except_op=except_operator,
        )

        return sql

    def valid_incremental_strategies(self):
        """The set of standard builtin strategies which this adapter supports out-of-the-box.
        Not used to validate custom strategies defined by end users.
        """
        return ["append", "delete+insert", "merge", "microbatch"]

    # This is for use in the test suite
    def run_sql_for_tests(self, sql, fetch, conn):
        cursor = conn.handle.cursor()
        try:
            cursor.execute(sql)
            if not fetch:
                conn.handle.commit()
            if fetch == "one":
                return _normalize_result_datetimes(cursor.fetchone())
            elif fetch == "all":
                return _normalize_result_datetimes(cursor.fetchall())
            else:
                return
        except BaseException:
            if conn.handle and not getattr(conn.handle, "closed", True):
                conn.handle.rollback()
            raise
        finally:
            conn.transaction_open = False

    @available
    @classmethod
    def render_column_constraint(cls, constraint: ColumnLevelConstraint) -> Optional[str]:
        rendered_column_constraint = None
        if constraint.type == ConstraintType.not_null:
            rendered_column_constraint = "not null "
        else:
            rendered_column_constraint = ""

        if rendered_column_constraint:
            rendered_column_constraint = rendered_column_constraint.strip()

        return rendered_column_constraint

    @classmethod
    def render_model_constraint(cls, constraint: ModelLevelConstraint) -> Optional[str]:
        constraint_prefix = "add constraint "
        column_list = ", ".join(constraint.columns)

        if constraint.name is None:
            raise dbt_common.exceptions.DbtDatabaseError(
                "Constraint name cannot be empty. Provide constraint name  - column "
                + column_list
                + " and run the project again."
            )

        if constraint.type == ConstraintType.unique:
            return constraint_prefix + f"{constraint.name} unique nonclustered({column_list})"
        elif constraint.type == ConstraintType.primary_key:
            return constraint_prefix + f"{constraint.name} primary key nonclustered({column_list})"
        elif constraint.type == ConstraintType.foreign_key and constraint.expression:
            return (
                constraint_prefix
                + f"{constraint.name} foreign key({column_list}) references "
                + constraint.expression
            )
        elif constraint.type == ConstraintType.check and constraint.expression:
            return f"{constraint_prefix} {constraint.name} check ({constraint.expression})"
        elif constraint.type == ConstraintType.custom and constraint.expression:
            return f"{constraint_prefix} {constraint.name} {constraint.expression}"
        else:
            return None

    def _get_row_count(self, relation) -> int:
        """Return the number of rows in the given relation."""
        sql = f"SELECT COUNT_BIG(*) FROM {relation}"
        _, cursor = self.connections.add_select_query(sql)
        try:
            row = cursor.fetchone()
        finally:
            _discard_pending_results(cursor)
        return int(row[0]) if row else 0

    def expand_column_types(self, goal, current, max_rows: int = 1000000):
        """Override to ensure we preserve nvarchar/nchar type family during
        column expansion. Necessary same-family resizes (e.g. varchar size)
        always proceed. Safe type expansions (cross-family promotions like
        varchar -> nvarchar) are guarded by column_type_expansion_max_rows.
        enable_safe_type_expansion is the future approach for widening."""

        reference_columns = {c.name: c for c in self.get_columns_in_relation(goal)}
        target_columns = {c.name: c for c in self.get_columns_in_relation(current)}

        enable_safe = self.behavior.dbt_sqlserver_enable_safe_type_expansion

        row_count_exceeds = False
        if enable_safe and max_rows != -1:
            if max_rows == 0:
                row_count_exceeds = True
                logger.info(
                    "Safe type expansion skipped for %s: column_type_expansion_max_rows is 0.",
                    current,
                )
            else:
                row_count = self._get_row_count(current)
                if row_count > max_rows:
                    row_count_exceeds = True
                    logger.warning(
                        "Safe type expansion skipped for %s: "
                        "%s rows exceeds column_type_expansion_max_rows (%s). "
                        "Set column_type_expansion_max_rows=-1 to disable "
                        "this check, or increase the limit.",
                        current,
                        row_count,
                        max_rows,
                    )

        for column_name, reference_column in reference_columns.items():
            target_column = target_columns.get(column_name)
            if target_column is None:
                continue

            if target_column.can_expand_to(reference_column):
                pass
            elif (
                enable_safe
                and not row_count_exceeds
                and target_column.can_expand_safe(reference_column)
            ):
                pass
            else:
                continue

            if reference_column.is_string():
                col_string_size = reference_column.string_size()
                new_type = reference_column.string_type_instance(col_string_size)
            else:
                new_type = reference_column.data_type
            fire_event(
                ColTypeChange(
                    orig_type=target_column.data_type,
                    new_type=new_type,
                    table=_make_ref_key_dict(current),
                )
            )
            self.alter_column_type(current, column_name, new_type)

    @available.parse_none
    def expand_target_column_types(
        self, from_relation: BaseRelation, to_relation: BaseRelation, max_rows: int = 1000000
    ) -> None:
        if not isinstance(from_relation, self.Relation):
            from dbt.adapters.base.impl import MacroArgTypeError

            raise MacroArgTypeError(
                method_name="expand_target_column_types",
                arg_name="from_relation",
                got_value=from_relation,
                expected_type=self.Relation,
            )
        if not isinstance(to_relation, self.Relation):
            from dbt.adapters.base.impl import MacroArgTypeError

            raise MacroArgTypeError(
                method_name="expand_target_column_types",
                arg_name="to_relation",
                got_value=to_relation,
                expected_type=self.Relation,
            )
        self.expand_column_types(from_relation, to_relation, max_rows)

    @available
    def parse_index(self, raw_index: Any) -> Optional[SQLServerIndexConfig]:
        return SQLServerIndexConfig.parse(raw_index)

    @available
    def index_needs_own_batch(self, raw_index: Any) -> bool:
        """True when raw_index's build_options (ONLINE / RESUMABLE) force its
        CREATE INDEX to run outside any transaction: SQL Server rejects
        RESUMABLE inside a user transaction (error 574), and an ONLINE build
        wrapped in one holds its locks until commit, negating the point."""
        parsed = self.parse_index(raw_index)
        if not parsed:
            return False
        return create_needs_own_batch(parsed.build_options)

    @available
    def commit_if_open(self) -> None:
        """Commit the current transaction if one is open - a no-op otherwise.

        dbt_sqlserver_use_dbt_transactions on (default) wraps a
        materialization's whole build, from its first statement through its
        own trailing ``adapter.commit()``, in one continuous ambient
        transaction. Some statements must not share that transaction with
        whatever runs after them: an ONLINE/RESUMABLE index build (SQL Server
        rejects RESUMABLE inside a user transaction outright, and ONLINE
        holds its locks until commit either way), or a full-refresh-in-
        progress marker, which exists specifically to survive a later
        failure and so must not roll back with it. This makes the prior
        statement durable on its own; pair with begin_if_closed once the
        statement(s) that must run outside a transaction are done, to leave
        later code (more such statements, or the materialization's own
        trailing ``adapter.commit()``, which raises if it finds nothing open)
        working as if this call had never happened.

        A no-op when no transaction is open: the caller's statement already
        ran autocommitted on its own (e.g. via ``run_query``'s
        ``auto_begin=false``, before anything else began one), so there is
        nothing to flush. Also a no-op, at the SQL level, whenever
        dbt_sqlserver_use_dbt_transactions is off: begin/commit still flip
        dbt-core's bookkeeping (see
        SQLServerConnectionManager.add_begin_query/add_commit_query), but
        emit no real T-SQL, matching the driver's own autocommit.
        """
        connection = self.connections.get_thread_connection()
        if connection is not None and connection.transaction_open:
            self.connections.commit()

    @available
    def begin_if_closed(self) -> None:
        """Begin a transaction if none is open - a no-op otherwise. See
        commit_if_open, which this pairs with."""
        connection = self.connections.get_thread_connection()
        if connection is not None and not connection.transaction_open:
            self.connections.begin()

    @available
    def transaction_is_open(self) -> bool:
        """True when a dbt-managed transaction is currently open.

        This is the same predicate ``SQLConnectionManager.add_query`` tests
        before honouring ``auto_begin``, so it answers the only question that
        actually matters to a materialization deciding how to scope a build:
        will the next statement join an existing transaction, or start on its
        own?

        Inferring that from config does not work. The obvious proxy - "does
        this model have an in-transaction pre-hook?" - is wrong in both
        directions. ``run_hooks`` skips a hook whose rendered SQL is empty
        (the common ``{% if target.name == 'prod' %}...{% endif %}`` idiom),
        so a model can declare one and open nothing; and macros that pair
        commit_if_open with begin_if_closed - sqlserver__mark_full_refresh_
        incomplete, sqlserver__create_indexes_no_txn - leave a transaction
        open with no hook involved at all. Ask the connection instead.

        Reads bookkeeping, not the server: when
        dbt_sqlserver_use_dbt_transactions is off, begin/commit flip this flag
        without emitting T-SQL, so this reports what dbt believes rather than
        @@TRANCOUNT. That is the right answer for deciding whether a statement
        would join something, since auto_begin keys off the same flag.
        """
        connection = self.connections.get_thread_connection()
        return connection is not None and bool(connection.transaction_open)

    @available
    def validate_indexes(
        self, raw_indexes: Any, as_columnstore: Any = False, drop_unmanaged: Any = False
    ) -> None:
        """Cross-config checks that individual index validation can't see.
        Also fail-fast validates drop_unmanaged_indexes so a bad value errors
        on the first build, not only when reconciliation first runs."""
        normalize_drop_unmanaged(drop_unmanaged)
        configs = []
        for raw_index in raw_indexes or []:
            parsed = self.parse_index(raw_index)
            if parsed:
                configs.append(parsed)

        clustered = [config for config in configs if config.type == SQLServerIndexType.clustered]
        if len(clustered) > 1:
            raise dbt_common.exceptions.DbtRuntimeError(
                f"A table can have at most one clustered index; "
                f"{len(clustered)} declared in the indexes config: "
                f"{[list(config.columns) for config in clustered]}"
            )
        if clustered and as_columnstore:
            raise dbt_common.exceptions.DbtRuntimeError(
                "A clustered rowstore index in the indexes config conflicts with "
                "as_columnstore=true (the default), which builds the table with a "
                "clustered columnstore index. Set as_columnstore: false on the "
                "model, or remove the clustered entry."
            )

    @available
    def index_changes(
        self,
        existing_indexes: Any,
        raw_indexes: Any,
        relation: BaseRelation,
        drop_unmanaged: Any = False,
    ) -> dict:
        """Diff existing indexes (agate table from sqlserver__describe_indexes)
        against the model's `indexes` config. Returns plain lists for jinja:
        drops (index names), creates (index config dicts to build inside the
        reconcile transaction), creates_no_txn (ONLINE/RESUMABLE creates that
        must run as standalone autocommitted statements), warnings (strings).
        Drops must be applied before creates (a replacement clustered index
        needs its predecessor gone first)."""
        rows = []
        if existing_indexes is not None:
            column_names = existing_indexes.column_names
            for row in existing_indexes.rows:
                rows.append(dict(zip(column_names, row)))

        expected = []
        for raw_index in raw_indexes or []:
            parsed = self.parse_index(raw_index)
            if parsed:
                expected.append(parsed)

        changes, warnings = index_config_changes(rows, expected, relation, drop_unmanaged)

        drops = []
        creates = []
        creates_no_txn = []
        for change in changes:
            if change.action == RelationConfigChangeAction.drop:
                drops.append(change.context.name)
            elif change.action == RelationConfigChangeAction.create:
                node_config = change.context.as_node_config
                if create_needs_own_batch(node_config.get("build_options")):
                    creates_no_txn.append(node_config)
                else:
                    creates.append(node_config)

        return {
            "drops": drops,
            "creates": creates,
            "creates_no_txn": creates_no_txn,
            "warnings": warnings,
        }

    @available
    def resolve_masks(self, model: Any, model_masks: Optional[dict] = None) -> Dict[str, str]:
        """Merge the column-level `masked_with` and model-level `masks` surfaces
        into one `{column: function}` map for `apply_masks`.

        `model` is the Jinja `model` dict (`node.to_dict()`), whose `columns`
        carry any `masked_with` as a flattened key (an explicit `masked_with:
        null` survives serialization as a present `None`, signalling opt-out).
        `model_masks` is `config.get('masks')` — already surface-merged by dbt.
        Precedence and conflict warnings are handled here; key existence is
        validated later in the macro against the real relation.
        """
        model = model or {}
        columns = model.get("columns") or {}
        column_masks = []
        for name, col in columns.items():
            col = col or {}
            column_masks.append(
                ColumnMask(
                    name=col.get("name", name),
                    masked_with_present=("masked_with" in col),
                    masked_with=col.get("masked_with"),
                )
            )
        model_name = model.get("name") or model.get("alias") or "<unknown>"
        mask_map, warnings = _resolve_masks(column_masks, model_masks, model_name)
        for warning in warnings:
            logger.warning(warning)
        return mask_map

    @available
    def mask_changes(
        self,
        existing_masks: Any,
        mask_config: Optional[dict],
        index_key_columns: Any = None,
        existing_columns: Any = None,
    ) -> dict:
        """Diff a resolved mask map against current `sys.masked_columns` state.

        `existing_masks` is the agate table from `get_show_mask_sql` (columns
        `name`, `masking_function`). Returns plain lists for jinja: `adds` /
        `changes` (each `[column, function]`), `drops` (column names), `skipped`
        (warnings for columns absent from the relation) and `errors` (an ADD onto
        a current index-key column, which SQL Server rejects). The macro emits
        DDL for adds/changes/drops, logs `skipped`, and raises on `errors`."""
        rows = []
        if existing_masks is not None:
            column_names = existing_masks.column_names
            for row in existing_masks.rows:
                rows.append(dict(zip(column_names, row)))
        return _mask_changes(
            rows,
            mask_config or {},
            set(index_key_columns or []),
            existing_columns=(list(existing_columns) if existing_columns is not None else None),
        )

    @available
    def resolve_denies(self, model: Any, model_denies: Optional[dict] = None) -> Dict[str, list]:
        """Normalise the model-level `denies` config into a `{privilege:
        [principals]}` map for `apply_denies`.

        `model` is the Jinja `model` dict (`node.to_dict()`); its `grants` config
        (under `model['config']`) is read only to warn when a principal is both
        granted and denied the same privilege. `model_denies` is
        `config.get('denies')`, already surface-merged by dbt.

        Unsupported privileges (anything other than the object-level table
        privileges) are warned and skipped rather than failing the run — the
        warning surfaces the likely typo without taking down the build.
        """
        model = model or {}
        grant_config = (model.get("config") or {}).get("grants")
        model_name = model.get("name") or model.get("alias") or "<unknown>"
        resolved, warnings, unsupported = _resolve_denies(model_denies, grant_config, model_name)
        for warning in warnings:
            logger.warning(warning)
        if unsupported:
            from dbt.adapters.sqlserver.sqlserver_deny import SUPPORTED_PRIVILEGES

            logger.warning(
                f"On model '{model_name}', the `denies` config lists unsupported "
                f"privilege(s): {', '.join(sorted(unsupported))}; skipping them. "
                f"Object-level DENY is supported only for the table privileges: "
                f"{', '.join(SUPPORTED_PRIVILEGES)}."
            )
        return resolved

    @available
    def deny_changes(self, existing_denies: Any, deny_config: Optional[dict]) -> dict:
        """Diff a resolved deny map against current `sys.database_permissions`.

        `existing_denies` is the agate table from `get_show_deny_sql` (columns
        `grantee`, `privilege_type`). Returns plain lists for jinja: `denies` and
        `revokes`, each a list of `[privilege, principal]` pairs. The macro emits
        `DENY` for the former and `REVOKE` for the latter."""
        rows = []
        if existing_denies is not None:
            column_names = existing_denies.column_names
            for row in existing_denies.rows:
                rows.append(dict(zip(column_names, row)))
        return _deny_changes(rows, deny_config or {})


COLUMNS_EQUAL_SQL = """
with diff_count as (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM (
            (SELECT {columns} FROM {relation_a} {except_op}
             SELECT {columns} FROM {relation_b})
             UNION ALL
            (SELECT {columns} FROM {relation_b} {except_op}
             SELECT {columns} FROM {relation_a})
        ) as a
), table_a as (
    SELECT COUNT(*) as num_rows FROM {relation_a}
), table_b as (
    SELECT COUNT(*) as num_rows FROM {relation_b}
), row_count_diff as (
    select
        1 as id,
        table_a.num_rows - table_b.num_rows as difference
    from table_a, table_b
)
select
    row_count_diff.difference as row_count_difference,
    diff_count.num_missing as num_mismatched
from row_count_diff
join diff_count on row_count_diff.id = diff_count.id
""".strip()
