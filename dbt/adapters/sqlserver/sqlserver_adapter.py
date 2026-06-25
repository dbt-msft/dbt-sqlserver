from typing import Any, List, Optional

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
from dbt.adapters.sqlserver.sqlserver_column import SQLServerColumn, SQLServerColumnNative
from dbt.adapters.sqlserver.sqlserver_configs import SQLServerConfigs
from dbt.adapters.sqlserver.sqlserver_connections import SQLServerConnectionManager
from dbt.adapters.sqlserver.sqlserver_relation import SQLServerRelation

logger = AdapterLogger("SQLServer")


class SQLServerAdapter(SQLAdapter):
    """
    Controls actual implementation of adapter, and ability to override certain methods.
    """

    ConnectionManager = SQLServerConnectionManager
    Column = SQLServerColumn
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
        return [
            {
                "name": "empty",
                "default": False,
                "description": (
                    "When enabled, table and view materializations will be created as empty "
                    "structures (no data)."
                ),
            },
            {
                "name": "dbt_sqlserver_use_default_schema_concat",
                "default": False,
                "description": (
                    "When True, uses dbt-core's standard schema concatenation "
                    "(`target.schema` + `_` + `custom_schema_name`). "
                    "When False (default), uses legacy adapter behaviour: "
                    "`custom_schema_name` is used directly without prefixing `target.schema`. "
                    "For a permanent solution, override the `sqlserver__generate_schema_name` "
                    "macro in your project instead."
                ),
            },
            {
                "name": "dbt_sqlserver_disable_empty_relation_aliases",
                "default": True,
                "description": (
                    "When True, SQL Server limited relations used by --empty and sample mode "
                    "do not automatically receive dbt-generated aliases. Set this false to opt "
                    "out of alias generation temporarily for testing."
                ),
            },
            {
                "name": "dbt_sqlserver_use_native_string_types",
                "default": False,
                "description": (
                    "When True, uses SQL Server-native string type mappings: "
                    "STRING -> VARCHAR(MAX), NCHAR -> NCHAR(1), NVARCHAR -> NVARCHAR(4000). "
                    "When False (default), preserves legacy mappings: "
                    "STRING and NVARCHAR -> VARCHAR(8000), NCHAR -> CHAR(1). "
                    "The new behaviour is intended to become the default in a future release."
                ),
            },
            {
                "name": "dbt_sqlserver_enable_safe_type_expansion",
                "default": False,
                "description": (
                    "Allow the SQL Server adapter to widen column types during schema expansion. "
                    "This enables promotions like varchar -> nvarchar, "
                    "bit -> tinyint -> smallint -> int -> bigint, "
                    "and numeric(p,s) -> numeric(p2,s2) using alter column."
                ),
            },
            {
                "name": "dbt_sqlserver_use_dbt_transactions",
                "default": False,
                "description": (
                    "When True, dbt transaction hooks (begin/commit) emit real T-SQL "
                    "BEGIN TRANSACTION / COMMIT TRANSACTION statements. "
                    "When False (default and legacy), begin/commit are no-ops and each statement "
                    "is auto-committed by the driver. This means earlier successful statements "
                    "are not rolled back if a later statement fails. "
                    "This behavior is intended to become the default in a future release."
                ),
            },
        ]

    @available.parse(lambda *a, **k: [])
    def get_column_schema_from_query(self, sql: str) -> List[BaseColumn]:
        """Get a list of the Columns with names and data types from the given sql."""
        _, cursor = self.connections.add_select_query(sql)

        columns = [
            self.Column.create(
                column_name, self.connections.data_type_code_to_name(column_type_code)
            )
            # https://peps.python.org/pep-0249/#description
            for column_name, column_type_code, *_ in cursor.description
        ]
        return columns

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
                return cursor.fetchone()
            elif fetch == "all":
                return cursor.fetchall()
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
        row = cursor.fetchone()
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
