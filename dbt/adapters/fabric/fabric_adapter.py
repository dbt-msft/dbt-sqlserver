from typing import List, Optional

import agate
from dbt.adapters.base import Column as BaseColumn

# from dbt.events.functions import fire_event
# from dbt.events.types import SchemaCreation
from dbt.adapters.base.impl import ConstraintSupport
from dbt.adapters.base.meta import available
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.cache import _make_ref_key_dict

# from dbt.adapters.cache import _make_ref_key_msg
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.sql.impl import CREATE_SCHEMA_MACRO_NAME
from dbt.contracts.graph.nodes import ColumnLevelConstraint, ConstraintType, ModelLevelConstraint
from dbt.events.functions import fire_event
from dbt.events.types import SchemaCreation

from dbt.adapters.fabric.fabric_column import FabricColumn
from dbt.adapters.fabric.fabric_configs import FabricConfigs
from dbt.adapters.fabric.fabric_connection_manager import FabricConnectionManager


class FabricAdapter(SQLAdapter):
    ConnectionManager = FabricConnectionManager
    Column = FabricColumn
    AdapterSpecificConfigs = FabricConfigs

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.ENFORCED,
    }

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
            macro_name = "fabric__create_schema_with_authorization"

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
        note: using is not supported on Synapse so COLUMNS_EQUAL_SQL is adjsuted
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
        return ["append", "delete+insert", "merge", "insert_overwrite"]

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

        if constraint.type == ConstraintType.unique:
            return (
                constraint_prefix
                + f"{constraint.name} unique nonclustered({column_list}) not enforced"
            )
        elif constraint.type == ConstraintType.primary_key:
            return (
                constraint_prefix
                + f"{constraint.name} primary key nonclustered({column_list}) not enforced"
            )
        elif constraint.type == ConstraintType.foreign_key and constraint.expression:
            return (
                constraint_prefix
                + f"{constraint.name} foreign key({column_list}) references {constraint.expression} not enforced"
            )
        elif constraint.type == ConstraintType.custom and constraint.expression:
            return f"{constraint_prefix}{constraint.expression}"
        else:
            return None


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
