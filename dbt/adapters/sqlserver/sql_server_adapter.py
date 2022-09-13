from typing import List, Optional

import agate
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.sql import SQLAdapter

from dbt.adapters.sqlserver.sql_server_column import SQLServerColumn
from dbt.adapters.sqlserver.sql_server_connection_manager import SQLServerConnectionManager


class SQLServerAdapter(SQLAdapter):
    ConnectionManager = SQLServerConnectionManager
    Column = SQLServerColumn

    @classmethod
    def date_function(cls):
        return "getdate()"

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        column = agate_table.columns[col_idx]
        # see https://github.com/fishtown-analytics/dbt/pull/2255
        lens = [len(d.encode("utf-8")) for d in column.values_without_nulls()]
        max_len = max(lens) if lens else 64
        length = max_len if max_len > 16 else 16
        return "varchar({})".format(length)

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "datetime"

    @classmethod
    def convert_boolean_type(cls, agate_table, col_idx):
        return "bit"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "float" if decimals else "int"

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        return "datetime"

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
