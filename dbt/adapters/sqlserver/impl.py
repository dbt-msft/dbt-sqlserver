from dbt.adapters.sql import SQLAdapter
from dbt.adapters.sqlserver import SQLServerConnectionManager


class SQLServerAdapter(SQLAdapter):
    ConnectionManager = SQLServerConnectionManager

    @classmethod
    def date_function(cls):
        return 'getdate()'

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        column = agate_table.columns[col_idx]
        lens = (len(d.encode("utf-8")) for d in column.values_without_nulls())
        max_len = max(lens) if lens else 64
        length = max_len if max_len > 16 else 16
        return "varchar({})".format(length)

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "datetime"
