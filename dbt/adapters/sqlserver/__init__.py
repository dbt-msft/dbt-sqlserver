from dbt.adapters.base import AdapterPlugin

from dbt.adapters.sqlserver.connections import SQLServerConnectionManager, SQLServerCredentials
from dbt.adapters.sqlserver.sql_server_adapter import SQLServerAdapter
from dbt.adapters.sqlserver.sql_server_column import SQLServerColumn
from dbt.include import sqlserver

Plugin = AdapterPlugin(
    adapter=SQLServerAdapter,
    credentials=SQLServerCredentials,
    include_path=sqlserver.PACKAGE_PATH,
)

__all__ = [
    "Plugin",
    "SQLServerConnectionManager",
    "SQLServerColumn",
    "SQLServerAdapter",
    "SQLServerCredentials",
]
