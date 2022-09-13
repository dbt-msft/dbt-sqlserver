from dbt.adapters.base import AdapterPlugin

from dbt.adapters.sqlserver.sql_server_adapter import SQLServerAdapter
from dbt.adapters.sqlserver.sql_server_column import SQLServerColumn
from dbt.adapters.sqlserver.sql_server_connection_manager import SQLServerConnectionManager
from dbt.adapters.sqlserver.sql_server_credentials import SQLServerCredentials
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
