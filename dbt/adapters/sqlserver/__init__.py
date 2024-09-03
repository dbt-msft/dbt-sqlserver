from dbt.adapters.base import AdapterPlugin

from dbt.adapters.sqlserver.sqlserver_adapter import SQLServerAdapter
from dbt.adapters.sqlserver.sqlserver_column import SQLServerColumn
from dbt.adapters.sqlserver.sqlserver_configs import SQLServerConfigs
from dbt.adapters.sqlserver.sqlserver_connections import SQLServerConnectionManager  # noqa
from dbt.adapters.sqlserver.sqlserver_credentials import SQLServerCredentials
from dbt.include import sqlserver

Plugin = AdapterPlugin(
    adapter=SQLServerAdapter,
    credentials=SQLServerCredentials,
    include_path=sqlserver.PACKAGE_PATH,
    dependencies=["fabric"],
)

__all__ = [
    "Plugin",
    "SQLServerConnectionManager",
    "SQLServerColumn",
    "SQLServerAdapter",
    "SQLServerCredentials",
    "SQLServerConfigs",
]
