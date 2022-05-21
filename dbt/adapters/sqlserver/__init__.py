from dbt.adapters.base import AdapterPlugin

from dbt.adapters.sqlserver.connections import SQLServerConnectionManager, SQLServerCredentials
from dbt.adapters.sqlserver.impl import SQLServerAdapter
from dbt.include import sqlserver

Plugin = AdapterPlugin(
    adapter=SQLServerAdapter,
    credentials=SQLServerCredentials,
    include_path=sqlserver.PACKAGE_PATH,
)

__all__ = ["Plugin", "SQLServerConnectionManager", "SQLServerAdapter", "SQLServerCredentials"]
