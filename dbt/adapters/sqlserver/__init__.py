from dbt.adapters.sqlserver.connections import SQLServerConnectionManager
from dbt.adapters.sqlserver.connections import SQLServerCredentials
from dbt.adapters.sqlserver.impl import SQLServerAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import sqlserver


Plugin = AdapterPlugin(
    adapter=SQLServerAdapter,
    credentials=SQLServerCredentials,
    include_path=sqlserver.PACKAGE_PATH,
)
