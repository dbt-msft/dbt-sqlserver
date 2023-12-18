from dataclasses import dataclass
from typing import Optional

from dbt.adapters.fabric import FabricCredentials

@dataclass
class SQLServerCredentials(FabricCredentials):
    port: Optional[int] = 1433

    @property
    def type(self):
        return "sqlserver"

    def _connection_keys(self):
        return super()._connection_keys() +("port",)
