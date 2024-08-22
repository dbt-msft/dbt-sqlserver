from __future__ import annotations

from dataclasses import dataclass

from dbt.adapters.fabric import FabricCredentials


@dataclass
class SQLServerCredentials(FabricCredentials):
    """
    Defines database specific credentials that get added to
    profiles.yml to connect to new adapter
    """

    port: int | None = 1433
    authentication: str | None = "sql"

    @property
    def type(self):
        return "sqlserver"

    def _connection_keys(self):
        return super()._connection_keys() + ("port",)
