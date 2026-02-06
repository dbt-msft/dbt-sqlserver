from dataclasses import dataclass
from typing import Literal, Optional

from dbt.adapters.fabric import FabricCredentials

# Source: https://learn.microsoft.com/en-us/sql/relational-databases/security/networking/tds-8?view=sql-server-ver17#additional-changes-to-connection-string-encryption-properties  # noqa: E501
EncryptType = Literal[
    "true",
    "yes",
    "mandatory",
    "strict",
    "optional",
    "false",
    "no",
    True,
    False,
]


@dataclass
class SQLServerCredentials(FabricCredentials):
    """
    Defines database specific credentials that get added to
    profiles.yml to connect to new adapter
    """

    port: Optional[int] = 1433
    authentication: Optional[str] = "sql"
    encrypt: Optional[EncryptType] = True

    @property
    def type(self):
        return "sqlserver"

    def _connection_keys(self):
        return super()._connection_keys() + ("port",)
