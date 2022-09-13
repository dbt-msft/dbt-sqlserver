from dataclasses import dataclass
from typing import Optional

from dbt.contracts.connection import Credentials


@dataclass
class SQLServerCredentials(Credentials):
    driver: str
    host: str
    database: str
    schema: str
    port: Optional[int] = 1433
    UID: Optional[str] = None
    PWD: Optional[str] = None
    windows_login: Optional[bool] = False
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    authentication: Optional[str] = "sql"
    encrypt: Optional[bool] = True  # default value in MS ODBC Driver 18 as well
    trust_cert: Optional[bool] = False  # default value in MS ODBC Driver 18 as well
    retries: int = 1

    _ALIASES = {
        "user": "UID",
        "username": "UID",
        "pass": "PWD",
        "password": "PWD",
        "server": "host",
        "trusted_connection": "windows_login",
        "auth": "authentication",
        "app_id": "client_id",
        "app_secret": "client_secret",
        "TrustServerCertificate": "trust_cert",
    }

    @property
    def type(self):
        return "sqlserver"

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'
        # raise NotImplementedError
        if self.windows_login is True:
            self.authentication = "Windows Login"

        return (
            "server",
            "database",
            "schema",
            "port",
            "UID",
            "client_id",
            "authentication",
            "encrypt",
            "trust_cert",
        )

    @property
    def unique_field(self):
        return self.host
