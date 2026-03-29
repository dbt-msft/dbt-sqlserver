from dataclasses import dataclass
from typing import Optional

from dbt.adapters.contracts.connection import Credentials


@dataclass
class SQLServerCredentials(Credentials):
    driver: Optional[str] = None
    host: str = ""
    database: str = ""
    schema: str = ""
    UID: Optional[str] = None
    PWD: Optional[str] = None
    port: Optional[int] = 1433
    windows_login: Optional[bool] = False
    trace_flag: Optional[bool] = False
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    access_token: Optional[str] = None
    access_token_expires_on: Optional[int] = 0
    authentication: str = "sql"
    encrypt: Optional[bool] = True
    trust_cert: Optional[bool] = False
    retries: int = 3
    schema_authorization: Optional[str] = None
    login_timeout: Optional[int] = 0
    query_timeout: Optional[int] = 0
    use_mssql_python: bool = False

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
        "schema_auth": "schema_authorization",
        "SQL_ATTR_TRACE": "trace_flag",
        "mssql_python": "use_mssql_python",
        "use_mssql_python_backend": "use_mssql_python",
    }

    @property
    def type(self):
        return "sqlserver"

    def _connection_keys(self):
        if self.windows_login is True:
            self.authentication = "Windows Login"

        if self.authentication.lower().strip() == "serviceprincipal":
            self.authentication = "ActiveDirectoryServicePrincipal"

        keys = (
            "server",
            "port",
            "database",
            "schema",
            "UID",
            "authentication",
            "retries",
            "login_timeout",
            "query_timeout",
            "trace_flag",
            "encrypt",
            "trust_cert",
            "use_mssql_python",
        )

        if not self.use_mssql_python:
            keys = ("driver",) + keys

        return keys

    @property
    def unique_field(self):
        return self.host
