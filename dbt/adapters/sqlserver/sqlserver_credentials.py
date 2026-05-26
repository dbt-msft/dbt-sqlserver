from dataclasses import dataclass
from typing import Optional, Union, cast

import dbt_common.exceptions
from dbt_common.dataclass_schema import StrEnum

from dbt.adapters.contracts.connection import Credentials


class SQLServerBackend(StrEnum):
    pyodbc = "pyodbc"
    mssql_python = "mssql-python"


@dataclass
class SQLServerCredentials(Credentials):
    backend: Union[SQLServerBackend, str] = SQLServerBackend.pyodbc
    driver: Optional[str] = None
    host: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None
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
    }

    def __post_init__(self) -> None:
        if isinstance(self.backend, str):
            try:
                self.backend = SQLServerBackend(self.backend)
            except ValueError as exc:
                raise dbt_common.exceptions.DbtRuntimeError(
                    "Unsupported sqlserver backend: '{}'. "
                    "Supported backends are 'pyodbc' and 'mssql-python'.".format(self.backend)
                ) from exc

        self.backend = cast(SQLServerBackend, self.backend)

    @property
    def type(self):
        return "sqlserver"

    def _effective_backend(self) -> SQLServerBackend:
        return cast(SQLServerBackend, self.backend)

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
            "backend",
        )

        if self._effective_backend() == SQLServerBackend.pyodbc:
            keys = ("driver",) + keys

        return keys

    @property
    def unique_field(self):
        return self.host
