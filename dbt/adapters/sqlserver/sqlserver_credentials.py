from dataclasses import dataclass
from typing import Optional, Union, cast

import dbt_common.exceptions
from dbt_common.dataclass_schema import StrEnum

from dbt.adapters.contracts.connection import Credentials
from dbt.adapters.sqlserver.sqlserver_auth import normalize_authentication_key
from dbt.adapters.sqlserver.sqlserver_constants import (
    MSSQL_AUTH_ACTIVE_DIRECTORY_SERVICE_PRINCIPAL,
    SQLSERVER_BACKEND_MSSQL_PYTHON,
    SQLSERVER_BACKEND_PYODBC,
    SUPPORTED_SQLSERVER_BACKENDS_MESSAGE,
)
from dbt.adapters.sqlserver.sqlserver_helpers import normalize_query_timeout


class SQLServerBackend(StrEnum):
    pyodbc = SQLSERVER_BACKEND_PYODBC
    mssql_python = SQLSERVER_BACKEND_MSSQL_PYTHON


DEFAULT_SQLSERVER_BACKEND = cast(SQLServerBackend, SQLServerBackend.pyodbc)


def coerce_backend(backend: Union[SQLServerBackend, str]) -> SQLServerBackend:
    try:
        return SQLServerBackend(backend)
    except ValueError as exc:
        raise dbt_common.exceptions.DbtRuntimeError(
            f"Unsupported sqlserver backend: '{backend}'. {SUPPORTED_SQLSERVER_BACKENDS_MESSAGE}"
        ) from exc


@dataclass
class SQLServerCredentials(Credentials):
    backend: SQLServerBackend = DEFAULT_SQLSERVER_BACKEND
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
        self.backend = coerce_backend(self.backend)
        self.query_timeout = normalize_query_timeout(self.query_timeout)

    @property
    def type(self):
        return "sqlserver"

    def _connection_keys(self):
        """Return the credential fields that distinguish reusable connections."""

        authentication = self.authentication
        if self.windows_login is True:
            authentication = "Windows Login"
        elif normalize_authentication_key(authentication) == "serviceprincipal":
            authentication = MSSQL_AUTH_ACTIVE_DIRECTORY_SERVICE_PRINCIPAL

        keys = (
            "server",
            "port",
            "database",
            "schema",
            "UID",
            "authentication",
            "windows_login",
            "retries",
            "login_timeout",
            "query_timeout",
            "trace_flag",
            "encrypt",
            "trust_cert",
            "backend",
        )

        if self.backend == SQLServerBackend.pyodbc:
            # Only the pyodbc path uses an ODBC driver name. The mssql-python
            # backend ignores `driver`, so excluding it keeps connection reuse
            # aligned with the actual connection string that backend produces.
            keys = ("driver",) + keys

        return keys

    @property
    def unique_field(self):
        return self.host
