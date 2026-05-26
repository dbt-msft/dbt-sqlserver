import datetime as dt
import struct
import time
from contextlib import contextmanager
from dataclasses import dataclass
from itertools import chain, repeat
from typing import Any, Callable, Dict, Mapping, Optional, Protocol, Tuple, Type, Union, cast

import agate  # type: ignore[import]
import dbt_common.exceptions
from dbt_common.clients.agate_helper import empty_table
from dbt_common.events.contextvars import get_node_info
from dbt_common.events.functions import fire_event
from dbt_common.utils.casting import cast_to_str

from dbt.adapters.contracts.connection import AdapterResponse, Connection, ConnectionState
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.events.types import AdapterEventDebug, ConnectionUsed, SQLQuery, SQLQueryStatus
from dbt.adapters.sql.connections import SQLConnectionManager
from dbt.adapters.sqlserver import __version__
from dbt.adapters.sqlserver.sqlserver_credentials import SQLServerBackend, SQLServerCredentials


class PyodbcModuleProtocol(Protocol):
    InternalError: type[Exception]
    OperationalError: type[Exception]
    InterfaceError: type[Exception]
    DatabaseError: type[Exception]
    pooling: bool

    def connect(self, *args: Any, **kwargs: Any) -> Any: ...


class MssqlPythonModuleProtocol(Protocol):
    InternalError: type[Exception]
    OperationalError: type[Exception]
    InterfaceError: type[Exception]
    DatabaseError: type[Exception]

    def connect(self, *args: Any, **kwargs: Any) -> Any: ...


class AccessTokenProtocol(Protocol):
    token: str
    expires_on: int


class TokenCredentialProtocol(Protocol):
    def get_token(self, *scopes: Optional[str], **kwargs: Any) -> AccessTokenProtocol: ...


class CredentialFactory(Protocol):
    def __call__(self, *args: Any, **kwargs: Any) -> TokenCredentialProtocol: ...


class AzureIdentityModuleProtocol(Protocol):
    AzureCliCredential: CredentialFactory
    DefaultAzureCredential: CredentialFactory
    EnvironmentCredential: CredentialFactory
    ManagedIdentityCredential: CredentialFactory
    ClientSecretCredential: CredentialFactory


class AzureCredentialsModuleProtocol(Protocol):
    AccessToken: Type[AccessTokenProtocol]


_PYODBC_MODULE: Optional[PyodbcModuleProtocol] = None
_PYODBC_IMPORT_ERROR: Optional[ModuleNotFoundError] = None

_MSSQL_PYTHON_MODULE: Optional[MssqlPythonModuleProtocol] = None
_MSSQL_PYTHON_IMPORT_ERROR: Optional[ModuleNotFoundError] = None

_AZURE_CREDENTIALS_MODULE: Optional[AzureCredentialsModuleProtocol] = None
_AZURE_CREDENTIALS_IMPORT_ERROR: Optional[ModuleNotFoundError] = None

_AZURE_IDENTITY_MODULE: Optional[AzureIdentityModuleProtocol] = None
_AZURE_IDENTITY_IMPORT_ERROR: Optional[ModuleNotFoundError] = None


@dataclass
class AccessToken:  # type: ignore[no-redef]
    token: str
    expires_on: int


def _get_azure_access_token_class() -> Type[Any]:
    global _AZURE_CREDENTIALS_MODULE, _AZURE_CREDENTIALS_IMPORT_ERROR

    if _AZURE_CREDENTIALS_MODULE is not None:
        return _AZURE_CREDENTIALS_MODULE.AccessToken

    if _AZURE_CREDENTIALS_IMPORT_ERROR is not None:
        return AccessToken

    try:
        import azure.core.credentials as azure_credentials  # type: ignore[import]
    except ModuleNotFoundError as exc:
        _AZURE_CREDENTIALS_IMPORT_ERROR = exc
        return AccessToken

    _AZURE_CREDENTIALS_MODULE = cast(AzureCredentialsModuleProtocol, azure_credentials)
    return azure_credentials.AccessToken


def _get_azure_identity_module() -> AzureIdentityModuleProtocol:
    global _AZURE_IDENTITY_MODULE, _AZURE_IDENTITY_IMPORT_ERROR

    if _AZURE_IDENTITY_MODULE is not None:
        return _AZURE_IDENTITY_MODULE

    if _AZURE_IDENTITY_IMPORT_ERROR is not None:
        raise _missing_azure_identity_error() from _AZURE_IDENTITY_IMPORT_ERROR

    try:
        import azure.identity as azure_identity  # type: ignore[import]
    except ModuleNotFoundError as exc:
        _AZURE_IDENTITY_IMPORT_ERROR = exc
        raise _missing_azure_identity_error() from exc

    _AZURE_IDENTITY_MODULE = cast(AzureIdentityModuleProtocol, azure_identity)
    return _AZURE_IDENTITY_MODULE


_TOKEN: Optional[AccessTokenProtocol] = None
AZURE_CREDENTIAL_SCOPE = "https://database.windows.net//.default"
AZURE_AUTH_FUNCTION_TYPE = Callable[[SQLServerCredentials, Optional[str]], AccessTokenProtocol]

logger = AdapterLogger("sqlserver")

# https://github.com/mkleehammer/pyodbc/wiki/Data-Types
datatypes = {
    "str": "varchar",
    "uuid.UUID": "uniqueidentifier",
    "uuid": "uniqueidentifier",
    "float": "bigint",
    "int": "int",
    "bytes": "varbinary",
    "bytearray": "varbinary",
    "bool": "bit",
    "datetime.date": "date",
    "datetime.datetime": "datetime2(6)",
    "datetime.time": "time",
    "decimal.Decimal": "decimal",
}

MSSQL_PYTHON_UNSUPPORTED_AUTHENTICATIONS = {
    "cli",
    "environment",
    "activedirectoryaccesstoken",
}


def _auth_key(authentication: Optional[str]) -> str:
    if authentication is None:
        return ""
    return authentication.replace("_", "").replace(" ", "").lower()


def _normalize_mssql_python_authentication(authentication: Optional[str]) -> Optional[str]:
    authentication = authentication or ""
    key = _auth_key(authentication)
    if not key:
        return None

    if key in {"msi", "activedirectorymsi"}:
        return "ActiveDirectoryMSI"

    if key in {"activedirectoryintegrated", "adintegrated"}:
        return "ActiveDirectoryIntegrated"

    if key in {"serviceprincipal", "activedirectoryserviceprincipal"}:
        return "ActiveDirectoryServicePrincipal"

    if key in {"auto", "default", "activedirectorydefault"}:
        return "ActiveDirectoryDefault"

    if key == "activedirectorypassword":
        return "ActiveDirectoryPassword"

    if key == "activedirectoryinteractive":
        return "ActiveDirectoryInteractive"

    if key == "activedirectorydevicecode":
        return "ActiveDirectoryDeviceCode"

    return authentication.strip()


def _missing_pyodbc_error() -> dbt_common.exceptions.DbtRuntimeError:
    return dbt_common.exceptions.DbtRuntimeError(
        "The legacy `pyodbc` backend was requested, but the optional dependency "
        "`pyodbc` is not installed. Install it with `pip install pyodbc` "
        "or set `backend: mssql-python` in the profile."
    )


def _get_pyodbc() -> PyodbcModuleProtocol:
    global _PYODBC_MODULE, _PYODBC_IMPORT_ERROR

    if _PYODBC_MODULE is not None:
        return _PYODBC_MODULE

    if _PYODBC_IMPORT_ERROR is not None:
        raise _missing_pyodbc_error() from _PYODBC_IMPORT_ERROR

    try:
        import pyodbc as imported_pyodbc  # type: ignore[import]
    except ModuleNotFoundError as exc:
        _PYODBC_IMPORT_ERROR = exc
        raise _missing_pyodbc_error() from exc

    _PYODBC_MODULE = cast(PyodbcModuleProtocol, imported_pyodbc)
    return _PYODBC_MODULE


def _missing_mssql_python_error() -> dbt_common.exceptions.DbtRuntimeError:
    return dbt_common.exceptions.DbtRuntimeError(
        "The `mssql-python` backend was requested, but the optional dependency "
        "`mssql-python` is not installed. Install it with `pip install mssql-python` "
        "or set `backend: pyodbc` in the profile."
    )


def _missing_azure_identity_error() -> dbt_common.exceptions.DbtRuntimeError:
    return dbt_common.exceptions.DbtRuntimeError(
        "Azure authentication requires the optional dependency 'azure-identity'. "
        "Install it with `pip install azure-identity` or use a non-Azure "
        "authentication mode."
    )


def _get_mssql_python() -> MssqlPythonModuleProtocol:
    global _MSSQL_PYTHON_MODULE, _MSSQL_PYTHON_IMPORT_ERROR

    if _MSSQL_PYTHON_MODULE is not None:
        return _MSSQL_PYTHON_MODULE

    if _MSSQL_PYTHON_IMPORT_ERROR is not None:
        raise _missing_mssql_python_error() from _MSSQL_PYTHON_IMPORT_ERROR

    try:
        import mssql_python as imported_mssql_python  # type: ignore[import]
    except ModuleNotFoundError as exc:
        _MSSQL_PYTHON_IMPORT_ERROR = exc
        raise _missing_mssql_python_error() from exc

    _MSSQL_PYTHON_MODULE = cast(MssqlPythonModuleProtocol, imported_mssql_python)
    return _MSSQL_PYTHON_MODULE


def _normalize_authentication(authentication: Optional[str]) -> str:
    if authentication is None:
        return "sql"

    normalized = authentication.strip().lower()
    if normalized == "activedirectorymsi":
        return "msi"
    return normalized


def _uses_pyodbc_token_authentication(credentials: SQLServerCredentials) -> bool:
    authentication = _normalize_authentication(credentials.authentication)
    return authentication in AZURE_AUTH_FUNCTIONS or authentication == "activedirectoryaccesstoken"


def _is_mssql_python_backend(credentials: SQLServerCredentials) -> bool:
    return credentials.backend == SQLServerBackend.mssql_python


def _validate_connection_requirements(credentials: SQLServerCredentials) -> None:
    for name in ("host", "database", "schema"):
        value = getattr(credentials, name)
        if value is None or not str(value).strip():
            raise dbt_common.exceptions.DbtRuntimeError(
                f"The `{name}` profile field is required for SQL Server connections."
            )

    if credentials.windows_login:
        normalized = _normalize_mssql_python_authentication(credentials.authentication)
        if normalized is not None and _auth_key(normalized).startswith("activedirectory"):
            raise dbt_common.exceptions.DbtRuntimeError(
                "windows_login/trusted_connection cannot be combined with ActiveDirectory "
                "authentication. Remove `authentication` or disable `windows_login`."
            )
    elif credentials.authentication is None or not str(credentials.authentication).strip():
        raise dbt_common.exceptions.DbtRuntimeError(
            "The `authentication` profile field is required for SQL Server connections."
        )

    if credentials.encrypt is None:
        raise dbt_common.exceptions.DbtRuntimeError(
            "The `encrypt` profile field is required for SQL Server connections."
        )
    if credentials.trust_cert is None:
        raise dbt_common.exceptions.DbtRuntimeError(
            "The `trust_cert` profile field is required for SQL Server connections."
        )


def _validate_pyodbc_requirements(credentials: SQLServerCredentials) -> None:
    if credentials.driver is None or not credentials.driver.strip():
        raise dbt_common.exceptions.DbtRuntimeError(
            "The pyodbc backend requires a SQL Server ODBC driver name "
            "in the `driver` profile field."
        )


def _validate_mssql_python_requirements(credentials: SQLServerCredentials) -> None:
    authentication = _normalize_mssql_python_authentication(credentials.authentication)
    authentication_key = _auth_key(authentication)

    if authentication_key in MSSQL_PYTHON_UNSUPPORTED_AUTHENTICATIONS:
        raise dbt_common.exceptions.DbtRuntimeError(
            "Authentication '{}' is currently only supported by the pyodbc backend "
            "in this adapter. "
            "Use `backend: pyodbc` or use a connection-string-supported "
            "authentication mode such as "
            "`sql`, `ActiveDirectoryPassword`, `ActiveDirectoryInteractive`, "
            "`ActiveDirectoryIntegrated`, "
            "`ActiveDirectoryMSI`, `ActiveDirectoryDeviceCode`, "
            "or `ActiveDirectoryDefault`.".format(authentication)
        )


def convert_bytes_to_mswindows_byte_string(value: bytes) -> bytes:
    """
    Convert bytes to a Microsoft windows byte string.

    Parameters
    ----------
    value : bytes
        The bytes.

    Returns
    -------
    out : bytes
        The Microsoft byte string.
    """
    encoded_bytes = bytes(chain.from_iterable(zip(value, repeat(0))))
    return struct.pack("<i", len(encoded_bytes)) + encoded_bytes


def convert_access_token_to_mswindows_byte_string(token: AccessTokenProtocol) -> bytes:
    """
    Convert an access token to a Microsoft windows byte string.

    Parameters
    ----------
    token : AccessTokenProtocol
        The token.

    Returns
    -------
    out : bytes
        The Microsoft byte string.
    """
    value = bytes(token.token, "UTF-8")
    return convert_bytes_to_mswindows_byte_string(value)


def get_cli_access_token(
    credentials: SQLServerCredentials, scope: Optional[str] = AZURE_CREDENTIAL_SCOPE
) -> AccessTokenProtocol:
    """
    Get an Azure access token using the CLI credentials

    First login with:

    ```bash
    az login
    ```

    Parameters
    ----------
    credentials: SQLServerCredentials
        The credentials.

    Returns
    -------
    out : AccessToken
        Access token.
    """
    _ = credentials
    azure_identity = _get_azure_identity_module()
    token = azure_identity.AzureCliCredential().get_token(
        scope, timeout=getattr(credentials, "login_timeout", None)
    )
    return token


def get_auto_access_token(
    credentials: SQLServerCredentials, scope: Optional[str] = AZURE_CREDENTIAL_SCOPE
) -> AccessTokenProtocol:
    """
    Get an Azure access token automatically through azure-identity

    Parameters
    -----------
    credentials: SQLServerCredentials
        Credentials.

    Returns
    -------
    out : AccessToken
        The access token.
    """
    azure_identity = _get_azure_identity_module()
    token = azure_identity.DefaultAzureCredential().get_token(
        scope, timeout=getattr(credentials, "login_timeout", None)
    )
    return token


def get_environment_access_token(
    credentials: SQLServerCredentials, scope: Optional[str] = AZURE_CREDENTIAL_SCOPE
) -> AccessTokenProtocol:
    """
    Get an Azure access token by reading environment variables

    Parameters
    -----------
    credentials: SQLServerCredentials
        Credentials.

    Returns
    -------
    out : AccessToken
        The access token.
    """
    azure_identity = _get_azure_identity_module()
    token = azure_identity.EnvironmentCredential().get_token(
        scope, timeout=getattr(credentials, "login_timeout", None)
    )
    return token


def get_msi_access_token(
    credentials: SQLServerCredentials, scope: Optional[str] = AZURE_CREDENTIAL_SCOPE
) -> AccessTokenProtocol:
    """
    Get an Azure access token from the system's managed identity

    Parameters
    -----------
    credentials: SQLServerCredentials
        Credentials.

    Returns
    -------
    out : AccessToken
        The access token.
    """
    _ = credentials
    azure_identity = _get_azure_identity_module()
    token = azure_identity.ManagedIdentityCredential().get_token(scope)
    return token


def get_sp_access_token(
    credentials: SQLServerCredentials, scope: Optional[str] = AZURE_CREDENTIAL_SCOPE
) -> AccessTokenProtocol:
    """
    Get an Azure access token using the SP credentials.

    Parameters
    ----------
    credentials : SQLServerCredentials
        Credentials.

    Returns
    -------
    out : AccessToken
        The access token.
    """
    _ = scope
    azure_identity = _get_azure_identity_module()
    token = azure_identity.ClientSecretCredential(
        str(credentials.tenant_id),
        str(credentials.client_id),
        str(credentials.client_secret),
    ).get_token(AZURE_CREDENTIAL_SCOPE)
    return token


AZURE_AUTH_FUNCTIONS: Mapping[str, AZURE_AUTH_FUNCTION_TYPE] = {
    "cli": get_cli_access_token,
    "auto": get_auto_access_token,
    "environment": get_environment_access_token,
    "serviceprincipal": get_sp_access_token,
    "msi": get_msi_access_token,
}


def get_pyodbc_attrs_before_credentials(credentials: SQLServerCredentials) -> Dict:
    """
    Get the pyodbc attributes for authentication.

    Parameters
    ----------
    credentials : SQLServerCredentials
        Credentials.

    Returns
    -------
    Dict
        The pyodbc attributes for authentication.
    """
    global _TOKEN
    sql_copt_ss_access_token = 1256  # ODBC constant for access token
    MAX_REMAINING_TIME = 300

    authentication = _normalize_authentication(credentials.authentication)

    if authentication in AZURE_AUTH_FUNCTIONS:
        if not _TOKEN or (_TOKEN.expires_on - time.time() < MAX_REMAINING_TIME):
            _TOKEN = AZURE_AUTH_FUNCTIONS[authentication](credentials, AZURE_CREDENTIAL_SCOPE)
        assert _TOKEN is not None
        token_bytes = convert_access_token_to_mswindows_byte_string(_TOKEN)
        return {sql_copt_ss_access_token: token_bytes}

    if authentication == "activedirectoryaccesstoken":
        if credentials.access_token is None or credentials.access_token_expires_on is None:
            raise ValueError(
                (
                    "Access token and access token expiry are "
                    "required for ActiveDirectoryAccessToken authentication."
                )
            )
        _TOKEN = _get_azure_access_token_class()(
            token=credentials.access_token,
            expires_on=int(
                time.time() + 4500.0
                if credentials.access_token_expires_on == 0
                else credentials.access_token_expires_on
            ),
        )
        assert _TOKEN is not None
        return {sql_copt_ss_access_token: convert_access_token_to_mswindows_byte_string(_TOKEN)}

    return {}


def bool_to_connection_string_arg(key: str, value: bool) -> str:
    """
    Convert a boolean to a connection string argument.

    Parameters
    ----------
    key : str
        The key to use in the connection string.
    value : bool
        The boolean to convert.

    Returns
    -------
    out : str
        The connection string argument.
    """
    return f"{key}={'Yes' if value else 'No'}"


def _escape_connection_string_value(value: Optional[str]) -> str:
    text = "" if value is None else str(value)
    if text.startswith(" ") or text.endswith(" ") or any(ch in text for ch in ";{}"):
        return "{" + text.replace("}", "}}") + "}"
    return text


def byte_array_to_datetime(value: bytes) -> dt.datetime:
    """
    Converts a DATETIMEOFFSET byte array to a timezone-aware datetime object

    Parameters
    ----------
    value : buffer
        A binary value conforming to SQL_SS_TIMESTAMPOFFSET_STRUCT

    Returns
    -------
    out : datetime

    Source
    ------
    SQL_SS_TIMESTAMPOFFSET datatype and SQL_SS_TIMESTAMPOFFSET_STRUCT layout:
    https://learn.microsoft.com/sql/relational-databases/native-client-odbc-date-time/data-type-support-for-odbc-date-and-time-improvements
    """
    # unpack 20 bytes of data into a tuple of 9 values
    tup = struct.unpack("<6hI2h", value)

    # construct a datetime object
    return dt.datetime(
        year=tup[0],
        month=tup[1],
        day=tup[2],
        hour=tup[3],
        minute=tup[4],
        second=tup[5],
        microsecond=tup[6] // 1000,  # https://bugs.python.org/issue15443
        tzinfo=dt.timezone(dt.timedelta(hours=tup[7], minutes=tup[8])),
    )


def _build_server_arg(credentials: SQLServerCredentials) -> str:
    host = credentials.host or ""
    if "\\" in host:
        # If there is a backslash \ in the host name, the host is a
        # SQL Server named instance. In this case then port number has to be omitted.
        return host
    return f"{host},{credentials.port}"


def _format_connection_string_value(value: Optional[str], mssql_python_backend: bool) -> str:
    if mssql_python_backend:
        return _escape_connection_string_value(value)
    return "{" + ("" if value is None else value) + "}"


def _build_common_connection_string_parts(
    credentials: SQLServerCredentials,
    mssql_python_backend: bool,
) -> list[str]:
    con_str = [f"SERVER={_build_server_arg(credentials)}"]
    con_str.append(f"Database={credentials.database}")

    authentication = credentials.authentication or ""
    if mssql_python_backend:
        authentication = _normalize_mssql_python_authentication(authentication) or ""

    if not authentication.strip() and not credentials.windows_login:
        raise dbt_common.exceptions.DbtRuntimeError(
            "The `authentication` profile field is required for SQL Server connections."
        )

    if "ActiveDirectory" in authentication and authentication != "ActiveDirectoryAccessToken":
        con_str.append(f"Authentication={authentication}")

        if authentication == "ActiveDirectoryPassword":
            con_str.append(
                f"UID={_format_connection_string_value(credentials.UID, mssql_python_backend)}"
            )
            con_str.append(
                f"PWD={_format_connection_string_value(credentials.PWD, mssql_python_backend)}"
            )
        elif authentication == "ActiveDirectoryServicePrincipal":
            con_str.append(
                "UID="
                + _format_connection_string_value(
                    credentials.client_id,
                    mssql_python_backend,
                )
            )
            con_str.append(
                "PWD="
                + _format_connection_string_value(
                    credentials.client_secret,
                    mssql_python_backend,
                )
            )
        elif authentication == "ActiveDirectoryInteractive":
            con_str.append(
                "UID=%s"
                % _format_connection_string_value(
                    credentials.UID,
                    mssql_python_backend,
                )
            )
        elif authentication == "ActiveDirectoryMSI":
            if credentials.PWD:
                raise dbt_common.exceptions.DbtRuntimeError(
                    "password is not valid with ActiveDirectoryMSI for the mssql-python backend."
                )
            if credentials.UID:
                con_str.append(
                    f"UID={_format_connection_string_value(credentials.UID, mssql_python_backend)}"
                )
        elif authentication == "ActiveDirectoryIntegrated":
            if credentials.PWD:
                raise dbt_common.exceptions.DbtRuntimeError(
                    "password is not valid with ActiveDirectoryIntegrated"
                    " for the mssql-python backend."
                )

    elif credentials.windows_login:
        if mssql_python_backend and (credentials.UID or credentials.PWD):
            raise dbt_common.exceptions.DbtRuntimeError(
                "user/password are not valid with windows_login/trusted_connection "
                "for the mssql-python backend."
            )
        con_str.append("Trusted_Connection=yes")
    elif authentication == "sql":
        con_str.append(
            f"UID={_format_connection_string_value(credentials.UID, mssql_python_backend)}"
        )
        con_str.append(
            f"PWD={_format_connection_string_value(credentials.PWD, mssql_python_backend)}"
        )

    if credentials.encrypt is None:
        raise dbt_common.exceptions.DbtRuntimeError(
            "The `encrypt` profile field is required for SQL Server connections."
        )
    if credentials.trust_cert is None:
        raise dbt_common.exceptions.DbtRuntimeError(
            "The `trust_cert` profile field is required for SQL Server connections."
        )

    con_str.append(bool_to_connection_string_arg("encrypt", credentials.encrypt))
    con_str.append(bool_to_connection_string_arg("TrustServerCertificate", credentials.trust_cert))

    if not mssql_python_backend:
        # Reserved keyword 'app' is controlled by the driver and cannot be specified by the user.
        application_name = f"dbt-{credentials.type}/{__version__.version}"
        con_str.append(f"APP={application_name}")

    return con_str


def _build_pyodbc_connection_string(credentials: SQLServerCredentials) -> str:
    con_str = [f"DRIVER={{{credentials.driver}}}"]
    con_str.extend(_build_common_connection_string_parts(credentials, mssql_python_backend=False))
    con_str.append("Pooling=true")

    if credentials.trace_flag:
        con_str.append("SQL_ATTR_TRACE=SQL_OPT_TRACE_ON")
    else:
        con_str.append("SQL_ATTR_TRACE=SQL_OPT_TRACE_OFF")

    con_str.append("ConnectRetryCount=3")
    con_str.append("ConnectRetryInterval=10")

    return ";".join(con_str)


def _build_mssql_python_connection_string(credentials: SQLServerCredentials) -> str:
    con_str = _build_common_connection_string_parts(credentials, mssql_python_backend=True)
    con_str.append("ConnectRetryCount=3")
    con_str.append("ConnectRetryInterval=10")
    return ";".join(con_str)


def _sanitize_connection_string_for_logging(connection_string: str) -> str:
    parts = connection_string.split(";")
    sanitized = []
    for part in parts:
        if part.lower().startswith("pwd="):
            sanitized.append("PWD=***")
        else:
            sanitized.append(part)
    return ";".join(sanitized)


def _connect_mssql_python(
    mssql_python: MssqlPythonModuleProtocol,
    credentials: SQLServerCredentials,
    connection_string: str,
) -> Any:
    handle = mssql_python.connect(
        connection_string,
        autocommit=True,
        timeout=credentials.login_timeout,
    )
    try:
        handle.timeout = credentials.query_timeout
    except Exception:
        logger.debug(
            "The mssql-python connection object does not expose a mutable `timeout` "
            "attribute; continuing without setting query timeout on the handle."
        )
    logger.debug(f"Connected to db: {credentials.database}")
    return handle


def _connect_pyodbc(
    pyodbc: PyodbcModuleProtocol,
    credentials: SQLServerCredentials,
    connection_string: str,
) -> Any:
    pyodbc.pooling = True
    attrs_before = get_pyodbc_attrs_before_credentials(credentials)

    handle = pyodbc.connect(
        connection_string,
        attrs_before=attrs_before,
        autocommit=True,
        timeout=credentials.login_timeout,
    )
    handle.timeout = credentials.query_timeout
    logger.debug(f"Connected to db: {credentials.database}")
    return handle


def _get_backend_exceptions(
    credentials: SQLServerCredentials,
) -> Tuple[Type[Exception], ...]:
    if _is_mssql_python_backend(credentials):
        mssql_python = _get_mssql_python()

        retryable_exceptions = [
            getattr(mssql_python, "InternalError", Exception),
            getattr(mssql_python, "OperationalError", Exception),
        ]

        if _uses_pyodbc_token_authentication(credentials):
            retryable_exceptions.append(getattr(mssql_python, "InterfaceError", Exception))

        return tuple(retryable_exceptions)

    pyodbc = _get_pyodbc()

    retryable_exceptions = [
        pyodbc.InternalError,
        pyodbc.OperationalError,
    ]

    if _uses_pyodbc_token_authentication(credentials):
        retryable_exceptions.append(pyodbc.InterfaceError)

    return tuple(retryable_exceptions)


def _is_pyodbc_handle(handle: Any) -> bool:
    return hasattr(handle, "add_output_converter")


class SQLServerConnectionManager(SQLConnectionManager):
    TYPE = "sqlserver"

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except Exception as e:
            credentials = self.get_thread_connection().credentials

            if not _is_mssql_python_backend(credentials):
                pyodbc = _PYODBC_MODULE
                if pyodbc is not None and isinstance(e, getattr(pyodbc, "DatabaseError", tuple())):
                    logger.debug("Database error: {}".format(str(e)))

                    try:
                        self.release()
                    except Exception:
                        logger.debug("Failed to release connection!")

                    raise dbt_common.exceptions.DbtDatabaseError(str(e).strip()) from e

            if _is_mssql_python_backend(credentials):
                mssql_python = _MSSQL_PYTHON_MODULE
                if mssql_python is not None and isinstance(
                    e, getattr(mssql_python, "DatabaseError", tuple())
                ):
                    logger.debug("Database error: {}".format(str(e)))

                    try:
                        self.release()
                    except Exception:
                        logger.debug("Failed to release connection!")

                    raise dbt_common.exceptions.DbtDatabaseError(str(e).strip()) from e

            logger.debug(f"Error running SQL: {sql}")
            logger.debug("Rolling back transaction.")
            self.release()
            if isinstance(e, dbt_common.exceptions.DbtRuntimeError):
                raise

            raise dbt_common.exceptions.DbtRuntimeError(e)

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = cls.get_credentials(connection.credentials)

        _validate_connection_requirements(credentials)

        if _is_mssql_python_backend(credentials):
            mssql_python = _get_mssql_python()
            _validate_mssql_python_requirements(credentials)
            con_str_concat = _build_mssql_python_connection_string(credentials)

            def connect() -> Any:
                logger.debug(
                    "Using connection string: %s"
                    % _sanitize_connection_string_for_logging(con_str_concat)
                )
                return _connect_mssql_python(mssql_python, credentials, con_str_concat)

        else:
            pyodbc = _get_pyodbc()
            _validate_pyodbc_requirements(credentials)
            con_str_concat = _build_pyodbc_connection_string(credentials)

            def connect() -> Any:
                logger.debug(
                    "Using connection string: %s"
                    % _sanitize_connection_string_for_logging(con_str_concat)
                )
                return _connect_pyodbc(pyodbc, credentials, con_str_concat)

        retryable_exceptions = _get_backend_exceptions(credentials)

        conn = cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=credentials.retries,
            retryable_exceptions=retryable_exceptions,
        )

        return conn

    def cancel(self, connection: Connection):
        logger.debug("Cancel query")

    def add_begin_query(self):
        pass

    def add_commit_query(self):
        pass

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
        retryable_exceptions: Tuple[Type[Exception], ...] = (),
        retry_limit: int = 2,
    ) -> Tuple[Connection, Any]:
        """
        Retry function encapsulated here to avoid commitment to some
        user-facing interface. Right now, Redshift commits to a 1 second
        retry timeout so this serves as a default.
        """

        def _execute_query_with_retry(
            cursor: Any,
            sql: str,
            bindings: Optional[Any],
            retryable_exceptions: Tuple[Type[Exception], ...],
            retry_limit: int,
            attempt: int,
        ):
            """
            A success sees the try exit cleanly and avoid any recursive
            retries. Failure begins a sleep and retry routine.
            """
            try:
                if bindings is None:
                    cursor.execute(sql)
                else:
                    bindings = [
                        binding if not isinstance(binding, dt.datetime) else binding.isoformat()
                        for binding in bindings
                    ]
                    cursor.execute(sql, bindings)
            except retryable_exceptions as e:
                if attempt >= retry_limit:
                    raise e

                fire_event(
                    AdapterEventDebug(
                        message=(
                            f"Got a retryable error {type(e)}. {retry_limit - attempt} "
                            "retries left. Retrying in 1 second.\n"
                            f"Error:\n{e}"
                        )
                    )
                )
                time.sleep(1)

                return _execute_query_with_retry(
                    cursor=cursor,
                    sql=sql,
                    bindings=bindings,
                    retryable_exceptions=retryable_exceptions,
                    retry_limit=retry_limit,
                    attempt=attempt + 1,
                )

        connection = self.get_thread_connection()

        if auto_begin and connection.transaction_open is False:
            self.begin()

        fire_event(
            ConnectionUsed(
                conn_type=self.TYPE,
                conn_name=cast_to_str(connection.name),
                node_info=get_node_info(),
            )
        )

        with self.exception_handler(sql):
            if abridge_sql_log:
                log_sql = "{}...".format(sql[:512])
            else:
                log_sql = sql

            fire_event(
                SQLQuery(
                    conn_name=cast_to_str(connection.name),
                    sql=log_sql,
                    node_info=get_node_info(),
                )
            )

            pre = time.time()

            cursor = connection.handle.cursor()
            credentials = self.get_credentials(connection.credentials)

            _execute_query_with_retry(
                cursor=cursor,
                sql=sql,
                bindings=bindings,
                retryable_exceptions=retryable_exceptions,
                retry_limit=credentials.retries if credentials.retries > 3 else retry_limit,
                attempt=1,
            )

            if _is_pyodbc_handle(connection.handle):
                connection.handle.add_output_converter(-155, byte_array_to_datetime)

            fire_event(
                SQLQueryStatus(
                    status=str(self.get_response(cursor)),
                    elapsed=round((time.time() - pre)),
                    node_info=get_node_info(),
                )
            )

            return connection, cursor

    @classmethod
    def get_credentials(cls, credentials: SQLServerCredentials) -> SQLServerCredentials:
        return credentials

    @classmethod
    def get_response(cls, cursor: Any) -> AdapterResponse:
        message = "OK"
        rows = cursor.rowcount
        return AdapterResponse(
            _message=message,
            rows_affected=rows,
        )

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[int, str]) -> str:
        data_type = str(type_code)[
            str(type_code).index("'") + 1 : str(type_code).rindex("'")  # noqa: E203
        ]
        return datatypes[data_type]

    def execute(
        self,
        sql: str,
        auto_begin: bool = True,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, agate.Table]:
        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin)
        try:
            response = self.get_response(cursor)
            if fetch:
                while cursor.description is None:
                    if not cursor.nextset():
                        break
                table = self.get_result_from_cursor(cursor, limit)
            else:
                table = empty_table()
            while cursor.nextset():
                pass
            return response, table
        finally:
            cursor.close()
