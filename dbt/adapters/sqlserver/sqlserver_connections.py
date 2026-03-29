import datetime as dt
import struct
import time
from contextlib import contextmanager
from dataclasses import dataclass
from itertools import chain, repeat
from typing import Any, Callable, Dict, Mapping, Optional, Tuple, Type, Union

import agate
import dbt_common.exceptions
import pyodbc

try:
    import mssql_python as MSSQL_PYTHON
except ModuleNotFoundError as exc:
    MSSQL_PYTHON = None
    _MSSQL_PYTHON_IMPORT_ERROR: Optional[ModuleNotFoundError] = exc
else:
    _MSSQL_PYTHON_IMPORT_ERROR = None

try:
    from azure.core.credentials import AccessToken
except ModuleNotFoundError:

    @dataclass
    class AccessToken:  # type: ignore[no-redef]
        token: str
        expires_on: int


try:
    from azure.identity import (
        AzureCliCredential,
        ClientSecretCredential,
        DefaultAzureCredential,
        EnvironmentCredential,
        ManagedIdentityCredential,
    )

    _AZURE_IDENTITY_IMPORT_ERROR = None
except ModuleNotFoundError as exc:
    AzureCliCredential = None
    ClientSecretCredential = None
    DefaultAzureCredential = None
    EnvironmentCredential = None
    ManagedIdentityCredential = None
    _AZURE_IDENTITY_IMPORT_ERROR = exc

from dbt.adapters.contracts.connection import AdapterResponse, Connection, ConnectionState
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.events.types import AdapterEventDebug, ConnectionUsed, SQLQuery, SQLQueryStatus
from dbt.adapters.sql.connections import SQLConnectionManager
from dbt_common.clients.agate_helper import empty_table
from dbt_common.events.contextvars import get_node_info
from dbt_common.events.functions import fire_event
from dbt_common.utils.casting import cast_to_str

from dbt.adapters.sqlserver import __version__
from dbt.adapters.sqlserver.sqlserver_credentials import SQLServerCredentials

_TOKEN: Optional[AccessToken] = None
AZURE_CREDENTIAL_SCOPE = "https://database.windows.net//.default"
AZURE_AUTH_FUNCTION_TYPE = Callable[[SQLServerCredentials, Optional[str]], AccessToken]

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
    "auto",
    "environment",
    "serviceprincipal",
    "activedirectoryaccesstoken",
}


def _require_azure_identity(authentication: str) -> None:
    if _AZURE_IDENTITY_IMPORT_ERROR is not None:
        raise dbt_common.exceptions.DbtRuntimeError(
            (
                "Azure authentication '{}' requires the optional "
                "dependency 'azure-identity'. Install it with `pip install "
                "azure-identity` or use a non-Azure authentication mode."
            ).format(authentication)
        ) from _AZURE_IDENTITY_IMPORT_ERROR


def _require_mssql_python() -> None:
    if _MSSQL_PYTHON_IMPORT_ERROR is not None:
        raise dbt_common.exceptions.DbtRuntimeError(
            "The `mssql-python` backend was requested, but the optional dependency "
            "`mssql-python` is not installed. Install it with `pip install mssql-python` "
            "or disable `use_mssql_python` in the profile."
        ) from _MSSQL_PYTHON_IMPORT_ERROR


def _requires_pyodbc_backend(credentials: SQLServerCredentials) -> bool:
    authentication = str(credentials.authentication or "sql").lower().strip()
    return authentication in AZURE_AUTH_FUNCTIONS or authentication == "activedirectoryaccesstoken"


def _use_mssql_python_backend(credentials: SQLServerCredentials) -> bool:
    return bool(getattr(credentials, "use_mssql_python", False))


def _validate_pyodbc_requirements(credentials: SQLServerCredentials) -> None:
    if not credentials.driver:
        raise dbt_common.exceptions.DbtRuntimeError(
            "The pyodbc backend requires a SQL Server ODBC driver name "
            "in the `driver` profile field."
        )


def _validate_mssql_python_requirements(credentials: SQLServerCredentials) -> None:
    authentication = str(credentials.authentication or "sql").strip()
    authentication_lower = authentication.lower()

    if authentication_lower in MSSQL_PYTHON_UNSUPPORTED_AUTHENTICATIONS:
        raise dbt_common.exceptions.DbtRuntimeError(
            "Authentication '{}' is currently only supported by the pyodbc backend "
            "in this adapter. "
            "Disable `use_mssql_python` or use a connection-string-supported "
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


def convert_access_token_to_mswindows_byte_string(token: AccessToken) -> bytes:
    """
    Convert an access token to a Microsoft windows byte string.

    Parameters
    ----------
    token : AccessToken
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
) -> AccessToken:
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
    _require_azure_identity("cli")
    token = AzureCliCredential().get_token(
        scope, timeout=getattr(credentials, "login_timeout", None)
    )
    return token


def get_auto_access_token(
    credentials: SQLServerCredentials, scope: Optional[str] = AZURE_CREDENTIAL_SCOPE
) -> AccessToken:
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
    _require_azure_identity("auto")
    token = DefaultAzureCredential().get_token(
        scope, timeout=getattr(credentials, "login_timeout", None)
    )
    return token


def get_environment_access_token(
    credentials: SQLServerCredentials, scope: Optional[str] = AZURE_CREDENTIAL_SCOPE
) -> AccessToken:
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
    _require_azure_identity("environment")
    token = EnvironmentCredential().get_token(
        scope, timeout=getattr(credentials, "login_timeout", None)
    )
    return token


def get_msi_access_token(
    credentials: SQLServerCredentials, scope: Optional[str] = AZURE_CREDENTIAL_SCOPE
) -> AccessToken:
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
    _require_azure_identity("msi")
    token = ManagedIdentityCredential().get_token(scope)
    return token


def get_sp_access_token(
    credentials: SQLServerCredentials, scope: Optional[str] = AZURE_CREDENTIAL_SCOPE
) -> AccessToken:
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
    _require_azure_identity("serviceprincipal")
    token = ClientSecretCredential(
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

    if credentials.authentication.lower() in AZURE_AUTH_FUNCTIONS:
        if not _TOKEN or (_TOKEN.expires_on - time.time() < MAX_REMAINING_TIME):
            _TOKEN = AZURE_AUTH_FUNCTIONS[credentials.authentication.lower()](
                credentials, AZURE_CREDENTIAL_SCOPE
            )
        token_bytes = convert_access_token_to_mswindows_byte_string(_TOKEN)
        return {sql_copt_ss_access_token: token_bytes}

    if credentials.authentication.lower() == "activedirectoryaccesstoken":
        if credentials.access_token is None or credentials.access_token_expires_on is None:
            raise ValueError(
                (
                    "Access token and access token expiry are "
                    "required for ActiveDirectoryAccessToken authentication."
                )
            )
        _TOKEN = AccessToken(
            token=credentials.access_token,
            expires_on=int(
                time.time() + 4500.0
                if credentials.access_token_expires_on == 0
                else credentials.access_token_expires_on
            ),
        )
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
    if "\\" in credentials.host:
        # If there is a backslash \ in the host name, the host is a
        # SQL Server named instance. In this case then port number has to be omitted.
        return credentials.host
    return f"{credentials.host},{credentials.port}"


def _build_common_connection_string_parts(
    credentials: SQLServerCredentials,
) -> list[str]:
    con_str = [f"SERVER={_build_server_arg(credentials)}"]
    con_str.append(f"Database={credentials.database}")

    assert credentials.authentication is not None

    if (
        "ActiveDirectory" in credentials.authentication
        and credentials.authentication != "ActiveDirectoryAccessToken"
    ):
        con_str.append(f"Authentication={credentials.authentication}")

        if credentials.authentication == "ActiveDirectoryPassword":
            con_str.append(f"UID={{{credentials.UID}}}")
            con_str.append(f"PWD={{{credentials.PWD}}}")
        if credentials.authentication == "ActiveDirectoryServicePrincipal":
            con_str.append(f"UID={{{credentials.client_id}}}")
            con_str.append(f"PWD={{{credentials.client_secret}}}")
        elif credentials.authentication == "ActiveDirectoryInteractive":
            con_str.append(f"UID={{{credentials.UID}}}")

    elif credentials.windows_login:
        con_str.append("trusted_connection=Yes")
    elif credentials.authentication == "sql":
        con_str.append(f"UID={{{credentials.UID}}}")
        con_str.append(f"PWD={{{credentials.PWD}}}")

    assert credentials.encrypt is not None
    assert credentials.trust_cert is not None

    con_str.append(bool_to_connection_string_arg("encrypt", credentials.encrypt))
    con_str.append(bool_to_connection_string_arg("TrustServerCertificate", credentials.trust_cert))

    return con_str


def _build_pyodbc_connection_string(credentials: SQLServerCredentials) -> str:
    con_str = [f"DRIVER={{{credentials.driver}}}"]
    con_str.extend(_build_common_connection_string_parts(credentials))
    con_str.append("Pooling=true")

    if credentials.trace_flag:
        con_str.append("SQL_ATTR_TRACE=SQL_OPT_TRACE_ON")
    else:
        con_str.append("SQL_ATTR_TRACE=SQL_OPT_TRACE_OFF")

    plugin_version = __version__.version
    application_name = f"dbt-{credentials.type}/{plugin_version}"
    con_str.append(f"APP={application_name}")

    try:
        con_str.append("ConnectRetryCount=3")
        con_str.append("ConnectRetryInterval=10")
    except Exception as e:
        logger.debug(
            (
                "Retry count should be a integer value. "
                "Skipping retries in the connection string."
            ),
            str(e),
        )

    return ";".join(con_str)


def _build_mssql_python_connection_string(credentials: SQLServerCredentials) -> str:
    con_str = _build_common_connection_string_parts(credentials)
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


def _get_backend_exceptions(
    credentials: SQLServerCredentials,
) -> Tuple[Type[Exception], ...]:
    if _use_mssql_python_backend(credentials):
        _require_mssql_python()
        retryable_exceptions = []
        retryable_exceptions.append(getattr(MSSQL_PYTHON, "InternalError", Exception))
        retryable_exceptions.append(getattr(MSSQL_PYTHON, "OperationalError", Exception))

        if _requires_pyodbc_backend(credentials):
            retryable_exceptions.append(getattr(MSSQL_PYTHON, "InterfaceError", Exception))

        return tuple(retryable_exceptions)

    retryable_exceptions = [  # https://github.com/mkleehammer/pyodbc/wiki/Exceptions
        pyodbc.InternalError,  # not used according to docs, but defined in PEP-249
        pyodbc.OperationalError,
    ]

    if credentials.authentication.lower() in AZURE_AUTH_FUNCTIONS:
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

        except pyodbc.DatabaseError as e:
            logger.debug("Database error: {}".format(str(e)))

            try:
                self.release()
            except pyodbc.Error:
                logger.debug("Failed to release connection!")

            raise dbt_common.exceptions.DbtDatabaseError(str(e).strip()) from e

        except Exception as e:
            if _use_mssql_python_backend(self.get_thread_connection().credentials):
                if MSSQL_PYTHON is not None and isinstance(
                    e, getattr(MSSQL_PYTHON, "DatabaseError", tuple())
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

        if _use_mssql_python_backend(credentials):
            _require_mssql_python()
            _validate_mssql_python_requirements(credentials)
            con_str_concat = _build_mssql_python_connection_string(credentials)
        else:
            _validate_pyodbc_requirements(credentials)
            con_str_concat = _build_pyodbc_connection_string(credentials)

        con_str_display = _sanitize_connection_string_for_logging(con_str_concat)
        retryable_exceptions = _get_backend_exceptions(credentials)

        def connect():
            logger.debug(f"Using connection string: {con_str_display}")

            if _use_mssql_python_backend(credentials):
                MSSQL_PYTHON.pooling(enabled=False)
                handle = MSSQL_PYTHON.connect(
                    con_str_concat,
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

            pyodbc.pooling = True
            attrs_before = get_pyodbc_attrs_before_credentials(credentials)

            handle = pyodbc.connect(
                con_str_concat,
                attrs_before=attrs_before,
                autocommit=True,
                timeout=credentials.login_timeout,
            )
            handle.timeout = credentials.query_timeout
            logger.debug(f"Connected to db: {credentials.database}")
            return handle

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
    def data_type_code_to_name(cls, type_code: Union[str, str]) -> str:
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
