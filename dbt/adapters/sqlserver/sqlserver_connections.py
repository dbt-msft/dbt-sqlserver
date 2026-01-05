import datetime as dt
import struct
import time
from contextlib import contextmanager
from itertools import chain, repeat
from typing import Any, Callable, Dict, Mapping, Optional, Tuple, Type, Union

import agate
import dbt_common.exceptions
import pyodbc
from azure.core.credentials import AccessToken
from azure.identity import (
    AzureCliCredential,
    ClientSecretCredential,
    DefaultAzureCredential,
    EnvironmentCredential,
    ManagedIdentityCredential,
)
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
        return {sql_copt_ss_access_token: convert_access_token_to_mswindows_byte_string(_TOKEN)}

    if credentials.authentication.lower() == "activedirectoryaccesstoken":
        if credentials.access_token is None or credentials.access_token_expires_on is None:
            raise ValueError(
                "Access token and access token expiry are required for ActiveDirectoryAccessToken authentication."
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
    return f'{key}={"Yes" if value else "No"}'


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


class SQLServerConnectionManager(SQLConnectionManager):
    TYPE = "sqlserver"

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except pyodbc.DatabaseError as e:
            logger.debug("Database error: {}".format(str(e)))

            try:
                # attempt to release the connection
                self.release()
            except pyodbc.Error:
                logger.debug("Failed to release connection!")

            raise dbt_common.exceptions.DbtDatabaseError(str(e).strip()) from e

        except Exception as e:
            logger.debug(f"Error running SQL: {sql}")
            logger.debug("Rolling back transaction.")
            self.release()
            if isinstance(e, dbt_common.exceptions.DbtRuntimeError):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt_common.exceptions.DbtRuntimeError(e)

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = cls.get_credentials(connection.credentials)

        con_str = [f"DRIVER={{{credentials.driver}}}"]

        if "\\" in credentials.host:
            # If there is a backslash \ in the host name, the host is a
            # SQL Server named instance. In this case then port number has to be omitted.
            con_str.append(f"SERVER={credentials.host}")
        else:
            con_str.append(f"SERVER={credentials.host},{credentials.port}")

        con_str.append(f"Database={credentials.database}")
        con_str.append("Pooling=true")

        # Enabling trace flag
        if credentials.trace_flag:
            con_str.append("SQL_ATTR_TRACE=SQL_OPT_TRACE_ON")
        else:
            con_str.append("SQL_ATTR_TRACE=SQL_OPT_TRACE_OFF")

        assert credentials.authentication is not None

        # Access token authentication does not additional connection string parameters. The access token
        # is passed in the pyodbc attributes.
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

        # https://docs.microsoft.com/en-us/sql/relational-databases/native-client/features/using-encryption-without-validation?view=sql-server-ver15
        assert credentials.encrypt is not None
        assert credentials.trust_cert is not None

        con_str.append(bool_to_connection_string_arg("encrypt", credentials.encrypt))
        con_str.append(
            bool_to_connection_string_arg("TrustServerCertificate", credentials.trust_cert)
        )

        plugin_version = __version__.version
        application_name = f"dbt-{credentials.type}/{plugin_version}"
        con_str.append(f"APP={application_name}")

        try:
            con_str.append("ConnectRetryCount=3")
            con_str.append("ConnectRetryInterval=10")

        except Exception as e:
            logger.debug(
                "Retry count should be a integer value. Skipping retries in the connection string.",
                str(e),
            )

        con_str_concat = ";".join(con_str)

        index = []
        for i, elem in enumerate(con_str):
            if "pwd=" in elem.lower():
                index.append(i)

        if len(index) != 0:
            con_str[index[0]] = "PWD=***"

        con_str_display = ";".join(con_str)

        retryable_exceptions = [  # https://github.com/mkleehammer/pyodbc/wiki/Exceptions
            pyodbc.InternalError,  # not used according to docs, but defined in PEP-249
            pyodbc.OperationalError,
        ]

        if credentials.authentication.lower() in AZURE_AUTH_FUNCTIONS:
            # Temporary login/token errors fall into this category when using AAD
            retryable_exceptions.append(pyodbc.InterfaceError)

        def connect():
            logger.debug(f"Using connection string: {con_str_display}")
            pyodbc.pooling = True

            # pyodbc attributes includes the access token provided by the user if required.
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
        # return self.add_query('BEGIN TRANSACTION', auto_begin=False)
        pass

    def add_commit_query(self):
        # return self.add_query('COMMIT TRANSACTION', auto_begin=False)
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
                # pyodbc does not handle a None type binding!
                if bindings is None:
                    cursor.execute(sql)
                else:
                    bindings = [
                        binding if not isinstance(binding, dt.datetime) else binding.isoformat()
                        for binding in bindings
                    ]
                    cursor.execute(sql, bindings)
            except retryable_exceptions as e:
                # Cease retries and fail when limit is hit.
                if attempt >= retry_limit:
                    raise e

                fire_event(
                    AdapterEventDebug(
                        message=f"Got a retryable error {type(e)}. {retry_limit-attempt} retries left. Retrying in 1 second.\nError:\n{e}"
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
                    conn_name=cast_to_str(connection.name), sql=log_sql, node_info=get_node_info()
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

            # convert DATETIMEOFFSET binary structures to datetime ojbects
            # https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
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
        data_type = str(type_code)[str(type_code).index("'") + 1 : str(type_code).rindex("'")]
        return datatypes[data_type]

    def execute(
        self, sql: str, auto_begin: bool = True, fetch: bool = False, limit: Optional[int] = None
    ) -> Tuple[AdapterResponse, agate.Table]:
        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin)
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
