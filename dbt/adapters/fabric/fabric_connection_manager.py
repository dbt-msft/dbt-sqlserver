import datetime as dt
import struct
import time
from contextlib import contextmanager
from itertools import chain, repeat
from typing import Any, Callable, Dict, Mapping, Optional, Tuple, Union

import agate
import dbt.exceptions
import pyodbc
from azure.core.credentials import AccessToken
from azure.identity import AzureCliCredential, DefaultAzureCredential, EnvironmentCredential
from dbt.adapters.sql import SQLConnectionManager
from dbt.clients.agate_helper import empty_table
from dbt.contracts.connection import AdapterResponse, Connection, ConnectionState
from dbt.events import AdapterLogger

from dbt.adapters.fabric import __version__
from dbt.adapters.fabric.fabric_credentials import FabricCredentials

AZURE_CREDENTIAL_SCOPE = "https://database.windows.net//.default"
_TOKEN: Optional[AccessToken] = None
AZURE_AUTH_FUNCTION_TYPE = Callable[[FabricCredentials], AccessToken]

logger = AdapterLogger("fabric")

# https://github.com/mkleehammer/pyodbc/wiki/Data-Types
datatypes = {
    # "str": "char",
    "str": "varchar",
    "uuid.UUID": "uniqueidentifier",
    "uuid": "uniqueidentifier",
    # "float": "real",
    # "float": "float",
    "float": "bigint",
    # "int": "smallint",
    # "int": "tinyint",
    "int": "int",
    "bytes": "varbinary",
    "bool": "bit",
    "datetime.date": "date",
    "datetime.datetime": "datetime2(6)",
    "datetime.time": "time",
    "decimal.Decimal": "decimal",
    # "decimal.Decimal": "numeric",
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


def get_cli_access_token(credentials: FabricCredentials) -> AccessToken:
    """
    Get an Azure access token using the CLI credentials

    First login with:

    ```bash
    az login
    ```

    Parameters
    ----------
    credentials: FabricConnectionManager
        The credentials.

    Returns
    -------
    out : AccessToken
        Access token.
    """
    _ = credentials
    token = AzureCliCredential().get_token(AZURE_CREDENTIAL_SCOPE)
    return token


def get_auto_access_token(credentials: FabricCredentials) -> AccessToken:
    """
    Get an Azure access token automatically through azure-identity

    Parameters
    -----------
    credentials: FabricCredentials
        Credentials.

    Returns
    -------
    out : AccessToken
        The access token.
    """
    token = DefaultAzureCredential().get_token(AZURE_CREDENTIAL_SCOPE)
    return token


def get_environment_access_token(credentials: FabricCredentials) -> AccessToken:
    """
    Get an Azure access token by reading environment variables

    Parameters
    -----------
    credentials: FabricCredentials
        Credentials.

    Returns
    -------
    out : AccessToken
        The access token.
    """
    token = EnvironmentCredential().get_token(AZURE_CREDENTIAL_SCOPE)
    return token


AZURE_AUTH_FUNCTIONS: Mapping[str, AZURE_AUTH_FUNCTION_TYPE] = {
    "cli": get_cli_access_token,
    "auto": get_auto_access_token,
    "environment": get_environment_access_token,
}


def get_pyodbc_attrs_before(credentials: FabricCredentials) -> Dict:
    """
    Get the pyodbc attrs before.

    Parameters
    ----------
    credentials : FabricCredentials
        Credentials.

    Returns
    -------
    out : Dict
        The pyodbc attrs before.

    Source
    ------
    Authentication for SQL server with an access token:
    https://docs.microsoft.com/en-us/sql/connect/odbc/using-azure-active-directory?view=sql-server-ver15#authenticating-with-an-access-token
    """
    global _TOKEN
    attrs_before: Dict
    MAX_REMAINING_TIME = 300

    authentication = str(credentials.authentication).lower()
    if authentication in AZURE_AUTH_FUNCTIONS:
        time_remaining = (_TOKEN.expires_on - time.time()) if _TOKEN else MAX_REMAINING_TIME

        if _TOKEN is None or (time_remaining < MAX_REMAINING_TIME):
            azure_auth_function = AZURE_AUTH_FUNCTIONS[authentication]
            _TOKEN = azure_auth_function(credentials)

        token_bytes = convert_access_token_to_mswindows_byte_string(_TOKEN)
        sql_copt_ss_access_token = 1256  # see source in docstring
        attrs_before = {sql_copt_ss_access_token: token_bytes}
    else:
        attrs_before = {}

    return attrs_before


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


class FabricConnectionManager(SQLConnectionManager):
    TYPE = "fabric"

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

            raise dbt.exceptions.DbtDatabaseError(str(e).strip()) from e

        except Exception as e:
            logger.debug(f"Error running SQL: {sql}")
            logger.debug("Rolling back transaction.")
            self.release()
            if isinstance(e, dbt.exceptions.DbtRuntimeError):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt.exceptions.DbtRuntimeError(e)

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
            con_str.append(f"SERVER={credentials.host}")

        con_str.append(f"Database={credentials.database}")

        assert credentials.authentication is not None

        if "ActiveDirectory" in credentials.authentication:
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
            raise pyodbc.DatabaseError("SQL Authentication is not supported by Microsoft Fabric")

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

            attrs_before = get_pyodbc_attrs_before(credentials)
            handle = pyodbc.connect(
                con_str_concat,
                attrs_before=attrs_before,
                autocommit=True,
                timeout=credentials.login_timeout,
            )
            handle.timeout = credentials.query_timeout
            logger.debug(f"Connected to db: {credentials.database}")
            return handle

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=credentials.retries,
            retryable_exceptions=retryable_exceptions,
        )

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
    ) -> Tuple[Connection, Any]:
        connection = self.get_thread_connection()

        if auto_begin and connection.transaction_open is False:
            self.begin()

        logger.debug('Using {} connection "{}".'.format(self.TYPE, connection.name))

        with self.exception_handler(sql):
            if abridge_sql_log:
                logger.debug("On {}: {}....".format(connection.name, sql[0:512]))
            else:
                logger.debug("On {}: {}".format(connection.name, sql))
            pre = time.time()

            cursor = connection.handle.cursor()

            # pyodbc does not handle a None type binding!
            if bindings is None:
                cursor.execute(sql)
            else:
                bindings = [
                    binding if not isinstance(binding, dt.datetime) else binding.isoformat()
                    for binding in bindings
                ]
                cursor.execute(sql, bindings)

            # convert DATETIMEOFFSET binary structures to datetime ojbects
            # https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
            connection.handle.add_output_converter(-155, byte_array_to_datetime)

            logger.debug(
                "SQL status: {} in {:0.2f} seconds".format(
                    self.get_response(cursor), (time.time() - pre)
                )
            )

            return connection, cursor

    @classmethod
    def get_credentials(cls, credentials: FabricCredentials) -> FabricCredentials:
        return credentials

    @classmethod
    def get_response(cls, cursor: Any) -> AdapterResponse:
        # message = str(cursor.statusmessage)
        message = "OK"
        rows = cursor.rowcount
        # status_message_parts = message.split() if message is not None else []
        # status_messsage_strings = [
        #    part
        #    for part in status_message_parts
        #    if not part.isdigit()
        # ]
        # code = ' '.join(status_messsage_strings)
        return AdapterResponse(
            _message=message,
            # code=code,
            rows_affected=rows,
        )

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[str, str]) -> str:
        data_type = str(type_code)[str(type_code).index("'") + 1 : str(type_code).rindex("'")]
        return datatypes[data_type]

    def execute(
        self, sql: str, auto_begin: bool = True, fetch: bool = False, limit: Optional[int] = None
    ) -> Tuple[AdapterResponse, agate.Table]:
        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)
        if fetch:
            # Get the result of the first non-empty result set (if any)
            while cursor.description is None:
                if not cursor.nextset():
                    break
            table = self.get_result_from_cursor(cursor, limit)
        else:
            table = empty_table()
        # Step through all result sets so we process all errors
        while cursor.nextset():
            pass
        return response, table
