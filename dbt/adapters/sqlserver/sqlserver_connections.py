import dbt_common.exceptions  # noqa
import pyodbc
from azure.core.credentials import AccessToken
from azure.identity import ClientSecretCredential, ManagedIdentityCredential
from dbt.adapters.contracts.connection import Connection, ConnectionState
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.fabric import FabricConnectionManager
from dbt.adapters.fabric.fabric_connection_manager import (
    AZURE_AUTH_FUNCTIONS as AZURE_AUTH_FUNCTIONS_FABRIC,
)
from dbt.adapters.fabric.fabric_connection_manager import (
    AZURE_CREDENTIAL_SCOPE,
    bool_to_connection_string_arg,
    get_pyodbc_attrs_before_accesstoken,
    get_pyodbc_attrs_before_credentials,
)

from dbt.adapters.sqlserver import __version__
from dbt.adapters.sqlserver.sqlserver_credentials import SQLServerCredentials

logger = AdapterLogger("sqlserver")


def get_msi_access_token(credentials: SQLServerCredentials) -> AccessToken:
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
    token = ManagedIdentityCredential().get_token(AZURE_CREDENTIAL_SCOPE)
    return token


def get_sp_access_token(credentials: SQLServerCredentials) -> AccessToken:
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
    token = ClientSecretCredential(
        str(credentials.tenant_id),
        str(credentials.client_id),
        str(credentials.client_secret),
    ).get_token(AZURE_CREDENTIAL_SCOPE)
    return token


AZURE_AUTH_FUNCTIONS = {
    **AZURE_AUTH_FUNCTIONS_FABRIC,
    "serviceprincipal": get_sp_access_token,
    "msi": get_msi_access_token,
}


class SQLServerConnectionManager(FabricConnectionManager):
    TYPE = "sqlserver"

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = cls.get_credentials(connection.credentials)
        if credentials.authentication != "sql":
            return super().open(connection)

        # sql login authentication

        con_str = [f"DRIVER={{{credentials.driver}}}"]

        if "\\" in credentials.host:
            # If there is a backslash \ in the host name, the host is a
            # SQL Server named instance. In this case then port number has to be omitted.
            con_str.append(f"SERVER={credentials.host}")
        else:
            con_str.append(f"SERVER={credentials.host},{credentials.port}")

        con_str.append(f"Database={credentials.database}")

        assert credentials.authentication is not None

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

            if credentials.authentication == "ActiveDirectoryAccessToken":
                attrs_before = get_pyodbc_attrs_before_accesstoken(credentials.access_token)
            else:
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

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=credentials.retries,
            retryable_exceptions=retryable_exceptions,
        )
