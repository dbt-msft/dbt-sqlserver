from contextlib import contextmanager
from typing import Any, Optional, Tuple, Type

import dbt_common.exceptions  # noqa
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
)

from dbt.adapters.sqlserver import __version__  # noqa: E402
from dbt.adapters.sqlserver.sqlserver_credentials import SQLServerCredentials  # noqa: E402

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

    @contextmanager
    def exception_handler(self, sql):
        """Override Fabric's exception_handler to avoid pyodbc exception types."""
        try:
            yield

        except Exception as e:
            logger.debug("Error running SQL: {}".format(str(e)))

            try:
                self.release()
            except Exception:
                logger.debug("Failed to release connection!")

            if isinstance(e, dbt_common.exceptions.DbtDatabaseError):
                raise
            if isinstance(e, dbt_common.exceptions.DbtRuntimeError):
                raise

            # Check for mssql_python exceptions
            try:
                import mssql_python

                if isinstance(e, (mssql_python.DatabaseError,)):
                    raise dbt_common.exceptions.DbtDatabaseError(str(e).strip()) from e
            except ImportError:
                pass

            raise dbt_common.exceptions.DbtRuntimeError(e)

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = cls.get_credentials(connection.credentials)

        driver_type = getattr(credentials, "driver_type", "mssql-python")

        if driver_type == "pyodbc":
            # Delegate to Fabric's pyodbc-based open()
            return super().open(connection)

        if credentials.authentication != "sql":
            return super().open(connection)

        # --- mssql-python path (lazy import) ---
        import mssql_python

        con_str = []

        con_str.append(f"Server={credentials.host},{credentials.port}")

        if "\\" in credentials.host:
            # If there is a backslash \ in the host name, the host is a
            # SQL Server named instance. In this case then port number has to be omitted.
            con_str[-1] = f"Server={credentials.host}"

        con_str.append(f"Database={credentials.database}")

        assert credentials.authentication is not None

        con_str.append(f"UID={{{credentials.UID}}}")
        con_str.append(f"PWD={{{credentials.PWD}}}")

        # https://docs.microsoft.com/en-us/sql/relational-databases/native-client/features/using-encryption-without-validation?view=sql-server-ver15
        assert credentials.encrypt is not None
        assert credentials.trust_cert is not None

        con_str.append(f"Encrypt={'yes' if credentials.encrypt else 'no'}")
        con_str.append(
            f"TrustServerCertificate={'yes' if credentials.trust_cert else 'no'}"
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

        retryable_exceptions = [  # DB-API 2.0 standard exceptions
            mssql_python.InternalError,
            mssql_python.OperationalError,
        ]

        if credentials.authentication.lower() in AZURE_AUTH_FUNCTIONS:
            # Temporary login/token errors fall into this category when using AAD
            retryable_exceptions.append(mssql_python.InterfaceError)

        def connect():
            logger.debug(f"Using connection string: {con_str_display}")

            handle = mssql_python.connect(con_str_concat)
            handle.setautocommit(True)
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

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
        retryable_exceptions: Tuple[Type[Exception], ...] = (),
        retry_limit: int = 2,
    ) -> Tuple[Connection, Any]:
        """Override to skip pyodbc-specific add_output_converter call for mssql-python."""
        connection = self.get_thread_connection()
        credentials = self.get_credentials(connection.credentials)
        driver_type = getattr(credentials, "driver_type", "mssql-python")

        if driver_type == "pyodbc":
            return super().add_query(
                sql, auto_begin, bindings, abridge_sql_log, retryable_exceptions, retry_limit
            )

        # For mssql-python, use parent's add_query but avoid the output_converter call.
        # We call the grandparent (SQLConnectionManager) add_query instead.
        from dbt.adapters.sql import SQLConnectionManager

        return SQLConnectionManager.add_query(
            self, sql, auto_begin, bindings, abridge_sql_log
        )
