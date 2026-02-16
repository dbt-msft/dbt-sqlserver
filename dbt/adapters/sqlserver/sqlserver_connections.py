from contextlib import contextmanager
from typing import Any, Optional, Tuple, Union

import agate
import dbt_common.exceptions
from adbc_driver_manager import dbapi as adbc_dbapi
from dbt.adapters.contracts.connection import AdapterResponse, Connection, ConnectionState
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.clients.agate_helper import empty_table

from dbt.adapters.sqlserver.sqlserver_credentials import SQLServerCredentials

logger = AdapterLogger("sqlserver")


class SQLServerConnectionManager(SQLConnectionManager):
    TYPE = "sqlserver"

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials: SQLServerCredentials = cls.get_credentials(connection.credentials)

        uri = credentials.build_adbc_uri()

        # Build a display URI with password masked
        display_uri = uri
        if credentials.PWD:
            from urllib.parse import quote_plus

            display_uri = uri.replace(quote_plus(credentials.PWD), "***")

        retryable_exceptions = [
            adbc_dbapi.OperationalError,
            adbc_dbapi.InterfaceError,
        ]

        def connect():
            logger.debug(f"Using ADBC URI: {display_uri}")
            handle = adbc_dbapi.connect(
                driver=credentials.driver,
                db_kwargs={"uri": uri},
                autocommit=True,
            )
            logger.debug(f"Connected to db: {credentials.database}")
            return handle

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=credentials.retries,
            retryable_exceptions=retryable_exceptions,
        )

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except adbc_dbapi.DatabaseError as e:
            logger.debug(f"Database error: {e}")
            self.release()
            raise dbt_common.exceptions.DbtDatabaseError(str(e).strip()) from e
        except Exception as e:
            logger.debug(f"Error running SQL: {sql}")
            logger.debug(f"Rolling back due to: {e}")
            self.release()
            if isinstance(e, dbt_common.exceptions.DbtRuntimeError):
                raise
            raise dbt_common.exceptions.DbtRuntimeError(str(e)) from e

    def cancel(self, connection: Connection):
        logger.debug("Cancel query")

    def add_begin_query(self):
        pass  # autocommit mode

    def add_commit_query(self):
        pass  # autocommit mode

    @classmethod
    def get_credentials(cls, credentials: SQLServerCredentials) -> SQLServerCredentials:
        return credentials

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[int, str]) -> str:
        """Map ADBC/Arrow type codes to SQL Server type names."""
        code = str(type_code).lower()

        # Arrow type code → SQL Server type name
        if code in ("int8", "int16", "int32"):
            return "int"
        if code == "int64":
            return "bigint"
        if code in ("float", "float32"):
            return "real"
        if code in ("double", "float64"):
            return "float"
        if code in ("string", "large_string", "utf8", "large_utf8"):
            return "varchar"
        if code == "bool":
            return "bit"
        if code.startswith("decimal"):
            return "decimal"
        if code.startswith("date"):
            return "date"
        if code.startswith("time") and "stamp" not in code:
            return "time"
        if code.startswith("timestamp"):
            return "datetime2"
        if code == "binary" or code == "large_binary":
            return "varbinary"

        return str(type_code)

    @classmethod
    def get_response(cls, cursor: Any) -> AdapterResponse:
        rows = cursor.rowcount if hasattr(cursor, "rowcount") else -1
        return AdapterResponse(_message="OK", rows_affected=rows)

    def execute(
        self,
        sql: str,
        auto_begin: bool = True,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, agate.Table]:
        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)

        if fetch:
            # ADBC cursors may not support nextset(), so guard it
            # Skip result sets without column descriptions (e.g. SET NOCOUNT ON)
            if hasattr(cursor, "nextset"):
                while cursor.description is None:
                    if not cursor.nextset():
                        break

            if cursor.description is not None:
                table = self.get_result_from_cursor(cursor, limit)
            else:
                table = empty_table()
        else:
            table = empty_table()

        return response, table
