import datetime as dt
import time
from contextlib import contextmanager
from typing import (
    Any,
    Optional,
    Tuple,
    Type,
    Union,
)

import agate  # type: ignore[import]
import dbt_common.exceptions
from dbt_common.clients.agate_helper import empty_table
from dbt_common.events.contextvars import get_node_info
from dbt_common.events.functions import fire_event
from dbt_common.utils.casting import cast_to_str

from dbt.adapters.contracts.connection import (
    AdapterResponse,
    Connection,
    ConnectionState,
)
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.events.types import (
    AdapterEventDebug,
    ConnectionUsed,
    SQLQuery,
    SQLQueryStatus,
)
from dbt.adapters.sql.connections import SQLConnectionManager
from dbt.adapters.sqlserver.sqlserver_auth import (
    is_mssql_python_backend,
)
from dbt.adapters.sqlserver.sqlserver_backend import (
    _connect_mssql_python,
    _connect_pyodbc,
    build_mssql_python_connection_string,
    build_pyodbc_connection_string,
    get_mssql_python_retryable_exceptions,
    get_pyodbc_retryable_exceptions,
    handle_backend_database_error,
    is_pyodbc_handle,
    log_connection_string,
)
from dbt.adapters.sqlserver.sqlserver_constants import datatypes
from dbt.adapters.sqlserver.sqlserver_credentials import SQLServerCredentials
from dbt.adapters.sqlserver.sqlserver_helpers import (
    byte_array_to_datetime,
    validate_connection_requirements,
    validate_mssql_python_requirements,
    validate_pyodbc_requirements,
)
from dbt.adapters.sqlserver.sqlserver_runtime import (
    _RUNTIME_STATE,
    _get_mssql_python,
    _get_pyodbc,
)

logger = AdapterLogger("sqlserver")


class SQLServerConnectionManager(SQLConnectionManager):
    TYPE = "sqlserver"

    @contextmanager
    def exception_handler(self, sql):
        """Translate backend database errors and re-raise everything else.

        The backend-specific ``DatabaseError`` type is discovered lazily so the
        handler can work with either optional backend. Non-database exceptions
        are logged, the connection is released on a best-effort basis, and the
        original exception is re-raised unchanged.
        """

        try:
            yield

        except Exception as e:
            credentials = self.get_thread_connection().credentials
            if is_mssql_python_backend(credentials.backend):
                database_error = _RUNTIME_STATE.get_mssql_python_database_error()
            else:
                database_error = _RUNTIME_STATE.get_pyodbc_database_error()

            if database_error is not None and isinstance(e, database_error):
                # The backend-specific handler releases the connection and raises
                # DbtDatabaseError, so this branch must not fall through into the
                # generic rollback / logging path below.
                handle_backend_database_error(e, database_error, self.release)

            logger.debug(f"SQL execution raised {type(e).__name__}: {e}")
            logger.debug(f"Error running SQL: {sql}")
            logger.debug("Rolling back transaction.")
            try:
                self.release()
            except Exception:
                logger.debug("Failed to release connection!")
            raise

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = cls.get_credentials(connection.credentials)

        validate_connection_requirements(credentials)

        if is_mssql_python_backend(credentials.backend):
            mssql_python = _get_mssql_python()
            validate_mssql_python_requirements(credentials)
            con_str_concat = build_mssql_python_connection_string(credentials)
            retryable_exceptions = get_mssql_python_retryable_exceptions(credentials, mssql_python)

            def connect() -> Any:
                log_connection_string(con_str_concat)
                return _connect_mssql_python(mssql_python, credentials, con_str_concat)

        else:
            pyodbc = _get_pyodbc()
            validate_pyodbc_requirements(credentials)
            con_str_concat = build_pyodbc_connection_string(credentials)
            retryable_exceptions = get_pyodbc_retryable_exceptions(credentials, pyodbc)

            def connect() -> Any:
                log_connection_string(con_str_concat)
                return _connect_pyodbc(pyodbc, credentials, con_str_concat)

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
                        (binding.isoformat() if isinstance(binding, dt.datetime) else binding)
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
            log_sql = f"{sql[:512]}..." if abridge_sql_log else sql

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
                retry_limit=(credentials.retries if credentials.retries > 3 else retry_limit),
                attempt=1,
            )

            if is_pyodbc_handle(connection.handle):
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
        if isinstance(type_code, int):
            raise dbt_common.exceptions.DbtRuntimeError(
                "Unsupported SQL Server type code "
                f"{type_code!r}: integer type codes are not mapped"
            )

        if isinstance(type_code, str) and type_code in datatypes:
            return datatypes[type_code]

        as_str = str(type_code)
        if "'" in as_str:
            try:
                start = as_str.index("'") + 1
                end = as_str.rindex("'")
                data_type = as_str[start:end]
            except ValueError:
                data_type = None
            else:
                if data_type in datatypes:
                    return datatypes[data_type]

        if as_str in datatypes:
            return datatypes[as_str]

        raise dbt_common.exceptions.DbtRuntimeError(
            "Unsupported SQL Server type code "
            f"{type_code!r}: no matching entry found in datatypes mapping"
        )

    def execute(
        self,
        sql: str,
        auto_begin: bool = True,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, agate.Table]:
        # Connection lifetime policy: the *connection handle* is intentionally
        # kept open here.  Open / release / cleanup are managed by the parent
        # SQLConnectionManager (called by dbt-core's thread-local connection
        # pool).  pyodbc.pooling=True additionally reuses handles across
        # tasks.  Only the cursor needs explicit cleanup after each query.
        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin)
        try:
            response = self.get_response(cursor)
            if fetch:
                while cursor.description is None and cursor.nextset():
                    pass
                table = self.get_result_from_cursor(cursor, limit)
            else:
                table = empty_table()
            while cursor.nextset():
                pass
            return response, table
        finally:
            cursor.close()
