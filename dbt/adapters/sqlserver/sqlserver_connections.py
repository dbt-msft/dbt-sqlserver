import datetime as dt
import time
import traceback
from contextlib import contextmanager
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
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
    RollbackFailed,
    SQLQuery,
    SQLQueryStatus,
)
from dbt.adapters.sql.connections import SQLConnectionManager
from dbt.adapters.sqlserver.sqlserver_auth import (
    is_adbc_backend,
    is_mssql_python_backend,
)
from dbt.adapters.sqlserver.sqlserver_backend import (
    _connect_adbc,
    _connect_mssql_python,
    _connect_pyodbc,
    build_adbc_connection_uri,
    build_mssql_python_connection_string,
    build_pyodbc_connection_string,
    get_adbc_retryable_exceptions,
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
    validate_adbc_requirements,
    validate_connection_requirements,
    validate_mssql_python_requirements,
    validate_pyodbc_requirements,
)
from dbt.adapters.sqlserver.sqlserver_runtime import (
    _RUNTIME_STATE,
    _get_adbc,
    _get_mssql_python,
    _get_pyodbc,
)

logger = AdapterLogger("sqlserver")


def _cursor_is_adbc(cursor: Any) -> bool:
    """Return True if *cursor* came from an ADBC dbapi connection."""
    return type(cursor).__module__ == "adbc_driver_manager.dbapi"


def _skip_quoted_span(sql: str, i: int) -> Optional[int]:
    """If ``sql[i]`` opens a bracket-quoted identifier (``[...]``) or a
    single-quoted string literal (``'...'``), return the index just past
    its matching close quote (``]]``/``''`` are escaped doubled quotes).
    Returns ``None`` if ``sql[i]`` does not open either.
    """
    ch = sql[i]
    if ch not in ("[", "'"):
        return None
    close = "]" if ch == "[" else "'"
    n = len(sql)
    end = i + 1
    while end < n:
        if sql[end] == close:
            if end + 1 < n and sql[end + 1] == close:
                end += 2
                continue
            return end + 1
        end += 1
    return end


def _replace_qmark_with_at_pn(sql: str, num_bindings: int) -> str:
    """Replace ``?`` placeholders with ``@p1``, ``@p2``, ... for ADBC.

    ADBC's go-mssqldb driver does not recognise PEP 249 ``qmark``
    placeholders but accepts T-SQL named parameters (``@p1``, ``@p2``,
    ...) that are bound positionally by the driver manager.

    Only ``?`` characters outside bracket-quoted identifiers (``[...]``,
    e.g. a seed column named ``[satisfied?]``) and single-quoted string
    literals (``'...'``) are treated as placeholders -- either may
    legally contain a literal ``?`` that must not consume a binding.
    """
    out = []
    i = 0
    n = len(sql)
    placeholder_num = 1
    while i < n:
        span_end = _skip_quoted_span(sql, i)
        if span_end is not None:
            out.append(sql[i:span_end])
            i = span_end
            continue
        ch = sql[i]
        if ch == "?" and placeholder_num <= num_bindings:
            out.append(f"@p{placeholder_num}")
            placeholder_num += 1
            i += 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _split_sql_statements(sql: str) -> Iterator[str]:
    """Split *sql* on top-level ``;`` statement separators.

    ``;`` characters inside bracket-quoted identifiers or string literals
    are not treated as separators.
    """
    start = 0
    i = 0
    n = len(sql)
    while i < n:
        span_end = _skip_quoted_span(sql, i)
        if span_end is not None:
            i = span_end
            continue
        if sql[i] == ";":
            yield sql[start:i]
            i += 1
            start = i
            continue
        i += 1
    yield sql[start:]


def _get_adbc_rowcount(handle: Any) -> int:
    """Query ``@@ROWCOUNT`` on a temporary ADBC cursor.

    Must be called **before** draining the original cursor's resultsets
    (``nextset()``), as ``nextset()`` resets ``@@ROWCOUNT`` on the server.
    """
    try:
        rc_cursor = handle.cursor()
        try:
            rc_cursor.execute("SELECT @@ROWCOUNT AS rc")
            row = rc_cursor.fetchone()
            if row and row[0] is not None and row[0] >= 0:
                return int(row[0])
        finally:
            rc_cursor.close()
    except Exception:
        pass
    return 0


_ADBC_ROWCOUNT_DML_KEYWORDS = ("INSERT", "UPDATE", "DELETE", "MERGE")


def _sql_affects_rows(sql: str) -> bool:
    """Best-effort check that *sql* contains DML whose row count dbt reports.

    dbt frequently batches multiple ``;``-separated statements into one
    string -- e.g. the delete+insert incremental strategy emits
    ``SET NOCOUNT ON; delete ...; SET NOCOUNT OFF; insert ...`` as a single
    call to ``add_query``. Checking only the leading keyword of the whole
    string would miss the DML hiding behind a leading ``SET`` or comment,
    so every top-level statement is checked: if any one of them leads with
    INSERT/UPDATE/DELETE/MERGE, the ADBC ``@@ROWCOUNT`` round trip (see
    ``_get_adbc_rowcount``) is worth paying for. Pure DDL, SELECT, and
    transaction-control (BEGIN/COMMIT/ROLLBACK) batches skip it.
    """
    for statement in _split_sql_statements(sql):
        text = statement
        while True:
            text = text.lstrip()
            if text.startswith("/*"):
                end = text.find("*/")
                if end == -1:
                    text = ""
                    break
                text = text[end + 2 :]
                continue
            if text.startswith("--"):
                newline = text.find("\n")
                text = "" if newline == -1 else text[newline + 1 :]
                continue
            break
        if text.upper().startswith(_ADBC_ROWCOUNT_DML_KEYWORDS):
            return True
    return False


def _try_drain_nextset(cursor: Any) -> bool:
    """Drain one additional result set from *cursor*, if supported.

    Returns ``True`` if a further result set was consumed, ``False`` if the
    cursor has no more result sets, or the backend does not support
    ``nextset()`` (notably ADBC).
    """
    try:
        return bool(cursor.nextset())
    except Exception as e:
        # Duck-typed on purpose: adbc_driver_manager is an optional
        # dependency, so it cannot be imported unconditionally to check
        # ``isinstance(e, NotSupportedError)`` here. The message text is
        # not checked -- only the DB-API 2.0-mandated exception name and
        # the module that raised it, both of which are stable across
        # adbc_driver_manager versions (unlike free-form message wording).
        if type(e).__name__ == "NotSupportedError" and type(e).__module__.startswith(
            "adbc_driver_manager"
        ):
            return False
        raise


# Mapping of Apache Arrow type codes (integers) to SQL Server type names.
# ADBC cursors report column types as Arrow type codes; this map translates
# them for use in get_column_schema_from_query() and related column expansion.
# Reference: pyarrow type enum values (pa.int32().__class__.__name__ yields the
# Arrow type name string, and the type's ``id`` property is the integer code).
ARROW_TYPE_CODE_TO_NAME: dict[int, str] = {
    1: "bit",  # pa.bool_()
    3: "varchar",  # pa.string() / pa.utf8()
    4: "varbinary",  # pa.binary()
    5: "varchar(max)",  # pa.large_string() / pa.large_utf8()
    6: "real",  # pa.float32()
    7: "float",  # pa.float64()
    8: "int",  # pa.int32()
    9: "bigint",  # pa.int64()
    10: "smallint",  # pa.int8()
    11: "smallint",  # pa.int16()
    12: "decimal",  # pa.decimal128()
    14: "date",  # pa.date32()
    16: "date",  # pa.date64()
    17: "datetime2(6)",  # pa.timestamp()
    18: "time",  # pa.time32()
    19: "time",  # pa.time64()
}

# Some ADBC drivers / cursor implementations report the Arrow type name as a
# string (e.g. "int32") rather than the integer code.  Map those here.
ARROW_STRING_TYPE_TO_NAME: dict[str, str] = {
    "int8": "smallint",
    "int16": "smallint",
    "int32": "int",
    "int64": "bigint",
    "float32": "real",
    "float64": "float",
    "float": "float",
    "double": "float",
    "string": "varchar",
    "utf8": "varchar",
    "large_string": "varchar(max)",
    "large_utf8": "varchar(max)",
    "bool": "bit",
    "boolean": "bit",
    "decimal128": "decimal",
    "decimal": "decimal",
    "date32": "date",
    "date64": "date",
    "date": "date",
    "time32": "time",
    "time64": "time",
    "time": "time",
    "timestamp": "datetime2(6)",
    "binary": "varbinary",
    "large_binary": "varbinary",
}

# Attribute used to stash the in-flight pyodbc / mssql-python cursor on a
# Connection so cancel() can reach it from another thread. See cancel().
_IN_FLIGHT_CURSOR_ATTR = "_dbt_sqlserver_in_flight_cursor"

# Fires once per process (not per connection/thread) when xact_abort is
# disabled. See SQLServerConnectionManager._warn_xact_abort_disabled_once.
_xact_abort_warning_logged = False


class SQLServerConnectionManager(SQLConnectionManager):
    TYPE = "sqlserver"

    _dbt_sqlserver_use_dbt_transactions: bool = True

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
            if is_adbc_backend(credentials.backend):
                database_error = _RUNTIME_STATE.get_adbc_database_error()
            elif is_mssql_python_backend(credentials.backend):
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

        elif is_adbc_backend(credentials.backend):
            _get_adbc()
            validate_adbc_requirements(credentials)
            con_str_concat = build_adbc_connection_uri(credentials)
            retryable_exceptions = get_adbc_retryable_exceptions()

            def connect() -> Any:
                log_connection_string(con_str_concat)
                return _connect_adbc(credentials, con_str_concat)

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

        if conn.state == ConnectionState.OPEN:
            if credentials.xact_abort:
                cls._apply_session_settings(conn)
            else:
                cls._warn_xact_abort_disabled_once()

        return conn

    @classmethod
    def _apply_session_settings(cls, connection: Connection) -> None:
        """Set session-level defaults that must hold for every batch this
        connection ever runs. Currently: XACT_ABORT, so a run-time error
        mid-batch (e.g. a NOT NULL violation in a DELETE+INSERT swap) kills
        the batch and rolls back any open transaction instead of falling
        through to a trailing COMMIT. See dbt-msft/dbt-sqlserver#718.

        A connection that fails this setup is not usable: it is closed and
        the error is re-raised rather than handed back to dbt.
        """
        try:
            cursor = connection.handle.cursor()
            try:
                cursor.execute("SET XACT_ABORT ON;")
            finally:
                cursor.close()

            if getattr(connection.handle, "autocommit", True) is False:
                connection.handle.commit()
        except Exception:
            connection.handle.close()
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise

    @classmethod
    def _warn_xact_abort_disabled_once(cls) -> None:
        global _xact_abort_warning_logged
        if _xact_abort_warning_logged:
            return
        _xact_abort_warning_logged = True

        use_dbt_transactions = cls._dbt_sqlserver_use_dbt_transactions
        msg = (
            "xact_abort is disabled (xact_abort: false in the profile). Without "
            "SET XACT_ABORT ON, a run-time error partway through a multi-statement "
            "batch (e.g. the DELETE+INSERT swap in the DML refresh materialization) "
            "aborts only the failing statement, not the batch, so a trailing COMMIT "
            "can still commit a partial result. "
            f"dbt_sqlserver_use_dbt_transactions is currently {use_dbt_transactions}"
        )
        if not use_dbt_transactions:
            msg += (
                " (dbt-managed transactions are off): the DML refresh materialization "
                "emits its own in-batch BEGIN/COMMIT, and that in-batch swap can commit "
                "a partial result in this configuration."
            )
        else:
            msg += "."
        logger.warning(msg)

    def cancel(self, connection: Connection) -> None:
        """Cancel the in-flight query on ``connection``, if any.

        dbt-core's ``cancel_open`` calls this for sibling connections when a
        run is interrupted (Ctrl-C) or another thread errors. We cancel by
        calling ``Cursor.cancel()`` on the connection's in-flight cursor:
        pyodbc exposes it and it is explicitly designed to be called from
        another thread (it issues ``SQLCancel``); mssql-python's cursor is
        used the same way when it supports it. Cancellation targets statement
        execution. If no statement is in flight, the cursor is gone, or the
        backend cursor does not support cancellation, this is a best-effort
        no-op.
        """

        cursor = getattr(connection, _IN_FLIGHT_CURSOR_ATTR, None)
        if cursor is None:
            logger.debug(f"No in-flight query to cancel for connection {connection.name}.")
            return

        cancel_cursor = getattr(cursor, "cancel", None)
        if not callable(cancel_cursor):
            logger.debug(
                f"Backend cursor for connection {connection.name} does not "
                "support cancellation; skipping."
            )
            return

        try:
            logger.debug(f"Cancelling in-flight query for connection {connection.name}.")
            cancel_cursor()
        except Exception as exc:
            # The statement may have completed between the lookup and the
            # cancel; cancellation is best-effort, so swallow and log.
            logger.debug(f"Failed to cancel query for connection {connection.name}: {exc}")

    def add_begin_query(self):
        if self._dbt_sqlserver_use_dbt_transactions:
            return self.add_query("BEGIN TRANSACTION", auto_begin=False)

    def add_commit_query(self):
        if self._dbt_sqlserver_use_dbt_transactions:
            return self.add_query("IF @@TRANCOUNT > 0 COMMIT TRANSACTION", auto_begin=False)

    @classmethod
    def _rollback_handle(cls, connection: Connection) -> None:
        if cls._dbt_sqlserver_use_dbt_transactions:
            cursor = None
            try:
                cursor = connection.handle.cursor()
                cursor.execute("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION")
            except Exception:
                fire_event(
                    RollbackFailed(
                        conn_name=cast_to_str(connection.name),
                        exc_info=traceback.format_exc(),
                        node_info=get_node_info(),
                    )
                )
            finally:
                if cursor is not None:
                    cursor.close()
        else:
            try:
                connection.handle.rollback()
            except Exception:
                fire_event(
                    RollbackFailed(
                        conn_name=cast_to_str(connection.name),
                        exc_info=traceback.format_exc(),
                        node_info=get_node_info(),
                    )
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
                    if _cursor_is_adbc(cursor):
                        sql = _replace_qmark_with_at_pn(sql, len(bindings))
                    cursor.execute(sql, bindings)
            except retryable_exceptions as e:
                if attempt >= retry_limit:
                    raise e

                fire_event(
                    AdapterEventDebug(
                        base_msg=(
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
            # Track the in-flight cursor so cancel() / cancel_open() can stop it
            # from another thread (e.g. on Ctrl-C); cleared once execution
            # finishes. See cancel().
            setattr(connection, _IN_FLIGHT_CURSOR_ATTR, cursor)
            credentials = self.get_credentials(connection.credentials)

            try:
                _execute_query_with_retry(
                    cursor=cursor,
                    sql=sql,
                    bindings=bindings,
                    retryable_exceptions=retryable_exceptions,
                    # ``retries`` caps total execute attempts, so ``retries: 1``
                    # means a single attempt with no retry.
                    retry_limit=credentials.retries,
                    attempt=1,
                )
            finally:
                setattr(connection, _IN_FLIGHT_CURSOR_ATTR, None)

            if is_pyodbc_handle(connection.handle):
                connection.handle.add_output_converter(-155, byte_array_to_datetime)

            fire_event(
                SQLQueryStatus(
                    status=str(self.get_response(cursor)),
                    elapsed=round((time.time() - pre)),
                    node_info=get_node_info(),
                )
            )

            # ADBC cursor.rowcount is always -1 (the dbapi always calls
            # execute_query). Capture @@ROWCOUNT now, BEFORE any draining
            # or subsequent statements reset it -- but only when it's both
            # meaningful (DML) and safe: opening a second cursor on the
            # same connection while this cursor's result set is still
            # unfetched would race an unfetched SELECT, since go-mssqldb
            # has no MARS support. ``cursor.description`` is empty for
            # DML/DDL (no pending rows to fetch) and populated for
            # anything that returns a result set.
            if _cursor_is_adbc(cursor) and not cursor.description and _sql_affects_rows(sql):
                setattr(
                    cursor,
                    "__dbt_sqlserver_adbc_rowcount",
                    _get_adbc_rowcount(connection.handle),
                )

            return connection, cursor

    @classmethod
    def get_credentials(cls, credentials: SQLServerCredentials) -> SQLServerCredentials:
        return credentials

    @classmethod
    def process_results(
        cls, column_names: "Iterable[str]", rows: "Iterable[Any]"
    ) -> "Iterator[Dict[str, Any]]":
        """Normalize datetime values by stripping timezone info.

        ADBC cursors return Arrow-backed timestamps with ``tzinfo=UTC``,
        but dbt and its test suite expect naive datetimes.  This override
        ensures consistent output across all backends.
        """
        unique_col_names: "dict[str, int]" = {}
        col_names_list = list(column_names)
        for idx, col_name in enumerate(col_names_list):
            if col_name in unique_col_names:
                unique_col_names[col_name] += 1
                col_names_list[idx] = f"{col_name}_{unique_col_names[col_name]}"
            else:
                unique_col_names[col_name] = 1

        for row in rows:
            normalized = tuple(
                (
                    val.replace(tzinfo=None)
                    if isinstance(val, dt.datetime) and val.tzinfo is not None
                    else val
                )
                for val in row
            )
            yield dict(zip(col_names_list, normalized))

    @classmethod
    def get_response(cls, cursor: Any) -> AdapterResponse:
        message = "OK"
        rows = cursor.rowcount
        if _cursor_is_adbc(cursor) and rows is not None and rows < 0:
            # Only the ADBC path needs recovering: its rowcount is always
            # -1. pyodbc/mssql-python already report a real rowcount, or
            # -1 for statements (e.g. SELECT) where "unknown" is the
            # correct, historical value -- that must pass through as-is.
            stashed = getattr(cursor, "__dbt_sqlserver_adbc_rowcount", None)
            rows = stashed if stashed is not None else rows
        return AdapterResponse(
            _message=message,
            rows_affected=rows,
        )

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[int, str]) -> str:
        # Arrow integer type codes (from ADBC cursors) take priority.
        # Non-ADBC backends (pyodbc / mssql-python) never emit bare integers,
        # so receiving one is a reliable signal that we are on the ADBC path.
        if isinstance(type_code, int):
            if type_code in ARROW_TYPE_CODE_TO_NAME:
                return ARROW_TYPE_CODE_TO_NAME[type_code]
            raise dbt_common.exceptions.DbtRuntimeError(
                "Unsupported SQL Server type code "
                f"{type_code!r}: no matching entry found in datatypes mapping"
            )

        # Arrow DataType objects (e.g. ``DataType(int32)``) from ADBC cursors.
        # We convert to string and check the known Arrow type-name map.
        if not isinstance(type_code, (int, str)):
            as_str = str(type_code)
            # Strip precision/tz qualifiers: "timestamp[us, tz=UTC]" -> "timestamp"
            # and "decimal128(5, 2)" -> "decimal128"
            base_type = as_str.split("[")[0].split("(")[0]
            if base_type in ARROW_STRING_TYPE_TO_NAME:
                return ARROW_STRING_TYPE_TO_NAME[base_type]
            if as_str in ARROW_STRING_TYPE_TO_NAME:
                return ARROW_STRING_TYPE_TO_NAME[as_str]

        # Arrow type name strings ("int32", "utf8", …) also from ADBC cursors.
        if isinstance(type_code, str) and type_code in ARROW_STRING_TYPE_TO_NAME:
            return ARROW_STRING_TYPE_TO_NAME[type_code]

        # --- existing pyodbc / mssql-python type-repr handling below ---
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
                while cursor.description is None and _try_drain_nextset(cursor):
                    pass
                table = self.get_result_from_cursor(cursor, limit)
            else:
                table = empty_table()
            while _try_drain_nextset(cursor):
                pass
            return response, table
        finally:
            cursor.close()
