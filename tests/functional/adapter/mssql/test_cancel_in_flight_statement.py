"""Cancellation (#922): an interrupt stops the statement on the server."""

import os
import threading
import time

import pytest

# The mssql ADBC driver answers AdbcStatementCancel with success and leaves the
# statement running (adbc-drivers/mssql, measured on 1.8.0), so the assertion
# below cannot pass on that backend until the driver cancels. Strict, so a
# driver that starts cancelling turns this into a failure to remove.
adbc_cannot_cancel = pytest.mark.xfail(
    condition=os.getenv("SQLSERVER_TEST_BACKEND", "mssql-python") == "adbc",
    strict=True,
    reason="mssql ADBC 1.8.0 accepts AdbcStatementCancel without stopping the statement",
)

# Long enough that a natural completion cannot be mistaken for a cancel.
_STATEMENT_SECONDS = 20
_POLL_SECONDS = 0.25


def _wait_for(predicate, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(_POLL_SECONDS)
    return predicate()


class TestCancelInFlightStatement:
    @adbc_cannot_cancel
    def test_cancel_open_connections_stops_the_statement(self, project):
        started: dict = {}
        errors: list = []
        finished = threading.Event()

        def run_statement():
            try:
                # dbt keys connections by thread, so this statement runs on a
                # session the test thread can cancel.
                with project.adapter.connection_named("cancel_victim"):
                    _, table = project.adapter.execute("select @@SPID as spid", fetch=True)
                    started["spid"] = table.rows[0][0]
                    project.adapter.execute(f"WAITFOR DELAY '00:00:{_STATEMENT_SECONDS}'")
            except BaseException as exc:  # the cancel surfaces as a database error
                errors.append(exc)
            finally:
                finished.set()

        worker = threading.Thread(target=run_statement)
        worker.start()

        try:
            # The probe connection is this thread's, so cancel_open_connections()
            # leaves it alone and only the worker's session is cancelled.
            with project.adapter.connection_named("cancel_probe"):
                assert _wait_for(lambda: "spid" in started, timeout=10)
                spid = started["spid"]
                assert _wait_for(lambda: self._requests(project, spid) == 1, timeout=10)

                project.adapter.cancel_open_connections()

                assert _wait_for(lambda: self._requests(project, spid) == 0, timeout=5)
                assert finished.wait(timeout=5)
                assert errors, "the cancelled statement returned instead of raising"
        finally:
            # Closing a connection whose statement is still running segfaults
            # adbc_driver_manager, so let a failed cancel (the xfail above) run
            # to completion before the project fixture tears connections down.
            worker.join(timeout=_STATEMENT_SECONDS + 5)

        assert not worker.is_alive()

    @staticmethod
    def _requests(project, spid: int) -> int:
        _, table = project.adapter.execute(
            f"select count(*) as n from sys.dm_exec_requests where session_id = {spid}",
            fetch=True,
        )
        return table.rows[0][0]
