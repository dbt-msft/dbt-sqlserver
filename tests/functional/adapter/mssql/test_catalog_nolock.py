"""The adapter's own catalog lookups must not queue behind unrelated DDL.

SQL Server takes transaction-scoped X locks on the system catalog for every
CREATE/ALTER/DROP. Any *other* session that reads the catalog under the default
READ COMMITTED isolation has to wait for those X locks to be released - and the
default LOCK_TIMEOUT is -1, so it waits forever.

That makes an un-hinted catalog query in the adapter a hard dependency on every
other writer in the database: a second dbt project, an ETL tool, a DBA running a
long deployment script. dbt appears to hang with no error.

``information_schema_hints()`` (``with (nolock)``) is the existing fix for this;
it is already applied to ``list_relations_without_caching`` and friends. This
test covers ``get_relation_last_modified`` - reached by ``dbt source freshness``
on a source with no ``loaded_at_field`` - which was missing it.

The hint only ever affects the adapter's own read-only metadata discovery. It is
deliberately NOT applied to catalog reads that act as correctness guards
(schema-exists before CREATE SCHEMA, the extended-properties full-refresh
marker), where a dirty read would weaken the guard rather than just widen
discovery.
"""

import threading
import time

import pytest

from dbt.tests.util import relation_from_name, run_dbt

# Enough uncommitted objects that the blocker holds locks across the catalog
# pages the reader has to scan. A handful is not reliably enough.
BLOCKER_OBJECT_COUNT = 40

# Upper bound on how long the blocker keeps its transaction open. The reader is
# expected to finish in milliseconds; this exists only so a regression fails the
# test instead of hanging the suite forever.
BLOCKER_HOLD_SECONDS = 60

# Without this the reader would wait indefinitely (SQL Server's default
# LOCK_TIMEOUT is -1) and the failure would look like a hang rather than a
# regression. With it, a regression surfaces as Msg 1222.
READER_LOCK_TIMEOUT_MS = 5000

probe_model_sql = """
{{ config(materialized='table') }}
select 1 as id
"""


class BlockingWriter:
    """A second session holding uncommitted DDL in the test schema.

    Runs on its own thread so it gets its own dbt connection - the connection
    manager keys connections by thread - and rolls back on exit, so the objects
    it creates never actually exist.
    """

    def __init__(self, project):
        self.project = project
        self.ready = threading.Event()
        self.release = threading.Event()
        self.error = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        try:
            with self.project.adapter.connection_named("nolock_blocker"):
                self.project.adapter.execute("begin transaction")
                try:
                    for i in range(BLOCKER_OBJECT_COUNT):
                        self.project.adapter.execute(
                            f"create table {self.project.test_schema}.nolock_blocker_{i} "
                            f"(id int not null)"
                        )
                    self.ready.set()
                    self.release.wait(timeout=BLOCKER_HOLD_SECONDS)
                finally:
                    self.project.adapter.execute("rollback transaction")
        except Exception as exc:  # surfaced by the test, not swallowed
            self.error = exc
        finally:
            self.ready.set()

    def __enter__(self):
        self._thread.start()
        assert self.ready.wait(timeout=BLOCKER_HOLD_SECONDS), (
            "blocker never opened its transaction"
        )
        if self.error is not None:
            raise AssertionError(f"blocker session failed: {self.error}")
        return self

    def __exit__(self, *exc_info):
        self.release.set()
        self._thread.join(timeout=BLOCKER_HOLD_SECONDS)
        return False


class TestCatalogLookupNotBlockedByExternalDdl:
    @pytest.fixture(scope="class")
    def models(self):
        return {"probe_model.sql": probe_model_sql}

    def test_get_relation_last_modified_ignores_uncommitted_ddl(self, project):
        run_dbt(["run"])
        relation = relation_from_name(project.adapter, "probe_model")

        with BlockingWriter(project):
            with project.adapter.connection_named("nolock_reader"):
                project.adapter.execute(f"set lock_timeout {READER_LOCK_TIMEOUT_MS}")

                started = time.time()
                try:
                    project.adapter.execute_macro(
                        "get_relation_last_modified",
                        kwargs={"information_schema": None, "relations": [relation]},
                    )
                except Exception as exc:
                    raise AssertionError(
                        "get_relation_last_modified blocked behind another session's "
                        f"uncommitted DDL after {time.time() - started:.2f}s: {exc}. "
                        "The sys.objects/sys.schemas reads need "
                        "{{ information_schema_hints() }}."
                    ) from exc
                finally:
                    # dbt keeps named connections open for reuse, so restore the
                    # server default rather than leaking a timeout into later tests.
                    project.adapter.execute("set lock_timeout -1")
