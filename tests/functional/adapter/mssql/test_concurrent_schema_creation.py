"""Creating a schema that may already exist must be safe against concurrency.

``IF NOT EXISTS (SELECT * FROM sys.schemas ...) BEGIN CREATE SCHEMA ... END`` is
check-then-act: with ``threads > 1``, or two dbt processes pointed at one
database, sessions pass the check together and all but one fail with ``Msg 2714,
There is already an object named '<schema>' in the database``.
``create_schema_if_not_exists`` serializes the check and the create behind an
application lock. See https://github.com/dbt-msft/dbt-sqlserver/issues/839.
"""

import threading

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture

seed_model = """
{{ config(materialized='table') }}
SELECT 1 AS id
"""

schema_yml = """
version: 2
models:
  - name: guarded_model
    columns:
      - name: id
        data_tests:
          - not_null
"""

MISSING_PRINCIPAL = "dbt_no_such_principal_839"


def lock_free_on_another_connection(project, resource: str) -> bool:
    """dbt keys connections by thread, so probe off-thread to get a second
    session rather than the one the caller is using."""
    result: list[bool] = []

    def probe() -> None:
        with project.adapter.connection_named("lock_probe"):
            _, table = project.adapter.execute(
                "DECLARE @rc int;"
                f" EXEC @rc = sp_getapplock @Resource = '{resource}',"
                " @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 2000;"
                " select @rc",
                fetch=True,
            )
            acquired = table.rows[0][0] >= 0
            if acquired:
                project.adapter.execute(
                    f"EXEC sp_releaseapplock @Resource = '{resource}', @LockOwner = 'Session'"
                )
            result.append(acquired)

    worker = threading.Thread(target=probe)
    worker.start()
    worker.join()
    return result[0]


class TestConcurrentSchemaCreation:
    """Every guard that creates a schema on demand, on one dbt project.

    Each test works on a schema of its own, so they share the project without
    sharing state.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"guarded_model.sql": seed_model, "schema.yml": schema_yml}

    def test_racing_sessions_create_schema_once(self, project):
        """Sessions racing to create the same schema must all succeed."""
        target = project.adapter.Relation.create(
            database=project.database, schema=f"{project.test_schema}_race"
        )
        threads = 8

        for _ in range(3):
            with project.adapter.connection_named("race_setup"):
                project.adapter.drop_schema(target)

            errors: list[str] = []
            barrier = threading.Barrier(threads)

            def attempt(n: int) -> None:
                try:
                    with project.adapter.connection_named(f"race_{n}"):
                        # Line every session up so they hit the check together;
                        # without the lock they then all try to create it.
                        barrier.wait()
                        project.adapter.create_schema(target)
                except Exception as exc:  # noqa: BLE001 - reported verbatim below
                    errors.append(str(exc))

            workers = [threading.Thread(target=attempt, args=(n,)) for n in range(threads)]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join()

            assert errors == [], f"concurrent CREATE SCHEMA failed: {errors[0]}"

            with project.adapter.connection_named("race_check"):
                _, table = project.adapter.execute(
                    f"select count(*) from sys.schemas where name = '{target.schema}'",
                    fetch=True,
                )
            assert table.rows[0][0] == 1

        with project.adapter.connection_named("race_teardown"):
            project.adapter.drop_schema(target)

    def test_test_sql_serializes_schema_creation(self, project):
        """A data test creates the target schema through its own copy of the guard.

        Racing ``dbt test`` deterministically is not practical, so assert the
        statement it emits is the serialized form rather than check-then-act.
        """
        run_dbt(["run"])
        results, log_output = run_dbt_and_capture(["--debug", "test"])
        assert len(results) == 1

        emitted = log_output.lower()
        assert "sp_getapplock" in emitted
        assert "sp_releaseapplock" in emitted

    def test_failed_create_releases_the_lock(self, project):
        """A create that fails must still hand the lock back.

        The lock is session-scoped and dbt reuses the connection, so a release
        skipped by ``SET XACT_ABORT ON`` strands it for the rest of the run and
        every other thread building that schema waits out the timeout.
        """
        target = project.adapter.Relation.create(
            database=project.database, schema=f"{project.test_schema}_lockfail"
        )
        resource = f"dbt_create_schema_{target.schema}"

        with project.adapter.connection_named("lock_owner"):
            project.adapter.drop_schema(target)

            # AUTHORIZATION to a principal that does not exist gets past the
            # existence check and then fails, which is the case that skips the
            # release.
            with pytest.raises(Exception) as excinfo:
                project.adapter.execute_macro(
                    "sqlserver__create_schema_with_authorization",
                    kwargs={"relation": target, "schema_authorization": MISSING_PRINCIPAL},
                )
            assert MISSING_PRINCIPAL in str(excinfo.value), (
                f"the original error must be rethrown, not swallowed: {excinfo.value}"
            )

            # Still inside the failed connection's lifetime, as dbt would be.
            assert lock_free_on_another_connection(project, resource), (
                f"'{resource}' is still held after a failed CREATE SCHEMA; every "
                "other thread building that schema now blocks for the 30s timeout"
            )
