"""Concurrent schema creation (#839): racing sessions and lock release on failure."""

import threading

import pytest

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
    """Each test uses its own schema, so they share one project."""

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

    def test_failed_create_releases_the_lock(self, project):
        """A failed create must release the session lock and rethrow."""
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
