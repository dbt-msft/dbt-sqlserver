"""Creating a schema that may already exist must be safe against concurrency.

``IF NOT EXISTS (SELECT * FROM sys.schemas ...) BEGIN CREATE SCHEMA ... END`` is
check-then-act: with ``threads > 1``, or two dbt processes pointed at one
database, several sessions pass the check together and all but one fail their
create with ``Msg 2714, There is already an object named '<schema>' in the
database``. The run dies on a schema that by then exists and is usable. CI that
builds a schema per pull request hits this on the first run of a new branch.

SQL Server has no ``CREATE SCHEMA IF NOT EXISTS``, and catching 2714 is not an
option: every connection runs ``SET XACT_ABORT ON`` (#718), under which the failed
create dooms the enclosing transaction, so swallowing the error would trade a clear
failure for a silently discarded transaction. ``create_schema_if_not_exists``
serializes the check and the create behind a database-scoped application lock
instead. See https://github.com/dbt-msft/dbt-sqlserver/issues/839.
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


class TestConcurrentCreateSchema:
    """Sessions racing to create the same schema must all succeed."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"guarded_model.sql": seed_model}

    def test_racing_sessions_create_schema_once(self, project):
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


class TestDataTestSchemaGuardIsConcurrencySafe:
    """The guard `sqlserver__get_test_sql` emits must be the safe one.

    A data test creates the target schema itself, so it carries its own copy of
    the guard - the one #839 was reported against. Racing it deterministically
    through ``dbt test`` is not practical, so assert instead that the statement it
    emits is the serialized form rather than a bare check-then-act.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"guarded_model.sql": seed_model, "schema.yml": schema_yml}

    def test_test_sql_serializes_schema_creation(self, project):
        run_dbt(["run"])
        results, log_output = run_dbt_and_capture(["--debug", "test"])
        assert len(results) == 1

        emitted = log_output.lower()
        assert "sp_getapplock" in emitted
        assert "sp_releaseapplock" in emitted
