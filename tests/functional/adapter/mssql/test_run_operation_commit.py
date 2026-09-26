import pytest

from dbt.tests.util import run_dbt

# dbt-core never commits after `run-operation`, so a write through
# statement() (auto_begin=True) used to roll back when the connection closed
# (dbt-labs/dbt#16434, dbt-msft/dbt-sqlserver#862).
MACROS = """
{% macro probe_setup() %}
  {% do run_query("drop table if exists " ~ target.schema ~ ".txn_probe") %}
  {% do run_query("create table " ~ target.schema ~ ".txn_probe (tag varchar(50))") %}
{% endmacro %}

{% macro probe_insert(tag) %}
  {% call statement('probe') %}
    insert into {{ target.schema }}.txn_probe values ('{{ tag }}')
  {% endcall %}
{% endmacro %}

{% macro probe_insert_then_fail(tag) %}
  {{ probe_insert(tag) }}
  {{ exceptions.raise_compiler_error('boom') }}
{% endmacro %}
"""


class TestRunOperationCommit:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"probe.sql": MACROS}

    def _tags(self, project):
        rows = project.run_sql(
            f"select tag from {project.test_schema}.txn_probe order by tag", fetch="all"
        )
        return [row[0] for row in rows]

    def test_writes_persist_only_on_success(self, project):
        run_dbt(["run-operation", "probe_setup"])

        run_dbt(["run-operation", "probe_insert", "--args", "{tag: macro}"])
        run_dbt(
            [
                "run-operation",
                "--sql",
                f"insert into {project.test_schema}.txn_probe values ('sql')",
            ]
        )
        assert self._tags(project) == ["macro", "sql"]

        # A failed macro must still roll back its partial write.
        run_dbt(
            ["run-operation", "probe_insert_then_fail", "--args", "{tag: failed}"],
            expect_pass=False,
        )
        assert self._tags(project) == ["macro", "sql"]
