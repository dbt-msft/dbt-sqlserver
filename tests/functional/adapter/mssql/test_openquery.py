"""Functional tests for the sqlserver__openquery helper macro.

ONE dbt invocation covers every case. Eight models live in a single project:

 - Three TABLE models execute OPENQUERY against a loopback linked server, so
   each behaviour is asserted twice — once on the emitted SQL, once on the rows
   the server actually returned (happy path, quote escaping, CR stripping).
 - One ephemeral model pins the 8000-character boundary. It is compile-only
   because 'SELECT xxx...' is not executable SQL; only its length matters.
 - Four models exercise the validation errors. Those raises are execution-phase
   (see openquery.sql), so they surface as per-node errors while every other
   model in the same run still succeeds.

The loopback linked server these run against is instance-wide state, created
once by devops/scripts/linked_server.sql when the instance is set up. Nothing
here provisions it; where it is absent these tests skip.
"""

import os

import pytest

from dbt.tests.util import run_dbt


def _find_compiled_sql(project, filename: str) -> str:
    """Locate a model's compiled SQL under target/compiled (or target/run) and
    return its contents."""
    for sub in ("compiled", "run"):
        target_dir = os.path.join(project.project_root, "target", sub)
        for root, _dirs, files in os.walk(target_dir):
            if filename in files:
                with open(os.path.join(root, filename), "r") as f:
                    return f.read()
    raise AssertionError(f"Could not find compiled {filename} under target/")


def _result_by_name(results, model_name: str):
    """Return the run result whose node is named model_name."""
    for result in results:
        if result.node.name == model_name:
            return result
    raise AssertionError(
        f"No result for model {model_name}; got: {[r.node.name for r in results]}"
    )


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

_table = "{{ config(materialized='table') }}"

# Happy path: two rows come back from the remote server.
basic_sql = (
    _table
    + """
select * from {{
    sqlserver__openquery(
        'LOCALLOOP',
        'SELECT 1 AS id, \\'alpha\\' AS name UNION ALL SELECT 2, \\'beta\\''
    )
}}
"""
)

# Jinja string literals do not treat '' as an escaped quote, so the backslash
# escapes carry a SQL Server string literal into remote_sql. The macro doubles
# those quotes so the literal survives the OPENQUERY string and returns it's.
quotes_sql = (
    _table
    + """
select * from {{ sqlserver__openquery('LOCALLOOP', 'SELECT \\'it\\'\\'s\\' AS msg') }}
"""
)

# The \r escape becomes a real carriage return that the macro must strip; the
# remaining newline is still valid remote SQL, so this also runs.
cr_sql = (
    _table
    + """
select * from {{ sqlserver__openquery('LOCALLOOP', 'SELECT 1\\r\\nAS id') }}
"""
)

# Escaped length is exactly 8000 ('SELECT ' is 7 chars + 7993 x's) - allowed.
# Ephemeral: the point is the length check, and this is not executable SQL.
max_length_sql = """
{{ config(materialized='ephemeral') }}
select {{ sqlserver__openquery('LOCALLOOP', 'SELECT ' ~ 'x' * 7993) }} as result
"""

empty_server_sql = """
select {{ sqlserver__openquery('', 'SELECT 1 AS id') }} as result
"""

none_server_sql = """
select {{ sqlserver__openquery(none, 'SELECT 1 AS id') }} as result
"""

empty_remote_sql = """
select {{ sqlserver__openquery('LOCALLOOP', '') }} as result
"""

too_long_sql = """
select {{ sqlserver__openquery('LOCALLOOP', 'SELECT ' ~ 'x' * 8001) }} as result
"""


class TestOpenquery:
    """One project, ONE `dbt run`, every case.

    `--dist loadscope` in pytest.ini is what makes that true under `-n auto`:
    it schedules a class as a single unit, so the class-scoped fixtures below -
    the project, the linked-server check, and the dbt invocation itself - are
    set up once rather than once per worker that happens to receive a test.

    `dbt run` (unlike `dbt compile`) records per-node errors instead of
    re-raising, so the four invalid models come back as status="error" results
    while the three table models build and the ephemeral model compiles.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "basic_model.sql": basic_sql,
            "quotes_model.sql": quotes_sql,
            "cr_model.sql": cr_sql,
            "max_length_model.sql": max_length_sql,
            "empty_server_model.sql": empty_server_sql,
            "none_server_model.sql": none_server_sql,
            "empty_remote_model.sql": empty_remote_sql,
            "too_long_model.sql": too_long_sql,
        }

    @pytest.fixture(scope="class")
    def _linked_server(self, project):
        """Require LOCALLOOP; never create it.

        Creating one from here is what this class stopped doing: a linked
        server is instance-wide, so a fixture that provisioned its own raced
        every other caller on the instance, and dropping it pulled the server
        out from under runs still using it (Msg 7202). Reading is the whole
        contract.

        Absent, these skip rather than fail. An instance may predate
        devops/scripts/linked_server.sql - a CI image published before it -
        or be one the adapter is merely pointed at, and Azure SQL Database
        cannot have a linked server at all.
        """
        rows = project.run_sql(
            "SELECT name FROM sys.servers WHERE name = 'LOCALLOOP'", fetch="all"
        )
        if not rows:
            pytest.skip(
                "No LOCALLOOP linked server on this instance. It is created at "
                "bootstrap by devops/scripts/linked_server.sql; an instance not "
                "built from devops/server.Dockerfile needs it added by hand."
            )
        assert rows[0][0] == "LOCALLOOP"

    @pytest.fixture(scope="class")
    def _run_all(self, project, _linked_server):
        results = run_dbt(["run"], expect_pass=False)
        # Three table models succeed, four invalid models error; the ephemeral
        # model compiles without producing a result.
        assert len(results) == 7
        assert sum(r.status == "success" for r in results) == 3
        assert sum(r.status == "error" for r in results) == 4
        return results

    def test_emits_openquery_and_returns_rows(self, project, _run_all):
        """Happy path: quoted server name, literal remote SQL, real rows."""
        sql = _find_compiled_sql(project, "basic_model.sql")
        assert 'OPENQUERY("LOCALLOOP", \'SELECT 1 AS id' in sql
        rows = project.run_sql(
            f"SELECT id, name FROM {project.test_schema}.basic_model ORDER BY id",
            fetch="all",
        )
        assert [(row[0], row[1]) for row in rows] == [(1, "alpha"), (2, "beta")]

    def test_single_quotes_are_doubled_and_survive(self, project, _run_all):
        """Quotes are doubled in the emitted SQL, and the remote literal
        round-trips to the value it's."""
        sql = _find_compiled_sql(project, "quotes_model.sql")
        assert "OPENQUERY(\"LOCALLOOP\", 'SELECT ''it''''s'' AS msg')" in sql
        rows = project.run_sql(f"SELECT msg FROM {project.test_schema}.quotes_model", fetch="all")
        assert [row[0] for row in rows] == ["it's"]

    def test_carriage_returns_are_stripped_and_query_runs(self, project, _run_all):
        sql = _find_compiled_sql(project, "cr_model.sql")
        assert "\r" not in sql
        assert 'OPENQUERY("LOCALLOOP", \'SELECT 1' in sql
        rows = project.run_sql(f"SELECT id FROM {project.test_schema}.cr_model", fetch="all")
        assert [row[0] for row in rows] == [1]

    def test_max_length_boundary_compiles(self, project, _run_all):
        """Exactly 8000 escaped characters is allowed."""
        sql = _find_compiled_sql(project, "max_length_model.sql")
        assert 'OPENQUERY("LOCALLOOP", \'SELECT ' in sql

    @pytest.mark.parametrize(
        "model_name,expected",
        [
            ("empty_server_model", "openquery: server_name must not be empty"),
            ("none_server_model", "openquery: server_name must not be empty"),
            ("empty_remote_model", "openquery: remote_sql must not be empty"),
            ("too_long_model", "exceeds SQL Server OPENQUERY 8 KB limit"),
        ],
    )
    def test_validation_errors(self, _run_all, model_name, expected):
        node = _result_by_name(_run_all, model_name)
        assert node.status == "error"
        assert expected in node.message

    def test_over_limit_reports_actual_length(self, _run_all):
        node = _result_by_name(_run_all, "too_long_model")
        assert "got 8008 characters after escaping" in node.message
