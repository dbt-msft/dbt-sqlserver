"""The full-refresh marker must resolve its object in the model's database.

``USE`` persists for the session, so a two-part name resolves against whichever
database the connection was last left on. Off-database the marker's two halves
disagree inside one statement: ``OBJECT_ID`` returns NULL, so the
``if not exists`` guard runs ``sp_addextendedproperty`` even when the marker is
already there, and the proc rejects the same name as absent (Msg 15135). The
reader has no proc to fail it, so it degrades silently: count 0 reads as "no
marker" and the append it exists to refuse goes through.

Source-level, because the failure needs a connection left on another database
to show up at all.
"""

from pathlib import Path

import jinja2
import pytest

from dbt.adapters.sqlserver.sqlserver_adapter import SQLServerAdapter
from dbt.adapters.sqlserver.sqlserver_relation import SQLServerRelation

CREATE_SQL = (
    Path(__file__).parents[4]
    / "dbt"
    / "include"
    / "sqlserver"
    / "macros"
    / "relations"
    / "table"
    / "create.sql"
)

# get_use_database_sql and escape_single_quotes dispatch through the adapter,
# which this bare environment has no registry for; these emit what the
# sqlserver__ and dbt default implementations emit.
_STUBS = """
{% macro get_use_database_sql(database) %}USE "{{ database }}";{% endmacro %}
{% macro escape_single_quotes(expression) %}{{ expression | replace("'", "''") }}{% endmacro %}
{% macro get_assert_columns_equivalent(sql) %}(/* assert_columns_equivalent */){% endmacro %}
{% macro build_columns_constraints(relation) %}(/* columns_constraints */){% endmacro %}
"""

MARKER_NAME = "dbt_full_refresh_incomplete"


class _Result:
    """Enough of an agate table for the reader's marker.rows[0][0]."""

    def __init__(self, marker_count):
        self.rows = [[marker_count]]


class _RunQuery:
    """Captures the SQL each macro would send, whitespace-normalised."""

    def __init__(self, marker_count=0):
        self.marker_count = marker_count
        self.queries = []

    def __call__(self, sql):
        self.queries.append(" ".join(str(sql).split()))
        return _Result(self.marker_count)

    @property
    def only(self):
        assert len(self.queries) == 1, f"expected one query, got {self.queries}"
        return self.queries[0]


class _Adapter:
    quote = staticmethod(SQLServerAdapter.quote)

    def __init__(self):
        self.commits = 0

    def commit_if_open(self):
        self.commits += 1
        return ""


class _CompilerError(Exception):
    pass


class _Exceptions:
    @staticmethod
    def raise_compiler_error(msg):
        raise _CompilerError(msg)


def _render(call, run_query, adapter=None):
    source = _STUBS + CREATE_SQL.read_text() + "\n" + call
    env = jinja2.Environment(
        undefined=jinja2.StrictUndefined,
        extensions=["jinja2.ext.do"],  # create.sql uses {% do %}, as dbt's env does
    )
    env.from_string(source).render(
        adapter=adapter if adapter is not None else _Adapter(),
        exceptions=_Exceptions(),
        run_query=run_query,
        relation=RELATION,
    )
    return run_query


RELATION = SQLServerRelation.create(
    database="other_db", schema="sch", identifier="rel", type="table"
)

MARK = "{{ sqlserver__mark_full_refresh_incomplete(relation) }}"
ASSERT = "{{ sqlserver__assert_no_incomplete_full_refresh(relation) }}"


@pytest.mark.parametrize("call", [MARK, ASSERT], ids=["mark", "assert"])
def test_marker_statement_selects_its_database_first(call):
    """The USE must lead: OBJECT_ID resolves at execution, so it reads the
    context this statement set, not the one the connection arrived with."""
    sql = _render(call, _RunQuery()).only

    assert sql.startswith('USE "other_db";'), (
        f"{call} sends a statement that resolves an object by name without "
        f"selecting its database first: {sql}"
    )


@pytest.mark.parametrize("call", [MARK, ASSERT], ids=["mark", "assert"])
def test_marker_statement_qualifies_and_quotes_the_object_name(call):
    """A bare `schema.table` inside the string literal makes OBJECT_ID return
    NULL for any name holding a '.' or a '"', which reads as "no such
    object"."""
    sql = _render(call, _RunQuery()).only

    assert 'OBJECT_ID(\'"sch"."rel"\')' in sql, (
        f"{call} passes an unquoted name to OBJECT_ID: {sql}"
    )


@pytest.mark.parametrize("call", [MARK, ASSERT], ids=["mark", "assert"])
def test_marker_statement_matches_only_the_object_level_property(call):
    """sys.extended_properties is keyed by (class, major_id, minor_id). Without
    class = 1 and minor_id = 0 the lookup also matches a column-level property
    of the same name, and an index-level one (class 7) sharing the major_id."""
    sql = _render(call, _RunQuery()).only

    assert "class = 1" in sql and "minor_id = 0" in sql, (
        f"{call} reads sys.extended_properties without pinning it to the "
        f"object-level property: {sql}"
    )


def test_marker_names_the_table_for_the_extended_property_proc():
    """sp_addextendedproperty takes @level0name/@level1name as sysname values,
    not identifiers, so they stay bare - quoting would land in the name."""
    sql = _render(MARK, _RunQuery()).only

    assert "@level0type = N'SCHEMA', @level0name = N'sch'" in sql, sql
    assert "@level1type = N'TABLE', @level1name = N'rel'" in sql, sql
    assert f"@name = N'{MARKER_NAME}'" in sql, sql


def test_marker_commits_so_it_survives_the_rebuild_it_guards():
    """The marker's purpose is to outlive a failed rebuild, so it must not be
    left inside a transaction that the failure would roll back."""
    adapter = _Adapter()
    _render(MARK, _RunQuery(), adapter=adapter)

    assert adapter.commits == 1


def test_reader_raises_when_a_previous_full_refresh_left_its_marker():
    with pytest.raises(_CompilerError, match="did not complete"):
        _render(ASSERT, _RunQuery(marker_count=1))


def test_reader_is_quiet_when_no_marker_is_present():
    _render(ASSERT, _RunQuery(marker_count=0))
