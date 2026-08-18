"""The shared table-build SQL macros (#819).

``sqlserver__get_create_table_empty_sql`` and
``sqlserver__get_tablock_insert_sql`` are the one place that decides how a
table gets created and loaded. Three call sites share them, so the emitted
shape is pinned here rather than in each functional suite.

These render the real macro file through Jinja2 - no database connection
required - with stubs for the ambient dbt context the macros touch.
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

# The macros under test reach for these; everything else in create.sql lives
# inside macros we never call, so it stays unresolved harmlessly.
_STUBS = """
{% macro get_assert_columns_equivalent(sql) %}(/* assert_columns_equivalent */){% endmacro %}
{% macro build_columns_constraints(relation) %}(/* columns_constraints */){% endmacro %}
"""

QUERY_LABEL = "OPTION (LABEL = 'dbt-sqlserver', MAXDOP 1);"


class _Adapter:
    quote = staticmethod(SQLServerAdapter.quote)


def _render(call, **context):
    """Render a call against the real create.sql plus the stubs above."""
    source = _STUBS + CREATE_SQL.read_text() + "\n" + call
    env = jinja2.Environment(
        undefined=jinja2.StrictUndefined,
        extensions=["jinja2.ext.do"],  # create.sql uses {% do %}, as dbt's env does
    )
    template = env.from_string(source)
    return " ".join(
        template.render(
            adapter=_Adapter(),
            model={"columns": {"id": {}, "my col": {}}},
            **context,
        ).split()
    )


@pytest.fixture
def target():
    return SQLServerRelation.create(database="db", schema="sch", identifier="rel", type="table")


@pytest.fixture
def tmp_vw():
    return SQLServerRelation.create(
        database="db", schema="sch", identifier="rel__dbt_tmp_vw", type="view"
    )


def _create(contract_enforced):
    return (
        "{{ sqlserver__get_create_table_empty_sql("
        f"target, tmp_vw, 'select 1 as id', {str(contract_enforced).lower()}) }}}}"
    ).replace("}}}}", "}}")


def _insert(contract_enforced):
    return (
        "{{ sqlserver__get_tablock_insert_sql("
        f"target, tmp_vw, query_label, {str(contract_enforced).lower()}) }}}}"
    ).replace("}}}}", "}}")


# -- the empty create --


def test_empty_create_moves_no_rows(target, tmp_vw):
    """TOP 0 is the whole point: Sch-M is held for an instant, not for the load."""
    sql = _render(_create(False), target=target, tmp_vw=tmp_vw)
    assert sql == 'SELECT TOP 0 * INTO "db"."sch"."rel" FROM "db"."sch"."rel__dbt_tmp_vw"'


def test_empty_create_under_contract_emits_ddl(target, tmp_vw):
    sql = _render(_create(True), target=target, tmp_vw=tmp_vw)
    assert sql.startswith('CREATE TABLE "db"."sch"."rel"')
    assert "assert_columns_equivalent" in sql
    assert "columns_constraints" in sql
    # The contract create is pure DDL - it must not move rows.
    assert "INSERT" not in sql
    assert "SELECT TOP 0" not in sql


def test_empty_create_carries_no_query_label(target, tmp_vw):
    """The OPTION clause rides the data-movement statement, not the create."""
    for contract_enforced in (True, False):
        sql = _render(_create(contract_enforced), target=target, tmp_vw=tmp_vw)
        assert "OPTION" not in sql
        assert "LABEL" not in sql


# -- the load --


def test_load_is_tablock_insert(target, tmp_vw):
    sql = _render(_insert(False), target=target, tmp_vw=tmp_vw, query_label=QUERY_LABEL)
    assert sql == (
        'INSERT INTO "db"."sch"."rel" WITH (TABLOCK) '
        'SELECT * FROM "db"."sch"."rel__dbt_tmp_vw" ' + QUERY_LABEL
    )


def test_load_under_contract_names_columns_on_both_sides(target, tmp_vw):
    sql = _render(_insert(True), target=target, tmp_vw=tmp_vw, query_label=QUERY_LABEL)
    assert sql == (
        'INSERT INTO "db"."sch"."rel" WITH (TABLOCK) ("id", "my col") '
        'SELECT "id", "my col" FROM "db"."sch"."rel__dbt_tmp_vw" ' + QUERY_LABEL
    )


@pytest.mark.parametrize("contract_enforced", [True, False])
def test_load_keeps_tablock_and_query_label(target, tmp_vw, contract_enforced):
    """Minimal logging needs the table lock; #613's OPTION clause has to survive
    alongside it (see test_interaction_tablock_query_options.py)."""
    sql = _render(
        _insert(contract_enforced), target=target, tmp_vw=tmp_vw, query_label=QUERY_LABEL
    )
    assert "WITH (TABLOCK)" in sql
    assert "MAXDOP 1" in sql
    assert "LABEL =" in sql


@pytest.mark.parametrize("contract_enforced", [True, False])
def test_load_never_creates_the_table(target, tmp_vw, contract_enforced):
    """Fusing the create back into the load is the bug in #819."""
    sql = _render(
        _insert(contract_enforced), target=target, tmp_vw=tmp_vw, query_label=QUERY_LABEL
    )
    assert "CREATE TABLE" not in sql
    assert " INTO " not in sql.replace("INSERT INTO", "INSERT")


@pytest.mark.parametrize("contract_enforced", [True, False])
def test_load_does_not_reassert_the_contract(target, tmp_vw, contract_enforced):
    """get_assert_columns_equivalent raises on mismatch, so it belongs to the
    create macro alone - once per build, not twice."""
    sql = _render(
        _insert(contract_enforced), target=target, tmp_vw=tmp_vw, query_label=QUERY_LABEL
    )
    assert "assert_columns_equivalent" not in sql
