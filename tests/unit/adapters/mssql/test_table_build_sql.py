"""The shared table-build SQL macros (#819).

``sqlserver__get_create_table_empty_sql`` and
``sqlserver__get_tablock_insert_sql`` are the one place that decides how a
table gets created and loaded. Three call sites share them, so the emitted
shape is pinned here rather than in each functional suite.

These render the real macro file through Jinja2 - no database connection
required - with stubs for the ambient dbt context the macros touch.
"""

import re
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


# -- the whole create_table_as batch --
#
# The two macros above are strings; this renders the batch that actually ships,
# to catch a missing statement terminator or a broken EXEC literal.

_BATCH_STUBS = """
{% macro get_query_options(parse_options=False) %}OPTION (LABEL = 'dbt-sqlserver');{% endmacro %}
{% macro get_use_database_sql(database) %}USE {{ database }};{% endmacro %}
{% macro get_create_view_as_sql(relation, sql) %}CREATE OR ALTER VIEW {{ relation }} AS {{ sql }};
{% endmacro %}
{% macro escape_single_quotes(value) %}{{ value | replace("'", "''") }}{% endmacro %}
{% macro sqlserver__create_clustered_columnstore_index(relation) %}CREATE CLUSTERED COLUMNSTORE
INDEX cci ON {{ relation }};{% endmacro %}
"""


class _Config:
    def __init__(self, contract_enforced=False, **values):
        self._values = values
        self._contract = type("Contract", (), {"enforced": contract_enforced})()

    def get(self, key, default=None):
        if key == "contract":
            return self._contract
        return self._values.get(key, default)


class _BatchAdapter(_Adapter):
    def drop_relation(self, relation):
        return ""


def _render_batch(temporary, relation, config):
    source = (
        _BATCH_STUBS
        + _STUBS
        + CREATE_SQL.read_text()
        + "\n{{ sqlserver__create_table_as(temporary, relation, 'select 1 as id') }}"
    )
    env = jinja2.Environment(undefined=jinja2.StrictUndefined, extensions=["jinja2.ext.do"])
    return env.from_string(source).render(
        adapter=_BatchAdapter(),
        model={"columns": {"id": {}}},
        config=config,
        temporary=temporary,
        relation=relation,
    )


def test_create_table_as_batch_terminates_the_empty_create(target):
    """The create and the load share one EXEC batch, so the create needs its
    own terminator or the batch is a parse error."""
    batch = _render_batch(False, target, _Config())
    normalized = " ".join(batch.split())
    assert "SELECT TOP 0 * INTO" in normalized
    assert "INSERT INTO" in normalized
    create_end = normalized.index("INSERT INTO")
    assert normalized[:create_end].rstrip().endswith(";"), normalized[:create_end]


def test_create_table_as_wraps_the_pair_in_one_escaped_exec(target):
    """Both statements go through the single EXEC that escape_single_quotes
    covers, so the load's OPTION clause cannot break out of the literal."""
    batch = _render_batch(False, target, _Config())
    # The query batch, plus the trailing DROP VIEW - no EXEC of its own for the
    # load, which would sit outside the escaping.
    assert batch.count("EXEC('") == 2
    # The label's quotes are doubled, proving the load text was escaped too.
    assert "''dbt-sqlserver''" in batch
    assert "'dbt-sqlserver'" not in batch.replace("''dbt-sqlserver''", "")


def test_create_table_as_orders_load_before_the_columnstore_index(target):
    """heap_then_index: load the heap, then build the CCI over it."""
    batch = _render_batch(False, target, _Config(as_columnstore=True))
    assert batch.index("INSERT INTO") < batch.index("CREATE CLUSTERED COLUMNSTORE")


def test_create_table_as_temp_build_is_split_too(target):
    """The incremental temp build is the path that most needs the split: it
    already autocommits, so the create's Sch-M goes as soon as it finishes."""
    temp = SQLServerRelation.create(
        database="db", schema="sch", identifier="rel__dbt_tmp", type="table"
    )
    batch = " ".join(_render_batch(True, temp, _Config(contract_enforced=True)).split())
    # Contracts are suppressed for temp builds, so this is the non-contract pair.
    assert "SELECT TOP 0 * INTO" in batch
    assert "INSERT INTO" in batch
    assert "CREATE TABLE" not in batch


# -- lock discipline at the call sites --
#
# Splitting the create from the load only helps if the two statements do not
# share an open transaction: locks are held to commit, not to end-of-statement.
# The call sites earn that with auto_begin=False and explicit commit
# boundaries, and these tests keep it that way (#819).

DML_REFRESH_SQL = CREATE_SQL.parents[2] / "materializations" / "models" / "table"
DML_REFRESH_SQL = DML_REFRESH_SQL / "table_dml_refresh.sql"

SWAP_MARKER = "statement('dml_refresh_swap'"


def _dml_refresh_source():
    source = DML_REFRESH_SQL.read_text()
    assert SWAP_MARKER in source, "the swap statement is the boundary these tests split on"
    before_swap, after_swap = source.split(SWAP_MARKER, 1)
    return source, before_swap, after_swap


def test_dml_refresh_scratch_build_is_split():
    source, _before, _after = _dml_refresh_source()
    assert "sqlserver__get_create_table_empty_sql" in source
    assert "sqlserver__get_tablock_insert_sql" in source
    # The fused form is the bug: one statement that both creates and loads.
    assert "SELECT * INTO {{ refresh_relation }}" not in source


def test_dml_refresh_declines_the_ambient_transaction_until_the_swap():
    """Every statement in the scratch build has to autocommit, or its catalog
    locks are held to the materialization's trailing commit anyway."""
    _source, before_swap, _after = _dml_refresh_source()
    # `call statement(`, so prose mentioning statement('main') is not a match.
    statements = re.findall(r"call statement\((.*?)\)", before_swap, re.DOTALL)
    assert statements, "expected the scratch build to issue statements"
    offenders = [s for s in statements if "auto_begin=False" not in s]
    assert not offenders, (
        "statements before the swap must pass auto_begin=False so they do not "
        f"open the ambient transaction (#819): {offenders}"
    )


MACRO_ROOT = CREATE_SQL.parents[2]
TABLE_SQL = MACRO_ROOT / "materializations" / "models" / "table" / "table.sql"


@pytest.mark.parametrize("macro_file", sorted(MACRO_ROOT.rglob("*.sql")), ids=lambda p: p.name)
def test_no_macro_fuses_a_create_with_its_load(macro_file):
    """`SELECT * INTO <relation>` creates and loads in one statement, holding
    Sch-M on the new object for the length of the load (#819). Build the table
    empty and load it separately - the two macros at the top of create.sql.

    Matches `INTO {{`, so prose describing the fused form does not count.
    """
    offenders = [
        f"  {macro_file.name}:{lineno}: {line.strip()}"
        for lineno, line in enumerate(macro_file.read_text().splitlines(), start=1)
        if re.search(r"(?i)SELECT\s+\*\s+INTO\s+\{\{", line)
    ]
    assert not offenders, (
        "use sqlserver__get_create_table_empty_sql + "
        "sqlserver__get_tablock_insert_sql instead of a fused SELECT * INTO "
        "(#819):\n" + "\n".join(offenders)
    )


def test_table_materialization_builds_outside_the_ambient_transaction():
    """The rename path's build is catalog DDL plus a load; inside the ambient
    transaction it holds the new table's Sch-M through to adapter.commit()."""
    source = TABLE_SQL.read_text()
    assert "call statement('main', auto_begin=False)" in source
    after_build = source.split("call statement('main', auto_begin=False)", 1)[1]
    commit = after_build.find("adapter.commit_if_open()")
    begin = after_build.find("adapter.begin_if_closed()")
    rename = after_build.find("adapter.rename_relation")
    assert -1 < commit < begin < rename, (
        "reopen the transaction after the build and before the renames, so "
        "they keep their semantics and adapter.commit() has a matching BEGIN"
    )


def test_dml_refresh_commits_the_swap_before_the_tail():
    """The DELETE holds X locks on the target until commit; index and mask
    reconciliation must not sit inside that window."""
    _source, _before, after_swap = _dml_refresh_source()
    commit = after_swap.find("adapter.commit_if_open()")
    reconcile = after_swap.find("sqlserver__reconcile_indexes")
    assert commit != -1, "the swap must be committed rather than run to the trailing commit"
    assert reconcile != -1
    assert commit < reconcile, "commit the swap before reconciling indexes"
    # And the tail needs a transaction again, or adapter.commit() raises.
    assert "adapter.begin_if_closed()" in after_swap
