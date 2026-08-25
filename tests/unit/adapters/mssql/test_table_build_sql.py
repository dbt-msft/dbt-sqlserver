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


def test_create_table_as_batch_terminates_the_drop_guard(target):
    """A statement sharing an EXEC literal with the create needs a terminator.

    The create and the load no longer share one (see the stage/load split), so
    the create is now last in its literal and needs nothing after it. The drop
    guard still precedes it there on throwaway builds, though, and an
    unterminated guard makes the literal a parse error.
    """
    throwaway = SQLServerRelation.create(
        database="db", schema="sch", identifier="rel__dbt_tmp", type="table"
    )
    normalized = " ".join(_render_batch(False, throwaway, _Config()).split())
    assert "IF OBJECT_ID(" in normalized
    create_start = normalized.index("SELECT TOP 0 * INTO")
    assert normalized[:create_start].rstrip().endswith(";"), normalized[:create_start]


def test_create_table_as_escapes_every_exec_literal(target):
    """The load's OPTION clause must not break out of its EXEC literal.

    The create and the load used to share one EXEC; since the stage/load split
    they have one each, so the escaping that protects the load now has to come
    from the load half rather than from a shared wrapper. That is the invariant
    worth pinning - the count below only documents which three there are.
    """
    batch = _render_batch(False, target, _Config())
    # stage: the empty create. load: the insert, then the tmp view drop.
    assert batch.count("EXEC('") == 3
    # The label's quotes are doubled, proving the load text was escaped.
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


def test_table_materialization_commits_between_the_create_and_the_load():
    """The create's Sch-M must be released before the load starts.

    Both statements decline to OPEN a transaction, which is not enough on its
    own - auto_begin=False still joins one a pre-hook left open, and then the
    create's Sch-M would be held to commit for the length of the load. The
    commit between them is what actually releases it.
    """
    source = TABLE_SQL.read_text()
    stage = source.find("call statement('create_table_stage', auto_begin=False)")
    assert stage != -1, "the stage half must be its own statement"
    after_stage = source[stage:]
    commit = after_stage.find("adapter.commit_if_open()")
    load = after_stage.find("call statement('main', auto_begin=False)")
    begin = after_stage.find("adapter.begin_if_closed()")
    rename = after_stage.find("adapter.rename_relation")
    assert -1 < commit < load < begin < rename, (
        "commit after the create and before the load, then reopen before the "
        "renames so the cutover is transactional and adapter.commit() has a "
        "matching BEGIN"
    )


def test_table_materialization_writes_the_whole_build_to_the_artifact():
    """statement() writes compiled SQL for 'main' only.

    On the split path 'main' is the load, so target/run/ would hold the INSERT
    without the CREATE that precedes it - and the constraint tests read exactly
    that file. Write both halves back over it.
    """
    source = TABLE_SQL.read_text()
    assert "write(stage_sql ~" in source


def test_scope_gate_is_sampled_before_any_branch_code():
    """transaction_is_open must be read before macros that open one of their own.

    sqlserver__mark_full_refresh_incomplete ends with begin_if_closed and so
    always leaves a transaction open. Sampled after that, the gate answers yes
    for reasons unrelated to any pre-hook, and every full refresh would
    silently take the transaction-spanning path (#819 unfixed, default config).
    """
    source = TABLE_SQL.read_text()
    gate = source.find("adapter.transaction_is_open()")
    pre_hooks = source.find("run_hooks(pre_hooks, inside_transaction=True)")
    first_branch = source.find("{% if use_dml_refresh %}")
    assert -1 < pre_hooks < gate < first_branch, (
        "sample the gate after the in-transaction pre-hooks and before the build branches"
    )


def test_masks_stay_inside_the_cutover_transaction_on_fresh_builds():
    """A brand-new table carries no masks until apply_masks runs.

    If that ran after the cutover committed, a mask failure would leave the
    newly loaded table live with the columns exposed. Index builds move out of
    the transaction; masks on fresh tables must not.
    """
    source = TABLE_SQL.read_text()
    # Anchor past the build's own stage/load commit, which is not the cutover.
    after_swap = source.split(
        "adapter.rename_relation(intermediate_relation, target_relation)", 1
    )[1]
    masks = after_swap.find("apply_masks(target_relation, mask_config)")
    post_hooks = after_swap.find("run_hooks(post_hooks, inside_transaction=True)")
    cutover_commit = after_swap.find("adapter.commit_if_open()")
    assert -1 < masks < post_hooks < cutover_commit, (
        "masks belong inside the cutover transaction, before the in-transaction "
        "post-hooks and the commit that closes the atomic unit"
    )


def test_dml_refresh_leaves_the_swap_transaction_for_the_tail():
    """The macro must not close the swap's transaction itself.

    It used to, which meant an in-transaction post-hook - running back in
    table.sql, after the macro returned - was NOT atomic with the swap it was
    written to accompany. The tail now owns that boundary.
    """
    _source, _before, after_swap = _dml_refresh_source()
    assert "adapter.commit_if_open()" not in after_swap, (
        "the swap's transaction is closed by table.sql after the post-hooks, not inside this macro"
    )
    assert "sqlserver__reconcile_indexes" not in after_swap, (
        "index reconciliation moved to the common tail, outside the transaction"
    )


def test_dml_refresh_swap_locks_do_not_span_reconciliation():
    """The DELETE holds X locks on the target until commit.

    Index and mask reconciliation must sit outside that window, which now means
    after the tail's commit rather than after one inside the macro.
    """
    source = TABLE_SQL.read_text()
    after_swap = source.split("run_hooks(post_hooks, inside_transaction=True)", 1)[1]
    commit = after_swap.find("adapter.commit_if_open()")
    reconcile = after_swap.find("sqlserver__reconcile_indexes")
    assert -1 < commit < reconcile, "commit the cutover before reconciling indexes"


def test_dml_scratch_table_is_dropped_after_the_cutover_commits():
    """Dropping it inside the transaction would put its catalog locks back in
    the window the tail exists to clear."""
    source = TABLE_SQL.read_text()
    after_swap = source.split("run_hooks(post_hooks, inside_transaction=True)", 1)[1]
    commit = after_swap.find("adapter.commit_if_open()")
    drop = after_swap.find("dml_refresh_cleanup_post")
    assert -1 < commit < drop


# -- the stage / load split (#819) --
#
# sqlserver__create_table_as is split at the seam between creating the object
# and loading it, so a caller can put a transaction boundary between the two.
# The halves are the source of truth and create_table_as is their
# concatenation, so the callers that still want one batch (snapshots, the
# incremental temp build) keep getting exactly that.

_SPLIT_STUBS = (
    """
{% macro get_use_database_sql(database) %}USE [{{ database }}];{% endmacro %}
{% macro get_create_view_as_sql(relation, sql) %}
EXEC('CREATE OR ALTER VIEW {{ relation }} AS {{ sql }}')
{%- endmacro %}
{% macro escape_single_quotes(value) %}{{ value | replace("'", "''") }}{% endmacro %}
{% macro get_query_options(parse_options=False) %}"""
    + QUERY_LABEL
    + """{% endmacro %}
{% macro sqlserver__create_clustered_columnstore_index(relation) %}
/* CCI on {{ relation }} */
{%- endmacro %}
"""
)


class _SplitContract:
    def __init__(self, enforced):
        self.enforced = enforced


class _SplitConfig:
    """Minimal stand-in for dbt's `config` context var."""

    def __init__(self, contract_enforced=False, as_columnstore=True):
        self._values = {
            "contract": _SplitContract(contract_enforced),
            "as_columnstore": as_columnstore,
            "full_refresh_build": "heap_then_index",
        }

    def get(self, key, default=None):
        return self._values.get(key, default)


class _SplitAdapter(_Adapter):
    """Adds the render-time side effect the stage half performs."""

    def __init__(self):
        self.dropped = []

    def drop_relation(self, relation):
        self.dropped.append(str(relation))


def _render_split(call, adapter=None, config=None, **context):
    source = _SPLIT_STUBS + _STUBS + CREATE_SQL.read_text() + "\n" + call
    env = jinja2.Environment(
        undefined=jinja2.StrictUndefined,
        extensions=["jinja2.ext.do"],
    )
    return " ".join(
        env.from_string(source)
        .render(
            adapter=adapter or _SplitAdapter(),
            config=config or _SplitConfig(),
            model={"columns": {"id": {}, "my col": {}}},
            **context,
        )
        .split()
    )


def _stage(temporary=False):
    return (
        "{{ sqlserver__get_create_table_stage_sql("
        f"{str(temporary).lower()}, target, 'select 1 as id') }}}}"
    ).replace("}}}}", "}}")


def _load(temporary=False):
    return (
        "{{ sqlserver__get_create_table_load_sql("
        f"{str(temporary).lower()}, target, 'select 1 as id') }}}}"
    ).replace("}}}}", "}}")


def _whole(temporary=False):
    return (
        f"{{{{ sqlserver__create_table_as({str(temporary).lower()}, target, 'select 1 as id') }}}}"
    )


@pytest.mark.parametrize("contract_enforced", [True, False])
def test_create_table_as_is_exactly_stage_then_load(target, contract_enforced):
    """The invariant that keeps every existing caller safe.

    Snapshots and the incremental temp build call create_table_as and run the
    result as one batch. Whatever the split does, their SQL must stay the
    concatenation of the two halves - no statement added, dropped or reordered.
    """
    config = _SplitConfig(contract_enforced=contract_enforced)
    stage = _render_split(_stage(), config=config, target=target)
    load = _render_split(_load(), config=config, target=target)
    whole = _render_split(_whole(), config=config, target=target)
    assert whole == " ".join(f"{stage} {load}".split())


def test_stage_creates_the_view_and_the_empty_table_only(target):
    sql = _render_split(_stage(), target=target)
    assert "CREATE OR ALTER VIEW" in sql
    assert "SELECT TOP 0 * INTO" in sql
    # The load's work belongs to the other half.
    assert "INSERT INTO" not in sql
    assert "CCI" not in sql


def test_load_inserts_drops_the_view_and_builds_the_cci(target):
    sql = _render_split(_load(), target=target)
    assert "INSERT INTO" in sql
    assert "WITH (TABLOCK)" in sql
    assert "DROP VIEW IF EXISTS" in sql
    assert "CCI" in sql
    # Creating the object is the other half's job.
    assert "SELECT TOP 0 * INTO" not in sql
    assert "CREATE OR ALTER VIEW" not in sql


def test_view_drop_follows_the_insert_not_the_create(target):
    """The load reads the view, so the drop cannot stay with the create."""
    sql = _render_split(_load(), target=target)
    assert sql.index("INSERT INTO") < sql.index("DROP VIEW IF EXISTS")


def test_tmp_view_is_dropped_once_by_the_stage_half(target):
    """adapter.drop_relation is a render-time side effect, not emitted SQL.

    It must fire exactly once per build - in the stage half - so rendering the
    halves separately does not drop the view twice, and rendering the whole
    macro does not skip it.
    """
    stage_adapter = _SplitAdapter()
    _render_split(_stage(), adapter=stage_adapter, target=target)
    assert len(stage_adapter.dropped) == 1

    load_adapter = _SplitAdapter()
    _render_split(_load(), adapter=load_adapter, target=target)
    assert load_adapter.dropped == []

    whole_adapter = _SplitAdapter()
    _render_split(_whole(), adapter=whole_adapter, target=target)
    assert len(whole_adapter.dropped) == 1


def test_temporary_build_skips_the_cci(target):
    """as_columnstore never applied to temp builds; that must survive the split."""
    sql = _render_split(_load(temporary=True), target=target)
    assert "CCI" not in sql
    assert "INSERT INTO" in sql
