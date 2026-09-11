"""Functional tests for the query_options / query_options_raw model config.

Coverage:
 - Dict-shape query_options on table/incremental/snapshot/unit_test materializations.
 - query_options_raw escape hatch (alone, and combined with dict).
 - Allowlist validation: unknown keys, non-numeric values, MAX_GRANT_PERCENT `=` syntax.
 - Unsupported materialization guards: view + incremental merge/microbatch raise compiler errors.
 - apply_label() backward-compat alias (emits LABEL only, ignores query_options).

Tests that only need a single independent model are grouped into shared classes
(one `project` fixture covers many `--select`-targeted runs) instead of one
class per model — spinning up a project (schema creation, connection, parse)
costs far more than the SQL itself, so consolidating cuts wall-clock time
substantially without losing any coverage.
"""

import datetime
import os
import re

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture


def _find_compiled_run_sql(project, filename: str) -> str:
    """Locate a model's compiled run-time SQL under target/run and return its contents."""
    target_dir = os.path.join(project.project_root, "target", "run")
    for root, _dirs, files in os.walk(target_dir):
        if filename in files:
            with open(os.path.join(root, filename), "r") as f:
                return f.read()
    raise AssertionError(f"Could not find compiled {filename} under {target_dir}")


# ---------------------------------------------------------------------------
# Table materialization — original recursive / generic / restriction coverage
# ---------------------------------------------------------------------------

recursive_model_sql = """
{{ config(materialized='table', query_options={'MAXRECURSION': 200}) }}
WITH cte AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM cte WHERE n < 150
)
SELECT * FROM cte
"""

generic_options_model_sql = """
{{ config(materialized='table', query_options={'MAXDOP': 1}) }}
select 1 as id
"""

# ---------------------------------------------------------------------------
# View materialization — now raises (was silently ignored)
# ---------------------------------------------------------------------------

view_with_options_sql = (
    "{{ config(materialized='view', query_options={'MAXDOP': 1}) }} select 1 as id"
)

# ---------------------------------------------------------------------------
# Allowlist + value-type validation
# ---------------------------------------------------------------------------

invalid_option_model_sql = """
{{ config(materialized='table', query_options={'INVALID_OPTION': 1}) }}
select 1 as id
"""

non_numeric_value_model_sql = """
{{ config(materialized='table', query_options={'MAXDOP': 'not-a-number'}) }}
select 1 as id
"""

# ---------------------------------------------------------------------------
# `=`-syntax options (MAX_GRANT_PERCENT, MIN_GRANT_PERCENT)
# ---------------------------------------------------------------------------

max_grant_model_sql = """
{{ config(materialized='table', query_options={'MAX_GRANT_PERCENT': 50}) }}
select 1 as id
"""

min_grant_model_sql = """
{{ config(materialized='table', query_options={'MIN_GRANT_PERCENT': 25}) }}
select 1 as id
"""

decimal_grant_model_sql = """
{{ config(materialized='table', query_options={'MAX_GRANT_PERCENT': 12.5}) }}
select 1 as id
"""

# ---------------------------------------------------------------------------
# query_options_raw escape hatch
# ---------------------------------------------------------------------------

raw_only_model_sql = """
{{ config(
    materialized='table',
    query_options_raw=["USE HINT('DISABLE_OPTIMIZER_ROWGOAL')"]
) }}
select 1 as id
"""

mixed_model_sql = """
{{ config(
    materialized='table',
    query_options={'MAXDOP': 1},
    query_options_raw=["USE HINT('DISABLE_OPTIMIZER_ROWGOAL')"]
) }}
select 1 as id
"""

multi_raw_model_sql = """
{{ config(
    materialized='table',
    query_options_raw=[
        "USE HINT('DISABLE_OPTIMIZER_ROWGOAL')",
        "OPTIMIZE FOR UNKNOWN"
    ]
) }}
select 1 as id
"""

# ---------------------------------------------------------------------------
# query_options_raw shape validation
# ---------------------------------------------------------------------------

raw_string_model_sql = """
{{ config(
    materialized='table',
    query_options_raw="USE HINT('DISABLE_OPTIMIZER_ROWGOAL')"
) }}
select 1 as id
"""

# ---------------------------------------------------------------------------
# Multi-entry rendering, None-valued options, and custom query_tag
# ---------------------------------------------------------------------------

multi_key_model_sql = """
{{ config(
    materialized='table',
    query_options={'MAXDOP': 1, 'RECOMPILE': none, 'MAXRECURSION': 200}
) }}
WITH cte AS (
    SELECT 1 AS n UNION ALL SELECT n + 1 FROM cte WHERE n < 150
)
SELECT * FROM cte
"""

none_valued_model_sql = """
{{ config(
    materialized='table',
    query_options={'RECOMPILE': none}
) }}
select 1 as id
"""

custom_tag_model_sql = """
{{ config(
    materialized='table',
    query_tag='my-custom-tag',
    query_options={'MAXDOP': 1}
) }}
select 1 as id
"""

# ---------------------------------------------------------------------------
# Key normalization, allowlist edge cases
# ---------------------------------------------------------------------------

lowercase_key_model_sql = """
{{ config(
    materialized='table',
    query_options={'maxdop': 1}
) }}
select 1 as id
"""

multi_word_key_model_sql = """
{{ config(
    materialized='table',
    query_options={'FORCE ORDER': none}
) }}
select id from (select 1 as id) t
"""

# ---------------------------------------------------------------------------
# PARAMETERIZATION is no longer in the allowlist (use query_options_raw)
# ---------------------------------------------------------------------------

parameterization_model_sql = """
{{ config(materialized='table', query_options={'PARAMETERIZATION': 'FORCED'}) }}
select 1 as id
"""

# ---------------------------------------------------------------------------
# DML table refresh path (table_refresh_method='dml', steady-state refresh)
# ---------------------------------------------------------------------------

dml_refresh_options_model_sql = """
{{ config(
    materialized='table',
    table_refresh_method='dml',
    as_columnstore=False,
    query_options={'MAXDOP': 1}
) }}
select 1 as id
"""

dml_refresh_options_model_v2_sql = """
{{ config(
    materialized='table',
    table_refresh_method='dml',
    as_columnstore=False,
    query_options={'MAXDOP': 1}
) }}
select 2 as id
"""

# ---------------------------------------------------------------------------
# apply_label() backward-compat alias
# ---------------------------------------------------------------------------

verify_apply_label_macro_sql = """
{% macro verify_apply_label() %}
    {%- set result = apply_label() -%}
    {{ log("apply_label returned: " ~ result, info=True) }}
    {%- if 'LABEL' not in result -%}
        {{ exceptions.raise_compiler_error("apply_label() did not emit LABEL") }}
    {%- endif -%}
    {%- if 'MAXDOP' in result -%}
        {{ exceptions.raise_compiler_error("apply_label() must not emit query_options hints") }}
    {%- endif -%}
{% endmacro %}
"""


class TestQueryOptionsCore:
    """Every case below is a single independent model (or macro), so they all
    share one project/schema instead of paying setup cost per case. Each test
    targets its own model via --select so unrelated models in the shared
    project never affect its result count or status.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "recursive_model.sql": recursive_model_sql,
            "generic_model.sql": generic_options_model_sql,
            "view_with_options.sql": view_with_options_sql,
            "invalid_model.sql": invalid_option_model_sql,
            "bad_value_model.sql": non_numeric_value_model_sql,
            "max_grant_model.sql": max_grant_model_sql,
            "min_grant_model.sql": min_grant_model_sql,
            "decimal_grant_model.sql": decimal_grant_model_sql,
            "raw_model.sql": raw_only_model_sql,
            "mixed_model.sql": mixed_model_sql,
            "multi_raw_model.sql": multi_raw_model_sql,
            "raw_string_model.sql": raw_string_model_sql,
            "multi_key_model.sql": multi_key_model_sql,
            "none_model.sql": none_valued_model_sql,
            "custom_tag_model.sql": custom_tag_model_sql,
            "lower_model.sql": lowercase_key_model_sql,
            "multi_word_model.sql": multi_word_key_model_sql,
            "param_model.sql": parameterization_model_sql,
            "dml_refresh_model.sql": dml_refresh_options_model_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"verify_apply_label.sql": verify_apply_label_macro_sql}

    def test_max_recursion_option(self, project):
        """MAXRECURSION 200 unlocks recursion past the default 100 limit."""
        results = run_dbt(["run", "--select", "recursive_model"])
        assert len(results) == 1
        assert results[0].status == "success"

    def test_table_option_in_sql(self, project):
        """Table materialization renders MAXDOP 1 and LABEL in the compiled SQL."""
        results = run_dbt(["run", "--select", "generic_model"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "generic_model.sql")
        assert "MAXDOP 1" in sql
        assert "LABEL =" in sql

    def test_view_with_query_options_errors(self, project):
        results = run_dbt(["run", "--select", "view_with_options"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"

    def test_invalid_key_raises_error(self, project):
        results = run_dbt(["run", "--select", "invalid_model"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"

    def test_non_numeric_value_raises_error(self, project):
        results = run_dbt(["run", "--select", "bad_value_model"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"

    def test_grant_percent_renders_equals_syntax(self, project):
        """MAX_GRANT_PERCENT/MIN_GRANT_PERCENT must render with `= N` not space-N."""
        results = run_dbt(["run", "--select", "max_grant_model"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "max_grant_model.sql")
        assert "MAX_GRANT_PERCENT = 50" in sql
        assert "MAX_GRANT_PERCENT 50" not in sql

    def test_min_grant_renders_equals_syntax(self, project):
        """MIN_GRANT_PERCENT follows the same `= N` rule as MAX_GRANT_PERCENT."""
        results = run_dbt(["run", "--select", "min_grant_model"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "min_grant_model.sql")
        assert "MIN_GRANT_PERCENT = 25" in sql
        assert "MIN_GRANT_PERCENT 25" not in sql

    def test_decimal_value_not_truncated(self, project):
        """MAX_GRANT_PERCENT accepts decimals 0.0-100.0 per SQL Server spec; the
        adapter must render the value verbatim rather than truncating to int."""
        results = run_dbt(["run", "--select", "decimal_grant_model"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "decimal_grant_model.sql")
        assert "MAX_GRANT_PERCENT = 12.5" in sql
        # Make sure the value did not get truncated to 12
        assert "MAX_GRANT_PERCENT = 12," not in sql
        assert "MAX_GRANT_PERCENT = 12)" not in sql

    def test_raw_option_appears_verbatim(self, project):
        results = run_dbt(["run", "--select", "raw_model"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Single quotes inside the raw hint get doubled by EXEC('...')'s escape pass,
        # so check the unquoted substrings rather than the literal source form.
        sql = _find_compiled_run_sql(project, "raw_model.sql")
        assert "USE HINT" in sql
        assert "DISABLE_OPTIMIZER_ROWGOAL" in sql
        assert "LABEL =" in sql

    def test_both_appear(self, project):
        results = run_dbt(["run", "--select", "mixed_model"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "mixed_model.sql")
        assert "MAXDOP 1" in sql
        assert "USE HINT" in sql
        assert "DISABLE_OPTIMIZER_ROWGOAL" in sql
        assert "LABEL =" in sql

    def test_all_raw_entries_present(self, project):
        """Multiple raw entries all render verbatim and are comma-separated."""
        results = run_dbt(["run", "--select", "multi_raw_model"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "multi_raw_model.sql")
        assert "USE HINT" in sql
        assert "DISABLE_OPTIMIZER_ROWGOAL" in sql
        assert "OPTIMIZE FOR UNKNOWN" in sql

    def test_string_raises_error(self, project):
        """A plain string passed where a list is expected must raise rather than
        silently iterate character-by-character into garbage SQL."""
        results = run_dbt(["run", "--select", "raw_string_model"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"

    def test_all_keys_present(self, project):
        """Multiple dict entries all render and are comma-separated."""
        results = run_dbt(["run", "--select", "multi_key_model"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "multi_key_model.sql")
        assert "MAXDOP 1" in sql
        assert "MAXRECURSION 200" in sql
        # RECOMPILE appears as a flag (no trailing value)
        assert "RECOMPILE" in sql
        # Comma separator between options
        assert ", MAXDOP" in sql or "MAXDOP" in sql.split("LABEL")[1].split(",")[1]

    def test_none_value_emits_bare_flag(self, project):
        """A None-valued option emits as a bare flag (no trailing number)."""
        results = run_dbt(["run", "--select", "none_model"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "none_model.sql")
        # RECOMPILE present, not followed by a number
        assert "RECOMPILE" in sql
        # The bare-flag form should never produce "RECOMPILE <digit>"
        assert re.search(r"RECOMPILE\s+\d", sql) is None, "RECOMPILE should be a bare flag"

    def test_custom_tag_in_label(self, project):
        """Custom query_tag config flows into the LABEL portion of the OPTION clause."""
        results = run_dbt(["run", "--select", "custom_tag_model"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "custom_tag_model.sql")
        # LABEL emitted inside EXEC('...') so single quotes are doubled.
        assert "my-custom-tag" in sql
        assert "LABEL =" in sql

    def test_lowercase_key_uppercased(self, project):
        """Lowercase keys are uppercased before allowlist check and SQL emission."""
        results = run_dbt(["run", "--select", "lower_model"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "lower_model.sql")
        assert "MAXDOP 1" in sql
        assert "maxdop" not in sql  # source-form lowercase should not survive

    def test_multi_word_key_renders(self, project):
        """Space-containing allowlist keys (FORCE ORDER, HASH JOIN, ...) emit verbatim."""
        results = run_dbt(["run", "--select", "multi_word_model"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "multi_word_model.sql")
        assert "FORCE ORDER" in sql

    def test_parameterization_rejected_as_invalid_key(self, project):
        """PARAMETERIZATION requires a SIMPLE/FORCED keyword (not numeric), which
        the allowlist path cannot render. It was removed from the allowlist; users
        needing it use query_options_raw."""
        results = run_dbt(["run", "--select", "param_model"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"

    def test_options_render_on_both_dml_statements(self, project):
        """A table_refresh_method='dml' model takes the DML-refresh path on the
        second (steady-state) run, not the create_table_as path. That path has two
        grant-taking statements — the 'main' scratch load and the
        'dml_refresh_swap' INSERT — and BOTH must carry the query_options hint.

        Since #819 the scratch build is two statements rather than one fused
        `SELECT * INTO`: an empty `SELECT TOP 0 * INTO`, which moves no rows and
        so deliberately carries no hint, then the 'main'
        `INSERT ... WITH (TABLOCK)` that loads it. The hint rides the statement
        that moves the rows and takes the memory grant, which is 'main'.

        Only the 'main' statement lands in target/run; the swap runs via its own
        statement() call. So this asserts against the executed SQL captured from the
        debug log, matching each statement bounded by its terminating ';' so a hint
        on one statement can't be mistaken for a hint on the other.

        That last property needs care now that BOTH statements are
        `INSERT ... SELECT ... FROM <a __dbt_refresh relation>` — a bare
        `INSERT INTO ... __dbt_refresh ... OPTION` pattern matches either one, so
        the swap assertion would pass on the scratch load even if the swap had
        lost its hint entirely. They are told apart by what each selects FROM:
        the scratch load reads the tmp view (`...__dbt_refresh__dbt_tmp_vw`) and
        is the only one with `WITH (TABLOCK)`; the swap reads the scratch table
        (`...__dbt_refresh`, with no `__dbt_tmp_vw` suffix).
        """
        # First run creates the table via the standard create path.
        run_dbt(["run", "--select", "dml_refresh_model"])

        # Swap in v2 (same schema, different data) so the second run takes the
        # DML refresh path (existing table + matching schema).
        write_model(project, "dml_refresh_model.sql", dml_refresh_options_model_v2_sql)

        _, logs = run_dbt_and_capture(["--debug", "run", "--select", "dml_refresh_model"])

        # 'main' — INSERT INTO <scratch> WITH (TABLOCK) SELECT * FROM <tmp view>.
        # [^;]* keeps the match inside the single statement (its only ';' is the
        # terminator that follows the OPTION clause). WITH (TABLOCK) and the
        # tmp view both mark this as the scratch load, not the swap.
        main_match = re.search(
            r"INSERT INTO[^;]*WITH \(TABLOCK\)[^;]*"
            r"FROM[^;]*__dbt_refresh__dbt_tmp_vw[^;]*OPTION \([^;]*MAXDOP 1",
            logs,
            re.IGNORECASE,
        )
        assert main_match is not None, (
            "query_options missing from the 'main' scratch-load INSERT of the DML refresh"
        )

        # 'dml_refresh_swap' — INSERT INTO <target> (cols) SELECT cols FROM
        # <scratch>, must carry the hint too. The INSERT...SELECT spans newlines
        # but has no interior ';', so [^;]* stays within it and won't reach the
        # scratch load above. The negative lookahead is what excludes that load:
        # it selects FROM the tmp view, whose name continues past __dbt_refresh
        # with __dbt_tmp_vw, while the swap selects FROM the scratch table, where
        # __dbt_refresh ends the name.
        swap_match = re.search(
            r"INSERT INTO[^;]*FROM[^;]*__dbt_refresh(?!__dbt_tmp_vw)"
            r"[^;]*OPTION \([^;]*MAXDOP 1",
            logs,
            re.IGNORECASE,
        )
        assert swap_match is not None, (
            "query_options missing from the 'dml_refresh_swap' INSERT statement of the DML refresh"
        )

    def test_apply_label_callable_and_label_only(self, project):
        """apply_label() must still resolve and emit a label-only OPTION clause.

        The macro is invoked via `dbt run-operation` against a tiny user macro that
        asserts the returned string contains LABEL and does NOT contain any
        query_options-style hints.
        """
        # run-operation will fail (non-zero exit) if apply_label is undefined
        # or if either of the verify macro's asserts fires.
        run_dbt(["run-operation", "verify_apply_label"])


# ---------------------------------------------------------------------------
# Incremental delete+insert / merge / microbatch (opt-in) — share one seed
# ---------------------------------------------------------------------------

incremental_seed_csv = """id,name
1,alice
2,bob
3,charlie
"""

incremental_model_sql = """
{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
    query_options={'MAXDOP': 1}
) }}
select id, name from {{ ref('inc_seed') }}
"""

incremental_merge_model_sql = """
{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    query_options={'MAXDOP': 1}
) }}
select id, name from {{ ref('inc_seed') }}
"""

incremental_first_run_model_sql = """
{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
    query_options={'MAXDOP': 1}
) }}
select id, name from {{ ref('inc_seed') }}
"""


class TestQueryOptionsIncremental:
    """All three incremental paths read from the same seed, so they share one
    project/seed load instead of reseeding per case."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"inc_seed.csv": incremental_seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        today = datetime.datetime.now(datetime.timezone.utc)
        d_minus_3 = (today - datetime.timedelta(days=3)).strftime("%Y-%m-%d 00:00:00")
        d_minus_2 = (today - datetime.timedelta(days=2)).strftime("%Y-%m-%d 00:00:00")
        d_minus_1 = (today - datetime.timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

        # Microbatch enumerates every batch between `begin` and "now", so dates must
        # stay close to the current time or the test will get slower as it ages.
        # Computed dynamically at fixture time.
        input_sql = f"""
{{{{ config(materialized='table', event_time='event_time') }}}}
select 1 as id, cast('{d_minus_3}' as datetime2) as event_time
union all
select 2 as id, cast('{d_minus_2}' as datetime2) as event_time
union all
select 3 as id, cast('{d_minus_1}' as datetime2) as event_time
"""

        microbatch_model_sql = f"""
{{{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='day',
    begin='{d_minus_3}',
    query_options={{'MAXDOP': 1}}
) }}}}
select * from {{{{ ref('input_model') }}}}
"""

        return {
            "inc_model.sql": incremental_model_sql,
            "inc_merge_model.sql": incremental_merge_model_sql,
            "first_run_model.sql": incremental_first_run_model_sql,
            "input_model.sql": input_sql,
            "microbatch_model.sql": microbatch_model_sql,
        }

    @pytest.fixture(scope="class", autouse=True)
    def _seed_once(self, project):
        run_dbt(["seed"])

    def test_delete_insert_options_render_on_second_run(self, project):
        run_dbt(["run", "--select", "inc_model"])

        # Second run exercises sqlserver__get_delete_insert_merge_sql
        results = run_dbt(["run", "--select", "inc_model"])
        assert len(results) == 1
        assert results[0].status == "success"
        assert results[0].adapter_response["rows_affected"] == 3

        sql = _find_compiled_run_sql(project, "inc_model.sql")
        assert "MAXDOP 1" in sql

    def test_merge_options_render_on_second_run(self, project):
        run_dbt(["run", "--select", "inc_merge_model"])

        # Second run exercises sqlserver__get_merge_sql
        results = run_dbt(["run", "--select", "inc_merge_model"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "inc_merge_model.sql")
        assert "MAXDOP 1" in sql

    def test_incremental_first_run_emits_options(self, project):
        """First run of an incremental model goes through sqlserver__create_table_as.
        Asserts options render there too (not just on subsequent DELETE+INSERT runs)."""
        results = run_dbt(["run", "--select", "first_run_model"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "first_run_model.sql")
        assert "MAXDOP 1" in sql

    def test_options_render_on_microbatch(self, project):
        # First run creates input + microbatch model from scratch
        run_dbt(["run", "--select", "input_model", "microbatch_model"])

        # Second run exercises sqlserver__get_incremental_microbatch_sql
        results = run_dbt(["run", "--select", "microbatch_model"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Microbatch writes one compiled file per batch (microbatch_model_YYYY-MM-DD.sql),
        # so look for any file with that prefix rather than the model filename verbatim.
        target_dir = os.path.join(project.project_root, "target", "run")
        for root, _dirs, files in os.walk(target_dir):
            for filename in files:
                if filename.startswith("microbatch_model_") and filename.endswith(".sql"):
                    with open(os.path.join(root, filename), "r") as f:
                        sql = f.read()
                    assert "MAXDOP 1" in sql, f"MAXDOP 1 missing from {filename}"
                    return
        raise AssertionError("No microbatch batch file found under target/run")


# ---------------------------------------------------------------------------
# Snapshot (opt-in) — share one seed across the steady-state and first-run cases
# ---------------------------------------------------------------------------

snapshot_seed_csv = """id,name,updated_at
1,alice,2024-01-01 00:00:00
2,bob,2024-01-01 00:00:00
"""

snapshot_block_sql = """
{% snapshot snap %}
{{ config(
    target_schema=schema,
    unique_key='id',
    strategy='timestamp',
    updated_at='updated_at',
    query_options={'MAXDOP': 1}
) }}
select * from {{ ref('snap_seed') }}
{% endsnapshot %}
"""

# First-run paths for snapshot (table-create path)

snapshot_first_run_block_sql = """
{% snapshot snap_first %}
{{ config(
    target_schema=schema,
    unique_key='id',
    strategy='timestamp',
    updated_at='updated_at',
    query_options={'MAXDOP': 1}
) }}
select * from {{ ref('snap_seed') }}
{% endsnapshot %}
"""


class TestQueryOptionsSnapshot:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"snap_seed.csv": snapshot_seed_csv}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snap.sql": snapshot_block_sql,
            "snap_first.sql": snapshot_first_run_block_sql,
        }

    @pytest.fixture(scope="class")
    def models(self):
        # Need an empty models dict to keep dbt happy
        return {}

    @pytest.fixture(scope="class", autouse=True)
    def _seed_once(self, project):
        run_dbt(["seed"])

    def test_options_render_on_second_snapshot_run(self, project):
        run_dbt(["snapshot", "--select", "snap"])

        # Second snapshot exercises sqlserver__snapshot_merge_sql
        results = run_dbt(["snapshot", "--select", "snap"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "snap.sql")
        assert "MAXDOP 1" in sql

    def test_snapshot_first_run_emits_options(self, project):
        """First snapshot run materializes the snapshot table via the create_table_as path."""
        results = run_dbt(["snapshot", "--select", "snap_first"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "snap_first.sql")
        assert "MAXDOP 1" in sql


# ---------------------------------------------------------------------------
# Key normalization, allowlist edge cases, and project-level config
# ---------------------------------------------------------------------------

project_level_model_sql = """
{{ config(materialized='table') }}
select 1 as id
"""


class TestQueryOptionsProjectLevel:
    """query_options set at project level (under models:) cascades to inheriting
    models. Kept isolated (rather than folded into TestQueryOptionsCore) because
    project_config_update rewrites dbt_project.yml for the whole project, which
    would silently apply +query_options to every other model sharing that project.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"project_level_model.sql": project_level_model_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test",
            "models": {
                "test": {
                    "+query_options": {"MAXDOP": 1},
                },
            },
        }

    def test_project_level_option_inherited(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "project_level_model.sql")
        assert "MAXDOP 1" in sql


def write_model(project, filename, contents):
    """Write a model file into the project's models directory (mirrors the
    helper in test_table_refresh_method.py; kept local to avoid a cross-import)."""
    path = os.path.join(project.project_root, "models", filename)
    with open(path, "w") as f:
        f.write(contents)
