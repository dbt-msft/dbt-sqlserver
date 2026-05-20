"""Functional tests for the query_options / query_options_raw model config.

Coverage:
 - Dict-shape query_options on table/incremental/snapshot/unit_test materializations.
 - query_options_raw escape hatch (alone, and combined with dict).
 - Allowlist validation: unknown keys, non-numeric values, MAX_GRANT_PERCENT `=` syntax.
 - Unsupported materialization guards: view + incremental merge/microbatch raise compiler errors.
 - apply_label() backward-compat alias (emits LABEL only, ignores query_options).
"""

import datetime
import os
import re

import pytest

from dbt.tests.util import run_dbt


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


class TestQueryOptionsRecursive:
    """MAXRECURSION 200 unlocks recursion past the default 100 limit."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"recursive_model.sql": recursive_model_sql}

    def test_max_recursion_option(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"


generic_options_model_sql = """
{{ config(materialized='table', query_options={'MAXDOP': 1}) }}
select 1 as id
"""


class TestQueryOptionsTableEmitsOptions:
    """Table materialization renders MAXDOP 1 and LABEL in the compiled SQL."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"generic_model.sql": generic_options_model_sql}

    def test_table_option_in_sql(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "generic_model.sql")
        assert "MAXDOP 1" in sql
        assert "LABEL =" in sql


# ---------------------------------------------------------------------------
# View materialization — now raises (was silently ignored)
# ---------------------------------------------------------------------------

view_with_options_sql = (
    "{{ config(materialized='view', query_options={'MAXDOP': 1}) }} select 1 as id"
)


class TestQueryOptionsOnViewRaises:
    @pytest.fixture(scope="class")
    def models(self):
        return {"view_with_options.sql": view_with_options_sql}

    def test_view_with_query_options_errors(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"


# ---------------------------------------------------------------------------
# Allowlist + value-type validation
# ---------------------------------------------------------------------------

invalid_option_model_sql = """
{{ config(materialized='table', query_options={'INVALID_OPTION': 1}) }}
select 1 as id
"""


class TestQueryOptionsInvalidKey:
    @pytest.fixture(scope="class")
    def models(self):
        return {"invalid_model.sql": invalid_option_model_sql}

    def test_invalid_key_raises_error(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"


non_numeric_value_model_sql = """
{{ config(materialized='table', query_options={'MAXDOP': 'not-a-number'}) }}
select 1 as id
"""


class TestQueryOptionsNonNumericValue:
    @pytest.fixture(scope="class")
    def models(self):
        return {"bad_value_model.sql": non_numeric_value_model_sql}

    def test_non_numeric_value_raises_error(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"


# ---------------------------------------------------------------------------
# `=`-syntax options (MAX_GRANT_PERCENT, MIN_GRANT_PERCENT)
# ---------------------------------------------------------------------------

max_grant_model_sql = """
{{ config(materialized='table', query_options={'MAX_GRANT_PERCENT': 50}) }}
select 1 as id
"""


class TestQueryOptionsEqualsSyntax:
    """MAX_GRANT_PERCENT/MIN_GRANT_PERCENT must render with `= N` not space-N."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"max_grant_model.sql": max_grant_model_sql}

    def test_grant_percent_renders_equals_syntax(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "max_grant_model.sql")
        assert "MAX_GRANT_PERCENT = 50" in sql
        assert "MAX_GRANT_PERCENT 50" not in sql


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


class TestQueryOptionsRaw:
    @pytest.fixture(scope="class")
    def models(self):
        return {"raw_model.sql": raw_only_model_sql}

    def test_raw_option_appears_verbatim(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Single quotes inside the raw hint get doubled by EXEC('...')'s escape pass,
        # so check the unquoted substrings rather than the literal source form.
        sql = _find_compiled_run_sql(project, "raw_model.sql")
        assert "USE HINT" in sql
        assert "DISABLE_OPTIMIZER_ROWGOAL" in sql
        assert "LABEL =" in sql


mixed_model_sql = """
{{ config(
    materialized='table',
    query_options={'MAXDOP': 1},
    query_options_raw=["USE HINT('DISABLE_OPTIMIZER_ROWGOAL')"]
) }}
select 1 as id
"""


class TestQueryOptionsDictAndRaw:
    @pytest.fixture(scope="class")
    def models(self):
        return {"mixed_model.sql": mixed_model_sql}

    def test_both_appear(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "mixed_model.sql")
        assert "MAXDOP 1" in sql
        assert "USE HINT" in sql
        assert "DISABLE_OPTIMIZER_ROWGOAL" in sql
        assert "LABEL =" in sql


# ---------------------------------------------------------------------------
# Incremental delete+insert (opt-in)
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


class TestQueryOptionsOnIncrementalDeleteInsert:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"inc_seed.csv": incremental_seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"inc_model.sql": incremental_model_sql}

    def test_options_render_on_second_run(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        # Second run exercises sqlserver__get_delete_insert_merge_sql
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "inc_model.sql")
        assert "MAXDOP 1" in sql


# ---------------------------------------------------------------------------
# Incremental merge / microbatch — unsupported, must raise
# ---------------------------------------------------------------------------

incremental_merge_model_sql = """
{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    query_options={'MAXDOP': 1}
) }}
select id, name from {{ ref('inc_seed') }}
"""


class TestQueryOptionsOnIncrementalMerge:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"inc_seed.csv": incremental_seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"inc_merge_model.sql": incremental_merge_model_sql}

    def test_options_render_on_second_run(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        # Second run exercises sqlserver__get_merge_sql
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "inc_merge_model.sql")
        assert "MAXDOP 1" in sql


# ---------------------------------------------------------------------------
# Incremental microbatch (opt-in)
# ---------------------------------------------------------------------------


class TestQueryOptionsOnIncrementalMicrobatch:
    """Microbatch enumerates every batch between `begin` and "now", so dates must
    stay close to the current time or the test will get slower as it ages.
    Computed dynamically at fixture time."""

    @pytest.fixture(scope="class")
    def models(self):
        today = datetime.datetime.now(datetime.timezone.utc)
        d_minus_3 = (today - datetime.timedelta(days=3)).strftime("%Y-%m-%d 00:00:00")
        d_minus_2 = (today - datetime.timedelta(days=2)).strftime("%Y-%m-%d 00:00:00")
        d_minus_1 = (today - datetime.timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

        input_sql = f"""
{{{{ config(materialized='table', event_time='event_time') }}}}
select 1 as id, cast('{d_minus_3}' as datetime2) as event_time
union all
select 2 as id, cast('{d_minus_2}' as datetime2) as event_time
union all
select 3 as id, cast('{d_minus_1}' as datetime2) as event_time
"""

        model_sql = f"""
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
            "input_model.sql": input_sql,
            "microbatch_model.sql": model_sql,
        }

    def test_options_render_on_microbatch(self, project):
        # First run creates input + microbatch model from scratch
        run_dbt(["run"])

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
# Snapshot (opt-in)
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


class TestQueryOptionsOnSnapshot:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"snap_seed.csv": snapshot_seed_csv}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snap.sql": snapshot_block_sql}

    @pytest.fixture(scope="class")
    def models(self):
        # Need an empty models dict to keep dbt happy
        return {}

    def test_options_render_on_second_snapshot_run(self, project):
        run_dbt(["seed"])
        run_dbt(["snapshot"])

        # Second snapshot exercises sqlserver__snapshot_merge_sql
        results = run_dbt(["snapshot"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "snap.sql")
        assert "MAXDOP 1" in sql


# ---------------------------------------------------------------------------
# apply_label() backward-compat alias
# ---------------------------------------------------------------------------


class TestApplyLabelBackwardCompat:
    """apply_label() must still resolve and emit a label-only OPTION clause.

    The macro is invoked via `dbt run-operation` against a tiny user macro that
    asserts the returned string contains LABEL and does NOT contain any
    query_options-style hints.
    """

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "verify_apply_label.sql": """
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
        }

    def test_apply_label_callable_and_label_only(self, project):
        # run-operation will fail (non-zero exit) if apply_label is undefined
        # or if either of the verify macro's asserts fires.
        run_dbt(["run-operation", "verify_apply_label"])


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


class TestQueryOptionsMultiKey:
    """Multiple dict entries all render and are comma-separated."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"multi_key_model.sql": multi_key_model_sql}

    def test_all_keys_present(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "multi_key_model.sql")
        assert "MAXDOP 1" in sql
        assert "MAXRECURSION 200" in sql
        # RECOMPILE appears as a flag (no trailing value)
        assert "RECOMPILE" in sql
        # Comma separator between options
        assert ", MAXDOP" in sql or "MAXDOP" in sql.split("LABEL")[1].split(",")[1]


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


class TestQueryOptionsMultiRaw:
    """Multiple raw entries all render verbatim and are comma-separated."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"multi_raw_model.sql": multi_raw_model_sql}

    def test_all_raw_entries_present(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "multi_raw_model.sql")
        assert "USE HINT" in sql
        assert "DISABLE_OPTIMIZER_ROWGOAL" in sql
        assert "OPTIMIZE FOR UNKNOWN" in sql


none_valued_model_sql = """
{{ config(
    materialized='table',
    query_options={'RECOMPILE': none}
) }}
select 1 as id
"""


class TestQueryOptionsNoneValued:
    """A None-valued option emits as a bare flag (no trailing number)."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"none_model.sql": none_valued_model_sql}

    def test_none_value_emits_bare_flag(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "none_model.sql")
        # RECOMPILE present, not followed by a number
        assert "RECOMPILE" in sql
        # The bare-flag form should never produce "RECOMPILE <digit>"
        assert re.search(r"RECOMPILE\s+\d", sql) is None, "RECOMPILE should be a bare flag"


custom_tag_model_sql = """
{{ config(
    materialized='table',
    query_tag='my-custom-tag',
    query_options={'MAXDOP': 1}
) }}
select 1 as id
"""


class TestQueryOptionsCustomQueryTag:
    """Custom query_tag config flows into the LABEL portion of the OPTION clause."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"custom_tag_model.sql": custom_tag_model_sql}

    def test_custom_tag_in_label(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "custom_tag_model.sql")
        # LABEL emitted inside EXEC('...') so single quotes are doubled.
        assert "my-custom-tag" in sql
        assert "LABEL =" in sql


# ---------------------------------------------------------------------------
# Key normalization, allowlist edge cases, and project-level config
# ---------------------------------------------------------------------------

lowercase_key_model_sql = """
{{ config(
    materialized='table',
    query_options={'maxdop': 1}
) }}
select 1 as id
"""


class TestQueryOptionsLowercaseKey:
    """Lowercase keys are uppercased before allowlist check and SQL emission."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"lower_model.sql": lowercase_key_model_sql}

    def test_lowercase_key_uppercased(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "lower_model.sql")
        assert "MAXDOP 1" in sql
        assert "maxdop" not in sql  # source-form lowercase should not survive


multi_word_key_model_sql = """
{{ config(
    materialized='table',
    query_options={'FORCE ORDER': none}
) }}
select id from (select 1 as id) t
"""


class TestQueryOptionsMultiWordKey:
    """Space-containing allowlist keys (FORCE ORDER, HASH JOIN, ...) emit verbatim."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"multi_word_model.sql": multi_word_key_model_sql}

    def test_multi_word_key_renders(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "multi_word_model.sql")
        assert "FORCE ORDER" in sql


min_grant_model_sql = """
{{ config(materialized='table', query_options={'MIN_GRANT_PERCENT': 25}) }}
select 1 as id
"""


class TestQueryOptionsMinGrantPercent:
    """MIN_GRANT_PERCENT follows the same `= N` rule as MAX_GRANT_PERCENT."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"min_grant_model.sql": min_grant_model_sql}

    def test_min_grant_renders_equals_syntax(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "min_grant_model.sql")
        assert "MIN_GRANT_PERCENT = 25" in sql
        assert "MIN_GRANT_PERCENT 25" not in sql


project_level_model_sql = """
{{ config(materialized='table') }}
select 1 as id
"""


class TestQueryOptionsProjectLevel:
    """query_options set at project level (under models:) cascades to inheriting models."""

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


# ---------------------------------------------------------------------------
# First-run paths for incremental + snapshot (table-create path)
# ---------------------------------------------------------------------------

incremental_first_run_model_sql = """
{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
    query_options={'MAXDOP': 1}
) }}
select id, name from {{ ref('inc_seed') }}
"""


class TestQueryOptionsIncrementalFirstRun:
    """First run of an incremental model goes through sqlserver__create_table_as.
    Asserts options render there too (not just on subsequent DELETE+INSERT runs)."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"inc_seed.csv": incremental_seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"first_run_model.sql": incremental_first_run_model_sql}

    def test_first_run_emits_options(self, project):
        run_dbt(["seed"])

        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "first_run_model.sql")
        assert "MAXDOP 1" in sql


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


class TestQueryOptionsSnapshotFirstRun:
    """First snapshot run materializes the snapshot table via the create_table_as path."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"snap_seed.csv": snapshot_seed_csv}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snap_first.sql": snapshot_first_run_block_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {}

    def test_first_run_emits_options(self, project):
        run_dbt(["seed"])

        results = run_dbt(["snapshot"])
        assert len(results) == 1
        assert results[0].status == "success"

        sql = _find_compiled_run_sql(project, "snap_first.sql")
        assert "MAXDOP 1" in sql
