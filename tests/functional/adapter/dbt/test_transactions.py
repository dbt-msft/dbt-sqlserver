import pytest

from dbt.tests.util import run_dbt


class BaseTransactionsEnabled:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"dbt_sqlserver_use_dbt_transactions": True}}


_incremental_model_sql = """
{{ config(materialized='incremental', unique_key='id') }}
select 1 as id, 'first' as value
{% if is_incremental() %}
union all
select 2 as id, 'second' as value
{% endif %}
"""

_failing_model_sql = """
{{ config(
    materialized='table',
    pre_hook=[
        "INSERT INTO {{ this.schema }}.audit_log "
        "(msg, created_at) VALUES ('from_model', getdate())"
    ]
) }}
select 1/0 as boom
"""

# The snapshot source is sized deliberately. A snapshot's second run probes the
# shape of its staging query through get_column_schema_from_query
# (check_time_data_types -> get_updated_at_column_data_type), and that query
# starts with a CTE, so sqlserver__get_empty_subquery_sql cannot wrap it in
# `where 1 = 0` and it runs in full. If the cursor holding that result set is
# abandoned while the server is still working on the request, the driver
# cancels it; the attention that sends rolls back the open transaction under
# `SET XACT_ABORT ON`, silently, taking the staging table with it.
#
# The failure is a race rather than a size threshold -- measured against SQL
# Server 2022, a few hundred rows is already enough at zero client delay, while
# ~10ms of client-side work before the close makes even 20MB safe -- so there is
# no constant to encode. These fixtures sit megabytes past the boundary so the
# server is unambiguously still streaming, whatever the runner's speed. A
# handful of narrow rows (which is what a seed gives you) never trips it, which
# is why this went unnoticed.
_SNAPSHOT_ROWS = 5000
_SNAPSHOT_PAYLOAD_WIDTH = 500

_snapshot_source_sql = """
{{ config(materialized='table') }}
select top (%d)
    row_number() over (order by (select null)) as id,
    cast(replicate('{{ var("payload_char", "x") }}', %d) as varchar(8000)) as payload,
    cast('{{ var("snap_updated_at", "2024-01-01") }}' as datetime2) as updated_at
from sys.all_objects a cross join sys.all_objects b
""" % (_SNAPSHOT_ROWS, _SNAPSHOT_PAYLOAD_WIDTH)

_snapshot_sql = """
{% snapshot snap %}
{{ config(
    target_schema=schema,
    unique_key='id',
    strategy='timestamp',
    updated_at='updated_at',
) }}
select * from {{ ref('snap_source') }}
{% endsnapshot %}
"""

_snapshot_check_sql = """
{% snapshot snap_check %}
{{ config(
    target_schema=schema,
    unique_key='id',
    strategy='check',
    check_cols=['payload'],
) }}
select * from {{ ref('snap_source') }}
{% endsnapshot %}
"""


class TestTransactionsEnabled(BaseTransactionsEnabled):
    """All cases below use the same project_config_update (transactions on)
    and target their own model/snapshot via an explicit --models/--select, so
    they share one project instead of paying setup cost per case."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": """
{{ config(materialized='table') }}
select 1 as id, 'hello' as name
""",
            "view_model.sql": """
{{ config(materialized='view') }}
select 42 as answer
""",
            "incremental_model.sql": _incremental_model_sql,
            "good_model.sql": """
{{ config(materialized='table') }}
select 1 as id
""",
            "bad_model.sql": """
{{ config(materialized='table') }}
select 1/0 as boom
""",
            "failing_model.sql": _failing_model_sql,
            "snap_source.sql": _snapshot_source_sql,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snap.sql": _snapshot_sql, "snap_check.sql": _snapshot_check_sql}

    def test_table_materialization(self, project):
        results = run_dbt(["run", "--models", "table_model"])
        assert len(results) == 1

        rows = project.run_sql("select id, name from {schema}.table_model", fetch="all")
        assert len(rows) == 1
        assert rows[0][0] == 1
        assert rows[0][1] == "hello"

    def test_view_materialization(self, project):
        results = run_dbt(["run", "--models", "view_model"])
        assert len(results) == 1

        rows = project.run_sql("select answer from {schema}.view_model", fetch="all")
        assert len(rows) == 1
        assert rows[0][0] == 42

    def test_incremental_materialization(self, project):
        results = run_dbt(["run", "--models", "incremental_model"])
        assert len(results) == 1

        rows = project.run_sql(
            "select count(*) as cnt from {schema}.incremental_model", fetch="one"
        )
        assert rows[0] == 1

        results = run_dbt(["run", "--models", "incremental_model"])
        assert len(results) == 1

        rows = project.run_sql(
            "select count(*) as cnt from {schema}.incremental_model", fetch="one"
        )
        assert rows[0] == 2

    def test_failed_then_successful_run(self, project):
        results = run_dbt(["run", "-m", "bad_model"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"

        results = run_dbt(["run", "-m", "good_model"])
        assert len(results) == 1
        assert results[0].status == "success"

        rows = project.run_sql("select id from {schema}.good_model", fetch="all")
        assert len(rows) == 1
        assert rows[0][0] == 1

    def test_side_effect_rolled_back(self, project):
        project.run_sql("CREATE TABLE {schema}.audit_log (msg varchar(100), created_at datetime)")
        run_dbt(["run", "-m", "failing_model"], expect_pass=False)
        rows = project.run_sql("SELECT COUNT(*) FROM {schema}.audit_log", fetch="one")
        assert rows[0] == 0

    def test_snapshot_create_and_merge(self, project):
        """Timestamp strategy, with a second run that has changes to write.

        The merge reads the staging table built earlier in the same
        transaction, so a probe that silently rolls that transaction back
        surfaces here as `Invalid object name '..._dbt_tmp'`.
        """
        run_dbt(["run", "--models", "snap_source"])
        results = run_dbt(["snapshot", "--select", "snap"])
        assert len(results) == 1
        assert results[0].status == "success"

        rows = project.run_sql("select count(*) from {schema}.snap", fetch="one")
        assert rows[0] == _SNAPSHOT_ROWS

        # move every row's updated_at forward, so the second run has a full
        # changeset to stage and merge rather than converging to zero rows
        run_dbt(["run", "--models", "snap_source", "--vars", "snap_updated_at: '2024-06-01'"])
        results = run_dbt(["snapshot", "--select", "snap"])
        assert len(results) == 1
        assert results[0].status == "success"

        rows = project.run_sql("select count(*) from {schema}.snap", fetch="one")
        assert rows[0] == _SNAPSHOT_ROWS * 2

    def test_snapshot_check_strategy_create_and_merge(self, project):
        """Same path as above via the check strategy: both strategies build
        their staging query through sqlserver__snapshot_staging_table, so both
        hand the probe a CTE-headed query that runs in full."""
        run_dbt(["run", "--models", "snap_source"])
        results = run_dbt(["snapshot", "--select", "snap_check"])
        assert len(results) == 1
        assert results[0].status == "success"

        rows = project.run_sql("select count(*) from {schema}.snap_check", fetch="one")
        assert rows[0] == _SNAPSHOT_ROWS

        run_dbt(["run", "--models", "snap_source", "--vars", "payload_char: y"])
        results = run_dbt(["snapshot", "--select", "snap_check"])
        assert len(results) == 1
        assert results[0].status == "success"

        rows = project.run_sql("select count(*) from {schema}.snap_check", fetch="one")
        assert rows[0] == _SNAPSHOT_ROWS * 2


class BaseFailingModelWithSideEffect:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "failing_model.sql": _failing_model_sql,
        }


class TestRollbackWithoutFlag(BaseFailingModelWithSideEffect):
    """Kept isolated: the opposite value of the transactions flag from every
    other case here (False vs True), so it cannot share the flag-on project."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"dbt_sqlserver_use_dbt_transactions": False}}

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "Without transactions flag (legacy deprecated behavior), "
            "DML in pre-hooks is auto-committed and not rolled back."
        ),
    )
    def test_side_effect_rolled_back(self, project):
        project.run_sql("CREATE TABLE {schema}.audit_log (msg varchar(100), created_at datetime)")
        run_dbt(["run", "-m", "failing_model"], expect_pass=False)
        rows = project.run_sql("SELECT COUNT(*) FROM {schema}.audit_log", fetch="one")
        assert rows[0] == 0


class TestAfterCommitModelHookTransactionsOn(BaseTransactionsEnabled):
    """Kept isolated: its project_config_update adds a project-wide post-hook,
    which would apply to every other model if folded into the shared class."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"dbt_sqlserver_use_dbt_transactions": True},
            "models": {
                "test": {
                    "post-hook": [
                        {"sql": "select 1", "transaction": False},
                    ],
                }
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"after_commit_hook_model.sql": "select 1 as id"}

    def test_after_commit_post_hook_does_not_double_commit(self, project):
        run_dbt()
