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

_snapshot_seed_csv = """id,name,updated_at
1,alice,2024-01-01 00:00:00
2,bob,2024-01-01 00:00:00
"""

_snapshot_sql = """
{% snapshot snap %}
{{ config(
    target_schema=schema,
    unique_key='id',
    strategy='timestamp',
    updated_at='updated_at',
) }}
select * from {{ ref('snap_seed') }}
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
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"snap_seed.csv": _snapshot_seed_csv}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snap.sql": _snapshot_sql}

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
        run_dbt(["seed"])
        results = run_dbt(["snapshot", "--select", "snap"])
        assert len(results) == 1
        assert results[0].status == "success"

        rows = project.run_sql("select count(*) from {schema}.snap", fetch="one")
        assert rows[0] == 2

        results = run_dbt(["snapshot", "--select", "snap"])
        assert len(results) == 1
        assert results[0].status == "success"


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
        reason="Without transactions flag, DML in pre-hooks is auto-committed and not rolled back,"
        " remove after migration to always use transactions.",
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
