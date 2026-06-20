import pytest

from dbt.tests.util import run_dbt


class BaseTransactionsEnabled:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"dbt_sqlserver_use_dbt_transactions": True}}


class TestTableMaterializationTransactionsOn(BaseTransactionsEnabled):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": """
{{ config(materialized='table') }}
select 1 as id, 'hello' as name
""",
        }

    def test_table_materialization(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        rows = project.run_sql("select id, name from {schema}.table_model", fetch="all")
        assert len(rows) == 1
        assert rows[0][0] == 1
        assert rows[0][1] == "hello"


class TestViewMaterializationTransactionsOn(BaseTransactionsEnabled):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": """
{{ config(materialized='view') }}
select 42 as answer
""",
        }

    def test_view_materialization(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        rows = project.run_sql("select answer from {schema}.view_model", fetch="all")
        assert len(rows) == 1
        assert rows[0][0] == 42


class TestIncrementalMaterializationTransactionsOn(BaseTransactionsEnabled):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_model.sql": """
{{ config(materialized='incremental', unique_key='id') }}
select 1 as id, 'first' as value
{% if is_incremental() %}
union all
select 2 as id, 'second' as value
{% endif %}
""",
        }

    def test_incremental_materialization(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        rows = project.run_sql(
            "select count(*) as cnt from {schema}.incremental_model", fetch="one"
        )
        assert rows[0] == 1

        results = run_dbt(["run"])
        assert len(results) == 1

        rows = project.run_sql(
            "select count(*) as cnt from {schema}.incremental_model", fetch="one"
        )
        assert rows[0] == 2


class BaseFailingModelWithSideEffect:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "failing_model.sql": """
{{ config(
    materialized='table',
    pre_hook=[
        "INSERT INTO {{ this.schema }}.audit_log "
        "(msg, created_at) VALUES ('from_model', getdate())"
    ]
) }}
select 1/0 as boom
""",
        }


class TestRollbackWithoutFlag(BaseFailingModelWithSideEffect):
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


class TestRollbackWithFlag(BaseFailingModelWithSideEffect):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"dbt_sqlserver_use_dbt_transactions": True}}

    def test_side_effect_rolled_back(self, project):
        project.run_sql("CREATE TABLE {schema}.audit_log (msg varchar(100), created_at datetime)")
        run_dbt(["run", "-m", "failing_model"], expect_pass=False)
        rows = project.run_sql("SELECT COUNT(*) FROM {schema}.audit_log", fetch="one")
        assert rows[0] == 0


class TestAfterCommitModelHookTransactionsOn(BaseTransactionsEnabled):
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


class TestFailedModelThenSuccessTransactionsOn(BaseTransactionsEnabled):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "good_model.sql": """
{{ config(materialized='table') }}
select 1 as id
""",
            "bad_model.sql": """
{{ config(materialized='table') }}
select 1/0 as boom
""",
        }

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


class TestSnapshotTransactionsOn(BaseTransactionsEnabled):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"snap_seed.csv": _snapshot_seed_csv}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snap.sql": _snapshot_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {}

    def test_snapshot_create_and_merge(self, project):
        run_dbt(["seed"])
        results = run_dbt(["snapshot"])
        assert len(results) == 1
        assert results[0].status == "success"

        rows = project.run_sql("select count(*) from {schema}.snap", fetch="one")
        assert rows[0] == 2

        results = run_dbt(["snapshot"])
        assert len(results) == 1
        assert results[0].status == "success"
