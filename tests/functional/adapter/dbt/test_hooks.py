import pytest
from dbt.tests.adapter.hooks import fixtures
from dbt.tests.util import run_dbt

seed_model_sql = """
drop table if exists {schema}.on_model_hook;

create table {schema}.on_model_hook (
    test_state       VARCHAR(8000), -- start|end
    target_dbname    VARCHAR(8000),
    target_host      VARCHAR(8000),
    target_name      VARCHAR(8000),
    target_schema    VARCHAR(8000),
    target_type      VARCHAR(8000),
    target_user      VARCHAR(8000),
    target_pass      VARCHAR(8000),
    target_threads   INTEGER,
    run_started_at   VARCHAR(8000),
    invocation_id    VARCHAR(8000),
    thread_id        VARCHAR(8000)
);
""".strip()

MODEL_PRE_HOOK = """
   insert into {{this.schema}}.on_model_hook (
        test_state,
        target_dbname,
        target_host,
        target_name,
        target_schema,
        target_type,
        target_user,
        target_pass,
        target_threads,
        run_started_at,
        invocation_id,
        thread_id
   ) VALUES (
    'start',
    '{{ target.database }}',
    '{{ target.server }}',
    '{{ target.name }}',
    '{{ target.schema }}',
    '{{ target.type }}',
    '{{ target.user }}',
    '{{ target.get("pass", "") }}',
    {{ target.threads }},
    '{{ run_started_at }}',
    '{{ invocation_id }}',
    '{{ thread_id }}'
   )
"""

MODEL_POST_HOOK = """
   insert into {{this.schema}}.on_model_hook (
        test_state,
        target_dbname,
        target_host,
        target_name,
        target_schema,
        target_type,
        target_user,
        target_pass,
        target_threads,
        run_started_at,
        invocation_id,
        thread_id
   ) VALUES (
    'end',
    '{{ target.database }}',
    '{{ target.server }}',
    '{{ target.name }}',
    '{{ target.schema }}',
    '{{ target.type }}',
    '{{ target.user }}',
    '{{ target.get("pass", "") }}',
    {{ target.threads }},
    '{{ run_started_at }}',
    '{{ invocation_id }}',
    '{{ thread_id }}'
   )
"""


class BaseTestPrePost:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql(seed_model_sql)

    def get_ctx_vars(self, state, count, project):
        fields = [
            "test_state",
            "target_dbname",
            "target_host",
            "target_name",
            "target_schema",
            "target_threads",
            "target_type",
            "target_user",
            "target_pass",
            "run_started_at",
            "invocation_id",
            "thread_id",
        ]
        field_list = ", ".join(['"{}"'.format(f) for f in fields])
        query = f"""
        select
            {field_list}
        from
            {project.test_schema}.on_model_hook where test_state = '{state}'"""

        vals = project.run_sql(query, fetch="all")
        assert len(vals) != 0, "nothing inserted into hooks table"
        assert len(vals) >= count, "too few rows in hooks table"
        assert len(vals) <= count, "too many rows in hooks table"
        return [{k: v for k, v in zip(fields, val)} for val in vals]

    def check_hooks(self, state, project, host, count=1):
        ctxs = self.get_ctx_vars(state, count=count, project=project)
        for ctx in ctxs:
            assert ctx["test_state"] == state
            # assert ctx["target_dbname"] == "TestDB"
            # assert ctx["target_host"] == host
            assert ctx["target_name"] == "default"
            assert ctx["target_schema"] == project.test_schema
            assert ctx["target_threads"] == 1
            assert ctx["target_type"] == project.adapter_type
            # assert ctx["target_user"] == "root"
            # assert ctx["target_pass"] == ""

            assert (
                ctx["run_started_at"] is not None and len(ctx["run_started_at"]) > 0
            ), "run_started_at was not set"
            assert (
                ctx["invocation_id"] is not None and len(ctx["invocation_id"]) > 0
            ), "invocation_id was not set"
            assert ctx["thread_id"].startswith("Thread-")


class BasePrePostModelHooks(BaseTestPrePost):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "pre-hook": [
                        # inside transaction (runs second)
                        MODEL_PRE_HOOK,
                    ],
                    "post-hook": [
                        # inside transaction (runs first)
                        MODEL_POST_HOOK,
                    ],
                }
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"hooks.sql": fixtures.models__hooks}

    def test_pre_and_post_run_hooks(self, project, dbt_profile_target):
        run_dbt()
        self.check_hooks("start", project, dbt_profile_target.get("host", None))
        self.check_hooks("end", project, dbt_profile_target.get("host", None))


class TestPrePostModelHooks(BasePrePostModelHooks):
    pass


class TestPrePostModelHooksUnderscores(BasePrePostModelHooks):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "pre_hook": [
                        # inside transaction (runs second)
                        MODEL_PRE_HOOK,
                    ],
                    "post_hook": [
                        # inside transaction (runs first)
                        MODEL_POST_HOOK,
                    ],
                }
            }
        }


class BaseHookRefs(BaseTestPrePost):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "hooked": {
                        "post-hook": [
                            """
                        insert into {{this.schema}}.on_model_hook select
                        test_state,
                        '{{ target.dbname }}' as target_dbname,
                        '{{ target.host }}' as target_host,
                        '{{ target.name }}' as target_name,
                        '{{ target.schema }}' as target_schema,
                        '{{ target.type }}' as target_type,
                        '{{ target.user }}' as target_user,
                        '{{ target.get(pass, "") }}' as target_pass,
                        {{ target.threads }} as target_threads,
                        '{{ run_started_at }}' as run_started_at,
                        '{{ invocation_id }}' as invocation_id,
                        '{{ thread_id }}' as thread_id
                        from {{ ref('post') }}""".strip()
                        ],
                    }
                },
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hooked.sql": fixtures.models__hooked,
            "post.sql": fixtures.models__post,
            "pre.sql": fixtures.models__pre,
        }

    def test_pre_post_model_hooks_refed(self, project, dbt_profile_target):
        run_dbt()
        self.check_hooks("start", project, dbt_profile_target.get("host", None))
        self.check_hooks("end", project, dbt_profile_target.get("host", None))


class TestHookRefs(BaseHookRefs):
    pass
