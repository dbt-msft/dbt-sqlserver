import pytest
from dbt.tests.adapter.empty._models import model_input_sql, schema_sources_yml

# switch for 1.9
# from dbt.tests.adapter.empty import _models
from dbt.tests.adapter.empty.test_empty import (  # MetadataWithEmptyFlag
    BaseTestEmpty,
    BaseTestEmptyInlineSourceRef,
)
from dbt.tests.util import run_dbt

model_sql_sqlserver = """
select *
from {{ ref('model_input') }}
union all
select *
from {{ source('seed_sources', 'raw_source') }}
"""

model_inline_sql_sqlserver = """
select * from {{ source('seed_sources', 'raw_source') }}
"""


class TestEmpty(BaseTestEmpty):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_input.sql": model_input_sql,
            # # no support for ephemeral models in SQLServer
            # "ephemeral_model_input.sql": _models.ephemeral_model_input_sql,
            "model.sql": model_sql_sqlserver,
            "sources.yml": schema_sources_yml,
        }

    def test_run_with_empty(self, project):
        # create source from seed
        run_dbt(["seed"])

        # run without empty - 3 expected rows in output - 1 from each input
        run_dbt(["run"])
        self.assert_row_count(project, "model", 2)

        # run with empty - 0 expected rows in output
        run_dbt(["run", "--empty"])
        self.assert_row_count(project, "model", 0)


class TestemptyInlineSourceRef(BaseTestEmptyInlineSourceRef):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_inline_sql_sqlserver,
            "sources.yml": schema_sources_yml,
        }
