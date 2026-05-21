import pytest

from dbt.tests.util import run_dbt

model_sql = """
{{ config(materialized="table") }}
select 1 as id
"""


class TestReservedKeywordsSchema:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": model_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_reserved_keywords_schema",
            "quoting": {
                "database": True,
                "schema": True,
                "identifier": True,
            },
            "models": {
                "test_reserved_keywords_schema": {
                    "schema": "group",
                }
            },
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"generate_schema_name.sql": """
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
"""}

    @pytest.fixture(autouse=True, scope="class")
    def cleanup_schema(self, project):
        yield
        project.run_sql("DROP TABLE IF EXISTS [group].[model]")
        project.run_sql("DROP SCHEMA IF EXISTS [group]")

    def test_reserved_schema(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"
