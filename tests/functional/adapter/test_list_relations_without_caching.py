import json

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture

NUM_VIEWS = 10
NUM_EXPECTED_RELATIONS = 1 + NUM_VIEWS

TABLE_BASE_SQL = """
{{ config(materialized='table') }}

select 1 as id
""".lstrip()

VIEW_X_SQL = """
select id from {{ ref('my_model_base') }}
""".lstrip()

# TODO - fix the call {% set relation_list_result = fabric__list_relations_without_caching(schema_relation) %}
MACROS__VALIDATE__FABRIC__LIST_RELATIONS_WITHOUT_CACHING = """
{% macro validate_list_relations_without_caching(schema_relation) -%}

    {% call statement('list_relations_without_caching', fetch_result=True) -%}
        select
            table_catalog as [database],
            table_name as [name],
            table_schema as [schema],
            case when table_type = 'BASE TABLE' then 'table'
                when table_type = 'VIEW' then 'view'
                else table_type
            end as table_type

        from INFORMATION_SCHEMA.TABLES
        where table_schema like '{{ schema_relation }}'
    {% endcall %}

    {% set relation_list_result = load_result('list_relations_without_caching').table %}
    {% set n_relations = relation_list_result | length %}
    {{ log("n_relations: " ~ n_relations) }}
{% endmacro %}
"""


def parse_json_logs(json_log_output):
    parsed_logs = []
    for line in json_log_output.split("\n"):
        try:
            log = json.loads(line)
        except ValueError:
            continue

        parsed_logs.append(log)

    return parsed_logs


def find_result_in_parsed_logs(parsed_logs, result_name):
    return next(
        (
            item["data"]["msg"]
            for item in parsed_logs
            if result_name in item["data"].get("msg", "msg")
        ),
        False,
    )


def find_exc_info_in_parsed_logs(parsed_logs, exc_info_name):
    return next(
        (
            item["data"]["exc_info"]
            for item in parsed_logs
            if exc_info_name in item["data"].get("exc_info", "exc_info")
        ),
        False,
    )


class TestListRelationsWithoutCachingSingle:
    @pytest.fixture(scope="class")
    def models(self):
        my_models = {"my_model_base.sql": TABLE_BASE_SQL}
        for view in range(NUM_VIEWS):
            my_models.update({f"my_model_{view}.sql": VIEW_X_SQL})

        return my_models

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "validate_list_relations_without_caching.sql": MACROS__VALIDATE__FABRIC__LIST_RELATIONS_WITHOUT_CACHING,
        }

    def test__fabric__list_relations_without_caching(self, project):
        """Validates that fabric__list_relations_without_caching
        macro returns a single record
        """
        run_dbt(["run", "-s", "my_model_base"])

        # database = project.database
        schemas = project.created_schemas

        for schema in schemas:
            # schema_relation = BaseRelation.create(schema=schema, database=database)
            # schema_relation = f"{database}.{schema}"
            kwargs = {"schema_relation": schema}
            _, log_output = run_dbt_and_capture(
                [
                    "--debug",
                    # "--log-format=json",
                    "run-operation",
                    "validate_list_relations_without_caching",
                    "--args",
                    str(kwargs),
                ],
            )

            # parsed_logs = parse_json_logs(log_output)
            # print(parsed_logs)
            # n_relations = find_result_in_parsed_logs(parsed_logs, "n_relations")

            # assert n_relations == "n_relations: 1"
            assert "n_relations: 1" in log_output


class TestListRelationsWithoutCachingFull:
    @pytest.fixture(scope="class")
    def models(self):
        my_models = {"my_model_base.sql": TABLE_BASE_SQL}
        for view in range(NUM_VIEWS):
            my_models.update({f"my_model_{view}.sql": VIEW_X_SQL})

        return my_models

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "validate_list_relations_without_caching.sql": MACROS__VALIDATE__FABRIC__LIST_RELATIONS_WITHOUT_CACHING,
        }

    def test__fabric__list_relations_without_caching(self, project):
        # purpose of the first run is to create the replicated views in the target schema
        run_dbt(["run"])

        # database = project.database
        schemas = project.created_schemas

        for schema in schemas:
            # schema_relation = f"{database}.{schema}"
            kwargs = {"schema_relation": schema}
            _, log_output = run_dbt_and_capture(
                [
                    "--debug",
                    # "--log-format=json",
                    "run-operation",
                    "validate_list_relations_without_caching",
                    "--args",
                    str(kwargs),
                ],
            )

            # parsed_logs = parse_json_logs(log_output)
            # n_relations = find_result_in_parsed_logs(parsed_logs, "n_relations")

            # assert n_relations == f"n_relations: {NUM_EXPECTED_RELATIONS}"
            assert f"n_relations: {NUM_EXPECTED_RELATIONS}" in log_output
