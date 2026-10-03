"""delete+insert incremental_predicates can reference DBT_INTERNAL_DEST."""

import pytest

from dbt.tests.util import run_dbt

MODEL = """
{{{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key={unique_key},
    incremental_predicates=['DBT_INTERNAL_DEST.id > 1'],
) }}}}
select id, '{{{{ var("v", "a") }}}}' as v from (values (1), (2)) as t(id)
"""


class TestDeleteInsertPredicateAliases:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "key_string.sql": MODEL.format(unique_key="'id'"),
            "key_list.sql": MODEL.format(unique_key="['id']"),
        }

    def rows(self, project, model):
        return sorted(
            tuple(row)
            for row in project.run_sql(
                f"select id, v from {project.test_schema}.{model}", fetch="all"
            )
        )

    def test_predicate_on_target_alias(self, project):
        run_dbt(["run"])
        run_dbt(["run", "--vars", "{v: b}"])

        # id 1 fails the predicate, so its old row is kept next to the new one.
        for model in ("key_string", "key_list"):
            assert self.rows(project, model) == [(1, "a"), (1, "b"), (2, "b")]
