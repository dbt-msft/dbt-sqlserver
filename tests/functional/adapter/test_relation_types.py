import pytest
from dbt.contracts.results import CatalogArtifact
from dbt.tests.util import run_dbt

MY_SEED = """
id,value
1,100
2,200
3,300
""".strip()


MY_TABLE = """
{{ config(
    materialized='table',
) }}
select * from {{ ref('my_seed') }}
"""


MY_VIEW = """
{{ config(
    materialized='view',
) }}
select * from {{ ref('my_seed') }}
"""


class TestCatalogRelationTypes:
    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_table.sql": MY_TABLE,
            "my_view.sql": MY_VIEW,
        }

    @pytest.fixture(scope="class", autouse=True)
    def docs(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield run_dbt(["docs", "generate"])

    @pytest.mark.parametrize(
        "node_name,relation_type",
        [
            ("seed.test.my_seed", "BASE TABLE"),
            ("model.test.my_table", "BASE TABLE"),
            ("model.test.my_view", "VIEW"),
        ],
    )
    def test_relation_types_populate_correctly(
        self, docs: CatalogArtifact, node_name: str, relation_type: str
    ):
        assert node_name in docs.nodes
        node = docs.nodes[node_name]
        assert node.metadata.type == relation_type
