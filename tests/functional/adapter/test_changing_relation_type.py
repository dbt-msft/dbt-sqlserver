import pytest
from dbt.tests.adapter.relations.test_changing_relation_type import BaseChangeRelationTypeValidator


@pytest.mark.skip(reason="CTAS is not supported without a underlying table definition.")
class TestChangeRelationTypesFabric(BaseChangeRelationTypeValidator):
    pass
