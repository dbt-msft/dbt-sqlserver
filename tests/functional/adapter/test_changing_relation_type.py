<<<<<<< HEAD
from dbt.tests.adapter.relations.test_changing_relation_type import (
    BaseChangeRelationTypeValidator,
)
=======
import pytest
from dbt.tests.adapter.relations.test_changing_relation_type import BaseChangeRelationTypeValidator
>>>>>>> fabric-1.7


@pytest.mark.skip(reason="CTAS is not supported without a underlying table definition.")
class TestChangeRelationTypesFabric(BaseChangeRelationTypeValidator):
    pass
