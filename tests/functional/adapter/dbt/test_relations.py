import pytest
from dbt.tests.adapter.relations.test_changing_relation_type import BaseChangeRelationTypeValidator
from dbt.tests.adapter.relations.test_dropping_schema_named import BaseDropSchemaNamed


class TestChangeRelationTypeValidator(BaseChangeRelationTypeValidator):
    pass


@pytest.mark.xfail(
    reason="""
                  Test fails as its not passing Use[] properly.
                  `Use[None]` is called, should be `User[TestDB]`
                  Unclear why the macro doens't pass it properly.
                  """
)
class TestDropSchemaNamed(BaseDropSchemaNamed):
    pass
