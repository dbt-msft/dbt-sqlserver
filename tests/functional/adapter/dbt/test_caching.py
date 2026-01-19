import pytest
from dbt.tests.adapter.caching.test_caching import (
    BaseCachingLowercaseModel,
    BaseCachingSelectedSchemaOnly,
    BaseCachingUppercaseModel,
    BaseNoPopulateCache,
)


class TestCachingLowercaseModel(BaseCachingLowercaseModel):
    pass


@pytest.mark.skip(reason="""
    Fails because of case sensitivity.
    MODEL is coereced to model which fails the test as it sees conflicting naming
    """)
class TestCachingUppercaseModel(BaseCachingUppercaseModel):
    pass


class TestCachingSelectedSchemaOnly(BaseCachingSelectedSchemaOnly):
    pass


class TestNoPopulateCache(BaseNoPopulateCache):
    pass
