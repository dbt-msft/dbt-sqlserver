import pytest
from dbt.tests.adapter.persist_docs.test_persist_docs import BasePersistDocs


@pytest.mark.skip(
    reason="""
    Persisted docs are not implemented in SQLServer.
    Could be implemented with sp_addextendedproperty
    """
)
class TestPersistDocs(BasePersistDocs):
    pass
