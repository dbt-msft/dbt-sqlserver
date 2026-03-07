import pytest
from dbt.tests.adapter.dbt_clone.test_dbt_clone import BaseCloneNotPossible, BaseClonePossible


@pytest.mark.skip(reason="SQLServer does not support cloning")
class TestCloneNotPossible(BaseCloneNotPossible):
    pass


@pytest.mark.skip(reason="SQLServer does not support cloning")
class TestClonePossible(BaseClonePossible):
    pass
