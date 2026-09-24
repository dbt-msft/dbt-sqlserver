import pytest

from dbt.adapters.base.impl import DEFAULT_BASE_BEHAVIOR_FLAGS
from dbt.adapters.sql.impl import SQLAdapter
from dbt.adapters.sqlserver import SQLServerAdapter


def _bare_adapter():
    adapter = object.__new__(SQLServerAdapter)
    adapter.config = None
    return adapter


def test_repeated_init_leaves_default_flags_unchanged():
    before = list(DEFAULT_BASE_BEHAVIOR_FLAGS)
    for _ in range(2):
        adapter = _bare_adapter()
        adapter.behavior = DEFAULT_BASE_BEHAVIOR_FLAGS
        assert adapter.behavior.dbt_sqlserver_disable_empty_relation_aliases
    assert DEFAULT_BASE_BEHAVIOR_FLAGS == before


@pytest.mark.temporary(reason="remove with the behavior setter override once dbt-adapters copies")
def test_upstream_setter_still_mutates_flags():
    flags = []
    SQLAdapter.behavior.fset(_bare_adapter(), flags)
    assert flags, "dbt-adapters no longer mutates flags: drop SQLServerAdapter.behavior"
