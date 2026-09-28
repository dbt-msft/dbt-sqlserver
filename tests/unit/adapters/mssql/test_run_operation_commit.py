from unittest import mock

import pytest

from dbt.adapters.sqlserver import SQLServerAdapter


@pytest.fixture
def adapter():
    adapter = object.__new__(SQLServerAdapter)
    adapter.connections = mock.MagicMock()
    adapter.commit_if_open = mock.MagicMock()
    return adapter


@pytest.mark.parametrize(
    "name, commits",
    [
        ("macro_foo", 1),
        ("inline_query", 1),
        ("model.proj.macro_foo", 0),
        ("sql_operation.proj.inline_query", 0),
        ("generate_catalog", 0),
    ],
)
def test_commits_only_run_operation_connections(adapter, name, commits):
    with adapter.connection_named(name):
        pass
    assert adapter.commit_if_open.call_count == commits


def test_failed_run_operation_does_not_commit(adapter):
    with pytest.raises(RuntimeError):
        with adapter.connection_named("macro_foo"):
            raise RuntimeError("boom")
    adapter.commit_if_open.assert_not_called()
    adapter.connections.release.assert_called_once()
