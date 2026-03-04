from unittest.mock import MagicMock, patch

import pytest

from dbt.adapters.sqlserver.sqlserver_health import (
    ActiveQuery,
    DatabaseSize,
    SQLServerHealthMixin,
    ServerInfo,
)


class FakeTable:
    def __init__(self, rows):
        self.rows = rows


class FakeAdapter(SQLServerHealthMixin):
    """Minimal fake adapter that provides execute()."""

    def __init__(self):
        self.connections = MagicMock()

    def execute(self, sql, fetch=False):
        # overridden per test
        raise NotImplementedError


@pytest.fixture
def adapter():
    return FakeAdapter()


def test_check_connection_health_success(adapter):
    adapter.execute = MagicMock(return_value=(None, FakeTable([(1,)])))
    assert adapter.check_connection_health() is True


def test_check_connection_health_failure(adapter):
    adapter.execute = MagicMock(side_effect=Exception("connection lost"))
    assert adapter.check_connection_health() is False


def test_get_server_info(adapter):
    adapter.execute = MagicMock(
        return_value=(None, FakeTable([("15.0.4123.1", "Developer Edition", "RTM", "SQL_Latin1_General_CP1_CI_AS")]))
    )
    info = adapter.get_server_info()
    assert isinstance(info, ServerInfo)
    assert info.version == "15.0.4123.1"
    assert info.edition == "Developer Edition"
    assert info.product_level == "RTM"
    assert info.collation == "SQL_Latin1_General_CP1_CI_AS"


def test_get_connection_count(adapter):
    adapter.execute = MagicMock(return_value=(None, FakeTable([(42,)])))
    assert adapter.get_connection_count() == 42


def test_get_active_session_count(adapter):
    adapter.execute = MagicMock(return_value=(None, FakeTable([(7,)])))
    assert adapter.get_active_session_count() == 7


def test_get_active_queries(adapter):
    adapter.execute = MagicMock(
        return_value=(None, FakeTable([
            (55, "running", "SELECT", "CXPACKET", 100, 200, 300, 400),
            (60, "suspended", "INSERT", None, 50, 100, 150, 200),
        ]))
    )
    queries = adapter.get_active_queries()
    assert len(queries) == 2
    assert queries[0].session_id == 55
    assert queries[0].wait_type == "CXPACKET"
    assert queries[1].wait_type is None


def test_get_database_size(adapter):
    adapter.execute = MagicMock(
        return_value=(None, FakeTable([
            ("mydb", "mydb_data", "DATA", 512.00),
            ("mydb", "mydb_log", "LOG", 128.00),
        ]))
    )
    files = adapter.get_database_size()
    assert len(files) == 2
    assert files[0].size_mb == 512.0
    assert files[1].file_type == "LOG"


def test_get_health_report(adapter):
    call_count = {"n": 0}
    responses = [
        (None, FakeTable([(1,)])),  # check_connection_health
        (None, FakeTable([("15.0", "Dev", "RTM", "Latin")])),  # get_server_info
        (None, FakeTable([(10,)])),  # get_connection_count
        (None, FakeTable([(5,)])),  # get_active_session_count
        (None, FakeTable([])),  # get_active_queries
        (None, FakeTable([("db", "f", "DATA", 100.0)])),  # get_database_size
    ]

    def mock_execute(sql, fetch=False):
        idx = call_count["n"]
        call_count["n"] += 1
        return responses[idx]

    adapter.execute = mock_execute
    report = adapter.get_health_report()
    assert report["healthy"] is True
    assert report["connection_count"] == 10
    assert report["active_sessions"] == 5
    assert len(report["database_files"]) == 1


def test_get_health_report_unhealthy(adapter):
    adapter.execute = MagicMock(side_effect=Exception("down"))
    report = adapter.get_health_report()
    assert report["healthy"] is False
    assert "server_info" not in report
