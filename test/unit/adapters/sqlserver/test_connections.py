import pytest

from dbt.adapters.sqlserver import SQLServerCredentials, connections


@pytest.fixture
def credentials() -> SQLServerCredentials:
    credentials = SQLServerCredentials(
        driver="ODBC Driver 17 for SQL Server",
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
    )
    return credentials


def test_get_pyodbc_attrs_before_empty_dict_when_service_principal(
    credentials: SQLServerCredentials,
) -> None:
    """
    When the authentication is set to sql we expect an empty attrs before.
    """
    attrs_before = connections.get_pyodbc_attrs_before(credentials)
    assert attrs_before == dict()


def test_get_pyodbc_attrs_before_contains_access_token_key(
    credentials: SQLServerCredentials,
) -> None:
    """
    When the Azure authentication is used, the attrs before should contain an
    access token key.
    """
    credentials.authentication = "CLI"
    attrs_before = connections.get_pyodbc_attrs_before(credentials)
    assert 1256 in attrs_before.keys()
