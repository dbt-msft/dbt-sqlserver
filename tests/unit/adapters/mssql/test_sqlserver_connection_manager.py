import pytest

from dbt.adapters.sqlserver.sqlserver_credentials import SQLServerCredentials


@pytest.fixture
def credentials() -> SQLServerCredentials:
    return SQLServerCredentials(
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
    )


@pytest.fixture
def sql_auth_credentials() -> SQLServerCredentials:
    return SQLServerCredentials(
        host="127.0.0.1",
        port=1433,
        database="TestDB",
        schema="dbo",
        UID="SA",
        PWD="L0calTesting!",
        authentication="sql",
        encrypt=True,
        trust_cert=True,
    )


class TestBuildAdbcUri:
    def test_basic_sql_auth(self, sql_auth_credentials: SQLServerCredentials) -> None:
        uri = sql_auth_credentials.build_adbc_uri()
        assert uri == (
            "sqlserver://SA:L0calTesting%21@127.0.0.1:1433"
            "?database=TestDB&encrypt=true&TrustServerCertificate=true"
        )

    def test_named_instance_omits_port(self) -> None:
        creds = SQLServerCredentials(
            host=r"myserver\SQLEXPRESS",
            database="TestDB",
            schema="dbo",
            UID="SA",
            PWD="test",
        )
        uri = creds.build_adbc_uri()
        assert r"myserver\SQLEXPRESS" in uri
        assert ":1433" not in uri

    def test_special_chars_in_password(self) -> None:
        creds = SQLServerCredentials(
            host="localhost",
            port=1433,
            database="TestDB",
            schema="dbo",
            UID="SA",
            PWD="p@ss:word/with+special=chars&more",
            encrypt=False,
            trust_cert=False,
        )
        uri = creds.build_adbc_uri()
        # Password should be URL-encoded
        assert "p%40ss%3Aword%2Fwith%2Bspecial%3Dchars%26more" in uri
        # Original password should NOT appear unencoded
        assert "p@ss:word" not in uri

    def test_encrypt_false(self) -> None:
        creds = SQLServerCredentials(
            host="localhost",
            port=1433,
            database="TestDB",
            schema="dbo",
            encrypt=False,
            trust_cert=False,
        )
        uri = creds.build_adbc_uri()
        assert "encrypt=false" in uri
        assert "TrustServerCertificate=false" in uri

    def test_login_timeout(self) -> None:
        creds = SQLServerCredentials(
            host="localhost",
            port=1433,
            database="TestDB",
            schema="dbo",
            login_timeout=30,
        )
        uri = creds.build_adbc_uri()
        assert "connection timeout=30" in uri

    def test_no_login_timeout_when_zero(self) -> None:
        creds = SQLServerCredentials(
            host="localhost",
            port=1433,
            database="TestDB",
            schema="dbo",
            login_timeout=0,
        )
        uri = creds.build_adbc_uri()
        assert "connection timeout" not in uri

    def test_no_user_no_password(self, credentials: SQLServerCredentials) -> None:
        uri = credentials.build_adbc_uri()
        assert "sqlserver://fake.sql.sqlserver.net:1433?" in uri
        assert "database=dbt" in uri


class TestCredentialProperties:
    def test_type(self, credentials: SQLServerCredentials) -> None:
        assert credentials.type == "sqlserver"

    def test_unique_field(self, credentials: SQLServerCredentials) -> None:
        assert credentials.unique_field == "fake.sql.sqlserver.net"

    def test_connection_keys(self, credentials: SQLServerCredentials) -> None:
        keys = credentials._connection_keys()
        assert "host" in keys
        assert "port" in keys
        assert "database" in keys
        assert "schema" in keys
