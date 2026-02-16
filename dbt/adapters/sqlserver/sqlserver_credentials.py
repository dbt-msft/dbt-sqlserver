from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote_plus

from dbt.adapters.contracts.connection import Credentials


@dataclass
class SQLServerCredentials(Credentials):
    """
    Defines database specific credentials that get added to
    profiles.yml to connect to new adapter
    """

    # Connection
    host: str = ""
    database: str = ""
    schema: str = ""
    port: int = 1433

    # Auth (SQL auth only for this spike)
    UID: Optional[str] = None
    PWD: Optional[str] = None
    authentication: str = "sql"

    # Connection options
    driver: str = "mssql"  # ADBC driver name (not ODBC driver)
    encrypt: Optional[bool] = True
    trust_cert: Optional[bool] = False
    retries: int = 3
    login_timeout: int = 0
    query_timeout: int = 0

    _ALIASES = {
        "user": "UID",
        "username": "UID",
        "pass": "PWD",
        "password": "PWD",
        "server": "host",
        "TrustServerCertificate": "trust_cert",
    }

    def build_adbc_uri(self) -> str:
        """Construct go-mssqldb connection URI from profile fields."""
        # URL-encode user and password for special characters
        user = quote_plus(self.UID) if self.UID else ""
        pwd = quote_plus(self.PWD) if self.PWD else ""

        # Build userinfo
        if user and pwd:
            userinfo = f"{user}:{pwd}@"
        elif user:
            userinfo = f"{user}@"
        else:
            userinfo = ""

        # Handle named instances (backslash in host) — omit port
        if "\\" in self.host:
            host_part = self.host
        else:
            host_part = f"{self.host}:{self.port}"

        # Build query parameters
        params = []
        if self.database:
            params.append(f"database={quote_plus(self.database)}")
        if self.encrypt is not None:
            params.append(f"encrypt={'true' if self.encrypt else 'false'}")
        if self.trust_cert is not None:
            params.append(
                f"TrustServerCertificate={'true' if self.trust_cert else 'false'}"
            )
        if self.login_timeout:
            params.append(f"connection timeout={self.login_timeout}")

        query_string = "&".join(params)
        return f"sqlserver://{userinfo}{host_part}?{query_string}"

    @property
    def type(self):
        return "sqlserver"

    @property
    def unique_field(self):
        return self.host

    def _connection_keys(self):
        return (
            "host",
            "port",
            "database",
            "schema",
            "UID",
            "authentication",
            "encrypt",
            "trust_cert",
        )
