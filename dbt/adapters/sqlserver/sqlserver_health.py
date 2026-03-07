from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("sqlserver")


@dataclass
class ServerInfo:
    version: str
    edition: str
    product_level: str
    collation: str


@dataclass
class ActiveQuery:
    session_id: int
    status: str
    command: str
    wait_type: Optional[str]
    cpu_time_ms: int
    total_elapsed_time_ms: int
    reads: int
    writes: int


@dataclass
class DatabaseSize:
    database_name: str
    file_name: str
    file_type: str
    size_mb: float


class SQLServerHealthMixin:
    """Mixin providing SQL Server health check and load monitoring methods.

    Designed to be mixed into SQLServerAdapter. Uses self.execute() and
    self.connections to run queries through the adapter's standard path.
    """

    def check_connection_health(self) -> bool:
        """Validate the current connection is alive."""
        try:
            _, table = self.execute("SELECT 1 AS healthy", fetch=True)
            return len(table.rows) == 1
        except Exception:
            logger.warning("Connection health check failed")
            return False

    def get_server_info(self) -> ServerInfo:
        """Return SQL Server version, edition, product level, and collation."""
        sql = (
            "SELECT "
            "CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128)) AS version, "
            "CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128)) AS edition, "
            "CAST(SERVERPROPERTY('ProductLevel') AS NVARCHAR(128)) AS product_level, "
            "CAST(SERVERPROPERTY('Collation') AS NVARCHAR(128)) AS collation"
        )
        _, table = self.execute(sql, fetch=True)
        row = table.rows[0]
        return ServerInfo(
            version=str(row[0]),
            edition=str(row[1]),
            product_level=str(row[2]),
            collation=str(row[3]),
        )

    def get_connection_count(self) -> int:
        """Return the number of active connections to the server."""
        sql = "SELECT COUNT(*) AS cnt FROM sys.dm_exec_connections"
        _, table = self.execute(sql, fetch=True)
        return int(table.rows[0][0])

    def get_active_session_count(self) -> int:
        """Return the number of active user sessions."""
        sql = "SELECT COUNT(*) AS cnt FROM sys.dm_exec_sessions " "WHERE is_user_process = 1"
        _, table = self.execute(sql, fetch=True)
        return int(table.rows[0][0])

    def get_active_queries(self) -> List[ActiveQuery]:
        """Return details about currently executing queries."""
        sql = (
            "SELECT "
            "session_id, status, command, wait_type, "
            "cpu_time, total_elapsed_time, reads, writes "
            "FROM sys.dm_exec_requests "
            "WHERE session_id > 50"  # exclude system sessions
        )
        _, table = self.execute(sql, fetch=True)
        return [
            ActiveQuery(
                session_id=int(row[0]),
                status=str(row[1]),
                command=str(row[2]),
                wait_type=str(row[3]) if row[3] else None,
                cpu_time_ms=int(row[4]),
                total_elapsed_time_ms=int(row[5]),
                reads=int(row[6]),
                writes=int(row[7]),
            )
            for row in table.rows
        ]

    def get_database_size(self) -> List[DatabaseSize]:
        """Return file sizes for the current database."""
        sql = (
            "SELECT "
            "DB_NAME() AS database_name, "
            "name AS file_name, "
            "CASE type WHEN 0 THEN 'DATA' WHEN 1 THEN 'LOG' ELSE 'OTHER' END AS file_type, "
            "CAST(size * 8.0 / 1024 AS DECIMAL(18,2)) AS size_mb "
            "FROM sys.database_files"
        )
        _, table = self.execute(sql, fetch=True)
        return [
            DatabaseSize(
                database_name=str(row[0]),
                file_name=str(row[1]),
                file_type=str(row[2]),
                size_mb=float(row[3]),
            )
            for row in table.rows
        ]

    def get_health_report(self) -> Dict[str, Any]:
        """Return a combined health report dictionary."""
        report: Dict[str, Any] = {}

        report["healthy"] = self.check_connection_health()
        if not report["healthy"]:
            return report

        report["server_info"] = self.get_server_info().__dict__
        report["connection_count"] = self.get_connection_count()
        report["active_sessions"] = self.get_active_session_count()
        report["active_queries"] = [q.__dict__ for q in self.get_active_queries()]
        report["database_files"] = [f.__dict__ for f in self.get_database_size()]

        return report
