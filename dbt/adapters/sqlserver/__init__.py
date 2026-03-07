# ---------------------------------------------------------------------------
# Lazy-loading proxy for pyodbc — MUST be installed before any dbt-fabric
# import since fabric_connection_manager.py has `import pyodbc` at module level.
# When driver_type='mssql-python', pyodbc is never truly loaded.
# ---------------------------------------------------------------------------
import sys
import types


class _PyodbcLazyProxy(types.ModuleType):
    """sys.modules shim that defers the real pyodbc import until first attribute access."""

    def __init__(self):
        super().__init__("pyodbc")
        self.__dict__["_real"] = None
        self.__dict__["_is_proxy"] = True

    def _load_real(self):
        if self.__dict__["_real"] is None:
            sys.modules.pop("pyodbc", None)
            import pyodbc as _real_pyodbc

            self.__dict__["_real"] = _real_pyodbc
            sys.modules["pyodbc"] = self
        return self.__dict__["_real"]

    def __getattr__(self, name):
        if name in ("_real", "_is_proxy"):
            return self.__dict__[name]
        return getattr(self._load_real(), name)


if "pyodbc" not in sys.modules:
    sys.modules["pyodbc"] = _PyodbcLazyProxy()

# ---------------------------------------------------------------------------
# Now safe to import dbt-fabric and adapter components
# ---------------------------------------------------------------------------
from dbt.adapters.base import AdapterPlugin

from dbt.adapters.sqlserver.sqlserver_adapter import SQLServerAdapter
from dbt.adapters.sqlserver.sqlserver_column import SQLServerColumn
from dbt.adapters.sqlserver.sqlserver_configs import SQLServerConfigs
from dbt.adapters.sqlserver.sqlserver_connections import SQLServerConnectionManager  # noqa
from dbt.adapters.sqlserver.sqlserver_credentials import SQLServerCredentials
from dbt.include import sqlserver

Plugin = AdapterPlugin(
    adapter=SQLServerAdapter,
    credentials=SQLServerCredentials,
    include_path=sqlserver.PACKAGE_PATH,
    dependencies=["fabric"],
)

__all__ = [
    "Plugin",
    "SQLServerConnectionManager",
    "SQLServerColumn",
    "SQLServerAdapter",
    "SQLServerCredentials",
    "SQLServerConfigs",
]
