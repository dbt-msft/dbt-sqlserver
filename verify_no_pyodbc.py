#!/usr/bin/env python
"""
Verification script: ensures pyodbc is NOT loaded when using mssql-python driver.

Checks that importing the dbt-sqlserver adapter does not cause pyodbc to be
truly loaded into sys.modules (the lazy proxy is acceptable).
"""
import sys


def main():
    # Import the adapter — this triggers FabricConnectionManager import too
    from dbt.adapters.sqlserver.sqlserver_connections import SQLServerConnectionManager  # noqa: F401

    pyodbc_module = sys.modules.get("pyodbc")

    if pyodbc_module is None:
        print("PASS: pyodbc is not in sys.modules at all.")
        return 0

    # Check if it's our lazy proxy (acceptable) vs the real pyodbc (failure)
    is_proxy = getattr(pyodbc_module, "_is_proxy", False)

    if is_proxy:
        # Verify the real module was never loaded through the proxy
        real = pyodbc_module.__dict__.get("_real")
        if real is None:
            print("PASS: pyodbc is only a lazy proxy — real pyodbc was never imported.")
            return 0
        else:
            print("FAIL: pyodbc lazy proxy exists but the real pyodbc was loaded!")
            return 1
    else:
        print(f"FAIL: real pyodbc module is loaded in sys.modules: {pyodbc_module}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
