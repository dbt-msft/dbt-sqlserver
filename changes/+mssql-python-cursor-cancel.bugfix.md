`SQLServerConnectionManager.cancel()` now stops in-flight statements on the
mssql-python backend. mssql-python has no public `Cursor.cancel()`
(microsoft/mssql-python#424), so the adapter feature-detects the statement
handle's SQLCancel binding, which is the cross-thread cancel the driver itself
uses. A public `cancel()` takes over as soon as the driver ships one.
