# ADBC Backend (Experimental)

dbt-sqlserver supports an **experimental ADBC backend** as an alternative to
pyodbc and mssql-python.  ADBC (Arrow Database Connectivity) uses the
[Apache Arrow](https://arrow.apache.org/) columnar format natively, avoiding
the overhead of row-based ODBC/DB-API bridges.

> **Experimental.**  The ADBC backend is new and opt-in.  It passes the full
> dbt-sqlserver functional test suite (320 passed, 0 failed, as of v1.12.0-rc1)
> and is covered by a dedicated CI job against the latest SQL Server.  Please
> report issues at
> [dbt-msft/dbt-sqlserver#771](https://github.com/dbt-msft/dbt-sqlserver/issues/771).

---

## 1. Install

### 1.1 Python packages

```bash
pip install dbt-sqlserver[adbc]
```

This pulls in:

| Package               | Purpose                                      |
|-----------------------|----------------------------------------------|
| `adbc-driver-manager` | Python DB-API 2.0 wrapper for ADBC drivers   |
| `pyarrow`             | Apache Arrow columnar format (result buffer) |

### 1.2 ADBC driver binary (one-time)

The Python packages above provide the *client library*.  You also need the
**driver binary** that speaks the SQL Server wire protocol.  It is shipped by
[Columnar Technologies](https://columnar.com/) and is **not** on PyPI.

Install it via the `dbc` CLI:

```bash
# Install the dbc driver manager (one-time)
pip install dbc

# Install the mssql driver
dbc install mssql
```

The driver lands at:

```
~/.config/adbc/drivers/mssql.toml          # driver manifest
~/.config/adbc/drivers/mssql_linux_amd64_v1.5.0/
```

Set the `ADBC_DRIVER_PATH` environment variable so the adapter can find it:

```bash
export ADBC_DRIVER_PATH="$HOME/.config/adbc/drivers"
```

Add that line to your shell profile (`~/.bashrc`, `~/.zshrc`) so every new
terminal picks it up automatically.

### 1.3 Verify

```bash
dbc list
# DRIVER  VERSION  LEVEL  LOCATION
# mssql   1.5.0    user   /home/…/.config/adbc/drivers
```

---

## 2. Configure your profile

Add `backend: adbc` to your dbt profile (`~/.dbt/profiles.yml`):

```yaml
your_profile:
  target: dev
  outputs:
    dev:
      type: sqlserver
      backend: adbc          # <-- opt-in
      host: localhost
      port: 1433
      user: SA
      password: YourPassword
      database: TestDB
      schema: dbo
      encrypt: true
      trust_cert: true
      retries: 2
```

The ``driver`` field (ODBC Driver 18 / mssql-python) is **not used** by the
ADBC backend and may be omitted.

The adapter constructs a **go-mssqldb URI** internally:

```
sqlserver://SA:YourPassword@localhost:1433?database=TestDB&encrypt=true&TrustServerCertificate=true
```

---

## 3. Authentication

The ADBC backend currently supports **SQL Server authentication** (user +
password) only.

| Authentication mode          | Supported?                    |
|------------------------------|-------------------------------|
| SQL Server (`user`/`password`)     | Yes                           |
| Azure Active Directory       | No (returns a clear error)    |
| Windows Integrated           | No (returns a clear error)    |
| Service Principal            | No (returns a clear error)    |

Azure AD and Windows auth are planned.  Track progress in
[#771](https://github.com/dbt-msft/dbt-sqlserver/issues/771).

---

## 4. How it works under the hood

The ADBC backend follows the same connection lifecycle as pyodbc and
mssql-python:

```
profiles.yml  →  build_adbc_connection_uri()
                 ↓
              _connect_adbc()
                 ↓
              adbc_driver_manager.dbapi.connect(
                  driver="mssql",
                  db_kwargs={"uri": "sqlserver://…"},
                  autocommit=True,
              )
                 ↓
              adbc_driver_manager  →  libadbc_driver_mssql.so
                 ↓
              go-mssqldb (Microsoft's TDS client)
                 ↓
              SQL Server
```

Key design decisions:

- **Transactions via explicit SQL.**  ADBC connections are opened with
  `autocommit=True` so DDL statements (`CREATE SCHEMA`, `CREATE TABLE`)
  auto-commit without extra round-trips.  When
  `dbt_sqlserver_use_dbt_transactions: true` (the default since 1.12),
  dbt emits explicit `BEGIN TRANSACTION` / `COMMIT TRANSACTION` SQL
  which correctly nest on top of the autocommit session, giving each model
  its own transaction boundary.  This is the same strategy pyodbc uses.

- **`?` → `@pN` parameter conversion.**  go-mssqldb does not understand PEP
  249 `qmark` (`?`) placeholders.  The adapter automatically rewrites them to
  T-SQL named parameters (`@p1`, `@p2`, …) at query time, so all dbt
  operations -- including seed CSV loading -- work without changes.

- **Arrow-native types in cursor descriptions.**  The ADBC driver returns
  PyArrow `DataType` objects (e.g. `DataType(int32)`, `TimestampType(…)`)
  instead of the integer type codes that pyodbc emits.  The adapter maps these
  to the same SQL Server type names used by the other backends.

- **Timezones.**  SQL Server `DATETIME2` / `DATETIME` do *not* store a
  timezone offset.  The ADBC driver wraps them as UTC by convention, but the
  adapter strips the `tzinfo` attribute so downstream code sees naive
  `datetime` objects -- identical to pyodbc and mssql-python.

---

## 5. Known differences from pyodbc / mssql-python

| Area              | pyodbc / mssql-python            | ADBC                          |
|-------------------|----------------------------------|-------------------------------|
| Connection URI    | ODBC connection string           | go-mssqldb URI                |
| Driver install    | OS package / pip wheel           | `dbc install mssql`           |
| Type system       | ODBC SQL type codes              | Arrow `DataType` objects      |
| Null handling     | Native DB-API `None`             | Arrow null bitmap → `None`    |
| Azure AD          | Fully supported                  | Not yet (clear error)         |
| Windows auth      | Fully supported                  | Not yet (clear error)         |
| `autocommit` attr | Yes                              | No (always autocommit)        |
| `cursor.nextset`  | Supported                        | Not supported (drained safely)|

---

## 6. Troubleshooting

### "adbc-driver-manager is not installed"

```bash
pip install "dbt-sqlserver[adbc]"
```

### "Could not load `mssql`" / driver not found

```bash
# Verify the driver is installed
dbc list

# Set the search path
export ADBC_DRIVER_PATH="$HOME/.config/adbc/drivers"
```

---

## 7. Feedback

This is an experimental feature.  We welcome bug reports, performance
benchmarks, and feature requests at
[Issues](https://github.com/dbt-msft/dbt-sqlserver/issues).
