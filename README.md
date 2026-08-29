# dbt-sqlserver

[dbt](https://www.getdbt.com) adapter for Microsoft SQL Server and Azure SQL services.

The adapter supports dbt-core 1.12 or newer and follows the same versioning scheme.
E.g. version 1.12.x of the adapter is compatible with dbt-core 1.12.x.

## Supported Python versions

The adapter is tested against:

| Python version | Status |
|---|---|
| 3.10 | Installable (not tested in CI) |
| 3.11 | Officially supported |
| 3.12 | Officially supported |
| 3.13 | Officially supported |
| 3.14 | Officially supported |

## Supported SQL Server versions

The adapter is tested against the following SQL Server versions:

| SQL Server version | Supported |
|---|---|
| SQL Server 2017 | ✅ (minimum supported version) |
| SQL Server 2019 | ✅ |
| SQL Server 2022 | ✅ |
| SQL Server 2025 | ✅ |

The minimum supported SQL Server version is SQL Server 2017; older versions are not supported.

SQL Server 2017, 2019, 2022, and 2025 are covered by the integration test suite. Azure SQL Database and Azure SQL Managed Instance are not covered by the integration test suite, but are expected to be compatible.

## Documentation

We've bundled all documentation on the dbt docs site:

- [Profile setup & authentication](https://docs.getdbt.com/reference/warehouse-profiles/mssql-profile)
- [Adapter documentation, usage and important notes](https://docs.getdbt.com/reference/resource-configs/mssql-configs)

Join us on the [dbt Slack](https://getdbt.slack.com/archives/CMRMDDQ9W) to ask questions, get help, or to discuss the project.

## Installation

The default install uses the `pyodbc` backend and includes the `pyodbc` dependency. If you want the optional `mssql-python` backend instead, install the `mssql` extra.

Latest version: ![PyPI](https://img.shields.io/pypi/v/dbt-sqlserver?label=latest%20stable&logo=pypi)  
Latest pre-release: ![GitHub tag (latest SemVer pre-release)](https://img.shields.io/github/v/tag/dbt-msft/dbt-sqlserver?include_prereleases&label=latest%20pre-release&logo=pypi)


### Backend requirements at a glance

| Backend | Python package | Debian/Ubuntu system packages |
|---|---|---|
| `pyodbc` | `dbt-sqlserver[pyodbc]` or `pyodbc` | `unixodbc-dev` plus the Microsoft ODBC Driver for SQL Server |
| `mssql-python` | `dbt-sqlserver[mssql]` or `mssql-python` | `libltdl7`, `libkrb5-3`, `libgssapi-krb5-2` |
| `adbc` *(experimental)* | `dbt-sqlserver[adbc]` | none (driver binary installed separately via the `dbc` CLI) |


### `pyodbc` backend

The legacy and currently default ODBC path uses `pyodbc` and the Microsoft ODBC driver.

```shell
pip install -U dbt-sqlserver
```

You should migrate to using an explicit extra in preparation for deprecation; the following is equivalent:

```shell
pip install -U "dbt-sqlserver[pyodbc]"
```

You also need the Microsoft ODBC driver for SQL Server installed on your system:
[Windows](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16#download-for-windows) |
[macOS](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos?view=sql-server-ver16) |
[Linux](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-sql-server?view=sql-server-ver16)

<details><summary>Debian/Ubuntu</summary>

Install the ODBC headers as well as the driver linked above:

```shell
sudo apt-get install -y unixodbc-dev
```

</details>

### `mssql-python` backend

An alternative backend that does not require the ODBC driver.

```shell
pip install -U "dbt-sqlserver[mssql]"
```

On Debian/Ubuntu-based systems, `mssql-python` requires these system libraries:

```shell
sudo apt-get install -y libltdl7 libkrb5-3 libgssapi-krb5-2
```

Enable it per target in your `profiles.yml`:

```yaml
your_profile:
  target: dev
  outputs:
    dev:
      type: sqlserver
      host: your-server
      port: 1433
      database: your-database
      schema: dbo
      user: your-user
      password: your-password
      encrypt: true
      trust_cert: false
      backend: mssql-python  # <-- enables this backend
```

### `adbc` backend *(experimental)*

An Arrow-native backend built on [ADBC](https://arrow.apache.org/adbc/), avoiding the row-based ODBC/DB-API bridge entirely. SQL Server authentication only (no Azure AD / Windows auth yet).

```shell
pip install -U "dbt-sqlserver[adbc]"
```

The driver binary is not on PyPI and must be installed once via the `dbc` CLI. See [docs/adbc_backend.md](docs/adbc_backend.md) for the full setup, configuration, and known-differences guide.

## Changelog

See [the changelog](CHANGELOG.md)

## Configuration

### `dbt_sqlserver_use_default_schema_concat`

*(default: `true`)* Controls schema name generation when a [custom schema](https://docs.getdbt.com/docs/build/custom-schemas) is set on a model.

| Flag value | `custom_schema_name` | Result |
|---|---|---|
| `true` (default, dbt-core standard) | *(none)* | `target.schema` |
| `true` (default, dbt-core standard) | `"reporting"` | `target.schema_reporting` |
| `false` (legacy, deprecated) | *(none)* | `target.schema` |
| `false` (legacy, deprecated) | `"reporting"` | `reporting` |

When `true` (the default), the adapter delegates to dbt-core's `default__generate_schema_name`, which concatenates `target.schema` + `_` + `custom_schema_name`. This also matches the behavior dbt-core v2 (Fusion) ships unconditionally.  
When `false`, the adapter uses its legacy behaviour: `custom_schema_name` is used **as-is** without being prefixed by `target.schema`. This legacy behavior is deprecated and will be removed in a future release.

**Example usage in `dbt_project.yml`:**

```yaml
flags:
  dbt_sqlserver_use_default_schema_concat: false  # Opt back into the deprecated legacy behavior
```

The same setting is also honoured via `vars:` for backwards compatibility; the behavior flag under `flags:` takes precedence when both are set.

> **Note:** If you want to permanently customise schema generation and avoid any future changes, override the `sqlserver__generate_schema_name` macro directly in your project instead.

### `backend`

*(default: `pyodbc`)* Set to `mssql-python` or `adbc` (experimental, see [docs/adbc_backend.md](docs/adbc_backend.md)) in a profile target to use that backend instead of `pyodbc`. The adapter fails if the required backend package (Python dependency), such as `pyodbc`, `mssql-python`, or `adbc-driver-manager`, is not installed.

### `dbt_sqlserver_enable_safe_type_expansion`

*(default: `false`)* When enabled, allows the adapter to widen column types during incremental model schema expansion beyond same-family string resizes. Supported safe expansions include:

- **Cross-family string**: `varchar`/`char` → `nvarchar`/`nchar` (same or larger size)
- **Integer family**: `bit` → `tinyint` → `smallint` → `int` → `bigint`
- **Integer → numeric**: `int` → `numeric` (with sufficient precision to hold the integer range)
- **Numeric precision/scale**: `numeric(p,s)` → `numeric(p2,s2)` where precision and scale both increase
- **Fixed-money**: `smallmoney` → `money`, `money` → `numeric` (with sufficient precision)

Safe expansions are further gated by `column_type_expansion_max_rows` (default 1,000,000 rows) to avoid long-running operations on large tables.

### `dbt_sqlserver_use_dbt_transactions`

_(default: `true`)_ Makes dbt's transaction hooks real at the SQL Server level by emitting `BEGIN TRANSACTION` / `COMMIT TRANSACTION` through the adapter's `add_begin_query` and `add_commit_query` methods.

The default is `true`, so dbt-managed transaction hooks emit real T-SQL transaction statements and rollback uses `IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION`. Set it to `false` to opt back into the deprecated legacy behavior where `begin`/`commit` hooks are logical no-ops and the driver auto-commits each statement.

The driver connection remains in autocommit mode (`autocommit=true`) in both modes.

This is now the default and should be tested carefully with project-specific materializations and hooks. Projects that depend on autocommit-only behavior should set the flag to `false` during migration.

```yaml
# dbt_project.yml
flags:
  dbt_sqlserver_enable_safe_type_expansion: true
  dbt_sqlserver_use_dbt_transactions: true # default
```

### `dbt_sqlserver_use_native_string_types`

*(default: `true`)* Controls the SQL Server-native mappings used for dbt string types. With the default enabled, `STRING` maps to `VARCHAR(MAX)`, `NCHAR` maps to `NCHAR(1)`, and `NVARCHAR` maps to `NVARCHAR(4000)`. Set it to `false` to opt back into the deprecated legacy mappings: `STRING` and `NVARCHAR` map to `VARCHAR(8000)`, while `NCHAR` maps to `CHAR(1)`.

```yaml
flags:
  dbt_sqlserver_use_native_string_types: false  # deprecated legacy behavior
```

### `xact_abort`

*(default: `true`)* Profile/connection field. When enabled, the adapter runs `SET XACT_ABORT ON;` once per connection, right after it opens. With `XACT_ABORT ON`, a run-time error partway through a multi-statement batch (e.g. a `NOT NULL`/constraint violation during the DML table refresh's DELETE+INSERT swap) aborts the whole batch and rolls back any open transaction, instead of only aborting the failing statement and letting a trailing `COMMIT` persist a partial result. See [#718](https://github.com/dbt-msft/dbt-sqlserver/issues/718).

This is independent of `dbt_sqlserver_use_dbt_transactions` above: that flag decides who owns the transaction boundary (dbt vs. the driver's autocommit), while `xact_abort` decides how the server reacts to a run-time error mid-batch. `XACT_ABORT ON` matters even when there is no explicit transaction at all, which is exactly the configuration `dbt_sqlserver_use_dbt_transactions` offers no protection in — so the two settings are not derived from one another and both need to be considered independently.

Turn it off only if a project intentionally relies on continue-on-error batch semantics (e.g. a hook that expects one failing statement in a batch not to abort the rest):

```yaml
# profiles.yml
your_profile:
  target: dev
  outputs:
    dev:
      type: sqlserver
      # ...
      xact_abort: false # <-- opt-out; default is true
```

### `column_type_expansion_max_rows`

*(default: `1000000`)* Per-model config that limits when safe type expansion runs. When the target table exceeds this row count, safe type expansion is skipped (basic same-family string resizes still proceed). Set to `-1` to disable the check entirely.

```sql
-- In an incremental model
{{ config(materialized='incremental', unique_key='id',
           column_type_expansion_max_rows=500000) }}
```

### `prefer_single_alter_column`

*(default: `false`)* Model-level config that controls how `alter_column_type` changes column types on tables. When `false` (default), the adapter uses the safer approach: add a temporary column, copy data, drop the original, and rename. When `true`, the adapter uses a single `ALTER COLUMN` statement, which is faster on small, medium tables and instant on safe type expansions but may fail for types that cannot be implicitly converted.

```sql
-- In an incremental model
{{ config(materialized='incremental', unique_key='id',
           prefer_single_alter_column=true) }}
```

**Compatibility notes:** Enabling `dbt_sqlserver_use_dbt_transactions: true` may expose transaction-state assumptions hidden by autocommit-only mode. Explicit transaction macros may interact with dbt-managed transactions, and cleanup after failed DDL/DML may differ. Review pre/post hooks for in-transaction vs out-of-transaction semantics.

### `as_columnstore`

*(default: `true`)* When building a table, the adapter creates a [clustered columnstore index](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/columnstore-indexes-overview) (CCI) on it. Set `as_columnstore: false` to build a plain rowstore table instead.

This matters for any table containing a `(n)varchar(max)` or other LOB column, because SQL Server does not allow those data types to participate in a columnstore index. The table build fails with:

> Column '...' has a data type that cannot participate in a columnstore index.

A common case is dbt's [test failure storage](https://docs.getdbt.com/reference/resource-configs/store_failures): the audit tables can contain `VARCHAR(MAX)` columns (dbt's `STRING` type maps to `VARCHAR(MAX)`), so disable the CCI on those resources:

```yaml
# dbt_project.yml
data_tests:
  +store_failures: true
  +as_columnstore: false  # avoids CCI on (n)varchar(max) audit columns
```

You can also set it per model:

```sql
{{ config(materialized="table", as_columnstore=false) }}
```

With `table_refresh_method: dml`, a schema change makes the refresh fall back to a rename-swap. On that run — and only that run — the scratch table is rebuilt through `CREATE TABLE … INSERT … WITH (TABLOCK)`, so it carries the model's columnstore index, and under an enforced contract its `NOT NULL`s, into the swap. That run therefore executes the model's SQL twice — once for the `SELECT … INTO` that probes for the schema change, once for the rebuild. Steady-state refreshes are unaffected and keep the single, cheaper `SELECT … INTO`.

### Constraints

Constraints declared in a model's yaml are applied when — and only when — the model's [contract](https://docs.getdbt.com/reference/resource-configs/contract) is enforced, which is what every dbt adapter does and keeps their cost opt-in. (dbt-core does not raise if you declare constraints with the contract off — they are simply never emitted.) `not_null`, `check`, `unique`, `primary_key` and `foreign_key` are all supported.

**Where a constraint lands depends on whether you name it.**

An unnamed constraint is rendered inline in the `CREATE TABLE` column list and SQL Server names it (`PK__my_model__3213E83F…`). It is validated as the table is built, so a violation fails before the new table is swapped in and the previous one is left untouched.

A *model-level* constraint carrying `name:` is applied by `ALTER TABLE … ADD CONSTRAINT` after the build swaps the new table into place and drops the old one. SQL Server scopes constraint names per schema, and a table is built alongside the one it replaces, so that is the first moment the name is free to reuse — naming a constraint inline would collide with the outgoing table (`Msg 2714`) on every rebuild after the first. The trade-off is that this runs after the model has committed: if the data violates the constraint, the model fails with the table already in place but unconstrained, and — as with any failure this late in a build — `post_hook`s declared with `transaction: false` do not run.

Name a constraint when you want it stable across environments (schema-comparison tools report the generated names as differences) or need to reference it later. A `name:` on a *column-level* constraint is ignored with a warning — declare it under the model's `constraints:` key instead.

```yaml
models:
  - name: fact_sales
    config:
      contract:
        enforced: true
    constraints:
      # named: applied by ALTER TABLE after the swap
      - type: primary_key
        name: PK_fact_sales
        columns: [sale_id]
      - type: foreign_key
        name: FK_fact_sales_customer
        columns: [customer_id]
        to: ref('dim_customer')
        to_columns: [customer_id]
      # unnamed: rendered into the CREATE TABLE
      - type: check
        expression: amount >= 0
    columns:
      - name: sale_id
        data_type: int
        constraints:
          - type: not_null
      - name: customer_id
        data_type: int
        constraints:
          - type: not_null
      - name: amount
        data_type: decimal(18,2)
```

#### Clustering

`primary_key` and `unique` are emitted as `NONCLUSTERED` by default so they can coexist with the clustered columnstore index built for [`as_columnstore`](#as_columnstore). Use dbt's own `expression` field to ask for something else:

```yaml
    constraints:
      - type: primary_key
        name: PK_fact_sales
        columns: [sale_id]
        expression: clustered   # requires as_columnstore: false
```

`clustered` and `nonclustered` are the only values understood here. `expression` is free text that dbt splices between the keyword and the column list, which is the one place T-SQL accepts nothing else — index options such as `with (fillfactor = 90)` belong on a separate index, not on the constraint.

#### Foreign keys

Two things are worth knowing before adding them:

- **They are not free at build time.** Every load is validated against them; add them where you want the guarantee, not everywhere the relationship exists.
- **A foreign key pointing at a model blocks that model's rebuild.** The build renames the outgoing table to a backup and drops it, but the child's foreign key follows the renamed object, so the drop fails with `Msg 3726`. SQL Server has no `DROP TABLE … CASCADE`.

  The adapter ships a macro for exactly this, meant as a `pre_hook` on the **referenced** (parent) model:

  ```sql
  {{ config(pre_hook="{{ drop_fk_constraints() }}") }}
  ```

  It drops the foreign keys in both directions — the inbound ones other tables hold against this model, and this model's own outbound ones — so the rebuild's backup drop succeeds. The trade-off is explicit and worth stating: **the child's foreign key does not exist between the parent's rebuild and the child's next build.** (dbt-postgres makes the same trade silently, by issuing every `drop table` with `cascade`.)

  `table_refresh_method: dml` is *not* a workaround. Its steady-state refresh issues `DELETE FROM <parent>`, which fails with `Msg 547` as soon as the child holds referencing rows, and its schema-change path falls back to the same rename-swap, hitting `Msg 3726` anyway.

- **SQL Server has no cross-database foreign keys.** `to: ref(...)` resolves to a fully qualified relation, database included, which SQL Server accepts as long as it names the current database. A target in another database fails with `references invalid table`.

A foreign key also does not order the build on its own: add an explicit `-- depends_on: {{ ref('dim_customer') }}` to the child model so the parent is built first.

#### Changing a constraint after the first build

Named constraints are applied by an `ALTER TABLE` whose `ADD` is guarded on the name already being present on the table, so:

- **Adding** a constraint to a model that already exists lands on its next run — no `--full-refresh` needed, on `table` and `incremental` alike.
- **Changing** an existing constraint's definition while keeping its name is *not* detected: a constraint name, unlike a `dbt_idx_` index name, says nothing about what the constraint does. Run `--full-refresh` to apply the new definition; that rebuilds the table, so the constraint is created fresh. (A `table` model rebuilds on every run and needs nothing special.)
- **Renaming** a constraint on a table that persists across runs adds the new name beside the old one. For a `check`, `unique` or `foreign_key` that means a duplicate; for a `primary_key` the run fails with `Msg 1779` (*table already has a primary key defined on it*) until the old one is dropped. Rename with `--full-refresh`.
- **Removing** a constraint from the yaml does not drop it from the database. Drop it yourself, or `--full-refresh`.

Every bullet above is about *named* constraints. Unnamed ones ride the `CREATE TABLE`, so they follow the table and change only when the table is rebuilt — on a materialization whose table persists across runs (`incremental` in its steady state, `table_refresh_method: dml`), adding an unnamed constraint to an existing model does nothing until a `--full-refresh`, and the run still reports success. Name it, or full-refresh.

#### Build-shape notes

An *unnamed* `primary_key` or `unique` constraint on a column that also carries a [data mask](#dynamic-data-masking-masked_with--masks) is rejected by the build. The constraint rides the `CREATE TABLE`, so its index already exists by the time `apply_masks` runs, and the adapter refuses to mask any column that an index has as a key: *is configured for masking but is also an index key column*. Declare that constraint at the model level **with a `name:`** instead — named constraints are applied by `ALTER TABLE` after the masks are in place, which the adapter allows.

With `full_refresh_build: prebuilt`, a `primary_key` or `unique` constraint creates a nonclustered index on the table *before* the bulk load. That secondary index has to be maintained row by row during `INSERT … WITH (TABLOCK)`, which is fully logged and adds to a load that `prebuilt` exists to make cheap. If a model is on `prebuilt` because its load time matters, weigh the key constraints against that; `check`, `not_null` and `foreign_key` do not create indexes and do not carry this cost.

### Dynamic Data Masking (`masked_with` / `masks`)

The adapter can apply SQL Server [Dynamic Data Masking](https://learn.microsoft.com/en-us/sql/relational-databases/security/dynamic-data-masking) (DDM) to columns as part of the materialization, so masks are re-applied on every build and survive dbt's drop-and-recreate on a full refresh. A principal granted `SELECT` but not `UNMASK` then sees masked values instead of real data (dbt's own build principal, being `db_owner`, keeps `UNMASK` and reads real data). Requires **SQL Server 2016+**.

There are two config surfaces, and you can use either or both:

**Column-level `masked_with:`** — a first-class column property in schema YAML (like `data_type:` or `constraints:`), whose value is the masking-function string:

```yaml
# models/schema.yml
version: 2
models:
  - name: core_patients
    columns:
      - name: surname
        masked_with: "default()"
      - name: nhs_number
        masked_with: 'partial(0,"XXXXXXXXXX",0)'
```

**Model-level `masks`** — a `{column: function}` dict, settable in the in-file `{{ config() }}`, the model's `.yml` `config:` block, or a directory-wide default in `dbt_project.yml`. It merges key-wise across those levels (like `meta`), so a directory default and a per-model tweak combine rather than clobber:

```sql
{{ config(masks={'surname': "default()", 'nhs_number': 'partial(0,"XXXXXXXXXX",0)'}) }}
```

```yaml
# dbt_project.yml — mask nhs_number on every model under datasets/ that has it
models:
  my_project:
    datasets:
      +masks: { nhs_number: "default()" }
```

Behaviour:

- **Precedence:** when both surfaces target the same column, the column-level `masked_with` wins, and a warning naming the model, column and both functions is emitted (even when they agree). This is not something dbt itself ranks, so the rule is the adapter's: a column is more specific than a model.
- **Opt out of an inherited default:** set `masked_with: null` on the column to remove a mask inherited from a directory/model-level `masks` entry.
- **Validation:** a `masks` (or `masked_with`) entry naming a column that is not in the built relation is skipped with a warning (a likely typo or stale rename); the run does not fail.
- **Unmaskable columns:** computed, `FILESTREAM`, sparse `COLUMN_SET` and `Always Encrypted` columns cannot carry a mask, and the run errors listing them rather than emitting DDL that fails.
- **Views/ephemeral/seeds:** masks apply to base tables only (`table`, `incremental`, `snapshot`). Views inherit masking from their base tables and cannot carry a mask; **seeds are not currently masked**.
- **Idempotent:** the adapter diffs the desired masks against `sys.masked_columns` and emits only the `ADD` / change / `DROP MASKED` statements that changed, so a persisted (incremental) re-run with no config change issues no DDL.

**Indexes and masking.** SQL Server cannot *add* a mask to a column an index depends on (documented for all versions: `ALTER TABLE ALTER COLUMN … failed because one or more objects access this column`). The adapter avoids this on fresh builds by applying masks **before** it creates (rowstore) indexes — which is exactly Microsoft's documented workaround order (mask, then create the index). The default clustered columnstore index is unaffected (its columns are included, not key columns). On a **persisted** table (incremental/snapshot without full refresh), adding a *new* mask to a column that is already an index key errors with a message pointing to the drop-index → mask → recreate-index workaround.

**Version notes.** All masking DDL the adapter emits (`ADD MASKED`, `MASKED WITH`, `DROP MASKED`) and the functions `default()`, `email()`, `random(a,b)` and `partial(...)` work on 2016+. The `datetime()` partial-date function and granular column/schema/table-scoped `UNMASK` are SQL Server 2022+ only; the adapter never emits them, but mask-function strings are passed through verbatim, so using a 2022-only function on an older server will be rejected by SQL Server.

### Object-level DENY (`denies`)

In SQL Server, an object-level `DENY` is the only way to carve an exception out of a schema-level `GRANT` — grant a principal `SELECT` on a whole schema, then `DENY SELECT` on the individual PII-bearing models. But a `DENY` is stored against the object's `object_id`, so dbt destroys it every time it drops and recreates the relation (which is *every* run for a view). The schema grant survives; its exceptions silently evaporate, leaving a **fail-open** posture that no run reports.

The `denies` config re-applies object-level DENYs after each build, diffed against `sys.database_permissions`, the same way `masks` re-applies Dynamic Data Masking. Its shape mirrors `grants` — a `{privilege: [principals]}` map — and it is settable in the in-file `{{ config() }}`, the model's `.yml` `config:` block, or a directory-wide default in `dbt_project.yml` (merging key-wise across those levels, like `masks`):

```sql
{{ config(denies={'select': ['Restricted_Read_Only']}) }}
```

- **Survives rebuilds:** the DENY is present after `dbt run` and *still present after the next run*, for `table`, `view`, `incremental` (append and full-refresh) and `snapshot`. Unlike masks, this **includes views** — a view is a valid securable and is recreated on every run, which is where a DENY is lost most often.
- **Reconciled:** the adapter diffs the config against the live DENY state and emits only what changed — `DENY` for configured-not-present, `REVOKE` for present-not-configured (a `REVOKE` removes a `DENY`). Removing a principal from `denies` revokes its DENY on the next run; a converged, persisted relation issues no DDL.
- **Scope:** object-level DENY for the table privileges `select`, `insert`, `update`, `delete`, `references`. An unsupported privilege is warned and skipped (the warning surfaces the likely typo without taking down the build). Column-, database- and server-scoped permissions are out of scope (the latter survive a rebuild already).
- **Absent principal:** a `DENY` to a non-existent database principal is warned-and-skipped (the run still succeeds), so one config runs unchanged across dev, CI and prod where the principal set differs.
- **Ordering vs `grants`:** grants are applied first, then denies, so the final state is unambiguous. A `(privilege, principal)` pair appearing in both `grants` and `denies` is warned as a likely mistake (a DENY overrides a GRANT in SQL Server).
- **Other adapters:** `denies` is a no-op on non-SQL-Server adapters, not an error. This is SQL-Server-specific by design — `DENY` is absent from the SQL standard and does not port.

## Contributing

[![Unit tests](https://github.com/dbt-msft/dbt-sqlserver/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/dbt-msft/dbt-sqlserver/actions/workflows/unit-tests.yml)
[![Integration tests on SQL Server](https://github.com/dbt-msft/dbt-sqlserver/actions/workflows/integration-tests-sqlserver.yml/badge.svg)](https://github.com/dbt-msft/dbt-sqlserver/actions/workflows/integration-tests-sqlserver.yml)

This adapter is community-maintained.
You are welcome to contribute by creating issues, opening or reviewing pull requests, or helping other users in the Slack channel.
If you're unsure how to get started, check out our [contributing guide](CONTRIBUTING.md).

## License

[![PyPI - License](https://img.shields.io/pypi/l/dbt-sqlserver)](https://github.com/dbt-msft/dbt-sqlserver/blob/master/LICENSE)

## Code of Conduct

This project and everyone involved is expected to follow the [dbt Code of Conduct](https://community.getdbt.com/code-of-conduct).
