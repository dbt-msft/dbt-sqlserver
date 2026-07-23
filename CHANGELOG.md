# Changelog

### v1.10.1

#### Features

- Add column-level Dynamic Data Masking (DDM). Declare masks via a first-class `masked_with:` column property in schema YAML or a model-level `masks` ({column: function}) config (which merges key-wise across `dbt_project.yml`, `.yml` and in-file `config()`), and the adapter (re)applies them on every build so they survive full-refresh rebuilds. Diffs against `sys.masked_columns` and emits only changed `ADD`/`MASKED WITH`/`DROP MASKED` DDL — the function comparison folds syntactic spacing/case (which SQL Server reformats) but preserves the `partial()` padding literal verbatim, so a whitespace- or case-only padding change (e.g. `"XX"` → `"X X"`) is still applied; column-level wins over model-level (warned); `masked_with: null` opts a column out of an inherited default; unknown columns are skipped with a warning; unmaskable column types error. Applies to `table`/`incremental`/`snapshot` base tables only (not views or seeds), and masks are applied before index creation so masked index-key columns work on fresh builds. Requires SQL Server 2016+.
- Add Postgres-style `indexes` model config for tables, incrementals, seeds and snapshots, covering most `CREATE INDEX` options. [#535](https://github.com/dbt-msft/dbt-sqlserver/issues/535)
- Index names are deterministic definition hashes (`dbt_idx_` prefix); creation is idempotent and unchanged definitions are never rebuilt.
- Reconcile indexes against the config on incremental, DML-refresh and snapshot runs, applied as one atomic batch. Constraint-backing, legacy post-hook and `as_columnstore` indexes are never dropped.
- Index introspection reads the catalog lock-free throughout (`NOLOCK` on every `sys` view, not just `sys.indexes`), so it no longer queues behind concurrent index DDL.
- Add `drop_unmanaged_indexes` config (`false` (default) / `warn` / `true`) for indexes dbt didn't create.
- Validate cross-index config conflicts (multiple clustered indexes, clustered vs `as_columnstore`).
- Document the minimum supported SQL Server version (2017). Partitioning, `XML_COMPRESSION` and ordered columnstore are not yet expressible in the `indexes` config.
- Add SQL Server 2025 to the integration-test matrix (pyodbc and `mssql-python` backends, ODBC Driver 18) and document it as a supported version.
- Add `dbt_sqlserver_enable_safe_type_expansion` behaviour flag to allow safe column type widening during schema expansion: `varchar` → `nvarchar`, integer family promotions (`bit` → `tinyint` → `smallint` → `int` → `bigint`), and `numeric`/`decimal` precision/scale upgrades. Gated by the per-model `column_type_expansion_max_rows` config (default 1,000,000 rows). [#699](https://github.com/dbt-msft/dbt-sqlserver/issues/699).
- Add `prefer_single_alter_column` model config to use a single `ALTER COLUMN` statement instead of the add+update+drop+rename pattern when altering column types on tables.
- Add `string_type_instance()` to preserve the NVARCHAR/NCHAR type family during column expansion, fixing incorrect promotion of NVARCHAR/NCHAR to VARCHAR.
- Restrict `is_integer()` detection to SQL Server integer types, including `tinyint` and `bit`, and remove PostgreSQL-only integer aliases.
- Add `dbt_sqlserver_use_dbt_transactions` flag for proper T-SQL `BEGIN TRAN`/`COMMIT`/`ROLLBACK` transaction handling. When enabled, operations use real server-side transactions instead of the default autocommit mode. [#708](https://github.com/dbt-msft/dbt-sqlserver/issues/708)
- Implement relation and column comment persistence via `persist_docs`. [#289](https://github.com/dbt-msft/dbt-sqlserver/issues/289)
- Add optional `mssql-python` connection backend alongside the default `pyodbc` backend. [#681](https://github.com/dbt-msft/dbt-sqlserver/issues/681)

#### Bugfixes

- Fix `rows_affected` reporting zero for delete+insert incremental models by suppressing the DELETE row-count token before restoring `NOCOUNT` for the INSERT. [#583](https://github.com/dbt-msft/dbt-sqlserver/issues/583)
- Fix unit tests with empty fixtures (`rows: []`) generating invalid `limit 0` syntax; emit `top 0` instead. Also fix `get_columns_in_query()` for queries starting with a CTE, which broke unit tests with an empty `expect` block; such queries are now described via `sp_describe_first_result_set` instead of being executed. [#698](https://github.com/dbt-msft/dbt-sqlserver/issues/698)
- Fix catalog generation for NVARCHAR/NCHAR columns: use `user_type_id` instead of `system_type_id` in catalog.sql, preventing them from appearing as `SYSNAME` in `dbt docs`. [#637](https://github.com/dbt-msft/dbt-sqlserver/issues/637)
- Fix `varchar(max)` / `nvarchar(max)` columns being incorrectly treated as size `-1` during type expansion, preventing `varchar(max)` → `varchar(100)` narrowing and properly allowing `varchar(100)` → `varchar(max)` expansion.
- Fix seed table ingestion of empty numeric cells by inlining `null` literals instead of binding parameters. [#425](https://github.com/dbt-msft/dbt-sqlserver/issues/425)
- Guard `run_hooks` commit with `@@trancount` check for autocommit safety when running post-hooks. [#444](https://github.com/dbt-msft/dbt-sqlserver/issues/444)
- Fix `columnstore IF EXISTS` guard to check `object_id('schema.table')` correctly.
- Gate the `optimize_for_sequential_key` and `resumable` index options on the detected engine version: they require SQL Server 2019+, so on 2017/2016 the index is now built without them (with a warning) instead of failing with "is not a recognized CREATE INDEX option".
- Fix index reconciliation for wide tables and clustered columnstore indexes: cast aggregated column names to `nvarchar(max)` to avoid `STRING_AGG` error 9829, and skip column aggregation for clustered columnstore indexes, which have no key columns and are matched by name/type only. [#735](https://github.com/dbt-msft/dbt-sqlserver/issues/735)
- Fix `add_query` retry handling to honor the configured retry count and avoid a retry-event crash. [#730](https://github.com/dbt-msft/dbt-sqlserver/pull/730)
- Escape closing braces in pyodbc connection-string values so credentials and other values containing `}` are parsed correctly. [#734](https://github.com/dbt-msft/dbt-sqlserver/pull/734)
- Cancel the in-flight cursor when dbt cancels an open SQL Server query, with best-effort handling when cancellation is unsupported or the statement has already completed. [#733](https://github.com/dbt-msft/dbt-sqlserver/pull/733)
- Apply `query_options` / `query_options_raw` (and the `LABEL` query tag) on the `table_refresh_method: dml` path. Both the scratch-build `SELECT ... INTO` and the swap `INSERT ... SELECT` now emit the `OPTION (...)` clause, matching `create_table_as`; previously they were silently dropped on every steady-state DML refresh.
- Escape single quotes in `query_tag` before building `OPTION (LABEL)` clause.
- Map Python `float` to SQL Server `float`, not `bigint`.
- Set default port to `1433` (instead of Postgres `5432`) in `dbt init` profile template.
- Make `drop_schema_name` usable again. [#563](https://github.com/dbt-msft/dbt-sqlserver/issues/563)
- Raise `DbtRuntimeError` instead of `ValueError` for missing access token.

#### Under the hood

- Consolidate linting tooling (isort, flake8, pycln, absolufy-imports) into Ruff. [#707](https://github.com/dbt-msft/dbt-sqlserver/issues/707)
- Drop Python 3.9 from publish-docker CI matrix.
- Remove dead clone macro and unused view scaffolding variable.
- Standardise devcontainer ODBC environment setup. [#722](https://github.com/dbt-msft/dbt-sqlserver/issues/722)
- Disable dbt telemetry and version check in test environment.
- Add `pytest-cov` and fix dead `from_dict` override found by coverage.
- Bump pip and GitHub Actions dependencies.

### v1.10.0

#### Features

- Official support for `dbt-core` 1.10.
- Add `query_options` / `query_options_raw` model configs for emitting SQL Server `OPTION` clauses on table, incremental (delete+insert / microbatch), snapshot, and unit_test materializations. See https://github.com/dbt-msft/dbt-sqlserver/issues/613.
- `get_query_options()` is the new extension point for customising the emitted `OPTION` clause.
- Add DML table refresh support for table materializations.
- Add opt-in native string type mappings via a behaviour flag.
- Add default schema concatenation flag support and update the documentation.
- Enable SQL Server limited-relation no-alias behavior by default.
- Support catalog generation across multiple databases. [#603](https://github.com/dbt-msft/dbt-sqlserver/issues/603)
- Fix view rematerialization when a view already exists, including preserving grants and avoiding unnecessary rebuilds. [#610](https://github.com/dbt-msft/dbt-sqlserver/issues/610)

#### Bugfixes

- Fix reserved-keyword quoting in table-create `DROP VIEW` handling.
- Fix CTE detection in empty-subquery wrapping when leading comments are present.
- Fix snapshot meta column name overrides on a second run.
- Fix connection-string port handling regressions.
- Fix `TABLOCK` interaction with contract-enforced inserts and `query_options`.
- Fix `get_view_definition()` escaping for `]` characters.
- Make the highest-frequency catalog queries sargable so they seek instead of scan. [#686](https://github.com/dbt-msft/dbt-sqlserver/issues/686)

#### Under the hood

- Drop Python 3.9 support; this release targets Python 3.10 and newer.
- Adopt standard `pyproject.toml`/PEP 621 packaging metadata for source installs and downloads.

#### Migration note

- `apply_label()` is preserved as a callable alias (emits LABEL only) in case you use it in your own project but is no longer called by adapter macros. Projects that override `apply_label()` to customise the OPTION clause must override `get_query_options()` instead.

### v1.9.2

- Add default schema concatenation flag support and update the documentation. See PR #685.

### v1.9.1

- Removes the dependency on `dbt-fabric`.

### v1.9.0

- Update to support `dbt-core` 1.9.
- Remove Python 3.8 from official support.

### v1.8.7

- Bump version for release 1.8.7.

### v1.8.6

- Bump version for release 1.8.6.

### v1.8.5

- Fix broken imports from Fabric.

### v1.8.4

- Minor fix to tests with CTEs which do not start on first line. https://github.com/dbt-msft/dbt-sqlserver/issues/560

### v1.8.3

- Minor fix in no lock behaviour https://github.com/dbt-msft/dbt-sqlserver/pull/557

### v1.8.2

- Restores no lock behaviour from 1.4.2

### v1.8.1

- Fixes problem where databases with dashes are rendered properly

### v1.8.0

Updates dbt-sqlserver to support dbt 1.8.

Notable changes

- Adopts `dbt-common` and `dbt-adapters` as the upstream, in line with dbt projects.
- Implements the majority of the tests from the `dbt-test-adapters` project to provide better coverage.
- Implements better testing for `dbt-sqlserver` specific functions, including indexes.
- Realigns to closer to the global project, overriding some fabric specific implementations
- Adds new 1.8 features (and tests), including Unit Tests

Update also fixes a number of regressions related to the fabric adapter and 1.7 releases.
These include

- Proper ALTER syntax for column changes (in both )
  - https://github.com/dbt-msft/dbt-sqlserver/pull/504/files
- Restoring cluster columntables post create on `tables`
  - https://github.com/dbt-msft/dbt-sqlserver/issues/473
- Adds proper constraints to tables and columns
  - https://github.com/dbt-msft/dbt-sqlserver/pull/500

There is a number of other changes as well, which can be found in the 1.8.0rc1, 1.8.0rc2 and 1.8.0 release notes.

While not directly included, credit also to **@ms32035**, **@axellpadilla**, **@gbarrington**, **@tkirschke** and others for their help with testing.

### v1.7.2

Huge thanks to GitHub users **@cody-scott** and **@prescode** for help with this long-awaited update to enable `dbt-core` 1.7.2 compatibility!

Updated to use dbt-fabric as the upstream adapter (https://github.com/dbt-msft/dbt-sqlserver/issues/441#issuecomment-1815837171)[https://github.com/dbt-msft/dbt-sqlserver/issues/441#issuecomment-1815837171] and (https://github.com/microsoft/dbt-fabric/issues/105)[https://github.com/microsoft/dbt-fabric/issues/105]

As the fabric adapter implements the majority of auth and required t-sql, this adapter delegates primarily to SQL auth and SQL Server specific
adaptations (using `SELECT INTO` vs `CREATE TABLE AS`).

Additional major changes pulled from fabric adapter:

- `TIMESTAMP` changing from `DATETIMEOFFSET` to `DATETIME2(6)`
- `STRING` changing from `VARCHAR(MAX)` to `VARCHAR(8000)`

#### Future work to be validated

- Fabric specific items that need further over-rides (clone for example needed overriding)
- Azure Auth elements to be deferred to Fabric, but should be validated
- T-SQL Package to be updated and validated with these changes.
- Integration of newer dbt-core features.

### v1.4.3

Another minor release to follow up on the 1.4 releases.

Replacing the usage of the `dm_sql_referencing_entities` stored procedure with a query to `sys.sql_expression_dependencies` for better compatibility with child adapters.

### v1.4.2

Minor release to follow up on 1.4.1 and 1.4.0.

Adding `nolock` to information_schema and sys tables/views can be overridden with the dispatched `information_schema_hints` macro. This is required for adapters inheriting from this one.

### v1.4.1

This is a minor release following up on 1.4.0 with fixes for long outstanding issues.
Contributors to this release are [@cbini](https://github.com/cbini), [@rlshuhart](https://github.com/rlshuhart), [@jacobm001](https://github.com/jacobm001), [@baldwicc](https://github.com/baldwicc) and [@sdebruyn](https://github.com/sdebruyn).

#### Features

- Added support for a custom schema owner. You can now add `schema_authorization` (or `schema_auth`) to your profile.
  If you do so, dbt will create schemas with the `authorization` option suffixed by this value.
  If you are authorizing dbt users or service principals on Azure SQL based on an Azure AD group,
  it's recommended to set this value to the name of the group. [#153](https://github.com/dbt-msft/dbt-sqlserver/issues/153) [#382](https://github.com/dbt-msft/dbt-sqlserver/issues/382)
- Documentation: added more information about the permissions which you'll need to grant to run dbt.
- Support for `DATETIMEOFFSET` as type to be used in dbt source freshness tests. [#254](https://github.com/dbt-msft/dbt-sqlserver/issues/254) [#346](https://github.com/dbt-msft/dbt-sqlserver/issues/346)
- Added 2 options related to timeouts to the profile: `login_timeout` and `query_timeout`.
  The default values are `0` (no timeout). [#162](https://github.com/dbt-msft/dbt-sqlserver/issues/162) [#395](https://github.com/dbt-msft/dbt-sqlserver/issues/395)

#### Bugfixes

- Fixed issues with databases with a case-sensitive collation
  and added automated testing for it so that we won't break it again. [#212](https://github.com/dbt-msft/dbt-sqlserver/issues/212) [#391](https://github.com/dbt-msft/dbt-sqlserver/issues/391)
- Index names are now MD5 hashed to avoid running into the maximum amount of characters in index names
  with index with lots of columns with long names. [#317](https://github.com/dbt-msft/dbt-sqlserver/issues/317) [#386](https://github.com/dbt-msft/dbt-sqlserver/issues/386)
- Fixed the batch size calculation for seeds. Seeds will run more efficiently now. [#396](https://github.com/dbt-msft/dbt-sqlserver/issues/396) [#179](https://github.com/dbt-msft/dbt-sqlserver/issues/179) [#210](https://github.com/dbt-msft/dbt-sqlserver/issues/210) [#211](https://github.com/dbt-msft/dbt-sqlserver/issues/211)
- Added `nolock` to queries for all information_schema/sys tables and views.
  dbt runs a lot of queries on these metadata schemas.
  This can often lead to deadlock issues if you are using a high number of threads or dbt processes.
  Adding `nolock` to these queries avoids the deadlocks. [#379](https://github.com/dbt-msft/dbt-sqlserver/issues/379) [#381](https://github.com/dbt-msft/dbt-sqlserver/issues/381)
- Fixed implementation of `{{ hash(...) }}` for null values. [#392](https://github.com/dbt-msft/dbt-sqlserver/issues/392)

#### Under the hood

- Fixed more concurrency issues with automated Azure integration testing.
- Removed extra `__init__.py` files. [#171](https://github.com/dbt-msft/dbt-sqlserver/issues/171) [#202](https://github.com/dbt-msft/dbt-sqlserver/issues/202)
- Added commits to be ignored in git blame for easier blaming. [#385](https://github.com/dbt-msft/dbt-sqlserver/issues/385)

### v1.4.0

- [@Elliot2718](https://github.com/Elliot2718) made their first contribution in https://github.com/dbt-msft/dbt-sqlserver/pull/204
- [@i-j](https://github.com/i-j) made their first contribution in https://github.com/dbt-msft/dbt-sqlserver/pull/345

#### Features

- Support for [dbt-core 1.4](https://github.com/dbt-labs/dbt-core/releases/tag/v1.4.0)
  - [Incremental predicates](https://docs.getdbt.com/docs/build/incremental-models#about-incremental_predicates)
  - Add support for Python 3.11
  - Replace deprecated exception functions
  - Consolidate timestamp macros

#### Bugfixes

- Add `nolock` query hint to several metadata queries to avoid deadlocks by [@Elliot2718](https://github.com/Elliot2718) in https://github.com/dbt-msft/dbt-sqlserver/pull/204
- Rework column metadata retrieval to avoid duplicate results and deadlocks by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/368
- Model removal will now cascade and also drop related views so that views are no longer in a broken state by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/366
- Fixed handling of on_schema_change for incremental models by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/376

#### Under the hood

- Fixed lots of testing concurrency issues
- Added all available tests as of dbt 1.4.6

**Full Changelog**: https://github.com/dbt-msft/dbt-sqlserver/compare/v1.3.1...v1.4.0

<details><summary>PR changelog</summary>
<p>

- Bump pre-commit from 2.20.0 to 3.2.0 by [@dependabot](https://github.com/dependabot) in https://github.com/dbt-msft/dbt-sqlserver/pull/344
- Bump docker/build-push-action from 3.2.0 to 4.0.0 by [@dependabot](https://github.com/dependabot) in https://github.com/dbt-msft/dbt-sqlserver/pull/331
- [pre-commit.ci] pre-commit autoupdate by [@pre-commit-ci](https://github.com/pre-commit-ci) in https://github.com/dbt-msft/dbt-sqlserver/pull/316
- Bump wheel from 0.38.4 to 0.40.0 by [@dependabot](https://github.com/dependabot) in https://github.com/dbt-msft/dbt-sqlserver/pull/343
- Copy for workflow schtuff by [@dataders](https://github.com/dataders) in https://github.com/dbt-msft/dbt-sqlserver/pull/350
- avoid publishing docker from other branches than master by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/351
- bump pre-commit by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/353
- fix pre-commit for python 3.7 by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/354
- use 127.0.0.1 to avoid issues with local testing by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/358
- allow for more flexible local testing with azure auth by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/359
- credit where due by [@dataders](https://github.com/dataders) in https://github.com/dbt-msft/dbt-sqlserver/pull/355
- remove condition for azure testing by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/360
- ignore owner when testing docs in azure by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/365
- impl of information_schema name closer to default by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/367
- Add nolock by [@Elliot2718](https://github.com/Elliot2718) in https://github.com/dbt-msft/dbt-sqlserver/pull/204
- Fix concurrency issues and document create as by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/368
- add debug tests by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/363
- add concurrency test by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/362
- add aliases tests by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/361
- add ephemeral error handling test by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/364
- mark db-wide tests as flaky by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/369
- remove azure max parallel test runs by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/370
- add nolock to more metadata calls to avoid deadlocks by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/374
- add query comment tests by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/375
- add seed tests and add cascade to drop relation by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/366
- make testing faster by running multithreaded by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/372
- add tests for changing relation type by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/373
- [incremental models] add tests, various bugfixes and support for incremental predicates by [@sdebruyn](https://github.com/sdebruyn) in https://github.com/dbt-msft/dbt-sqlserver/pull/376

</p>
</details>

### 1.3.1

####

Minor release to loosen dependency on dbt-core and pyodbc

### v1.3.0

#### Features

- Support for [dbt-core 1.3](https://github.com/dbt-labs/dbt-core/releases/tag/v1.3.0)
  - Python models are currently not supported in this adapter
  - The following cross-db macros are not supported in this adapter: `bool_or`, `array_construct`, `array_concat`, `array_append`

#### Fixes

- The macro `type_boolean` now returns the correct data type (`bit`)

#### Chores

- Update adapter testing framework
- Update dependencies and pre-commit hooks

### v1.2.0

#### Possibly breaking change: connection encryption

For compatibility with MS ODBC Driver 18, the settings `Encrypt` and `TrustServerCertificate` are now always added to the connection string.
These are configured with the keys `encrypt` and `trust_cert` in your profile.
In previous versions, these settings were only added if they were set to `True`.

The new version of the MS ODBC Driver sets `Encrypt` to `True` by default.
The adapter is following this change and also defaults to `True` for `Encrypt`.

The default value for `TrustServerConnection` remains `False` as it would be a security risk otherwise.

This means that connections made with this version of the adapter will now have `Encrypt=Yes` and `TrustServerCertificate=No` set if you are using the default settings.
You should change the settings `encrypt` or `trust_cert` to accommodate for your use case.

#### Features

- Support for [dbt-core 1.2](https://github.com/dbt-labs/dbt-core/releases/tag/v1.2.0)
  - Full support for the new [grants config](https://docs.getdbt.com/reference/resource-configs/grants)
  - New configuration option: `auto_provision_aad_principals` - setting this to `true` will automatically create contained database users linked to Azure AD principals or groups if they don't exist yet when they're being used in grant configs
- Support for MS ODBC Driver 18
- Support automatic retries with new `retries` setting introduced in core
- The correct owner of a table/view is now visible in generated documentation (and in catalog.json)
- A lot of features of dbt-utils & T-SQL utils are now available out-of-the-box in dbt-core and this adapter. A new release of T-SQL utils will follow.
  - Support for all `type_*` macros
  - Support for all [cross-database macros](https://docs.getdbt.com/reference/dbt-jinja-functions/cross-database-macros), except:
  - `bool_or`
  - `listagg` will only work in SQL Server 2017 or newer or the cloud versions. The `limit_num` option is unsupported. `DISTINCT` cannot be used in the measure.

#### Fixes

- In some cases the `TIMESTAMP` would be used as data type instead of `DATETIMEOFFSET`, fixed that

#### Chores

- Update adapter testing framework to 1.2.1
- Update pre-commit, tox, pytest and pre-commit hooks
- Type hinting in connection class
- Automated testing with SQL Server 2017, 2019 and 2022
- Automated testing with MS ODBC 17 and MS ODBC 18

### v1.1.0

See changes included in v1.1.0rc1 below as well

#### Fixes

- [#251](https://github.com/dbt-msft/dbt-sqlserver/pull/251) fix incremental models with arrays for unique keys ([@sdebruyn](https://github.com/sdebruyn) & [@johnnytang24](https://github.com/johnnytang24))
- [#214](https://github.com/dbt-msft/dbt-sqlserver/pull/214) fix for sources with spaces in the names ([@Freia3](https://github.com/Freia3))
- [#238](https://github.com/dbt-msft/dbt-sqlserver/pull/238) fix snapshots breaking when new columns are added ([@jakemcaferty](https://github.com/jakemcaferty))

#### Chores

- [#249](https://github.com/dbt-msft/dbt-sqlserver/pull/249) & [#250](https://github.com/dbt-msft/dbt-sqlserver/pull/251) add Python 3.10 to automated testing ([@sdebruyn](https://github.com/sdebruyn))
- [#248](https://github.com/dbt-msft/dbt-sqlserver/pull/248) update all documentation, README and include on dbt docs ([@sdebruyn](https://github.com/sdebruyn))
- [#252](https://github.com/dbt-msft/dbt-sqlserver/pull/252) add automated test for [#214](https://github.com/dbt-msft/dbt-sqlserver/pull/214) ([@sdebruyn](https://github.com/sdebruyn))

### v1.1.0.rc1

#### Features

- update to dbt 1.1

#### Fixes

- [#194](https://github.com/dbt-msft/dbt-sqlserver/pull/194) uppercased information_schema ([@TrololoLi](https://github.com/TrololoLi))
- [#215](https://github.com/dbt-msft/dbt-sqlserver/pull/215) Escape schema names so they can contain strange characters ([@johnf](https://github.com/johnf))

#### Chores

- Documentation on how to contribute to the adapter
- Automatic release process by adding a new tag
- Consistent code style with pre-commit
- [#201](https://github.com/dbt-msft/dbt-sqlserver/pull/201) use new dbt 1.0 logger ([@semcha](https://github.com/semcha))
- [#216](https://github.com/dbt-msft/dbt-sqlserver/pull/216) use new dbt testing framework ([@dataders](https://github.com/dataders) & [@sdebruyn](https://github.com/sdebruyn))

### v1.0.0

Please see [dbt-core v1.0.0 release notes](https://github.com/dbt-labs/dbt-core/releases/tag/v1.0.0) for upstream changes

#### Fixes

- fix index naming when columns contain spaces [#175](https://github.com/dbt-msft/dbt-sqlserver/pull/175)

#### Under the Hood

- re-organize macros to match new structure [#184](https://github.com/dbt-msft/dbt-sqlserver/pull/184)

### v0.21.1

#### features

- Added support for more authentication methods: automatic, environment variables, managed identity. All of them are documented in the readme. [#178](https://github.com/dbt-msft/dbt-sqlserver/pull/178) contributed by [@sdebruyn](https://github.com/sdebruyn)

#### fixes

- fix for [#186](https://github.com/dbt-msft/dbt-sqlserver/issues/186) and [#177](https://github.com/dbt-msft/dbt-sqlserver/issues/177) where new columns weren't being added when snapshotting or incrementing [#188](https://github.com/dbt-msft/dbt-sqlserver/pull/188)

### v0.21.0

Please see [dbt-core v0.21.0 release notes](https://github.com/dbt-labs/dbt-core/releases/tag/v0.21.0) for upstream changes

#### fixes

- in dbt-sqlserver v0.20.0, users couldn't use some out of the box tests, such as accepted_values. users can now also use CTEs in their ~bespoke~ custom data tests
- fixes issue with changing column types in incremental table column type [#152](https://github.com/dbt-msft/dbt-sqlserver/issue/152) [#169](https://github.com/dbt-msft/dbt-sqlserver/pull/169)
- workaround for Azure CLI token expires after one hour. Now we get new tokens for every transaction. [#156](https://github.com/dbt-msft/dbt-sqlserver/issue/156) [#158](https://github.com/dbt-msft/dbt-sqlserver/pull/158)

### v0.20.1

#### fixes:

- workaround for Azure CLI token expires after one hour. Now we get new tokens for every transaction. [#156](https://github.com/dbt-msft/dbt-sqlserver/issue/156) [#158](https://github.com/dbt-msft/dbt-sqlserver/pull/158)

### v0.20.0

#### features:

- dbt-sqlserver will now work with dbt `v0.20.0`. Please see dbt's [upgrading to `v0.20.0` docs](https://docs.getdbt.com/docs/guides/migration-guide/upgrading-to-0-20-0) for more info.
- users can now declare a custom `max_batch_size` in the project configuration to set the batch size used by the seed file loader. [#127](https://github.com/dbt-msft/dbt-sqlserver/issues/127) and [#151](https://github.com/dbt-msft/dbt-sqlserver/pull/151) thanks [@jacobm001](https://github.com/jacobm001)

#### under the hood

- `sqlserver__load_csv_rows` now has a safety provided by `calc_batch_size()` to ensure the insert statements won't exceed SQL Server's 2100 parameter limit. [#127](https://github.com/dbt-msft/dbt-sqlserver/issues/127) and [#151](https://github.com/dbt-msft/dbt-sqlserver/pull/151) thanks [@jacobm001](https://github.com/jacobm001)
- switched to using a `MANIFEST.in` to declare which files should be included
- updated `pyodbc` and `azure-identity` dependencies to their latest versions

### v0.19.2

#### fixes

- fixing and issue with empty seed table that dbt-redshift already addressed with [fishtown-analytics/dbt#2255](https://github.com/fishtown-analytics/dbt/pull/2255) [#147](https://github.com/dbt-msft/dbt-sqlserver/pull/147)
- drop unneeded debugging code that only was run when "Active Directory integrated" was given as the auth method [#149](https://github.com/dbt-msft/dbt-sqlserver/pull/149)
- hotfix for regression introduced by [#126](https://github.com/dbt-msft/dbt-sqlserver/issues/126) that wouldn't surface syntax errors from the SQL engine [#140](https://github.com/dbt-msft/dbt-sqlserver/issues/140) thanks [@jeroen-mostert](https://github.com/jeroen-mostert)!

#### under the hood:

- ensure that macros are not recreated for incremental models [#116](https://github.com/dbt-msft/dbt-sqlserver/issues/116) thanks [@infused-kim](https://github.com/infused-kim)
- authentication now is case-insensitive and accepts both `CLI` and `cli` as options. [#100](https://github.com/dbt-msft/dbt-sqlserver/issues/100) thanks (@JCZuurmond)[https://github.com/JCZuurmond]
- add unit tests for azure-identity related token fetching

### v0.19.1

#### features:

- users can now declare a model's database to be other than the one specified in the profile. This will only work for on-premise SQL Server and Azure SQL Managed Instance. [#126](https://github.com/dbt-msft/dbt-sqlserver/issues/126) thanks [@semcha](https://github.com/semcha)!

#### under the hood

- abandon four-part version names (`v0.19.0.2`) in favor of three-part version names because it isn't [SemVer](https://semver.org/) and it causes problems with the `~=` pip operator used dbt-synapse, a package that depends on dbt-sqlserver
- allow CI to work with the lower-cost serverless Azure SQL [#132](https://github.com/dbt-msft/dbt-sqlserver/pull/132)

### v0.19.0.2

#### fixes

- solved a bug in snapshots introduced in v0.19.0. Fixes: [#108](https://github.com/dbt-msft/dbt-sqlserver/issues/108), [#117](https://github.com/dbt-msft/dbt-sqlserver/issues/117).

### v0.19.0.1

#### fixes

- we now use the correct connection string parameter so MSFT can montior dbt adoption in their telemetry. [#98](https://github.com/dbt-msft/dbt-sqlserver/pull/98)

#### under the hood

- dbt-sqlserver's incremental materialization is now 100% aligneed logically to dbt's global_project behavior! this makes maintaining `dbt-sqlserver` easier by decreasing code footprint. [#102](https://github.com/dbt-msft/dbt-sqlserver/pull/102)
- clean up CI config and corresponding Docker image [#122](https://github.com/dbt-msft/dbt-sqlserver/pull/122)

### v0.19.0

#### New Features:

- dbt-sqlserver's snapshotting now 100% aligneed logically to dbt's snapshotting behavior! Users can now snapshot 'hard-deleted' record as mentioned in the [dbt v0.19.0 release notes](https://github.com/fishtown-analytics/dbt/releases/tag/v0.19.0). An added benefit is that it makes maintaining `dbt-sqlserver` by decreasing code footprint. [#81](https://github.com/dbt-msft/dbt-sqlserver/pull/81) [fishtown-analytics/dbt#3003](https://github.com/fishtown-analytics/dbt/issues/3003)

#### Fixes:

- small snapshot bug addressed via [#81](https://github.com/dbt-msft/dbt-sqlserver/pull/81)
- support for clustered columnstore index creation pre SQL Server 2016. [#88](https://github.com/dbt-msft/dbt-sqlserver/pull/88) thanks [@alangsbo](https://github.com/alangsbo)
- support for scenarios where the target db's collation is different than the server's [#87](https://github.com/dbt-msft/dbt-sqlserver/pull/87) [@alangsbo](https://github.com/alangsbo)

#### Under the hood:

- This adapter has separate CI tests to ensure all the connection methods are working as they should [#75](https://github.com/dbt-msft/dbt-sqlserver/pull/75)
- This adapter has a CI job for running unit tests [#103](https://github.com/dbt-msft/dbt-sqlserver/pull/103)
- Update the tox setup [#105](https://github.com/dbt-msft/dbt-sqlserver/pull/105)

### v0.18.1

#### New Features:

Adds support for:

- SQL Server down to version 2012
- authentication via:
  - Azure CLI (see #71, thanks [@JCZuurmond](https://github.com/JCZuurmond) !), and
  - MSFT ODBC Active Directory options (#53 #55 #58 thanks to [@NandanHegde15](https://github.com/NandanHegde15) and [@alieus](https://github.com/alieus))
- using a named instance (#51 thanks [@alangsbo](https://github.com/alangsbo))
- Adds support down to SQL Server 2012
- The adapter is now automatically tested with Fishtowns official adapter-tests to increase stability when making
  changes and upgrades to the adapter.

#### Fixes:

- Fix for lack of precision in the snapshot check strategy. Previously when executing two check snapshots the same
  second, there was inconsistent data as a result. This was mostly noted when running the automatic adapter tests.
  NOTE: This fix will create a new snapshot version in the target table
  on first run after upgrade.

### v0.18.0.1

#### New Features:

- Adds support for Azure Active Directory as authentication provider

#### Fixes:

- Fix for lack of precision in the snapshot check strategy. (#74 and #56 thanks [@qed](https://github.com/qed)) Previously when executing two check snapshots the same second, there was inconsistent data as a result. This was mostly noted when running the automatic adapter tests.
  NOTE: This fix will create a new snapshot version in the target table
  on first run after upgrade.
- #52 Fix deprecation warning (Thanks [@jnoynaert](https://github.com/jnoynaert))

#### Testing

- The adapter is now automatically tested with Fishtowns official adapter-tests to increase stability when making changes and upgrades to the adapter. (#62 #64 #69 #74)
- We are also now testing specific target configs to make the devs more confident that everything is in working order (#75)

### v0.18.0

#### New Features:

- Adds support for dbt v0.18.0

### v0.15.3.1

#### Fixes:

- Snapshots did not work on dbt v0.15.1 to v0.15.3

### v0.15.3

#### Fixes:

- Fix output of sql in the log files.
- Limited the version of dbt to 0.15, since later versions are unsupported.

### v0.15.2

#### Fixes:

- Fixes an issue with clustered columnstore index not being created.

### v0.15.1

#### New Features:

- Ability to define an index in a poosthook

#### Fixes:

- Previously when a model run was interrupted unfinished models prevented the next run and you had to manually delete them. This is now fixed so that unfinished models will be deleted on next run.

### v0.15.0.1

Fix release for v0.15.0

#### Fixes:

- Setting the port had no effect. Issue #9
- Unable to generate docs. Issue #12

### v0.15.0

Requires dbt v0.15.0 or greater

### pre v0.15.0

Requires dbt v0.14.x
