# Changelog

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

Adding `nolock` to information_schema and sys tables/views can be overriden with the dispatched `information_schema_hints` macro. This is required for adapters inheriting from this one.

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

- users can now delcare a model's database to be other than the one specified in the profile. This will only work for on-premise SQL Server and Azure SQL Managed Instance. [#126](https://github.com/dbt-msft/dbt-sqlserver/issues/126) thanks [@semcha](https://github.com/semcha)!

#### under the hood

- abandon four-part version names (`v0.19.0.2`) in favor of three-part version names because it isn't [SemVer](https://semver.org/) and it causes problems with the `~=` pip operator used dbt-synapse, a pacakge that depends on dbt-sqlserver
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

- Fixes an issue with clustered columnstore index not beeing created.

### v0.15.1

#### New Features:

- Ability to define an index in a poosthook

#### Fixes:

- Previously when a model run was interupted unfinished models prevented the next run and you had to manually delete them. This is now fixed so that unfinished models will be deleted on next run.

### v0.15.0.1

Fix release for v0.15.0

#### Fixes:

- Setting the port had no effect. Issue #9
- Unable to generate docs. Issue #12

### v0.15.0

Requires dbt v0.15.0 or greater

### pre v0.15.0

Requires dbt v0.14.x
