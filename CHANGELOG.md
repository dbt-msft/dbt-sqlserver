# Changelog
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
- dbt-sqlserver's snapshotting now 100% aligneed logically to dbt's snapshotting behavior! Users can now snapshot 'hard-deleted' record as mentioned in the [dbt v0.19.0 release notes](https://github.com/fishtown-analytics/dbt/releases/tag/v0.19.0). An added benefit is that it makes maintaining `dbt-sqlserver` by decreasing code footprint. [#81](https://github.com/dbt-msft/dbt-sqlserver/pull/81)  [fishtown-analytics/dbt#3003](https://github.com/fishtown-analytics/dbt/issues/3003)
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
    - Azure CLI (see #71, thanks @JCZuurmond !), and
    - MSFT ODBC Active Directory options (#53 #55 #58 thanks to @NandanHegde15 and @alieus) 
- using a named instance (#51 thanks @alangsbo)
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
- Fix for lack of precision in the snapshot check strategy. (#74 and #56 thanks @qed) Previously when executing two check snapshots the same second, there was inconsistent data as a result. This was mostly noted when running the automatic adapter tests. 
NOTE: This fix will create a new snapshot version in the target table
on first run after upgrade.
- #52 Fix deprecation warning (Thanks @jnoynaert)

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
