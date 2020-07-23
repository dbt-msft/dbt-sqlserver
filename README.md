# :construction: dbt-synapse :construction:

custom [dbt](https://www.getdbt.com) adapter for [Azure Synapse](https://azure.microsoft.com/en-us/services/synapse-analytics/). Major credit due to @mikaelene and [his `sqlserver` custom adapter](https://github.com/mikaelene/dbt-sqlserver).

## major differences b/w `dbt-synapse` and `dbt-sqlserver`
- macros use only Azure Synapse `T-SQL`. [Relevant GitHub issue](https://github.com/MicrosoftDocs/azure-docs/issues/55713)
- use of [Create Table as Select (CTAS)](https://docs.microsoft.com/en-us/sql/t-sql/statements/create-table-as-select-azure-sql-data-warehouse?view=aps-pdw-2016-au7) means you don't need post-hooks to create indices
- Azure Active Directory Authentication options


## status & support

Passing all tests in [dbt-integration-tests](https://github.com/fishtown-analytics/dbt-integration-tests/). 

### outstanding work:
- test incremental materializations more thoroughly than is done with [`dbt-integration-tests`](https://github.com/fishtown-analytics/dbt-integration-tests/).
- Add support for `ActiveDirectoryMsi`
- Publish as package to `pypi`
- Use CTAS to create seeds?
- staging external tables as sources (in progress)
- [officially rename the adapter from `sqlserver` to `synapse`](https://github.com/swanderz/dbt-synapse/pull/6)

### `dbt` version support
as of now, only support for dbt `0.15.3`, support for forthcoming `0.18.0` in development

Easiest install is to use pip (not yet registered on PyPI).

First install [ODBC Driver version 17](https://www.microsoft.com/en-us/download/details.aspx?id=56567).

```bash
pip install git+https://github.com/swanderz/dbt-synapse.git
```

On Ubuntu make sure you have the ODBC header files before installing

```
sudo apt install unixodbc-dev
```

## Authentication
`SqlPassword` is the default connection method, but you can also use the following [`pyodbc`-supported ActiveDirectory methods](https://docs.microsoft.com/en-us/sql/connect/odbc/using-azure-active-directory?view=sql-server-ver15#new-andor-modified-dsn-and-connection-string-keywords)  to authenticate:
- ActiveDirectory Password
- ActiveDirectory Interactive
- ActiveDirectory Integrated
- ActiveDirectory MSI (to be implemented)
#### boilerplate
this should be in every target definition
```
type: sqlserver
driver: 'ODBC Driver 17 for SQL Server' (The ODBC Driver installed on your system)
server: server-host-name or ip
port: 1433
schema: schemaname
```
#### SQL Server authentication 
```
user: username
password: password
```
#### ActiveDirectory Password 
Definitely not ideal, but available
```
authentication: ActiveDirectoryPassword
user: bill.gates@microsoft.com
password: i<3opensource?
```
#### ActiveDirectory Interactive (*Windows only*)
brings up the Azure AD prompt so you can MFA if need be.
```
authentication: ActiveDirectoryInteractive
user: bill.gates@microsoft.com
```
##### ActiveDirectory Integrated (*Windows only*)
uses your machine's credentials (might be disabled by your AAD admins)
```
authentication: ActiveDirectoryIntegrated
```
##### ActiveDirectory MSI (*to be implemented*)
```
authentication: ActiveDirectoryMsi
```

## Table Materializations
CTAS allows you to materialize tables with indices and distributions at creation time, which obviates the need for post-hooks to set indices.

### Example
You can also configure `index` and `dist` in `dbt_project.yml`.
#### `models/stage/absence.sql
```
{{
    config(
        index='HEAP',
        dist='ROUND_ROBIN'
        )
}}

select *
from ...
```

is turned into the relative form (minus `__dbt`'s `_backup` and `_tmp` tables)

```SQL
  CREATE TABLE ajs_stg.absence_hours
    WITH(
      DISTRIBUTION = ROUND_ROBIN,
      HEAP
      )
    AS (SELECT * FROM ajs_stg.absence_hours__dbt_tmp_temp_view)
```
#### Indices
- `CLUSTERED COLUMNSTORE INDEX` (default)
- `HEAP`
- `CLUSTERED INDEX ({COLUMN})`
  
#### Distributions
- `ROUND_ROBIN` (default)
- `HASH({COLUMN})`
- `REPLICATE`


## Changelog

### v0.15.2
Initial release