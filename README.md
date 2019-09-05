# dbt-sqlserver
[dbt](https://www.getdbt.com) adapter for sql server. Based on pymssql. 

Passing all tests in [dbt-integration-tests](https://github.com/fishtown-analytics/dbt-integration-tests/). Only supports dbt 0.14 and newer!

Easiest install is to use pip:

    pip install dbt-sqlserver

Since version 0.14.0, pyodbc is used for connecting to SQL Server.
 
## Configure your profile
Configure your dbt profile for using SQL Server authentication or Integrated Security:
##### SQL Server authentication 
      type: sqlserver
      driver: 'ODBC Driver 17 for SQL Server' (The ODBC Driver installed on your system)
      server: server-host-name or ip
      port: 1433
      user: username
      password: password
      database: databasename
      schema: schemaname

##### Integrated Security
      type: sqlserver
      driver: 'ODBC Driver 17 for SQL Server'
      server: server-host-name or ip
      port: 1433
      user: username
      schema: schemaname
      windows_login: True

## Supported features

### Materializations
- Table: 
    - Will be materialized as columns store index by default (requires SQL Server 2017 as least). To override:
{{
  config(
    as_columnstore = false,
  )
}}
- View
- Incremental
- Ephemeral

### Seeds

### Hooks

### Custom schemas

### Sources

### Testing & documentation
- Schema test supported
- Data tests supported from dbt 0.14.1
- Docs

### Snapshots
- Timestamp
- Check

But, columns in source table can not have any constraints. If for example any column has a NOT NULL constraint, an error will be thrown.