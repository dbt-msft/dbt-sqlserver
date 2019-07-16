# dbt-sqlserver
https://www.getdbt.com/ adapter for sql server. Based on pymssql. 

Only supports dbt 0.14!

Easiest install is to use pip:

    pip install dbt_sqlserver


## Configure your profile

      type: sqlserver
      threads: 1
      server: server-host-name or ip
      port: 1433
      user: username
      password: password
      database: databasename
      schema: schemaname

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
- All tests supported 
- Docs

### Snapshots
- Timestamp
- Check

But, columns in source table can not have any constraints. If for example any column has a NOT NULL constraint, an error will be thrown.