# dbt-sqlserver
https://www.getdbt.com/ adapter for sql server. Based on pymssql. 

Only supports dbt 0.14!

Easiest install is to use pip:

    pip install git+https://github.com/mikaelene/dbt-sqlserver#egg=dbt_sqlserver


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
    - Will be materialized as columns store index by default (requires SQL Server 2017 as least). To override specify:
{{
  config(
    as_columnstore = false,
  )
}}
- View
- Incremental


- Ephemeral: NOT SUPPORTED!

### Seeds

### Hooks

### Custom schemas

### Sources

### Testing & documentation
- Unique: Is the only test tested so far. The rest will be tested and added if possible soon.

- Docs

### Snapshots
- Timestamp
- Check
