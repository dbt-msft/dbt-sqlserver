# dbt-sqlserver
https://www.getdbt.com/ adapter for sql server. Based on pymssql. 

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

- Tables will be materialized as columns store index by default. To override specify:
{{
  config(
    as_columnstore = false,
  )
}}

