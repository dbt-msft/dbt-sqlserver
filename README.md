# dbt-sqlserver
[dbt](https://www.getdbt.com) adapter for sql server.

Passing all tests in [dbt-integration-tests](https://github.com/fishtown-analytics/dbt-integration-tests/). 

Only supports dbt 0.14 and newer!
- For dbt 0.14.x use dbt-sqlserver 0.14.x
- For dbt 0.15.x use dbt-sqlserver 0.15.x

Easiest install is to use pip:

    pip install dbt-sqlserver

On Ubuntu make sure you have the ODBC header files before installing
    
    sudo apt install unixodbc-dev
 
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

### Indexes
There is now possible to define a regular sql server index on a table. 
This is best used when the default clustered columnstore index materialisation is not suitable. 
One reason would be that you need a large table that usually is queried one row at a time.

Clusterad and non-clustered index are supported:
- create_clustered_index(columns, unique=False)
- create_nonclustered_index(columns, includes=False)
- drop_all_indexes_on_table(): Drops current indexex on a table. Only meaningfull if model is incremental.


Example of applying Unique clustered index on two columns, Ordinary index on one column, Ordinary index on one column with another column included

    {{
        config({
            "as_columnstore": false, 
            "materialized": 'table',
            "post-hook": [
                "{{ create_clustered_index(columns = ['row_id', 'row_id_complement'], unique=True) }}",
                "{{ create_nonclustered_index(columns = ['modified_date']) }}",
                "{{ create_nonclustered_index(columns = ['row_id'], includes = ['modified_date']) }}",
            ]
        })
    }}


## Changelog

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
