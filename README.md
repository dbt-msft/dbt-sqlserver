# dbt-sqlserver
adapter for dbt sql server 



- Tables will be materialized as columns store index by default. To override specify:
{{
  config(
    as_columnstore = false,
  )
}}

