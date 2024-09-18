{% macro sqlserver__get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) -%}

  -- Create target schema if it does not
  USE [{{ target.database }}];
  IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{{ target.schema }}')
  BEGIN
    EXEC('CREATE SCHEMA [{{ target.schema }}]')
  END

  {% set testview %}
    [{{ target.schema }}.testview_{{ range(1300, 19000) | random }}]
  {% endset %}

  {% set sql = main_sql.replace("'", "''")%}
  EXEC('create view {{testview}} as {{ sql }};')
  select
    {{ "top (" ~ limit ~ ')' if limit != none }}
    {{ fail_calc }} as failures,
    case when {{ fail_calc }} {{ warn_if }}
      then 'true' else 'false' end as should_warn,
    case when {{ fail_calc }} {{ error_if }}
      then 'true' else 'false' end as should_error
  from (
    select * from {{testview}}
  ) dbt_internal_test;

  EXEC('drop view {{testview}};')

{%- endmacro %}
