{% macro sqlserver__get_unit_test_sql(main_sql, expected_fixture_sql, expected_column_names) -%}

USE [{{ target.database }}];
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{{ target.schema }}')
BEGIN
EXEC('CREATE SCHEMA [{{ target.schema }}]')
END

{% set test_view %}
    [{{ target.schema }}].[testview_{{ local_md5(main_sql) }}_{{ range(1300, 19000) | random }}]
{% endset %}
{% set test_sql = main_sql.replace("'", "''")%}
EXEC('create view {{test_view}} as {{ test_sql }};')

{% set expected_view %}
    [{{ target.schema }}].[expectedview_{{ local_md5(expected_fixture_sql) }}_{{ range(1300, 19000) | random }}]
{% endset %}
{% set expected_sql = expected_fixture_sql.replace("'", "''")%}
EXEC('create view {{expected_view}} as {{ expected_sql }};')

-- Build actual result given inputs
{% set unittest_sql %}
with dbt_internal_unit_test_actual as (
  select
    {% for expected_column_name in expected_column_names %}{{expected_column_name}}{% if not loop.last -%},{% endif %}{%- endfor -%}, {{ dbt.string_literal("actual") }} as {{ adapter.quote("actual_or_expected") }}
  from
    {{ test_view }}
),
-- Build expected result
dbt_internal_unit_test_expected as (
  select
    {% for expected_column_name in expected_column_names %}{{expected_column_name}}{% if not loop.last -%}, {% endif %}{%- endfor -%}, {{ dbt.string_literal("expected") }} as {{ adapter.quote("actual_or_expected") }}
  from
    {{ expected_view }}
)
-- Union actual and expected results
select * from dbt_internal_unit_test_actual
union all
select * from dbt_internal_unit_test_expected
{% endset %}

EXEC('{{- escape_single_quotes(unittest_sql) -}}')

EXEC('drop view {{test_view}};')
EXEC('drop view {{expected_view}};')

{%- endmacro %}
