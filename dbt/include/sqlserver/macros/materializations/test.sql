{% macro sqlserver__get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) -%}
    select
      {{ "top (" ~ limit ~ ')' if limit != none }}
      {{ fail_calc }} as failures,
      case when {{ fail_calc }} {{ warn_if }}
        then 'true' else 'false' end as should_warn,
      case when {{ fail_calc }} {{ error_if }}
        then 'true' else 'false' end as should_error
    from (
      {{ main_sql }}
    ) dbt_internal_test
{%- endmacro %}

{%- materialization test, adapter='sqlserver' -%}

  {% set relations = [] %}

  {% if should_store_failures() %}

    {% set identifier = model['alias'] %}
    {% set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) %}
    {% set target_relation = api.Relation.create(
        identifier=identifier, schema=schema, database=database, type='table') -%} %}
    
    {% if old_relation %}
        {% do adapter.drop_relation(old_relation) %}
    {% endif %}
    
    {% call statement(auto_begin=True) %}
        {{ create_table_as(False, target_relation, sql) }}
    {% endcall %}
    
    {% do relations.append(target_relation) %}
  
    {% set main_sql %}
        select *
        from {{ target_relation }}
    {% endset %}
    
    {{ adapter.commit() }}
  
  {% else %}

      {% set main_sql = sql %}
  
  {% endif %}

  {% set limit = config.get('limit') %}
  {% set fail_calc = config.get('fail_calc') %}
  {% set warn_if = config.get('warn_if') %}
  {% set error_if = config.get('error_if') %}

  {% call statement('main', fetch_result=True) -%}

    {{ get_test_sql(main_sql, fail_calc, warn_if, error_if, limit)}}

  {%- endcall %}
  
  {{ return({'relations': relations}) }}

{%- endmaterialization -%}