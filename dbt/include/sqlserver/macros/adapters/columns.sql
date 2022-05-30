{% macro sqlserver__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      SELECT
          column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale
      FROM
          (select
              ordinal_position,
              column_name,
              data_type,
              character_maximum_length,
              numeric_precision,
              numeric_scale
          from [{{ relation.database }}].INFORMATION_SCHEMA.COLUMNS
          where table_name = '{{ relation.identifier }}'
            and table_schema = '{{ relation.schema }}'
          UNION
          select
              ordinal_position,
              column_name collate database_default,
              data_type collate database_default,
              character_maximum_length,
              numeric_precision,
              numeric_scale
          from tempdb.INFORMATION_SCHEMA.COLUMNS
          where table_name like '{{ relation.identifier }}%') cols
      order by ordinal_position


  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}


{% macro sqlserver__get_columns_in_query(select_sql) %}
    {% call statement('get_columns_in_query', fetch_result=True, auto_begin=False) -%}
        select TOP 0 * from (
            {{ select_sql }}
        ) as __dbt_sbq
        where 0 = 1
    {% endcall %}

    {{ return(load_result('get_columns_in_query').table.columns | map(attribute='name') | list) }}
{% endmacro %}

{% macro sqlserver__alter_column_type(relation, column_name, new_column_type) %}

  {%- set tmp_column = column_name + "__dbt_alter" -%}

  {% call statement('alter_column_type') -%}

    alter {{ relation.type }} {{ relation }} add {{ tmp_column }} {{ new_column_type }};
    update {{ relation }} set {{ tmp_column }} = {{ column_name }};
    alter {{ relation.type }} {{ relation }} drop column {{ column_name }};
    exec sp_rename '{{ relation | replace('"', '') }}.{{ tmp_column }}', '{{ column_name }}', 'column'

  {%- endcall -%}

{% endmacro %}
