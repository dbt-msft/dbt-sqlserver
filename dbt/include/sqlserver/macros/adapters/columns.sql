{% macro sqlserver__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}

    with mapping as (
        select
            row_number() over (partition by object_name(c.object_id) order by c.column_id) as ordinal_position,
            c.name collate database_default as column_name,
            t.name as data_type,
            c.max_length as character_maximum_length,
            c.precision as numeric_precision,
            c.scale as numeric_scale
        from [{{ 'tempdb' if '#' in relation.identifier else relation.database }}].sys.columns c with (nolock)
        inner join sys.types t with (nolock)
        on c.user_type_id = t.user_type_id
        where c.object_id = object_id('{{ 'tempdb..' ~ relation.include(database=false, schema=false) if '#' in relation.identifier else relation }}')
    )

    select
        column_name,
        data_type,
        character_maximum_length,
        numeric_precision,
        numeric_scale
    from mapping
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
    alter {{ relation.type }} {{ relation }} add "{{ tmp_column }}" {{ new_column_type }};
  {%- endcall -%}
  {% call statement('alter_column_type') -%}
    update {{ relation }} set "{{ tmp_column }}" = "{{ column_name }}";
  {%- endcall -%}
  {% call statement('alter_column_type') -%}
    alter {{ relation.type }} {{ relation }} drop column "{{ column_name }}";
  {%- endcall -%}
  {% call statement('alter_column_type') -%}
    exec sp_rename '{{ relation | replace('"', '') }}.{{ tmp_column }}', '{{ column_name }}', 'column'
  {%- endcall -%}

{% endmacro %}


{% macro sqlserver__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}
  {% call statement('add_drop_columns') -%}
    {% if add_columns %}
        alter {{ relation.type }} {{ relation }}
        add {% for column in add_columns %}"{{ column.name }}" {{ column.data_type }}{{ ', ' if not loop.last }}{% endfor %};
    {% endif %}

    {% if remove_columns %}
        alter {{ relation.type }} {{ relation }}
        drop column {% for column in remove_columns %}"{{ column.name }}"{{ ',' if not loop.last }}{% endfor %};
    {% endif %}
  {%- endcall -%}
{% endmacro %}
