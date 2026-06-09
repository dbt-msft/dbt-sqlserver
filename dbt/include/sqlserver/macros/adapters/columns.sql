{% macro sqlserver__get_empty_subquery_sql(select_sql, select_sql_header=none) %}
    {%- set select_sql_stripped = modules.re.sub('(?s)/\\*.*?\\*/|--[^\n]*\n', '', select_sql) -%}
    {% if select_sql_stripped.strip().lower().startswith('with') %}
        {{ select_sql }}
    {% else -%}
        select * from (
        {{ select_sql }}
    ) dbt_sbq_tmp
    where 1 = 0
    {%- endif -%}

{% endmacro %}

{% macro sqlserver__get_columns_in_query(select_sql) %}
    {% set query_label = get_query_options() %}
    {% call statement('get_columns_in_query', fetch_result=True, auto_begin=False) -%}
        select TOP 0 * from (
            {{ select_sql }}
        ) as __dbt_sbq
        where 0 = 1
        {{ query_label }}
    {% endcall %}

    {{ return(load_result('get_columns_in_query').table.columns | map(attribute='name') | list) }}
{% endmacro %}

{% macro sqlserver__alter_column_type(relation, column_name, new_column_type) %}

    {% set prefer_single = config.get('prefer_single_alter_column', false) %}

    {% if prefer_single and relation.type == 'table' %}
        {% set alter_sql %}
            alter {{ relation.type }} {{ relation }}
            alter column "{{ column_name }}" {{ new_column_type }};
        {%- endset %}
        {% do run_query(alter_sql) %}

    {% else %}
        {%- set tmp_column = column_name + "__dbt_alter" -%}

        {% set add_column %}
            alter {{ relation.type }} {{ relation }}
            add "{{ tmp_column }}" {{ new_column_type }};
        {%- endset %}
        {% set update_column %}
            update {{ relation }} set "{{ tmp_column }}" = "{{ column_name }}";
        {%- endset %}
        {% set drop_column %}
            alter {{ relation.type }} {{ relation }}
            drop column "{{ column_name }}";
        {%- endset %}
        {% set rename_column %}
            exec sp_rename '{{ relation | replace('"', '') }}.{{ tmp_column }}', '{{ column_name }}', 'column'
        {%- endset %}

        {% do run_query(add_column) %}
        {% do run_query(update_column) %}
        {% do run_query(drop_column) %}
        {% do run_query(rename_column) %}
    {% endif %}

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

{% macro sqlserver__get_columns_in_relation(relation) -%}
    {% set query_label = get_query_options() %}
    {% call statement('get_columns_in_relation', fetch_result=True) %}
        {{ get_use_database_sql(relation.database) }}
        select
            c.name collate database_default as column_name,
            t.name as data_type,
            case
                when (t.name in ('nchar', 'nvarchar', 'sysname') and c.max_length <> -1) then c.max_length / 2
                else c.max_length
            end as character_maximum_length,
            c.precision as numeric_precision,
            c.scale as numeric_scale
        from sys.columns c {{ information_schema_hints() }}
        inner join sys.types t {{ information_schema_hints() }}
        on c.user_type_id = t.user_type_id
        where c.object_id = object_id('{{ 'tempdb..' ~ relation.include(database=false, schema=false) if '#' in relation.identifier else relation }}')
        order by c.column_id
        {{ query_label }}

    {% endcall %}
    {% set table = load_result('get_columns_in_relation').table %}
    {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}
