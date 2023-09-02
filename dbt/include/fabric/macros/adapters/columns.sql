{% macro fabric__get_empty_subquery_sql(select_sql, select_sql_header=none) %}
    {% if sql.strip().lower().startswith('with') %}
        {{ select_sql }}
    {% else -%}
        select * from (
        {{ select_sql }}
    ) dbt_sbq_tmp
    where 1 = 0
    {%- endif -%}

{% endmacro %}

{% macro fabric__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}

    with mapping as (
        select
            row_number() over (partition by object_name(c.object_id) order by c.column_id) as ordinal_position,
            c.name collate database_default as column_name,
            t.name as data_type,
            c.max_length as character_maximum_length,
            c.precision as numeric_precision,
            c.scale as numeric_scale
        from [{{ 'tempdb' if '#' in relation.identifier else relation.database }}].sys.columns c {{ information_schema_hints() }}
        inner join sys.types t {{ information_schema_hints() }}
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

{% macro fabric__get_columns_in_query(select_sql) %}
    {% call statement('get_columns_in_query', fetch_result=True, auto_begin=False) -%}
        select TOP 0 * from (
            {{ select_sql }}
        ) as __dbt_sbq
        where 0 = 1
    {% endcall %}

    {{ return(load_result('get_columns_in_query').table.columns | map(attribute='name') | list) }}
{% endmacro %}

{% macro fabric__alter_column_type(relation, column_name, new_column_type) %}

    {%- set table_name= tmp_relation.include(database=False).include(schema=False)-%}
    {%- set schema_name = tmp_relation.include(database=False).include(identifier=False) -%}

    {% set generate_tmp_relation_script %}
        SELECT TRIM(REPLACE(STRING_AGG(ColumnName + ' ', ',-'), '-', CHAR(10)))  AS ColumnDef
        FROM
        (
            SELECT
            CAST(c.COLUMN_NAME AS VARCHAR) AS ColumnName
            FROM INFORMATION_SCHEMA.TABLES t
            JOIN INFORMATION_SCHEMA.COLUMNS c
                ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
                AND t.TABLE_NAME = c.TABLE_NAME
                WHERE t.TABLE_NAME = REPLACE('{{table_name}}','"','')
                AND t.TABLE_SCHEMA = REPLACE('{{schema_name}}','"','')
                AND c.COLUMN_NAME <> REPLACE('{{column_name}}','"','')
        ) T
    {% endset %}

    {%- set query_result = run_query(generate_tmp_relation_script) -%}
    {%- set query_result_text = query_result.rows[0][0] -%}

    {% set tempTableName %}
        {{ relation.schema }}.{{ relation.identifier }}_{{ range(1300, 19000) | random }}
    {% endset %}

    {% set tempTable %}
        CREATE TABLE {{tempTableName}}
        AS SELECT {{query_result_text}}, CAST({{ column_name }} AS {{new_column_type}}) AS {{column_name}} FROM {{ relation.schema }}.{{ relation.identifier }}
    {% endset %}

    {% call statement('create_temp_table') -%}
        {{ tempTable }}
    {%- endcall %}

    {% set dropTable %}
        DROP TABLE {{ relation.schema }}.{{ relation.identifier }}
    {% endset %}

    {% call statement('drop_table') -%}
        {{ dropTable }}
    {%- endcall %}

    {% set createTable %}
        CREATE TABLE {{ relation.schema }}.{{ relation.identifier }}
        AS SELECT * FROM {{tempTableName}}
    {% endset %}

    {% call statement('create_Table') -%}
        {{ createTable }}
    {%- endcall %}

    {% set dropTempTable %}
        DROP TABLE {{tempTableName}}
    {% endset %}

    {% call statement('drop_temp_table') -%}
        {{ dropTempTable }}
    {%- endcall %}
{% endmacro %}

--TODO
{% macro fabric__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}
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
