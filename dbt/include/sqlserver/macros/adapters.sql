{% macro sqlserver__information_schema_name(database) -%}
    information_schema
{%- endmacro %}


{% macro sqlserver__get_columns_in_query(select_sql) %}
    {% call statement('get_columns_in_query', fetch_result=True, auto_begin=False) -%}
        SELECT TOP(0)
            *
        FROM
            (
                {{ select_sql }}
            ) as __dbt_sbq
        WHERE
            0 = 1;
    {% endcall %}
    {{ return(load_result('get_columns_in_query').table.columns | map(attribute='name') | list) }}
{% endmacro %}


{% macro sqlserver__list_relations_without_caching(schema_relation) %}
    {% call statement('list_relations_without_caching', fetch_result=True) -%}
        SELECT
            table_catalog AS [database],
            table_name AS [name],
            table_schema AS [schema],
            CASE
                WHEN table_type = 'BASE TABLE' THEN 'table'
                WHEN table_type = 'VIEW' THEN 'view'
                ELSE table_type
            END AS table_type
        from
            [{{ schema_relation.database }}].information_schema.tables
        WHERE
            table_schema LIKE '{{ schema_relation.schema }}';
    {% endcall %}
    {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}


{% macro sqlserver__list_schemas(database) %}
    {% call statement('list_schemas', fetch_result=True, auto_begin=False) -%}
        USE {{ database }};
        SELECT
            name AS [schema]
        FROM
            sys.schemas;
    {% endcall %}
    {{ return(load_result('list_schemas').table) }}
{% endmacro %}


{% macro sqlserver__create_schema(relation) -%}
    {% call statement('create_schema') -%}
        USE [{{ relation.database }}];
        IF NOT EXISTS (
            SELECT
                1
            FROM
                sys.schemas
            WHERE
                name = '{{ relation.without_identifier().schema }}'
        )
        BEGIN
            EXECUTE('CREATE SCHEMA {{ relation.without_identifier().schema }};');
        END
    {% endcall %}
{% endmacro %}

{% macro sqlserver__drop_schema(relation) -%}
    {%- set tables_in_schema_query %}
        SELECT
            TABLE_NAME
        FROM
            INFORMATION_SCHEMA.TABLES
        WHERE
            TABLE_SCHEMA = '{{ relation.schema }}';
    {% endset %}
    {% set tables_to_drop = run_query(tables_in_schema_query).columns[0].values() %}
    {% for table in tables_to_drop %}
        {%- set schema_relation = adapter.get_relation(database=relation.database,
                                                   schema=relation.schema,
                                                   identifier=table) -%}
        {% do drop_relation(schema_relation) %}
    {%- endfor %}
    {% call statement('drop_schema') -%}
        IF EXISTS (
            SELECT
                1
            FROM
                sys.schemas
            WHERE
                name = '{{ relation.schema }}'
        )
        BEGIN
            EXECUTE('DROP SCHEMA {{ relation.schema }};');
        END
    {% endcall %}
{% endmacro %}


{% macro sqlserver__drop_relation(relation) -%}
    {% call statement('drop_relation', auto_begin=False) -%}
        {{ sqlserver__drop_relation_script(relation) }}
    {%- endcall %}
{% endmacro %}


{% macro sqlserver__drop_relation_script(relation) -%}
    {% if relation.type == 'view' -%}
        {% set object_id_type = 'V' %}
    {% elif relation.type == 'table'%}
        {% set object_id_type = 'U' %}
    {%- else -%} invalid target name
    {% endif %}
    USE [{{ relation.database }}];
    IF object_id('{{ relation.include(database=False) }}','{{ object_id_type }}') IS NOT NULL
    BEGIN
        DROP {{ relation.type }} {{ relation.include(database=False) }};
    END
{% endmacro %}

{% macro sqlserver__check_schema_exists(information_schema, schema) -%}
    {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) -%}
        SELECT
            count(*) as schema_exist
        FROM
            sys.schemas
        WHERE
            name = '{{ schema }}';
    {%- endcall %}
    {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}


{% macro sqlserver__create_view_exec(relation, sql) -%}
    {%- set temp_view_sql = sql.replace("'", "''") -%}
    EXECUTE('create view {{ relation.include(database=False) }} as
    {{ temp_view_sql }}
    ');
{% endmacro %}


{% macro sqlserver__create_view_as(relation, sql) -%}
    USE [{{ relation.database }}];
    {{ sqlserver__create_view_exec(relation, sql) }}
{% endmacro %}


{% macro sqlserver__rename_relation(from_relation, to_relation) -%}
    {% call statement('rename_relation') -%}
        USE [{{ to_relation.database }}];
        EXECUTE sp_rename '{{ from_relation.schema }}.{{ from_relation.identifier }}', '{{ to_relation.identifier }}';
        IF EXISTS(
            SELECT
                *
            FROM
                sys.indexes
            WHERE
                name = '{{ from_relation.schema }}_{{ from_relation.identifier }}_cci'
                AND object_id = object_id('{{ from_relation.schema }}.{{ to_relation.identifier }}')
        )
        BEGIN
            EXECUTE sp_rename N'{{ from_relation.schema }}.{{ to_relation.identifier }}.{{ from_relation.schema }}_{{ from_relation.identifier }}_cci', N'{{ from_relation.schema }}_{{ to_relation.identifier }}_cci', N'INDEX';
        END
    {%- endcall %}
{% endmacro %}


{% macro sqlserver__create_clustered_columnstore_index(relation) -%}
    {%- set cci_name = relation.schema ~ '_' ~ relation.identifier ~ '_cci' -%}
    {%- set relation_name = relation.schema ~ '_' ~ relation.identifier -%}
    {%- set full_relation = relation.schema ~ '.' ~ relation.identifier -%}
    USE [{{ relation.database }}];
    IF EXISTS (
        SELECT
            1
        FROM
            sys.indexes
        WHERE
            name = '{{cci_name}}'
            AND object_id = object_id('{{relation_name}}')
    )
    BEGIN
        DROP INDEX {{full_relation}}.{{cci_name}};
    END
    CREATE CLUSTERED COLUMNSTORE INDEX {{cci_name}}
        ON {{full_relation}};
{% endmacro %}


{% macro sqlserver__create_table_as(temporary, relation, sql) -%}
    {%- set as_columnstore = config.get('as_columnstore', default=true) -%}
    {% set tmp_relation = relation.incorporate(
    path={"identifier": relation.identifier.replace("#", "") ~ '_temp_view'},
    type='view')-%}
    {%- set temp_view_sql = sql.replace("'", "''") -%}

    {{ sqlserver__drop_relation_script(tmp_relation) }}
    {{ sqlserver__drop_relation_script(relation) }}
    USE [{{ relation.database }}];
    EXECUTE('create view {{ tmp_relation.include(database=False) }} as
    {{ temp_view_sql }}
    ');

    SELECT
        *
    INTO
        {{ relation }}
    FROM
        {{ tmp_relation }};
    {{ sqlserver__drop_relation_script(tmp_relation) }}

    {% if not temporary and as_columnstore -%}
        {{ sqlserver__create_clustered_columnstore_index(relation) }}
    {% endif %}
{% endmacro %}


{% macro sqlserver__insert_into_from(to_relation, from_relation) -%}
    SELECT
        *
    INTO
        {{ to_relation }}
    FROM
        {{ from_relation }};
{% endmacro %}


{% macro sqlserver__current_timestamp() -%}
    SYSDATETIME()
{%- endmacro %}


{% macro sqlserver__get_columns_in_relation(relation) -%}
    {% call statement('get_columns_in_relation', fetch_result=True) %}
        SELECT
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM
            (
                SELECT
                    ordinal_position,
                    column_name,
                    data_type,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale
                FROM
                    [{{ relation.database }}].INFORMATION_SCHEMA.COLUMNS
                WHERE
                    table_name = '{{ relation.identifier }}'
                    AND table_schema = '{{ relation.schema }}'
                UNION ALL
                SELECT
                    ordinal_position,
                    column_name COLLATE database_default,
                    data_type COLLATE database_default,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale
                FROM
                    tempdb.INFORMATION_SCHEMA.COLUMNS
                WHERE
                    table_name LIKE '{{ relation.identifier }}%'
            ) AS cols
        ORDER BY ordinal_position;
    {% endcall %}
    {% set table = load_result('get_columns_in_relation').table %}
    {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}


{% macro sqlserver__make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = '#' ~  base_relation.identifier ~ suffix %}
    {% set tmp_relation = base_relation.incorporate(
                                path={"identifier": tmp_identifier}) -%}
    {% do return(tmp_relation) %}
{% endmacro %}


{% macro sqlserver__snapshot_string_as_time(timestamp) -%}
    {%- set result = "convert(datetime2, '" ~ timestamp ~ "')" -%}
    {{ return(result) }}
{%- endmacro %}
