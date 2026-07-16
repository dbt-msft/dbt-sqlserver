{% macro sqlserver__create_table_as(temporary, relation, sql) -%}
    {%- set query_label = get_query_options(parse_options=True) -%}
    {%- set full_refresh_build = config.get('full_refresh_build', 'heap_then_index') -%}
    {%- if full_refresh_build not in ['heap_then_index', 'prebuilt'] -%}
      {{ exceptions.raise_compiler_error(
        "Invalid full_refresh_build '" ~ full_refresh_build ~ "'. "
        "Valid values are: 'heap_then_index' (default), 'prebuilt'."
      ) }}
    {%- endif -%}
    {%- set tmp_relation = relation.incorporate(path={"identifier": relation.identifier ~ '__dbt_tmp_vw'}, type='view') -%}

    {%- do adapter.drop_relation(tmp_relation) -%}
    USE [{{ relation.database }}];
    {{ get_create_view_as_sql(tmp_relation, sql) }}

    {%- set table_name -%}
        {{ relation }}
    {%- endset -%}


    {%- set contract_config = config.get('contract') -%}
    {%- set query -%}
        {% if contract_config.enforced and (not temporary) %}
            CREATE TABLE {{table_name}}
            {{ get_assert_columns_equivalent(sql)  }}
            {{ build_columns_constraints(relation) }}
            {% set listColumns %}
                {% for column in model['columns'] %}
                    {{ "["~column~"]" }}{{ ", " if not loop.last }}
                {% endfor %}
            {%endset%}
            INSERT INTO {{relation}} WITH (TABLOCK) ({{listColumns}})
            SELECT {{listColumns}} FROM {{tmp_relation}} {{ query_label }}

        {% else %}
            SELECT * INTO {{ table_name }} FROM {{ tmp_relation }} {{ query_label }}
        {% endif %}
    {%- endset -%}

    EXEC('{{- escape_single_quotes(query) -}}')

    {# For some reason drop_relation is not firing. This solves the issue for now. #}
    EXEC('DROP VIEW IF EXISTS {{ tmp_relation.include(database=False) }}')



    {% set as_columnstore = config.get('as_columnstore', default=true) %}
    {% if not temporary and as_columnstore -%}
        {#-
        add columnstore index
        this creates with dbt_temp as its coming from a temporary relation before renaming
        could alter relation to drop the dbt_temp portion if needed
        -#}
        {{ sqlserver__create_clustered_columnstore_index(relation) }}
   {% endif %}

{% endmacro %}


{% macro sqlserver__create_table_as_prebuilt(relation, sql) -%}
    {#-
      In-place build for full_refresh_build=prebuilt: create the table empty
      under its final name with its clustered design (the as_columnstore CCI
      or the clustered entry from the indexes config), then bulk-load it via
      INSERT WITH (TABLOCK). Callers drop any existing target first.
      See the CHANGELOG for the trade-offs of this build mode.
    -#}
    {%- set query_label = get_query_options(parse_options=True) -%}
    {%- set as_columnstore = config.get('as_columnstore', default=true) -%}
    {%- set contract_config = config.get('contract') -%}
    {%- set contract_enforced = contract_config.enforced -%}
    {%- set tmp_relation = relation.incorporate(path={"identifier": relation.identifier ~ '__dbt_tmp_vw'}, type='view') -%}

    {#- find the clustered entry (validate_indexes guarantees at most one);
        without one the table loads in place as a heap -#}
    {%- set prebuilt_ns = namespace(clustered_dict=none) -%}
    {%- if not as_columnstore -%}
        {%- for raw_index in config.get('indexes', default=[]) -%}
            {%- set parsed = adapter.parse_index(raw_index) -%}
            {%- if parsed and parsed.type == 'clustered' and prebuilt_ns.clustered_dict is none -%}
                {%- set prebuilt_ns.clustered_dict = raw_index -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {%- set has_clustered_design = as_columnstore or prebuilt_ns.clustered_dict is not none -%}
    {%- if has_clustered_design -%}
        {{ log("Rebuilding " ~ relation ~ " in place with full_refresh_build=prebuilt", info=true) }}
    {%- else -%}
        {% do log("full_refresh_build=prebuilt on " ~ relation ~ " has no clustered index in the indexes config; loading in place as a heap") %}
    {%- endif -%}

    {%- do adapter.drop_relation(tmp_relation) -%}
    USE [{{ relation.database }}];
    {{ get_create_view_as_sql(tmp_relation, sql) }}

    {% if contract_enforced %}
        {%- set ddl_query -%}
            CREATE TABLE {{ relation }}
            {{ get_assert_columns_equivalent(sql)  }}
            {{ build_columns_constraints(relation) }}
        {%- endset -%}
        EXEC('{{- escape_single_quotes(ddl_query) -}}')
    {% else %}
        EXEC('SELECT TOP 0 * INTO {{ relation }} FROM {{ tmp_relation }}')
    {% endif %}

    {# mark the rebuild in progress; removed atomically with the load below #}
    EXEC sp_addextendedproperty @name = N'dbt_full_refresh_incomplete', @value = '1',
        @level0type = N'SCHEMA', @level0name = N'{{ relation.schema }}',
        @level1type = N'TABLE', @level1name = N'{{ relation.identifier }}'

    {% if as_columnstore %}
        {{ sqlserver__create_clustered_columnstore_index(relation) }}
    {% elif prebuilt_ns.clustered_dict is not none %}
        {{ sqlserver__get_create_index_sql(relation, prebuilt_ns.clustered_dict) }}
    {% endif %}

    {%- if contract_enforced -%}
        {%- set listColumns -%}
            {%- for column in model['columns'] -%}
                {{ "["~column~"]" }}{{ ", " if not loop.last }}
            {%- endfor -%}
        {%- endset -%}
        {%- set insert_statement -%}
            INSERT INTO {{ relation }} WITH (TABLOCK) ({{ listColumns }})
            SELECT {{ listColumns }} FROM {{ tmp_relation }} {{ query_label }}
        {%- endset -%}
    {%- else -%}
        {%- set insert_statement -%}
            INSERT INTO {{ relation }} WITH (TABLOCK)
            SELECT * FROM {{ tmp_relation }} {{ query_label }}
        {%- endset -%}
    {%- endif %}
    {#- load and unmark atomically: any failure rolls back and keeps the marker -#}
    {%- set insert_query -%}
        SET XACT_ABORT ON;
        BEGIN TRANSACTION;
        {{ insert_statement }};
        EXEC sp_dropextendedproperty @name = N'dbt_full_refresh_incomplete',
            @level0type = N'SCHEMA', @level0name = N'{{ relation.schema }}',
            @level1type = N'TABLE', @level1name = N'{{ relation.identifier }}';
        COMMIT TRANSACTION;
    {%- endset %}
    EXEC('{{- escape_single_quotes(insert_query) -}}')

    EXEC('DROP VIEW IF EXISTS {{ tmp_relation.include(database=False) }}')

{% endmacro %}


{% macro sqlserver__mark_full_refresh_incomplete(relation) -%}
    {#- mark a table as having a full refresh in flight; the marker rides the
        object, so a successful swap (old table dropped) or in-place rebuild
        (table dropped) clears it naturally -#}
    {% do run_query(
        "if not exists (select 1 from sys.extended_properties where major_id = OBJECT_ID('"
        ~ relation.schema ~ "." ~ relation.identifier
        ~ "') and name = 'dbt_full_refresh_incomplete')"
        ~ " EXEC sp_addextendedproperty @name = N'dbt_full_refresh_incomplete', @value = '1',"
        ~ " @level0type = N'SCHEMA', @level0name = N'" ~ relation.schema ~ "',"
        ~ " @level1type = N'TABLE', @level1name = N'" ~ relation.identifier ~ "'"
    ) %}
{%- endmacro %}


{% macro sqlserver__assert_no_incomplete_full_refresh(relation) -%}
    {%- set marker = run_query(
        "select count(*) as marker_count from sys.extended_properties where major_id = OBJECT_ID('"
        ~ relation.schema ~ "." ~ relation.identifier
        ~ "') and name = 'dbt_full_refresh_incomplete'"
    ) -%}
    {%- if marker.rows[0][0] > 0 -%}
        {{ exceptions.raise_compiler_error(
            "A previous full refresh of " ~ relation
            ~ " did not complete; the table may be stale, empty or partially "
            "loaded. Rerun with --full-refresh."
        ) }}
    {%- endif -%}
{%- endmacro %}


{% macro sqlserver__assert_no_unguarded_self_reference(relation, compiled_sql) -%}
    {#- a self-reference that survives full-refresh compilation (where
        is_incremental() is false) would run against a table prebuilt has
        already dropped: fail fast while the data is intact -#}
    {%- if relation.render() | lower in compiled_sql | lower -%}
        {{ exceptions.raise_compiler_error(
            "Model " ~ relation ~ " contains an unguarded self-reference "
            "({{ this }}) and full_refresh_build=prebuilt drops the table "
            "before rebuilding. Guard the reference with is_incremental(), "
            "or keep the default heap_then_index."
        ) }}
    {%- endif -%}
{%- endmacro %}
