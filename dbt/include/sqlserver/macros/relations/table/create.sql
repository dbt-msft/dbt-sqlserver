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
      INSERT WITH (TABLOCK). The create is idempotent: it drops any physical
      target first (see the OBJECT_ID guard below), so it is safe even when
      dbt's relation cache is stale.
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

    {#- Setup runs as its own statement (named 'main', so dbt's compiled-SQL
        artifacts and adapter_response still resolve normally), separate from
        the load below: the extended-property marker set here exists to
        survive a failed load, so it must not share a transaction with it.
        With dbt_sqlserver_use_dbt_transactions on (default), the
        materialization's whole build - including this statement - runs
        inside one continuous ambient transaction; without the split below, a
        later failure during the load would roll the marker back right along
        with it, defeating the point. -#}
    {%- set setup_sql -%}
        USE [{{ relation.database }}];
        {{ get_create_view_as_sql(tmp_relation, sql) }}

        {#- drop any physical target before the bare CREATE / SELECT ... INTO
            below. OBJECT_ID reads the database directly, so this is robust to
            dbt's relation cache being stale (reporting none while the table
            exists) - which would otherwise collide with Msg 2714 (object
            already exists) -#}
        IF OBJECT_ID('{{ escape_single_quotes(relation.schema) }}.{{ escape_single_quotes(relation.identifier) }}', 'U') IS NOT NULL
            EXEC('DROP TABLE {{ relation }}');

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
            @level0type = N'SCHEMA', @level0name = N'{{ escape_single_quotes(relation.schema) }}',
            @level1type = N'TABLE', @level1name = N'{{ escape_single_quotes(relation.identifier) }}'
    {%- endset %}
    {% call statement('main') -%}
        {{ setup_sql }}
    {%- endcall %}

    {#- Commit the marker onto its own, then reopen so the load below (and
        the rest of the materialization: grants, persist_docs, its own
        trailing adapter.commit()) is unaffected. See adapter.commit_if_open /
        begin_if_closed; 'main' above always opens a transaction (default
        auto_begin), so this always actually commits here. -#}
    {{ adapter.commit_if_open() }}
    {{ adapter.begin_if_closed() }}

    {%- set load_sql -%}
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
    {#- load and unmark atomically: any failure rolls back and keeps the marker.
        Relies on the session-level SET XACT_ABORT ON applied at connection
        open (see #718) so a run-time error here rolls back instead of
        falling through to COMMIT; EXEC('...') runs in the same session, so
        that setting still applies inside this dynamic SQL sub-batch. -#}
    {%- set insert_query -%}
        BEGIN TRANSACTION;
        {{ insert_statement }};
        EXEC sp_dropextendedproperty @name = N'dbt_full_refresh_incomplete',
            @level0type = N'SCHEMA', @level0name = N'{{ escape_single_quotes(relation.schema) }}',
            @level1type = N'TABLE', @level1name = N'{{ escape_single_quotes(relation.identifier) }}';
        COMMIT TRANSACTION;
    {%- endset %}
    EXEC('{{- escape_single_quotes(insert_query) -}}')

    EXEC('DROP VIEW IF EXISTS {{ tmp_relation.include(database=False) }}')
    {%- endset %}
    {% call statement('create_table_as_prebuilt_load') -%}
        {{ load_sql }}
    {%- endcall %}
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
    {#- This marker exists to survive a failed rebuild, so it must not ride on
        the same transaction as the rebuild it's guarding: if a prior
        statement this run (e.g. a pre-hook) already opened the ambient
        dbt-managed transaction, a later failure would roll the marker back
        right along with it, defeating the point. Commit it, then reopen so
        later code sees a transaction open exactly as it would without this
        pair (begin_if_closed always leaves one open, whether or not
        commit_if_open just found one to close) - commit_if_open alone is a
        no-op when run_query above ran standalone, i.e. the common case of
        this being the first statement of the run. See adapter.commit_if_open
        / begin_if_closed. -#}
    {{ adapter.commit_if_open() }}
    {{ adapter.begin_if_closed() }}
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
