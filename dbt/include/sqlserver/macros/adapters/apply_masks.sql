{#-
    Column-level Dynamic Data Masking (DDM), modelled on apply_grants.

    Post-materialization step: (re)apply the masks a model declares so they
    survive dbt's drop-and-recreate on every full refresh. Reads the current
    state from sys.masked_columns, diffs against the resolved desired map, and
    emits only the ALTERs that changed. A no-op when nothing is configured, on
    non-table relations, and on adapters other than SQL Server.

    Config surfaces (merged + precedence-resolved in adapter.resolve_masks):
      * column-level  `masked_with:` property in schema YAML, and
      * model-level   `masks` dict config.
-#}

{% macro apply_masks(relation, mask_config) %}
    {{ return(adapter.dispatch('apply_masks', 'dbt')(relation, mask_config)) }}
{% endmacro %}

{#- Non-SQL-Server adapters (and SQL Server < 2016) are unaffected. -#}
{% macro default__apply_masks(relation, mask_config) %}{% endmacro %}


{% macro get_show_mask_sql(relation) %}
    {{ return(adapter.dispatch('get_show_mask_sql', 'dbt')(relation)) }}
{% endmacro %}

{% macro default__get_show_mask_sql(relation) %}
    {{ return('') }}
{% endmacro %}

{% macro sqlserver__get_show_mask_sql(relation) %}
    select
        c.name as name,
        c.masking_function as masking_function
    from sys.masked_columns c {{ information_schema_hints() }}
    where c.object_id = OBJECT_ID('{{ relation.schema }}.{{ relation.identifier }}')
{% endmacro %}


{#- Key columns of any index on the relation. Included columns (is_included
    _column = 1) are excluded, which also excludes the default clustered
    columnstore index (it reports every column as included, never as a key), so
    a normal columnstore table has no index-key columns and masks apply freely. -#}
{% macro sqlserver__get_mask_index_key_columns(relation) %}
    {% call statement('get_mask_index_key_columns', fetch_result=True) %}
        select distinct col.name as name
        from sys.index_columns ic {{ information_schema_hints() }}
        inner join sys.columns col {{ information_schema_hints() }}
            on col.object_id = ic.object_id and col.column_id = ic.column_id
        where ic.object_id = OBJECT_ID('{{ relation.schema }}.{{ relation.identifier }}')
          and ic.is_included_column = 0
    {% endcall %}
    {% set result = [] %}
    {% for row in load_result('get_mask_index_key_columns').table.rows %}
        {% do result.append(row[0]) %}
    {% endfor %}
    {{ return(result) }}
{% endmacro %}


{#- Columns that DDM cannot mask at all (a mask ALTER would fail): computed,
    FILESTREAM, sparse COLUMN_SET, and Always Encrypted columns. -#}
{% macro sqlserver__get_unmaskable_columns(relation) %}
    {% call statement('get_unmaskable_columns', fetch_result=True) %}
        select col.name as name
        from sys.columns col {{ information_schema_hints() }}
        where col.object_id = OBJECT_ID('{{ relation.schema }}.{{ relation.identifier }}')
          and (
                col.is_computed = 1
             or col.is_filestream = 1
             or col.is_column_set = 1
             or col.encryption_type is not null
          )
    {% endcall %}
    {% set result = [] %}
    {% for row in load_result('get_unmaskable_columns').table.rows %}
        {% do result.append(row[0]) %}
    {% endfor %}
    {{ return(result) }}
{% endmacro %}


{% macro sqlserver__apply_masks(relation, mask_config) %}
    {#-- If mask_config is {} or None, this is a no-op (mirrors apply_grants). --#}
    {% if not mask_config %}
        {{ return(none) }}
    {% endif %}

    {#-- DDM applies to base tables only. Views/ephemeral inherit masking from
         their base tables but cannot carry an ALTER-ed mask, so no-op. --#}
    {% if relation.type != 'table' %}
        {{ return(none) }}
    {% endif %}

    {#-- DDM requires SQL Server 2016 (major 13)+ ("Applies to: SQL Server 2016
         (13.x) and later"); sys.masked_columns does not exist before then. Fail
         clearly rather than with a cryptic error. --#}
    {% set major = sqlserver__server_major_version() %}
    {% if major is not none and major < 13 %}
        {{ exceptions.raise_compiler_error(
            "Dynamic Data Masking (the `masks` / `masked_with` config) requires SQL "
            ~ "Server 2016 (major version 13) or newer; detected major version "
            ~ major ~ " for " ~ relation ~ ". Remove the mask configuration.") }}
    {% endif %}

    {#-- Current mask state and the metadata needed to validate the desired map. --#}
    {% set existing_masks = run_query(get_show_mask_sql(relation)) %}
    {% set existing_columns = adapter.get_columns_in_relation(relation)
                                | map(attribute='name') | list %}
    {% set index_key_columns = sqlserver__get_mask_index_key_columns(relation) %}
    {% set unmaskable_columns = sqlserver__get_unmaskable_columns(relation) %}

    {% set changes = adapter.mask_changes(
        existing_masks, mask_config, index_key_columns, existing_columns) %}

    {#-- Surface skipped columns (typo / renamed) as warnings, don't fail. --#}
    {% for message in changes['skipped'] %}
        {% do exceptions.warn("apply_masks on " ~ relation ~ ": " ~ message) %}
    {% endfor %}

    {#-- Unmaskable column types: raise rather than let the ALTER fail. --#}
    {% set unmaskable_lower = unmaskable_columns | map('lower') | list %}
    {% set unmaskable_hits = [] %}
    {% for col, fn in changes['adds'] + changes['changes'] %}
        {% if col | lower in unmaskable_lower %}
            {% do unmaskable_hits.append(col) %}
        {% endif %}
    {% endfor %}
    {% if unmaskable_hits %}
        {{ exceptions.raise_compiler_error(
            "apply_masks on " ~ relation ~ ": cannot mask column(s) "
            ~ (unmaskable_hits | join(", ")) ~ " — computed, FILESTREAM, sparse "
            ~ "COLUMN_SET and Always Encrypted columns cannot carry a mask.") }}
    {% endif %}

    {#-- Index-key collision (SQL Server < 2022): raise with a descriptive
         message instead of emitting DDL that fails mid-transaction. --#}
    {% if changes['errors'] %}
        {{ exceptions.raise_compiler_error(
            "apply_masks on " ~ relation ~ ":\n" ~ (changes['errors'] | join("\n"))) }}
    {% endif %}

    {#-- Emit only what changed. --#}
    {% set statements = [] %}
    {% for col, fn in changes['adds'] %}
        {% do statements.append(
            "alter table " ~ relation ~ " alter column ["
            ~ (col | replace(']', ']]')) ~ "] add masked with (function = '"
            ~ (fn | replace("'", "''")) ~ "')") %}
    {% endfor %}
    {% for col, fn in changes['changes'] %}
        {% do statements.append(
            "alter table " ~ relation ~ " alter column ["
            ~ (col | replace(']', ']]')) ~ "] masked with (function = '"
            ~ (fn | replace("'", "''")) ~ "')") %}
    {% endfor %}
    {% for col in changes['drops'] %}
        {% do statements.append(
            "alter table " ~ relation ~ " alter column ["
            ~ (col | replace(']', ']]')) ~ "] drop masked") %}
    {% endfor %}

    {% if statements %}
        {% do run_query(statements | join(";\n")) %}
        {% do log("Applied " ~ statements | length ~ " data-mask change(s) on "
                  ~ relation, info=true) %}
    {% else %}
        {% do log("On " ~ relation ~ ": all data masks are in place, no changes needed.") %}
    {% endif %}
{% endmacro %}
