{% macro sqlserver__get_incremental_microbatch_sql(arg_dict) %}
    {%- set target = arg_dict["target_relation"] -%}
    {%- set source = arg_dict["temp_relation"] -%}
    {%- set dest_columns = arg_dict["dest_columns"] -%}
    {%- set incremental_predicates = [] if arg_dict.get('incremental_predicates') is none else arg_dict.get('incremental_predicates') -%}

    {#-- Add additional incremental_predicates to filter for batch --#}
    {% if model.config.get("__dbt_internal_microbatch_event_time_start") -%}
    {{ log("incremental append event start time > DBT_INTERNAL_TARGET." ~ model.config.event_time ~ " >= cast('" ~ model.config.__dbt_internal_microbatch_event_time_start ~ "' as datetimeoffset)") }}
        {% do incremental_predicates.append("DBT_INTERNAL_TARGET." ~ model.config.event_time ~ " >= cast('" ~ model.config.__dbt_internal_microbatch_event_time_start ~ "' as datetimeoffset)") %}
    {% endif %}
    {% if model.config.__dbt_internal_microbatch_event_time_end -%}
    {{ log("incremental append event end time < DBT_INTERNAL_TARGET." ~ model.config.event_time ~ " < cast('" ~ model.config.__dbt_internal_microbatch_event_time_end ~ "' as datetimeoffset)") }}
        {% do incremental_predicates.append("DBT_INTERNAL_TARGET." ~ model.config.event_time ~ " < cast('" ~ model.config.__dbt_internal_microbatch_event_time_end ~ "' as datetimeoffset)") %}
    {% endif %}
    {% do arg_dict.update({'incremental_predicates': incremental_predicates}) %}

    delete DBT_INTERNAL_TARGET from {{ target }} AS DBT_INTERNAL_TARGET
    where (
    {% for predicate in incremental_predicates %}
        {%- if not loop.first %}and {% endif -%} {{ predicate }}
    {% endfor %}
    );

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    )
{% endmacro %}
