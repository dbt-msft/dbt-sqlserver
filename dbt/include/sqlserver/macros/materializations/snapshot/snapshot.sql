{% macro snapshot_staging_table_inserts(strategy, source_sql, target_relation) -%}

    select
        'insert' as dbt_change_type,
        source_data.*

    from (

        select *,
            COALESCE({{ strategy.scd_id }}, NULL) as dbt_scd_id,
            COALESCE({{ strategy.unique_key }}, NULL) as dbt_unique_key,
            COALESCE({{ strategy.updated_at }}, NULL) as dbt_updated_at,
            COALESCE({{ strategy.updated_at }}, NULL) as dbt_valid_from,
            nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to

        from (
        {{ source_sql }}
        ) snapshot_query
        ) source_data
    left outer join (

        select *,
            {{ strategy.unique_key }} as dbt_unique_key

        from {{ target_relation }}

    ) snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
    where snapshotted_data.dbt_unique_key is null
       or (
            snapshotted_data.dbt_unique_key is not null
        and snapshotted_data.dbt_valid_to is null
        and (
            {{ strategy.row_changed }}
        )
    )

{%- endmacro %}


{% macro snapshot_staging_table_updates(strategy, source_sql, target_relation) -%}

    select
        'update' as dbt_change_type,
        snapshotted_data.dbt_scd_id,
        source_data.dbt_valid_from as dbt_valid_to

    from (

    select
        *,
        COALESCE({{ strategy.scd_id }}, NULL) as dbt_scd_id,
        COALESCE({{ strategy.unique_key }}, NULL) as dbt_unique_key,
        COALESCE({{ strategy.updated_at }}, NULL) as dbt_updated_at,
        COALESCE({{ strategy.updated_at }}, NULL) as dbt_valid_from

    from (
        {{ source_sql }}
        ) snapshot_query
        ) source_data
    join (

    select *,
        {{ strategy.unique_key }} as dbt_unique_key

    from {{ target_relation }}

) snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
    where snapshotted_data.dbt_valid_to is null
    and (
        {{ strategy.row_changed }}
    )

{%- endmacro %}


{% macro build_snapshot_staging_table(strategy, sql, target_relation) %}
    {% set tmp_relation = make_temp_relation(target_relation) %}

    {% set inserts_select = snapshot_staging_table_inserts(strategy, sql, target_relation) %}
    {% set updates_select = snapshot_staging_table_updates(strategy, sql, target_relation) %}

    {% call statement('build_snapshot_staging_relation_inserts') %}
        {{ create_table_as(False, tmp_relation, inserts_select) }}
    {% endcall %}

    {% call statement('build_snapshot_staging_relation_updates') %}
        insert into {{ tmp_relation }} (dbt_change_type, dbt_scd_id, dbt_valid_to)
        select dbt_change_type, dbt_scd_id, dbt_valid_to from (
            {{ updates_select }}
        ) dbt_sbq;
    {% endcall %}

    {% do return(tmp_relation) %}
{% endmacro %}


{% macro sqlserver__post_snapshot(staging_relation) %}
  -- Clean up the snapshot temp table
  {% do drop_relation(staging_relation) %}
{% endmacro %}


{% macro build_snapshot_table(strategy, sql) %}

    select *,
        COALESCE({{ strategy.scd_id }}, NULL)  as dbt_scd_id,
        COALESCE({{ strategy.updated_at }}, NULL) as dbt_updated_at,
        COALESCE({{ strategy.updated_at }}, NULL) as dbt_valid_from,
        nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to
    from (
        {{ sql }}
    ) sbq

{% endmacro %}

{% macro sqlserver__create_columns(relation, columns) %}
  {% for column in columns %}
    {% call statement() %}
      alter table {{ relation }} add "{{ column.name }}" {{ column.data_type }};
    {% endcall %}
  {% endfor %}
{% endmacro %}