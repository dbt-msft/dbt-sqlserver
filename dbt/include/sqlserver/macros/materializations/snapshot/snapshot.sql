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


{% materialization snapshot, default %}
  {%- set config = model['config'] -%}

  {%- set target_database = config.get('target_database') -%}
  {%- set target_schema = config.get('target_schema') -%}
  {%- set target_table = model.get('alias', model.get('name')) -%}

  {%- set strategy_name = config.get('strategy') -%}
  {%- set unique_key = config.get('unique_key') %}

  {% if not adapter.check_schema_exists(target_database, target_schema) %}
    {% do create_schema(target_database, target_schema) %}
  {% endif %}

  {% set target_relation_exists, target_relation = get_or_create_relation(
          database=target_database,
          schema=target_schema,
          identifier=target_table,
          type='table') -%}

  {%- if not target_relation.is_table -%}
    {% do exceptions.relation_wrong_type(target_relation, 'table') %}
  {%- endif -%}

  {% set strategy_macro = strategy_dispatch(strategy_name) %}
  {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", config) %}

  {% if not target_relation_exists %}

      {% set build_sql = build_snapshot_table(strategy, model['injected_sql']) %}
      {% call statement('main') -%}
          {{ create_table_as(False, target_relation, build_sql) }}
      {% endcall %}

  {% else %}

      {{ adapter.valid_snapshot_target(target_relation) }}

      {% set staging_table = build_snapshot_staging_table(strategy, sql, target_relation) %}

      {% do adapter.expand_target_column_types(from_relation=staging_table,
                                               to_relation=target_relation) %}

      {% set missing_columns = adapter.get_missing_columns(staging_table, target_relation)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

      {% do create_columns(target_relation, missing_columns) %}

      {% set source_columns = adapter.get_columns_in_relation(staging_table)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

      {% set quoted_source_columns = [] %}
      {% for column in source_columns %}
        {% do quoted_source_columns.append(adapter.quote(column.name)) %}
      {% endfor %}

      {% call statement('main') %}
          {{ snapshot_merge_sql(
                target = target_relation,
                source = staging_table,
                insert_cols = quoted_source_columns
             )
          }}
          {{ drop_relation(staging_table) }}
      {% endcall %}

  {% endif %}

  {{ adapter.commit() }}

  {% if staging_table is defined %}
      {% do post_snapshot(staging_table) %}
  {% endif %}

{% endmaterialization %}

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