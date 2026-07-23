{% macro sqlserver__table_dml_refresh(target_relation, sql) %}
  {#
    DML-only table refresh for use under RCSI.

    Instead of rename-swap (which uses DDL and creates a window where the
    table name doesnt resolve), this macro:
    1. Builds new data into a scratch table via SELECT INTO (minimally logged)
    2. Compares schemas — if columns changed, falls back to rename-swap
    3. Swaps data via DELETE + INSERT inside an explicit transaction
       (RCSI ensures concurrent readers see old data until COMMIT)
    4. Cleans up the scratch table

    The scratch table is a regular table with a __dbt_refresh suffix,
    not a global temp table. This avoids cross-session visibility issues
    and ensures cleanup on failure (DROP IF EXISTS at the start of each run).
  #}

  {%- set refresh_relation = target_relation.incorporate(
      path={"identifier": target_relation.identifier ~ '__dbt_refresh'}
  ) -%}
  {%- set tmp_vw_relation = refresh_relation.incorporate(
      path={"identifier": refresh_relation.identifier ~ '__dbt_tmp_vw'}
  ) -%}

  {#- Query hint for the grant-taking data-movement statements below (SELECT INTO
      and the swap INSERT). get_query_options() emits the OPTION (...) clause and
      terminates it with ';', matching how create_table_as appends it. -#}
  {%- set query_label = get_query_options(parse_options=True) -%}

  {# Clean up any leftovers from a prior failed run #}
  {% call statement('dml_refresh_cleanup_pre') -%}
    DROP VIEW IF EXISTS {{ tmp_vw_relation.include(database=False) }};
    DROP TABLE IF EXISTS {{ refresh_relation }};
  {%- endcall %}

  {# Build new data into scratch table via temp view (handles CTEs in model SQL) #}
  {# Named 'main' because dbt requires a statement('main') call in every materialization #}
  {% call statement('dml_refresh_create_view') -%}
    {{ get_create_view_as_sql(tmp_vw_relation, sql) }}
  {%- endcall %}

  {% call statement('main') -%}
    SELECT * INTO {{ refresh_relation }} FROM {{ tmp_vw_relation }} {{ query_label }}
  {%- endcall %}

  {% call statement('dml_refresh_drop_view') -%}
    DROP VIEW IF EXISTS {{ tmp_vw_relation.include(database=False) }};
  {%- endcall %}

  {# Compare schemas: if columns differ, fall back to rename-swap #}
  {%- set schema_changes = check_for_schema_changes(refresh_relation, target_relation) -%}
  {%- set schema_match = not schema_changes['schema_changed'] -%}

  {% if schema_match %}
    {# Use the target's physical column order for both INSERT and SELECT. #}
    {# The scratch table has the same columns but possibly in a different order, #}
    {# so naming columns explicitly makes the swap order-independent. #}
    {%- set target_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set column_list = target_columns | map(attribute='quoted') | join(', ') -%}

    {# Atomic DML swap — RCSI protects concurrent readers #}
    {# When dbt_sqlserver_use_dbt_transactions is off (default), autocommit #}
    {# means we need the explicit BEGIN/COMMIT. When the flag is on, dbt #}
    {# already wraps the statement call in a transaction, so skip it. #}
    {% call statement('dml_refresh_swap') -%}
      {% if not adapter.behavior.dbt_sqlserver_use_dbt_transactions %}
      BEGIN TRANSACTION;
      {% endif %}
      DELETE FROM {{ target_relation }};
      INSERT INTO {{ target_relation }} ({{ column_list }})
        SELECT {{ column_list }} FROM {{ refresh_relation }} {{ query_label }}
      {% if not adapter.behavior.dbt_sqlserver_use_dbt_transactions %}
      COMMIT TRANSACTION;
      {% endif %}
    {%- endcall %}

    {# Cleanup scratch table #}
    {% call statement('dml_refresh_cleanup_post') -%}
      DROP TABLE IF EXISTS {{ refresh_relation }};
    {%- endcall %}

    {# The target table persisted (no rebuild), so converge its indexes on
       the config. Runs after the swap's self-contained transaction. #}
    {% do sqlserver__reconcile_indexes(target_relation) %}

  {% else %}
    {# Schema changed — fall back to rename-swap for this run #}
    {{ log("Schema change detected for " ~ target_relation ~ " — falling back to rename-swap", info=true) }}

    {%- set backup_relation_type = target_relation.type -%}
    {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
    {{ drop_relation_if_exists(backup_relation) }}

    {# Rename scratch table into position #}
    {% set existing_relation = load_cached_relation(target_relation) %}
    {% if existing_relation is not none %}
      {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {% endif %}

    {{ adapter.rename_relation(refresh_relation, target_relation) }}

    {% do create_indexes(target_relation) %}

    {{ drop_relation_if_exists(backup_relation) }}

    {# scratch table is now the target, nothing to drop #}
  {% endif %}

{% endmacro %}
