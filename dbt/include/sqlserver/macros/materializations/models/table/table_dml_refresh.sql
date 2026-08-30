{% macro sqlserver__table_dml_refresh(target_relation, sql) %}
  {#
    The DELETE + INSERT swap below (dml_refresh_swap) is only safe because
    every connection sets SET XACT_ABORT ON at session level (see
    dbt/adapters/sqlserver/sqlserver_connections.py, xact_abort credential,
    dbt-msft/dbt-sqlserver#718). Without it, a run-time error partway
    through the swap (e.g. a NOT NULL/constraint violation on the INSERT)
    only aborts that statement, not the batch — the DELETE can still
    commit, silently emptying the target. Do not add a per-macro
    SET XACT_ABORT ON here; the session-level default is the single source
    of truth, and do not "simplify" this back into an unguarded batch.

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

  {#- Resolve the mask map once; applied per-branch below at the right point
      relative to index creation (see apply_masks). -#}
  {%- set mask_config = adapter.resolve_masks(model, config.get('masks')) -%}

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

    {# Persisted-table path: masks already exist from the prior build; this
       reconciles any config change. Runs after reconcile so index drops land
       first. #}
    {% do apply_masks(target_relation, mask_config) %}

  {% else %}
    {# Schema changed — fall back to rename-swap for this run #}
    {{ log("Schema change detected for " ~ target_relation ~ " — falling back to rename-swap", info=true) }}

    {%- set backup_relation_type = target_relation.type -%}
    {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
    {{ drop_relation_if_exists(backup_relation) }}

    {#- The scratch table above came from SELECT * INTO, which is the right
        shape for the schema probe and the wrong one for the object that is
        about to be renamed into position: it copies no constraint and no
        index, and takes nullability from the query rather than from a
        contract. Left as-is it silently strips the model of its clustered
        columnstore index - create_indexes only builds what the `indexes`
        config names, never the as_columnstore CCI - and, under a contract, of
        its NOT NULLs and inline constraints too. None of it came back on a
        later run, because every later run matched the new schema and took the
        DELETE+INSERT path above.

        So rebuild it the way this adapter builds every other table. The
        rebuild belongs on this branch alone: doing it up front would build,
        and then throw away, a full columnstore index on every steady-state
        refresh, which on a large table dominates the run. A schema change is
        rare, so one extra build here is much the cheaper trade.

        It is not free, though: the model's SQL runs a second time here, the
        SELECT * INTO above having already run it once as the schema probe.
        Probing the tmp view instead of the materialized scratch would avoid
        that, at the cost of changing how the probe behaves - a separate
        change, not this one.

        create_table_as builds and drops its own __dbt_tmp_vw. -#}
    {{ drop_relation_if_exists(refresh_relation) }}
    {% call statement('dml_refresh_rebuild') -%}
      {{ get_create_table_as_sql(False, refresh_relation, sql) }}
    {%- endcall %}

    {# Rename scratch table into position #}
    {% set existing_relation = load_cached_relation(target_relation) %}
    {% if existing_relation is not none %}
      {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {% endif %}

    {{ adapter.rename_relation(refresh_relation, target_relation) }}

    {# Freshly rebuilt (no masks carried), so apply masks before
       create_indexes — a mask cannot be added to a column an index depends
       on (documented for all SQL Server versions). #}
    {% do apply_masks(target_relation, mask_config) %}

    {% do create_indexes(target_relation) %}

    {{ drop_relation_if_exists(backup_relation) }}

    {# scratch table is now the target, nothing to drop #}
  {% endif %}

{% endmacro %}
