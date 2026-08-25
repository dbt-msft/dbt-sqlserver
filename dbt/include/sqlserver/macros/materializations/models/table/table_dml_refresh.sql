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
    1. Creates a scratch table empty, then bulk-loads it with
       INSERT ... WITH (TABLOCK) (minimally logged, same as the SELECT INTO
       this replaces)
    2. Compares schemas — if columns changed, falls back to rename-swap
    3. Swaps data via DELETE + INSERT inside an explicit transaction
       (RCSI ensures concurrent readers see old data until COMMIT)
    4. Cleans up the scratch table

    The scratch table is a regular table with a __dbt_refresh suffix,
    not a global temp table. This avoids cross-session visibility issues
    and ensures cleanup on failure (DROP IF EXISTS at the start of each run).

    Lock discipline (dbt-msft/dbt-sqlserver#819). The scratch build used to be
    one fused `SELECT * INTO`, which holds Sch-M on the new object for the
    whole load, and it ran inside the materialization's ambient transaction,
    which held that Sch-M through to the trailing adapter.commit(). Sch-M is
    the one mode incompatible with the Sch-S lock every metadata reader takes,
    so a slow model blocked metadata readers in every other session for the
    length of its load. Both halves are fixed here:

      - the create and the load are separate statements (see
        sqlserver__get_create_table_empty_sql), so Sch-M is held for the
        instant of the create, not the length of the load; and
      - every statement before the swap passes auto_begin=False, so each one
        autocommits and drops its catalog locks as it finishes instead of
        holding them to commit. This mirrors the incremental temp build, which
        declines the ambient transaction for the same reason
        (see incremental.sql).

    Splitting alone would not have helped: locks are held to commit, not to
    end-of-statement, so inside the ambient transaction the split create holds
    Sch-M just as long as the fused statement did. Both changes are needed.

    Caveat: a pre-hook configured with inside_transaction=true (the dbt
    default) opens the ambient transaction before this macro runs, and
    auto_begin=False only declines to *open* a transaction - a statement still
    joins one that is already open. Projects that pre-hook a model on this
    path and care about the blocking should use inside_transaction=false. This
    is the same trade-off the incremental temp build already makes.
  #}

  {%- set refresh_relation = target_relation.incorporate(
      path={"identifier": target_relation.identifier ~ '__dbt_refresh'}
  ) -%}
  {%- set tmp_vw_relation = refresh_relation.incorporate(
      path={"identifier": refresh_relation.identifier ~ '__dbt_tmp_vw'}
  ) -%}

  {#- Query hint for the grant-taking data-movement statements below (the scratch
      load and the swap INSERT; not the empty create, which moves no rows).
      get_query_options() emits the OPTION (...) clause and terminates it with
      ';', matching how create_table_as appends it. -#}
  {%- set query_label = get_query_options(parse_options=True) -%}

  {# Clean up any leftovers from a prior failed run. auto_begin=False here and
     on every statement up to the swap: see the lock discipline note above. #}
  {% call statement('dml_refresh_cleanup_pre', auto_begin=False) -%}
    DROP VIEW IF EXISTS {{ tmp_vw_relation.include(database=False) }};
    DROP TABLE IF EXISTS {{ refresh_relation }};
  {%- endcall %}

  {# Build new data into scratch table via temp view (handles CTEs in model SQL) #}
  {% call statement('dml_refresh_create_view', auto_begin=False) -%}
    {{ get_create_view_as_sql(tmp_vw_relation, sql) }}
  {%- endcall %}

  {#- Create the scratch table empty, then load it, as two statements. The
      scratch table is never contract-enforced: contracts describe the model's
      target, and this table exists only to stage rows for the swap below,
      which then inserts into the real (already contracted) target. -#}
  {% call statement('dml_refresh_create_scratch', auto_begin=False) -%}
    {{ sqlserver__get_create_table_empty_sql(refresh_relation, tmp_vw_relation, sql, false) }}
  {%- endcall %}

  {#- Named 'main' because dbt requires a statement('main') call in every
      materialization, and this is the statement worth having there: it is the
      one that moves the rows, so adapter_response still reports a meaningful
      row count. -#}
  {% call statement('main', auto_begin=False) -%}
    {{ sqlserver__get_tablock_insert_sql(refresh_relation, tmp_vw_relation, query_label, false) }}
  {%- endcall %}

  {% call statement('dml_refresh_drop_view', auto_begin=False) -%}
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
    {# When dbt_sqlserver_use_dbt_transactions is off, autocommit means we #}
    {# need the explicit BEGIN/COMMIT. When the flag is on (the default), this #}
    {# statement's auto_begin supplies the transaction, and it is now the only #}
    {# thing that can: the scratch build above declines to open one, and the #}
    {# metadata reads just above (schema compare, column list) no longer do #}
    {# either - they are read-only probes and pass auto_begin=False (#819). #}
    {# The commit_if_open below closes it either way. #}
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

    {#- End the swap's transaction here rather than letting it run to the
        materialization's trailing adapter.commit(). The DELETE holds X locks
        on the target until commit, and everything after this point - dropping
        the scratch table, index reconciliation, masks, grants, persist_docs -
        would otherwise sit inside that window, with the index DDL adding Sch-M
        on the *target* on top (#819).

        The atomicity boundary this macro cares about is the swap itself: the
        target is never seen half-swapped. Index and mask reconciliation land
        outside it, so a failure there leaves the new data committed with
        indexes not yet converged - which the next run fixes, since both
        reconcile against the config rather than applying a delta. This is
        already how the path behaves with dbt_sqlserver_use_dbt_transactions
        off, where the in-batch COMMIT above closes the swap the same way. -#}
    {% do adapter.commit_if_open() %}

    {# Cleanup scratch table — still outside a transaction, so its Sch-M goes
       the moment the drop finishes. #}
    {% call statement('dml_refresh_cleanup_post', auto_begin=False) -%}
      DROP TABLE IF EXISTS {{ refresh_relation }};
    {%- endcall %}

    {#- Reopen the ambient transaction for the tail, so the rest of the
        materialization keeps its semantics and table.sql's adapter.commit()
        has a matching BEGIN rather than raising. No-op at the SQL level when
        the flag is off. -#}
    {% do adapter.begin_if_closed() %}

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

    {#- The scratch build above declined to open the ambient transaction, so
        open one here: this branch's renames and drops keep the transactional
        semantics they had before #819, and table.sql's adapter.commit() needs
        a matching BEGIN either way. -#}
    {% do adapter.begin_if_closed() %}

    {%- set backup_relation_type = target_relation.type -%}
    {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
    {{ drop_relation_if_exists(backup_relation) }}

    {# Rename scratch table into position #}
    {% set existing_relation = load_cached_relation(target_relation) %}
    {% if existing_relation is not none %}
      {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {% endif %}

    {{ adapter.rename_relation(refresh_relation, target_relation) }}

    {# Freshly built scratch table (no masks carried), so apply masks before
       create_indexes — a mask cannot be added to a column an index depends
       on (documented for all SQL Server versions). #}
    {% do apply_masks(target_relation, mask_config) %}

    {% do create_indexes(target_relation) %}

    {{ drop_relation_if_exists(backup_relation) }}

    {# scratch table is now the target, nothing to drop #}
  {% endif %}

{% endmacro %}
