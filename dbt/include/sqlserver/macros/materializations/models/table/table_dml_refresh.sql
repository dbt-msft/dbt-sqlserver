{% macro sqlserver__table_dml_refresh_stage(target_relation, sql) %}
  {#-
    Schema resolution for the dml refresh: drop leftovers by name, create
    the tmp view over the model SQL, create the scratch table EMPTY. Every
    statement passes auto_begin=False. table.sql calls this before the in-tx
    pre-hooks under load (nothing open: each statement autocommits, so the
    scratch table's Sch-M ends with its create) and after them under build.
    Returns the two relations the load and the tail need.
  -#}
  {%- set refresh_relation = target_relation.incorporate(
      path={"identifier": target_relation.identifier ~ '__dbt_refresh'}
  ) -%}
  {%- set tmp_vw_relation = refresh_relation.incorporate(
      path={"identifier": refresh_relation.identifier ~ '__dbt_tmp_vw'}, type='view'
  ) -%}

  {% call statement('dml_refresh_cleanup_pre', auto_begin=False) -%}
    DROP VIEW IF EXISTS {{ tmp_vw_relation.include(database=False) }};
    DROP TABLE IF EXISTS {{ refresh_relation }};
  {%- endcall %}

  {# Build new data into scratch table via temp view (handles CTEs in model SQL) #}
  {% call statement('dml_refresh_create_view', auto_begin=False) -%}
    {{ get_create_view_as_sql(tmp_vw_relation, sql) }}
  {%- endcall %}

  {#- Create the scratch table empty. It is never contract-enforced: contracts
      describe the model's target, and this table exists only to stage rows
      for the swap, which then inserts into the real (already contracted)
      target. -#}
  {% call statement('dml_refresh_create_scratch', auto_begin=False) -%}
    {{ sqlserver__get_create_table_empty_sql(refresh_relation, tmp_vw_relation, sql, false) }}
  {%- endcall %}

  {{ return({'refresh_relation': refresh_relation, 'tmp_vw_relation': tmp_vw_relation}) }}
{% endmacro %}


{% macro sqlserver__table_dml_refresh(target_relation, sql, stage) %}
  {#-
    XACT_ABORT. The DELETE + INSERT swap below is only safe because every
    connection runs SET XACT_ABORT ON at session level (#718): without it a
    run-time error in the INSERT aborts that statement only, and the DELETE
    can still commit, silently emptying the target. Do not add a per-macro
    SET, and do not fold the swap back into an unguarded batch.

    Flow (#819). Sch-M conflicts with the Sch-S every metadata reader takes,
    so no statement that holds one may share a transaction with the load.

      stage (table.sql, before the in-tx hooks)            autocommit
      |- DROP leftovers, CREATE VIEW, SELECT TOP 0 * INTO   Sch-M ends per statement
      BEGIN                        first in-tx pre-hook, else the swap
      |- INSERT scratch WITH (TABLOCK)                      X table lock only
      |- schema compare                                     read-only probes
      |- DELETE target + INSERT target                      X on the target
      |- in-tx post-hooks                                   atomic with the swap
      COMMIT                       table.sql
      |- DROP VIEW, reconcile indexes, masks, DROP scratch, grants, docs

    RCSI keeps concurrent readers on the old rows until COMMIT. On a schema
    change the swap is skipped and the scratch table is rebuilt and renamed
    into place instead (below). The scratch table is a real table with a
    __dbt_refresh suffix, not a global temp table, so it is visible for the
    schema compare and droppable by name on the next run.
  -#}

  {%- set refresh_relation = stage['refresh_relation'] -%}
  {%- set tmp_vw_relation = stage['tmp_vw_relation'] -%}

  {#- Query hint for the grant-taking data-movement statements below (the scratch
      load and the swap INSERT; not the empty create, which moves no rows).
      get_query_options() emits the OPTION (...) clause and terminates it with
      ';', matching how create_table_as appends it. -#}
  {%- set query_label = get_query_options(parse_options=True) -%}

  {#- Named 'main' because dbt requires a statement('main') call in every
      materialization, and this is the statement worth having there: it is the
      one that moves the rows, so adapter_response still reports a meaningful
      row count. -#}
  {% call statement('main', auto_begin=False) -%}
    {{ sqlserver__get_tablock_insert_sql(refresh_relation, tmp_vw_relation, query_label, false) }}
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

    {#- The swap. With dbt-managed transactions off, the in-batch
        BEGIN/COMMIT makes it atomic; with them on (default) this statement's
        auto_begin opens the transaction unless a pre-hook already did, and
        table.sql commits it after the in-tx post-hooks. -#}
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

    {#- Deliberately left open: table.sql commits after the in-tx post-hooks,
        so a transaction: true post-hook is atomic with the swap. Index and
        mask reconciliation run on the tail, outside; if they fail the new
        rows are committed with indexes not yet converged, and the next run
        converges them (both reconcile against config, not a delta). -#}

  {% else %}
    {# Schema changed — fall back to rename-swap for this run #}
    {{ log("Schema change detected for " ~ target_relation ~ " — falling back to rename-swap", info=true) }}

    {#- The scratch load joined no transaction of its own; open one for this
        branch's renames and drops, which stay transactional. -#}
    {% do adapter.begin_if_closed() %}

    {%- set backup_relation_type = target_relation.type -%}
    {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
    {{ drop_relation_if_exists(backup_relation) }}

    {#- The scratch table above came from the empty-create load (SELECT TOP 0
        INTO plus the TABLOCK insert), which is the right shape for the schema
        probe and the wrong one for the object that is about to be renamed into
        position: it copies no constraint and no index, and takes nullability
        from the query rather than from a contract. Left as-is it silently
        strips the model of its clustered columnstore index - create_indexes
        only builds what the `indexes`
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
        scratch load above having already run it once as the schema probe.
        Any side effect in that SQL therefore happens twice, and the two runs
        are not interchangeable - the schema decision came from the first, the
        table renamed into position comes from the second. A model whose column
        shape can differ between them lands the second shape unchecked, unless
        a contract is enforced and create_table_as re-asserts it. Probing the
        tmp view instead of the materialized scratch would collapse the two
        back into one, at the cost of changing how the probe behaves - a
        separate change, not this one.

        create_table_as builds and drops its own __dbt_tmp_vw - the same name
        as the stage's view, which it replaces. -#}
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

    {#- Freshly rebuilt above (no masks carried), so apply masks before
        create_indexes — a mask cannot be added to a column an index depends
        on (documented for all SQL Server versions). Applied here, inside the
        cutover transaction, rather than on table.sql's tail: this table is new
        and unmasked, so a mask failure after the cutover committed would leave
        it live with the columns exposed. create_indexes runs on the tail
        instead; index_strategy='create' keeps the mask-then-index order. -#}
    {% do apply_masks(target_relation, mask_config) %}

    {{ drop_relation_if_exists(backup_relation) }}

    {# scratch table is now the target, nothing to drop #}
  {% endif %}

  {#- schema_match picks the tail's index strategy: reconcile on the swap
      path, create on the fallback (masked above, mask-then-index order).
      refresh_relation is the scratch table for the tail to drop after the
      commit; none on the fallback, where it was renamed into the target. -#}
  {{ return({
    'schema_match': schema_match,
    'refresh_relation': refresh_relation if schema_match else none
  }) }}
{% endmacro %}
