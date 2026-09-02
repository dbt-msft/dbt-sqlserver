{% materialization incremental, adapter='sqlserver' -%}

  -- relations
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set temp_relation = make_temp_relation(target_relation)-%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation)-%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}

  -- configs
  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (should_full_refresh()  or existing_relation.is_view) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set full_refresh_build = config.get('full_refresh_build', 'heap_then_index') -%}

  {#- Decide the build branch up front, from state that is fixed before any
      hook runs, so schema resolution can be staged ahead of the in-transaction
      pre-hooks (see pre_hook_transaction_scope below).
        'prebuilt' - in-place build into the target's clustered design
        'create'   - build the intermediate from scratch and rename-swap it in
                     (first build, --full-refresh, view -> table)
        'append'   - the incremental strategy's DML against the target -#}
  {%- if existing_relation is none -%}
    {%- set branch = 'prebuilt' if full_refresh_build == 'prebuilt' else 'create' -%}
  {%- elif full_refresh_mode -%}
    {#- explicit --full-refresh only; view->table conversions keep the default path -#}
    {%- set branch = 'prebuilt' if (full_refresh_build == 'prebuilt' and should_full_refresh()) else 'create' -%}
  {%- else -%}
    {%- set branch = 'append' -%}
  {%- endif -%}
  {#- a table built fresh this run carries no masks or indexes yet -#}
  {%- set fresh_build = branch != 'append' -%}

  {#- Where schema resolution (the tmp view and the empty CREATE) runs relative
      to the in-transaction pre-hooks - see sqlserver__pre_hook_transaction_scope
      and docs/transaction_scope.md. 'load', the default, stages it BEFORE them
      so it autocommits and the new object's Sch-M is released in an instant;
      the load then joins the pre-hook's transaction (X table lock only) and a
      transaction: true pre-hook keeps rolling back with a failed load. This
      covers the __dbt_temp build of the append branch too: under a
      transactional pre-hook its fused create used to hold Sch-M on the temp
      table for the whole temp load. 'build' stages after the hooks, inside
      their transaction. Inert on prebuilt, whose setup has to follow the
      hooks (a hook may read {{ this }} before the rebuild drops it) and which
      commits them with its in-progress marker regardless. -#}
  {%- set pre_hook_transaction_scope = sqlserver__pre_hook_transaction_scope() -%}
  {%- set stage_before_hooks = pre_hook_transaction_scope == 'load' and branch != 'prebuilt' -%}
  {%- set build_relation = temp_relation if branch == 'append' else intermediate_relation -%}
  {%- set build_is_temporary = branch == 'append' -%}
  {%- set tmp_vw_relation = build_relation.incorporate(
      path={"identifier": build_relation.identifier ~ '__dbt_tmp_vw'}, type='view'
  ) -%}

  -- the temp_ and backup_ relations should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation. This has to happen before
  -- BEGIN, in a separate transaction
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation)-%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
   -- grab current tables grants config for comparison later on
  {% set grant_config = config.get('grants') %}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {#- Schema resolution, ahead of the transaction: auto_begin=False with
      nothing open (the outside-transaction hooks autocommit and the contract
      describe probe never begins one), so it autocommits. A transaction: true
      pre-hook that creates an object the model reads fails here, since the
      view must bind now - declare that hook transaction: false or set
      pre_hook_transaction_scope: build. -#}
  {% if stage_before_hooks %}
    {%- set stage_sql = sqlserver__get_create_table_stage_sql(build_is_temporary, build_relation, sql) -%}
    {% call statement('create_table_stage', auto_begin=False) %}
        {{ stage_sql }}
    {% endcall %}
  {% endif %}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}

  {% if branch == 'prebuilt' %}
    {% if existing_relation is not none %}
      {#- in-place full refresh: drop the existing table, rebuild the target
          directly with no intermediate or swap. The target is marked as
          having a full refresh in flight (blocking normal runs until one
          completes), but only AFTER anything that can fail on config alone -
          a pure config error must not mark a healthy table -#}
      {% do sqlserver__assert_no_unguarded_self_reference(target_relation, sql) %}
      {#- validate the index config BEFORE marking or dropping anything -#}
      {% do adapter.validate_indexes(
          config.get('indexes', default=[]),
          config.get('as_columnstore', default=true),
          config.get('drop_unmanaged_indexes', default=false)
      ) %}
      {% if existing_relation.type == 'table' %}
        {% do sqlserver__mark_full_refresh_incomplete(existing_relation) %}
      {% endif %}
      {% do adapter.drop_relation(existing_relation) %}
    {% endif %}
    {#- Calls its own statement() blocks (including 'main') rather than
        returning SQL to run below, so it can commit its in-progress marker
        independently of the load that follows - see the macro for why. -#}
    {% do sqlserver__create_table_as_prebuilt(target_relation, sql) %}
    {#- the prebuilt path lands the table via raw SQL, not a cache-maintaining
        adapter method (rename_relation/drop_relation), so register it here to
        keep dbt's relation cache in sync with the database. On the
        full-refresh branch this also re-adds the target that drop_relation
        removed from the cache. -#}
    {% do adapter.cache_added(target_relation) %}

  {% elif branch == 'create' %}
    {#- Build into the intermediate and swap, rather than straight into the
        target. The build's create and load commit independently, so building
        into the target would mean a failed load commits an EMPTY table under
        the model's real name: dbt's next run then sees a relation that exists
        and is not a view, takes the append/merge branch, and merges that
        run's window into an empty table - no error, and every row the first
        build should have loaded is gone. Staging into __dbt_tmp leaves the
        target absent on failure, which is what dbt should see, and restores
        the OBJECT_ID drop guard for the throwaway (build_into_temp keys off
        the suffix). -#}
    {% if existing_relation is not none and existing_relation.type == 'table' %}
      {#- marks the full refresh in flight and commits that on its own - which
          also commits any transaction: true pre-hook, so 'build' scope cannot
          deliver rollback here (docs/transaction_scope.md) -#}
      {% do sqlserver__mark_full_refresh_incomplete(existing_relation) %}
    {% endif %}
    {% if stage_before_hooks %}
      {#- The stage ran and committed before the hooks. The load joins the
          pre-hook's transaction if one is open and autocommits otherwise;
          either way it takes an X table lock, never Sch-M. The tmp view is
          dropped on the tail, after the cutover commits. -#}
      {%- set load_sql = sqlserver__get_create_table_load_sql(False, intermediate_relation, sql, drop_tmp_view=False) -%}
      {% call statement("main", auto_begin=False) %}
          {{ load_sql }}
      {% endcall %}
      {#- statement() writes the compiled artifact for 'main' only, so write
          the whole build back over it rather than leaving target/run/ with
          the load and no CREATE. -#}
      {% do write(stage_sql ~ '\n' ~ load_sql) %}
    {% else %}
      {#- pre_hook_transaction_scope='build': create and load in one
          transaction, holding the new table's Sch-M for the load (#819).
          Chosen explicitly. -#}
      {%- set stage_sql = sqlserver__get_create_table_stage_sql(False, intermediate_relation, sql) -%}
      {%- set load_sql = sqlserver__get_create_table_load_sql(False, intermediate_relation, sql) -%}
      {% call statement("main") %}
          {{ stage_sql }}
          {{ load_sql }}
      {% endcall %}
    {% endif %}
    {#- the swap and the tail need a transaction; with no pre-hook one,
        nothing above leaves one open -#}
    {% do adapter.begin_if_closed() %}

    {#- There is nothing to back up on a first build: an unconditional rename
        would be sp_rename against a name that does not exist (Msg 15225).
        Guard it as table.sql does, and only queue a backup for dropping when
        one was actually made. -#}
    {% if existing_relation is not none %}
      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% do to_drop.append(backup_relation) %}
    {% endif %}
    {% do adapter.rename_relation(intermediate_relation, target_relation) %}

  {% else %}
    {#- refuse to append onto a table whose last full refresh never
        completed -#}
    {% if existing_relation.type == 'table' %}
      {% do sqlserver__assert_no_incomplete_full_refresh(existing_relation) %}
    {% endif %}

    {#- The temp build is catalog DDL plus a load and must not hold catalog
        locks to the strategy DML's commit: held that long, its sysschobjs X
        keylocks deadlock a second worker. With the create staged before the
        hooks, only the load runs here; it joins a pre-hook's transaction
        (X table lock on the temp table, harmless) or autocommits. The
        strategy DML below still runs transactionally, via statement('main')'s
        default auto_begin through to adapter.commit(). -#}
    {% if stage_before_hooks %}
      {% do run_query(sqlserver__get_create_table_load_sql(True, temp_relation, sql, drop_tmp_view=False)) %}
    {% else %}
      {% do run_query(get_create_table_as_sql(True, temp_relation, sql)) %}
    {% endif %}

    {% set contract_config = config.get('contract') %}
    {% if not contract_config or not contract_config.enforced %}
      {% set expansion_max_rows = config.get('column_type_expansion_max_rows', 1000000) %}
      {% do adapter.expand_target_column_types(
               from_relation=temp_relation,
               to_relation=target_relation,
               max_rows=expansion_max_rows) %}
    {% endif %}
    {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
    {% set dest_columns = process_schema_changes(on_schema_change, temp_relation, existing_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}

    {#-- Get the incremental_strategy, the macro to use for the strategy, and build the sql --#}
    {% set incremental_strategy = config.get('incremental_strategy') or 'default' %}
    {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
    {% set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(context, incremental_strategy) %}
    {% set strategy_arg_dict = ({'target_relation': target_relation, 'temp_relation': temp_relation, 'unique_key': unique_key, 'dest_columns': dest_columns, 'incremental_predicates': incremental_predicates }) %}
    {% set build_sql = strategy_sql_macro_func(strategy_arg_dict) %}

    {% call statement("main") %}
        {{ build_sql }}
    {% endcall %}

    {% do to_drop.append(temp_relation) %}
  {% endif %}

  {% set mask_config = adapter.resolve_masks(model, config.get('masks')) %}
  {% if fresh_build %}
    {#- Freshly built table: mask before creating (rowstore) indexes, since a
        mask cannot be added to a column an index depends on (all versions).
        Inside the transaction, deliberately: this table carries no masks
        yet, so a mask failure after the cutover committed would leave it live
        with the columns exposed. -#}
    {% do apply_masks(target_relation, mask_config) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {#- The atomic unit ends here, as in table.sql: in-transaction pre-hooks,
      the load or strategy DML, the cutover, fresh-table masks, and
      in-transaction post-hooks. Index work, grants, denies and persist_docs
      are the adapter's housekeeping and run outside it, so sp_rename's Sch-M
      on the live target does not span the index builds (#819). A post-hook
      that needs the indexes present should declare transaction: false. -#}
  {% do adapter.commit_if_open() %}

  {#- The tmp view, dropped now that no transaction is open: an uncommitted
      DROP VIEW blocks catalog scans as an uncommitted CREATE does. -#}
  {% if stage_before_hooks %}
    {% call statement('drop_tmp_view', auto_begin=False) -%}
      DROP VIEW IF EXISTS {{ tmp_vw_relation.include(database=False) }};
    {%- endcall %}
  {% endif %}

  {% if fresh_build %}
    {% do create_indexes(target_relation) %}
  {% else %}
    {# Table persisted across this run: converge its indexes on the config,
       then reconcile masks (index drops land first). The table already
       carries its previous masks, so a failure here exposes nothing. #}
    {% do sqlserver__reconcile_indexes(target_relation) %}
    {% do apply_masks(target_relation, mask_config) %}
  {% endif %}

  {#- sqlserver__create_indexes_no_txn ends with begin_if_closed, so an
      ONLINE/RESUMABLE index leaves a transaction open here; close it so the
      grants and persist_docs below do not run inside one held to commit. -#}
  {% do adapter.commit_if_open() %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {#-- Re-apply object-level DENYs after grants (covers the append and
       full-refresh paths alike). --#}
  {% set deny_config = adapter.resolve_denies(model, config.get('denies')) %}
  {% do apply_denies(target_relation, deny_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {#- adapter.commit() raises if it finds nothing open, and apply_grants only
      opens one when the model configures grants. State the precondition
      instead of relying on that. -#}
  {% do adapter.begin_if_closed() %}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
