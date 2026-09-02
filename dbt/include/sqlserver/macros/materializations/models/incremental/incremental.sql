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

  {#- load: stage before the in-tx pre-hooks; build: stage after them. See
      sqlserver__pre_hook_transaction_scope for the two flows. Covers the
      append branch's __dbt_temp build too. Inert on prebuilt, whose setup
      must follow the hooks (a hook may read {{ this }} before the rebuild
      drops it). -#}
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

  {#- Stage now: nothing is open (outside-tx hooks autocommit, the contract
      probe never begins), so with auto_begin=False each statement autocommits
      and the new object's Sch-M ends with its statement (#819). -#}
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
    {#- Build into the intermediate and swap, never straight into the target:
        the create and the load commit independently, so a failed load would
        leave an EMPTY table under the real name, and the next run would take
        the append branch and merge into it - silent data loss. Staging into
        __dbt_tmp leaves no target on failure and gets the OBJECT_ID drop
        guard for free. -#}
    {% if existing_relation is not none and existing_relation.type == 'table' %}
      {#- marks the refresh in flight and commits that on its own - taking any
          transaction: true pre-hook with it, so build cannot deliver rollback
          here (docs/transaction_scope.md) -#}
      {% do sqlserver__mark_full_refresh_incomplete(existing_relation) %}
    {% endif %}
    {% if stage_before_hooks %}
      {#- The stage committed before the hooks. The load joins a pre-hook's
          transaction if one is open, else autocommits; X table lock either
          way. The tmp view is dropped on the tail, after the commit. -#}
      {%- set load_sql = sqlserver__get_create_table_load_sql(False, intermediate_relation, sql, drop_tmp_view=False) -%}
      {% call statement("main", auto_begin=False) %}
          {{ load_sql }}
      {% endcall %}
      {#- statement() writes target/run/ for 'main' only; put the CREATE back. -#}
      {% do write(stage_sql ~ '\n' ~ load_sql) %}
    {% else %}
      {#- build: create and load inside the pre-hook's transaction, Sch-M held
          for the whole load (#819). Chosen explicitly. -#}
      {%- set stage_sql = sqlserver__get_create_table_stage_sql(False, intermediate_relation, sql) -%}
      {%- set load_sql = sqlserver__get_create_table_load_sql(False, intermediate_relation, sql) -%}
      {% call statement("main") %}
          {{ stage_sql }}
          {{ load_sql }}
      {% endcall %}
    {% endif %}
    {#- the swap and the tail need a transaction; with no pre-hook one,
        nothing above left one open -#}
    {% do adapter.begin_if_closed() %}

    {#- nothing to back up on a first build (sp_rename on a missing name is
        Msg 15225); only queue a backup for dropping when one was made -#}
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

    {#- Only the temp load runs here; its create was staged before the hooks.
        It joins a pre-hook's transaction (X lock on the temp table) or
        autocommits, so its catalog locks never wait for the strategy DML's
        commit - held that long they deadlocked a second worker. The strategy
        DML below is transactional through to adapter.commit(). -#}
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
    {#- Fresh table: masks before create_indexes (a mask cannot be added to
        an index key column), and inside the transaction - the table carries
        no masks yet, so a failure after the commit would leave it live and
        exposed. -#}
    {% do apply_masks(target_relation, mask_config) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {#- Atomic unit ends here, as in table.sql: in-tx pre-hooks, load or
      strategy DML, cutover, fresh-table masks, in-tx post-hooks. Index work,
      grants, denies and persist_docs run outside, so sp_rename's Sch-M on
      the live target does not span the index builds (#819). A post-hook
      that needs the indexes: transaction: false. -#}
  {% do adapter.commit_if_open() %}

  {#- tmp view, dropped by name now that nothing is open (an uncommitted
      DROP VIEW blocks catalog scans like an uncommitted CREATE) -#}
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

  {#- an ONLINE/RESUMABLE index build leaves a transaction open; close it so
      grants and persist_docs do not run inside one held to commit -#}
  {% do adapter.commit_if_open() %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {#-- Re-apply object-level DENYs after grants (covers the append and
       full-refresh paths alike). --#}
  {% set deny_config = adapter.resolve_denies(model, config.get('denies')) %}
  {% do apply_denies(target_relation, deny_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {#- adapter.commit() raises with nothing open, and apply_grants only opens
      one when grants are configured; state the precondition -#}
  {% do adapter.begin_if_closed() %}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
