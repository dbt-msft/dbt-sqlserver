{% materialization table, adapter='sqlserver' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') %}
  {%- set intermediate_relation =  make_intermediate_relation(target_relation) -%}
  -- the intermediate_relation should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  /*
      See ../view/view.sql for more information about this relation.
  */
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  -- as above, the backup_relation should not already exist
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  -- grab current tables grants config for comparison later on
  {% set grant_config = config.get('grants') %}

  {%- set table_refresh_method = config.get('table_refresh_method', 'rename') -%}
  {%- if table_refresh_method not in ['rename', 'dml'] -%}
    {{ exceptions.raise_compiler_error(
      "Invalid table_refresh_method '" ~ table_refresh_method ~ "'. "
      "Valid values are: 'rename' (default), 'dml'."
    ) }}
  {%- endif -%}
  {%- set full_refresh_build = config.get('full_refresh_build', 'heap_then_index') -%}
  {#- prebuilt owns the rebuild boundaries (--full-refresh, first build);
      table_refresh_method governs the steady-state refreshes in between -#}
  {%- set use_prebuilt = (
    full_refresh_build == 'prebuilt'
    and (should_full_refresh() or existing_relation is none)
  ) -%}
  {%- set use_dml_refresh = (
    table_refresh_method == 'dml'
    and not use_prebuilt
    and existing_relation is not none
    and existing_relation.type == 'table'
  ) -%}

  {#- load: stage before the in-tx pre-hooks; build: stage after them. See
      sqlserver__pre_hook_transaction_scope for the two flows. Inert on
      prebuilt, whose setup must follow the hooks (a hook may read {{ this }}
      before the rebuild drops it). -#}
  {%- set pre_hook_transaction_scope = sqlserver__pre_hook_transaction_scope() -%}
  {%- set stage_before_hooks = pre_hook_transaction_scope == 'load' and not use_prebuilt -%}
  {%- set tmp_vw_relation = intermediate_relation.incorporate(
      path={"identifier": intermediate_relation.identifier ~ '__dbt_tmp_vw'}, type='view'
  ) -%}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {#- Stage now: nothing is open (outside-tx hooks autocommit, the contract
      probe never begins), so with auto_begin=False each statement autocommits
      and the new object's Sch-M ends with its statement (#819). -#}
  {% if stage_before_hooks %}
    {% if use_dml_refresh %}
      {% set dml_stage = sqlserver__table_dml_refresh_stage(target_relation, sql) %}
    {% else %}
      {%- set stage_sql = sqlserver__get_create_table_stage_sql(False, intermediate_relation, sql) -%}
      {% call statement('create_table_stage', auto_begin=False) -%}
        {{ stage_sql }}
      {%- endcall %}
    {% endif %}
  {% endif %}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {#- Resolved once: the rename and prebuilt paths apply masks inside the
      cutover transaction, the dml path reconciles them outside it. -#}
  {% set mask_config = adapter.resolve_masks(model, config.get('masks')) %}
  {#- 'create' = fresh table, mask-then-create-index. 'reconcile' = the table
      persisted, so indexes converge on config first and masks follow (an index
      drop has to land before a column it covers can be masked). -#}
  {% set index_strategy = 'create' %}

  {% if use_dml_refresh %}
    {% if not stage_before_hooks %}
      {% set dml_stage = sqlserver__table_dml_refresh_stage(target_relation, sql) %}
    {% endif %}
    {#- Leaves the swap's transaction open for the tail to close after the
        post-hooks; returns schema_match (picks the tail's index strategy) and
        the scratch table for the tail to drop after the commit. -#}
    {% set dml_result = sqlserver__table_dml_refresh(target_relation, sql, dml_stage) %}
    {% set index_strategy = 'reconcile' if dml_result['schema_match'] else 'create' %}
  {% elif use_prebuilt %}
    {#- in-place rebuild: drop the existing table, then build the target
        directly with no intermediate or swap -#}
    {% if existing_relation is not none %}
      {% do sqlserver__assert_no_unguarded_self_reference(target_relation, sql) %}
    {% endif %}
    {#- validate the index config BEFORE dropping anything -#}
    {% do adapter.validate_indexes(
        config.get('indexes', default=[]),
        config.get('as_columnstore', default=true),
        config.get('drop_unmanaged_indexes', default=false)
    ) %}
    {% if existing_relation is not none %}
      {% set existing_relation = load_cached_relation(existing_relation) %}
      {% if existing_relation is not none %}
        {{ adapter.drop_relation(existing_relation) }}
      {% endif %}
    {% endif %}

    {#- create_table_as_prebuilt issues its own statement() calls (including
        'main') rather than being wrapped in one here, so it can commit its
        in-progress marker independently of the load that follows - see the
        macro for why. -#}
    {% do sqlserver__create_table_as_prebuilt(target_relation, sql) %}

    {#- the prebuilt path lands the table via raw SQL, not a cache-maintaining
        adapter method (rename_relation), and above it may have dropped the
        existing relation from the cache; register the rebuilt target so dbt's
        relation cache stays in sync with the database -#}
    {% do adapter.cache_added(target_relation) %}

    {#- Masks before create_indexes (a mask cannot be added to an index key
         column). prebuilt already built its clustered design: a CCI exposes
         no key columns, but a mask on a clustered rowstore key column fails
         here with a descriptive error (recovery: heap_then_index). -#}
    {% do apply_masks(target_relation, mask_config) %}
  {% else %}
    -- build model
    {% if stage_before_hooks %}
      {#- The stage committed before the hooks. The load joins a pre-hook's
          transaction if one is open, else autocommits; X table lock either
          way. The tmp view is dropped on the tail, after the commit. -#}
      {%- set load_sql = sqlserver__get_create_table_load_sql(False, intermediate_relation, sql, drop_tmp_view=False) -%}
      {% call statement('main', auto_begin=False) -%}
        {{ load_sql }}
      {%- endcall %}
      {#- statement() writes target/run/ for 'main' only; put the CREATE back. -#}
      {% do write(stage_sql ~ '\n' ~ load_sql) %}
    {% else %}
      {#- build: create and load inside the pre-hook's transaction, Sch-M held
          for the whole load (#819). Chosen explicitly. -#}
      {%- set stage_sql = sqlserver__get_create_table_stage_sql(False, intermediate_relation, sql) -%}
      {%- set load_sql = sqlserver__get_create_table_load_sql(False, intermediate_relation, sql) -%}
      {% call statement('main') -%}
        {{ stage_sql }}
        {{ load_sql }}
      {%- endcall %}
    {% endif %}
    {#- the renames and the tail need a transaction; with no pre-hook one,
        nothing above left one open -#}
    {% do adapter.begin_if_closed() %}

    -- cleanup
    {% if existing_relation is not none %}
       /* Do the equivalent of rename_if_exists. 'existing_relation' could have been dropped
          since the variable was first set. */
      {% set existing_relation = load_cached_relation(existing_relation) %}
      {% if existing_relation is not none %}
          {{ adapter.rename_relation(existing_relation, backup_relation) }}
      {% endif %}
    {% endif %}

    {{ adapter.rename_relation(intermediate_relation, target_relation) }}

    {#- Masks before create_indexes (a mask cannot be added to an index key
         column; a CCI exposes none), and INSIDE the cutover transaction: this
         table carries no masks yet, so a failure after the commit would leave
         it live and exposed. Rolling the swap back keeps the old, masked table
         serving. -#}
    {% do apply_masks(target_relation, mask_config) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {#- Atomic unit ends here: in-tx pre-hooks, load, cutover, fresh-table
      masks, in-tx post-hooks - what a transaction: true hook asks to be
      atomic with. The rest is adapter housekeeping and runs outside, so
      sp_rename's Sch-M on the live target does not span the index builds
      (#819). A post-hook that needs the indexes: transaction: false. -#}
  {% do adapter.commit_if_open() %}

  {#- Tmp views, dropped by name now that nothing is open. build scope
      dropped its own in the fused batch; the dml fallback rebuild dropped
      its own too; IF EXISTS covers both. -#}
  {% if use_dml_refresh %}
    {% call statement('dml_refresh_drop_view', auto_begin=False) -%}
      DROP VIEW IF EXISTS {{ dml_stage['tmp_vw_relation'].include(database=False) }};
    {%- endcall %}
  {% elif stage_before_hooks %}
    {% call statement('drop_tmp_view', auto_begin=False) -%}
      DROP VIEW IF EXISTS {{ tmp_vw_relation.include(database=False) }};
    {%- endcall %}
  {% endif %}

  {#- Index work outside the cutover transaction. reconcile (dml swap: the
       table persisted): indexes converge on config first so a drop lands
       before apply_masks re-masks a column it covered; previous masks stay in
       place on failure. create (fresh table): masks were applied inside the
       transaction above. -#}
  {% if index_strategy == 'reconcile' %}
    {% do sqlserver__reconcile_indexes(target_relation) %}
    {% do apply_masks(target_relation, mask_config) %}
  {% else %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {#- sqlserver__create_indexes_no_txn ends with begin_if_closed, so an
      ONLINE/RESUMABLE index leaves a transaction open here. Close it, or the
      grants and persist_docs below would run inside one held to the commit -
      putting back part of the window this tail exists to remove. -#}
  {% do adapter.commit_if_open() %}

  {#- dml scratch table, dropped after the commit so its catalog locks end
      with the statement. -#}
  {% if use_dml_refresh and dml_result['refresh_relation'] is not none %}
    {% call statement('dml_refresh_cleanup_post', auto_begin=False) -%}
      DROP TABLE IF EXISTS {{ dml_result['refresh_relation'] }};
    {%- endcall %}
  {% endif %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {#-- Re-apply object-level DENYs after grants, so the final permission state is
       unambiguous. Runs on the common tail, so it covers the rename, prebuilt and
       DML-refresh build paths alike. --#}
  {% set deny_config = adapter.resolve_denies(model, config.get('denies')) %}
  {% do apply_denies(target_relation, deny_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {#- apply_grants opens a transaction of its own (dbt's call_dcl_statements
      uses the default auto_begin), so one is usually open by now - but not
      when a model configures no grants. adapter.commit() raises if it finds
      nothing open, so state the precondition rather than relying on that. -#}
  {% do adapter.begin_if_closed() %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {% if not use_dml_refresh %}
    -- finally, drop the existing/backup relation after the commit
    {{ drop_relation_if_exists(backup_relation) }}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
