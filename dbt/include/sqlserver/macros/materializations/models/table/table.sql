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

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {#- Decide the build's transaction scope HERE, before any branch code runs.
      The question is only ever "did the pre-hooks leave a transaction open?",
      and it has to be asked now: macros further down open one of their own
      (sqlserver__mark_full_refresh_incomplete ends with begin_if_closed, and
      always leaves one open), so a later sample would answer yes for reasons
      that have nothing to do with a pre-hook - silently selecting the
      transaction-spanning path for models that never asked for it.

      Not derived from the pre_hooks config: run_hooks skips a hook whose
      rendered SQL is empty, so the common {% if target.name == 'prod' %}
      idiom declares a transactional pre-hook that opens nothing. Ask the
      connection instead - see adapter.transaction_is_open. -#}
  {%- set pre_hook_transaction_scope = config.get('pre_hook_transaction_scope') -%}
  {%- if pre_hook_transaction_scope is none -%}
    {%- set pre_hook_transaction_scope = (
      'schema' if adapter.behavior.dbt_sqlserver_pre_hook_schema_scope else 'build'
    ) -%}
  {%- endif -%}
  {%- if pre_hook_transaction_scope not in ['schema', 'build'] -%}
    {{ exceptions.raise_compiler_error(
      "Invalid pre_hook_transaction_scope '" ~ pre_hook_transaction_scope ~ "'. "
      "Valid values are: 'schema', 'build'."
    ) }}
  {%- endif -%}
  {#- 'build' only means anything when a pre-hook actually opened a transaction;
      with none open the build always takes the narrow path, so a model without
      transactional pre-hooks gets the #819 fix whatever the flag says. -#}
  {%- set keep_pre_hook_txn = (
    adapter.transaction_is_open() and pre_hook_transaction_scope == 'build'
  ) -%}

  {#- Resolved once: the rename and prebuilt paths apply masks inside the
      cutover transaction, the dml path reconciles them outside it. -#}
  {% set mask_config = adapter.resolve_masks(model, config.get('masks')) %}
  {#- 'create' = fresh table, mask-then-create-index. 'reconcile' = the table
      persisted, so indexes converge on config first and masks follow (an index
      drop has to land before a column it covers can be masked). -#}
  {% set index_strategy = 'create' %}

  {% if use_dml_refresh %}
    {#- The macro leaves the swap's transaction open for the tail to close
        after the post-hooks, and reports back what only it can know: whether
        the schemas matched (which decides the tail's index strategy) and the
        scratch table to drop once the cutover has committed. -#}
    {% set dml_result = sqlserver__table_dml_refresh(target_relation, sql) %}
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

    {#-- Apply masks after the load but before create_indexes, mirroring the
         standard build path so masks on nonclustered-index key columns land
         before those indexes exist (mask-then-index). prebuilt builds the
         clustered design inside create_table_as_prebuilt before we get here:
         a CCI exposes no key columns so masks apply freely, but a mask on a
         clustered *rowstore* key column cannot be added after the fact and
         apply_masks raises a descriptive index-key error (recovery: switch
         that model to the default heap_then_index). --#}
    {% do apply_masks(target_relation, mask_config) %}
  {% else %}
    -- build model
    {%- set stage_sql = sqlserver__get_create_table_stage_sql(False, intermediate_relation, sql) -%}
    {%- set load_sql = sqlserver__get_create_table_load_sql(False, intermediate_relation, sql) -%}
    {% if keep_pre_hook_txn %}
      {#- The pre-hook's transaction spans the whole build, so its writes roll
          back with a failed load - at the cost of holding the new table's
          Sch-M for the length of that load (#819). Chosen explicitly via
          pre_hook_transaction_scope='build'. -#}
      {% call statement('main') -%}
        {{ stage_sql }}
        {{ load_sql }}
      {%- endcall %}
    {% else %}
      {#- Create, commit, then load. auto_begin=False declines to OPEN a
          transaction but still joins one a pre-hook left open, so the create
          sees those writes; committing straight after releases its Sch-M
          before the load starts. The load holds an X table lock, never Sch-M,
          so it cannot block the metadata readers #819 is about. -#}
      {% call statement('create_table_stage', auto_begin=False) -%}
        {{ stage_sql }}
      {%- endcall %}
      {% do adapter.commit_if_open() %}
      {% call statement('main', auto_begin=False) -%}
        {{ load_sql }}
      {%- endcall %}
      {#- statement() writes the compiled artifact for 'main' only, so on this
          path target/run/ would hold the load without the CREATE that precedes
          it. Write the whole build back over it. -#}
      {% do write(stage_sql ~ '\n' ~ load_sql) %}
      {#- The renames below and the tail need a transaction; nothing above
          leaves one open on this path. -#}
      {% do adapter.begin_if_closed() %}
    {% endif %}

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

    {#-- Apply data masks before create_indexes: a mask cannot be added to a
         column an index depends on (documented for all SQL Server versions;
         the fix is to mask first, then create the index — exactly this order),
         so masking must happen while the (rowstore) indexes do not yet exist.
         The clustered columnstore index built during CTAS is fine — columnstore
         columns are reported as included, not index keys, and can be masked.

         Masks stay INSIDE the cutover transaction, unlike the index builds
         that follow it. This table is brand new and carries no masks yet, so a
         mask failure after the swap committed would leave it live with the
         columns exposed. Rolling the swap back instead keeps the old, masked
         table serving. The ALTERs are cheap, so holding the transaction across
         them costs almost nothing next to the index builds. --#}
    {% do apply_masks(target_relation, mask_config) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {#- The atomic unit ends here: in-transaction pre-hooks, the cutover, the
      masks that must not fail open, and in-transaction post-hooks. That is
      what a hook declaring transaction: true is asking to be atomic with -
      the model. What follows is the adapter's own reconciliation, which was
      never part of that promise, and holding sp_rename's Sch-M on the LIVE
      target across the index builds below is the larger half of #819.

      A post-hook that needs the indexes present should declare
      transaction: false; that slot runs after this whole tail. -#}
  {% do adapter.commit_if_open() %}

  {#-- Index reconciliation, outside the cutover transaction. 'reconcile' is
       the persisted-table path (dml swap), where indexes converge on config
       first so an index drop lands before apply_masks re-masks a column it
       covered; the table already carries its previous masks, so a failure
       leaves those in place rather than exposing anything. 'create' is the
       fresh-table path, whose masks were applied inside the transaction
       above. --#}
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

  {#- Drop the dml path's scratch table now the cutover has committed. Outside
      a transaction, so its catalog locks go the moment the drop finishes. -#}
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
