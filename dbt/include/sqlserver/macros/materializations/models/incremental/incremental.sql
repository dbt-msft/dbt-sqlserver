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

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}

  {% if existing_relation is none %}
    {% if config.get('full_refresh_build', 'heap_then_index') == 'prebuilt' %}
      {#- first build: load straight into the clustered design -#}
      {% set build_sql = sqlserver__create_table_as_prebuilt(target_relation, sql) %}
      {% set prebuilt_cache_add = true %}
    {% else %}
      {% set build_sql = get_create_table_as_sql(False, target_relation, sql) %}
    {% endif %}
  {% elif full_refresh_mode %}
    {#- the target is marked as having a full refresh in flight (blocking
        normal runs until one completes), but only AFTER anything that can
        fail on config alone - a pure config error must not mark a healthy
        table -#}
    {% if config.get('full_refresh_build', 'heap_then_index') == 'prebuilt' and should_full_refresh() %}
      {#- in-place full refresh: drop the existing table, rebuild the target
          directly with no intermediate or swap (explicit --full-refresh
          only; view->table conversions keep the default path) -#}
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
      {% set build_sql = sqlserver__create_table_as_prebuilt(target_relation, sql) %}
      {% set prebuilt_cache_add = true %}
    {% else %}
      {% set build_sql = get_create_table_as_sql(False, intermediate_relation, sql) %}
      {% if existing_relation.type == 'table' %}
        {% do sqlserver__mark_full_refresh_incomplete(existing_relation) %}
      {% endif %}
      {% set need_swap = true %}
    {% endif %}
  {% else %}

    {#- refuse to append onto a table whose last full refresh never
        completed -#}
    {% if existing_relation.type == 'table' %}
      {% do sqlserver__assert_no_incomplete_full_refresh(existing_relation) %}
    {% endif %}

    {% do run_query(get_create_table_as_sql(True, temp_relation, sql)) %}

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

    {% do to_drop.append(temp_relation) %}
  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

  {% if need_swap %}
      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% do adapter.rename_relation(intermediate_relation, target_relation) %}
      {% do to_drop.append(backup_relation) %}
  {% endif %}

  {% if prebuilt_cache_add %}
      {#- the prebuilt path lands the table via raw SQL, not a cache-maintaining
          adapter method (rename_relation/drop_relation), so register it here to
          keep dbt's relation cache in sync with the database. On the
          full-refresh branch this also re-adds the target that drop_relation
          removed from the cache. -#}
      {% do adapter.cache_added(target_relation) %}
  {% endif %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% else %}
    {# Table persisted across this run: converge its indexes on the config. #}
    {% do sqlserver__reconcile_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
