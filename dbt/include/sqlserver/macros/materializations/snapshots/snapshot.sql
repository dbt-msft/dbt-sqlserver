{% materialization snapshot, adapter='sqlserver' %}

  {%- set config = model['config'] -%}
  {%- set target_table = model.get('alias', model.get('name')) -%}
  {%- set strategy_name = config.get('strategy') -%}
  {%- set unique_key = config.get('unique_key') %}
  -- grab current tables grants config for comparison later on
  {%- set grant_config = config.get('grants') -%}

  {% set target_relation_exists, target_relation = get_or_create_relation(
          database=model.database,
          schema=model.schema,
          identifier=target_table,
          type='table') -%}

  {%- if not target_relation.is_table -%}
    {% do exceptions.relation_wrong_type(target_relation, 'table') %}
  {%- endif -%}

  {#- load: stage before the in-tx pre-hooks; build: stage after them. See
      sqlserver__pre_hook_transaction_scope for the two flows. -#}
  {%- set pre_hook_transaction_scope = sqlserver__pre_hook_transaction_scope() -%}
  {%- set stage_before_hooks = pre_hook_transaction_scope == 'load' -%}

  {% set strategy_macro = strategy_dispatch(strategy_name) %}
  {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", config, target_relation_exists) %}

  {% set temp_snapshot_relation_exists, temp_snapshot_relation = get_or_create_relation(
          database=model.database,
          schema=model.schema,
          identifier=target_table+"_snapshot_staging_temp_view",
          type='view') -%}
  -- A view over the user SQL, so a query that opens with a CTE can be read from
  {% set temp_snapshot_relation_sql = model['compiled_code'] %}

  {#- First build: through the __dbt_tmp intermediate, renamed into place,
      as table does - the create and the load commit independently, so
      building straight into the target would leave an EMPTY snapshot table
      under the real name after a failed load, for the next run to merge
      into. Later runs: build the __dbt_temp staging table and merge it. -#}
  {% if not target_relation_exists %}
    {% set build_relation = make_intermediate_relation(target_relation) %}
    {% set build_is_temporary = false %}
    {{ drop_relation_if_exists(load_cached_relation(build_relation)) }}
  {% else %}
    {% set columns = get_snapshot_table_column_names() %}
    {% set meta = config.get("snapshot_meta_column_names") %}
    {% if meta %}
        {% if meta.dbt_valid_from %}{% do columns.update({"dbt_valid_from": meta.dbt_valid_from}) %}{% endif %}
        {% if meta.dbt_valid_to %}{% do columns.update({"dbt_valid_to": meta.dbt_valid_to}) %}{% endif %}
        {% if meta.dbt_scd_id %}{% do columns.update({"dbt_scd_id": meta.dbt_scd_id}) %}{% endif %}
        {% if meta.dbt_updated_at %}{% do columns.update({"dbt_updated_at": meta.dbt_updated_at}) %}{% endif %}
        {% if meta.dbt_is_deleted %}{% do columns.update({"dbt_is_deleted": meta.dbt_is_deleted}) %}{% endif %}
    {% endif %}
    {{ adapter.valid_snapshot_target(target_relation, columns) }}
    {% set staging_table = make_temp_relation(target_relation) %}
    {% set build_relation = staging_table %}
    {% set build_is_temporary = true %}
    {{ adapter.drop_relation(staging_table) }}
  {% endif %}
  {%- set tmp_vw_relation = build_relation.incorporate(
      path={"identifier": build_relation.identifier ~ '__dbt_tmp_vw'}, type='view'
  ) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {#- Stage now (sqlserver__snapshot_stage): nothing is open, so each
      statement autocommits and the new object's Sch-M ends with its
      statement (#819). -#}
  {% if stage_before_hooks %}
    {% set stage = sqlserver__snapshot_stage(
        strategy, temp_snapshot_relation, temp_snapshot_relation_sql,
        target_relation, target_relation_exists,
        build_relation, build_is_temporary, auto_begin=False) %}
  {% endif %}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if not stage_before_hooks %}
    {#- build: the same statements inside the pre-hook's transaction, Sch-M
        held for the load (#819). Chosen explicitly. -#}
    {% set stage = sqlserver__snapshot_stage(
        strategy, temp_snapshot_relation, temp_snapshot_relation_sql,
        target_relation, target_relation_exists,
        build_relation, build_is_temporary, auto_begin=True) %}
  {% endif %}
  {% set build_sql = stage['build_sql'] %}
  {% set stage_sql = stage['stage_sql'] %}

  {{ check_time_data_types(build_sql) }}

  {#- The load joins a pre-hook's transaction if one is open, else
      autocommits; X table lock either way. Tmp views are dropped on the
      tail, after the commit. -#}
  {%- set load_sql = sqlserver__get_create_table_load_sql(build_is_temporary, build_relation, build_sql, drop_tmp_view=False) -%}

  {% if not target_relation_exists %}
    {% call statement('main', auto_begin=False) -%}
      {{ load_sql }}
    {%- endcall %}
    {#- statement() writes target/run/ for 'main' only; put the CREATE back -#}
    {% do write(stage_sql ~ '\n' ~ load_sql) %}
    {#- the rename and the tail need a transaction; with no pre-hook one,
        nothing above left one open -#}
    {% do adapter.begin_if_closed() %}
    {% do adapter.rename_relation(build_relation, target_relation) %}
  {% else %}
    {% do run_query(load_sql) %}

    -- this may no-op if the database does not require column expansion
    {% set expansion_max_rows = config.get('column_type_expansion_max_rows', 1000000) %}
    {% do adapter.expand_target_column_types(from_relation=staging_table,
                                             to_relation=target_relation,
                                             max_rows=expansion_max_rows) %}

    {% set remove_columns = ['dbt_change_type', 'DBT_CHANGE_TYPE', 'dbt_unique_key', 'DBT_UNIQUE_KEY'] %}
    {% if unique_key | is_list %}
        {% for key in strategy.unique_key %}
            {{ remove_columns.append('dbt_unique_key_' + loop.index|string) }}
            {{ remove_columns.append('DBT_UNIQUE_KEY_' + loop.index|string) }}
        {% endfor %}
    {% endif %}
    {% set missing_columns = adapter.get_missing_columns(staging_table, target_relation)
                                 | rejectattr('name', 'in', remove_columns)
                                 | list %}
    {% if missing_columns|length > 0 %}
      {{log("Missing columns length is: "~ missing_columns|length)}}
      {% do create_columns(target_relation, missing_columns) %}
    {% endif %}
    {% set source_columns = adapter.get_columns_in_relation(staging_table)
                                 | rejectattr('name', 'in', remove_columns)
                                 | list %}
    {% set quoted_source_columns = [] %}
    {% for column in source_columns %}
      {% do quoted_source_columns.append(adapter.quote(column.name)) %}
    {% endfor %}
    {% set final_sql = snapshot_merge_sql(
          target = target_relation,
          source = staging_table,
          insert_cols = quoted_source_columns
       )
    %}
    {% call statement('main') %}
        {{ final_sql }}
    {% endcall %}
  {% endif %}

  {% set mask_config = adapter.resolve_masks(model, config.get('masks')) %}
  {% if not target_relation_exists %}
    {#- Fresh table: masks before create_indexes (a mask cannot be added to
        an index key column), and inside the transaction - the table carries
        no masks yet, so a failure after the commit would leave it live and
        exposed. -#}
    {% do apply_masks(target_relation, mask_config) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {#- Atomic unit ends here, as in table and incremental: in-tx pre-hooks,
      load or merge, cutover, fresh-table masks, in-tx post-hooks. Index
      work, grants, denies and persist_docs run outside (#819). -#}
  {% do adapter.commit_if_open() %}

  {{ adapter.drop_relation(temp_snapshot_relation) }}
  {% call statement('drop_tmp_view', auto_begin=False) -%}
    DROP VIEW IF EXISTS {{ tmp_vw_relation.include(database=False) }};
  {%- endcall %}

  {% if not target_relation_exists %}
    {% do create_indexes(target_relation) %}
  {% else %}
    {# Snapshot table persisted: converge its indexes on the config, then
       reconcile masks (index drops land first). #}
    {% do sqlserver__reconcile_indexes(target_relation) %}
    {% do apply_masks(target_relation, mask_config) %}
  {% endif %}

  {#- an ONLINE/RESUMABLE index build leaves a transaction open; close it so
      grants and persist_docs do not run inside one held to commit -#}
  {% do adapter.commit_if_open() %}

  {% set should_revoke = should_revoke(target_relation_exists, full_refresh_mode=False) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {#-- Re-apply object-level DENYs after grants. --#}
  {% set deny_config = adapter.resolve_denies(model, config.get('denies')) %}
  {% do apply_denies(target_relation, deny_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% do adapter.begin_if_closed() %}
  {{ adapter.commit() }}

  {% if staging_table is defined %}
      {% do post_snapshot(staging_table) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
