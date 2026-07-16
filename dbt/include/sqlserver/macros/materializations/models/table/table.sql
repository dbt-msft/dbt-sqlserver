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
  {%- set use_dml_refresh = (
    table_refresh_method == 'dml'
    and existing_relation is not none
    and existing_relation.type == 'table'
  ) -%}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if use_dml_refresh %}
    {{ sqlserver__table_dml_refresh(target_relation, sql) }}
  {% else %}
    -- build model
    {% call statement('main') -%}
      {{ get_create_table_as_sql(False, intermediate_relation, sql) }}
    {%- endcall %}

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
         columns are reported as included, not index keys, and can be masked. --#}
    {% set mask_config = adapter.resolve_masks(model, config.get('masks')) %}
    {% do apply_masks(target_relation, mask_config) %}

    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {% if not use_dml_refresh %}
    -- finally, drop the existing/backup relation after the commit
    {{ drop_relation_if_exists(backup_relation) }}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
