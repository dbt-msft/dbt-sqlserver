{%- materialization view, adapter='sqlserver' -%}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='view') -%}
  {%- set intermediate_relation =  make_intermediate_relation(target_relation) -%}

  -- the intermediate_relation should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  /*
     This relation (probably) doesn't exist yet. If it does exist, it's a leftover from
     a previous run, and we're going to try to drop it immediately. At the end of this
     materialization, we're going to rename the "existing_relation" to this identifier,
     and then we're going to drop it. In order to make sure we run the correct one of:
       - drop view ...
       - drop table ...

     We need to set the type of this relation to be the type of the existing_relation, if it exists,
     or else "view" as a sane default if it does not. Note that if the existing_relation does not
     exist, then there is nothing to move out of the way and subsequentally drop. In that case,
     this relation will be effectively unused.
  */
  {%- set backup_relation_type = 'view' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  -- as above, the backup_relation should not already exist
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
  -- grab current tables grants config for comparison later on
  {% set grant_config = config.get('grants') %}
  {% set preserved_grants = {} %}
  {% set should_skip_view_update = false %}
  {% set build_sql = none %}

  {% if existing_relation is not none and existing_relation.type != 'view' %}
    {% set current_grants_table = run_query(get_show_grant_sql(existing_relation)) %}
    {% set current_grants_dict = adapter.standardize_grants_dict(current_grants_table) %}
    {% set preserved_grants = {} %}
    {% for privilege, grantees in diff_of_two_dicts(current_grants_dict, grant_config).items() %}
      {% if privilege | lower in ['select', 'insert', 'update', 'delete'] %}
        {% do preserved_grants.update({privilege: grantees}) %}
      {% endif %}
    {% endfor %}
    {% set build_sql = get_create_view_as_sql(intermediate_relation, sql) %}
  {% elif existing_relation is not none and existing_relation.type == 'view' %}
    {% set current_view_definition_table = run_query(get_view_definition_sql(existing_relation)) %}
    {% if current_view_definition_table is not none and current_view_definition_table.rows | length > 0 %}
      {#- Compare the view *body* exactly, not by suffix. The stored definition is
          the whole statement (CREATE [OR ALTER] VIEW <name> AS <body>); the model is
          only the body. The header ends at the separating ' AS ' - split there and
          compare the remainder verbatim. A suffix test (endswith) would wrongly skip
          any edit whose new body is a tail of the old one, e.g. deleting a leading
          comment or CTE - it lands as PASS but never reaches the database, and
          --full-refresh does not fix it. Do NOT lowercase or strip whitespace: both
          make genuinely different bodies compare equal (a string literal differing
          only in case, or any literal containing spaces). The asymmetry is deliberate - a skip that
          fails to fire costs one rebuild; a skip that fires wrongly costs correctness -
          so when we cannot be certain, we rebuild. -#}
      {% set stored = current_view_definition_table.rows[0][0] %}
      {#- First ' as ' is the header/body separator: CREATE [OR ALTER] VIEW <quoted
          relation> AS has no other, the relation being quoted. -#}
      {% set marker = (stored | lower).find(' as ') %}
      {% if marker < 0 %}
        {% set should_skip_view_update = false %}
      {% else %}
        {% set stored_body = stored[marker + 4:] | replace('\r\n', '\n') | trim %}
        {% set stored_body = (stored_body[:-1] if stored_body.endswith(';') else stored_body) | trim %}
        {% set model_body = sql | replace('\r\n', '\n') | trim %}
        {% set model_body = (model_body[:-1] if model_body.endswith(';') else model_body) | trim %}
        {% set should_skip_view_update = stored_body == model_body %}
      {% endif %}
    {% endif %}
    {% if should_skip_view_update %}
      {#- The view's SQL text is unchanged, so the CREATE/ALTER is skipped -
          but a *referenced* table can still have changed shape (columns
          added, dropped, or reordered) since this view was last built. SQL
          Server resolves an unqualified `select *` at CREATE/ALTER time and
          caches the result; skipping that statement here means the cached
          column list silently goes stale relative to the underlying table,
          even though this view's own definition never changed. sp_refreshview
          re-derives that cached metadata from the table's current shape
          without re-running the CREATE, so a skip stays a skip (no DDL, no
          grant/deny churn) while the view keeps reporting the right columns. -#}
      {#- sp_refreshview resolves its argument in the *current* database, so this needs
          the same USE prefix every other name-resolving statement here carries -
          without it a cross-database view model would refresh nothing and error. -#}
      {% set object_name = "quotename('" ~ target_relation.schema ~ "') + '.' + quotename('" ~ target_relation.identifier ~ "')" %}
      {% set build_sql = get_use_database_sql(target_relation.database) ~ " declare @dbt_sqlserver_refresh_target nvarchar(max) = " ~ object_name ~ "; exec sp_refreshview @dbt_sqlserver_refresh_target;" %}
    {% else %}
      {% set build_sql = get_create_view_as_sql(target_relation, sql) %}
    {% endif %}
  {% else %}
    {% set build_sql = get_create_view_as_sql(target_relation, sql) %}
  {% endif %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_relation is not none and existing_relation.type != 'view' %}
    -- build model
    {% call statement('main') -%}
      {{ build_sql }}
    {%- endcall %}

    -- cleanup
    -- move the existing relation out of the way
    {% set existing_relation = load_cached_relation(existing_relation) %}
    {% if existing_relation is not none %}
        {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {% endif %}

    {{ adapter.rename_relation(intermediate_relation, target_relation) }}
  {% else %}
    -- build model
    {% call statement('main') -%}
      {{ build_sql }}
    {%- endcall %}
  {% endif %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% if preserved_grants %}
    {% do apply_grants(target_relation, preserved_grants, should_revoke=False) %}
  {% endif %}

  {#-- Re-apply object-level DENYs after grants. A view is a valid securable and
       is recreated on every run, so this is where an object-level DENY is lost
       most often — unlike apply_masks, which is absent here (DDM attaches to
       base-table columns only). --#}
  {% set deny_config = adapter.resolve_denies(model, config.get('denies')) %}
  {% do apply_denies(target_relation, deny_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}
