{% macro build_columns_constraints(relation, only_not_null=False) %}
    {{ return(adapter.dispatch('build_columns_constraints', 'dbt')(relation, only_not_null)) }}
{% endmacro %}

{% macro sqlserver__build_columns_constraints(relation, only_not_null=False) %}
  {#- The parenthesised column list for CREATE TABLE. Carries every column-level
      constraint plus the *unnamed* model-level ones: both are anonymous, so
      SQL Server names them itself and nothing collides with the table this
      build is about to replace. Named model-level constraints are applied
      afterwards by build_model_constraints. -#}
    {%- set raw_column_constraints = adapter.render_raw_columns_constraints(
        raw_columns=model['columns'], only_not_null=only_not_null) -%}
    {%- set raw_model_constraints = [] if only_not_null
        else adapter.render_raw_model_constraints(
            raw_constraints=model.get('constraints') or []) -%}
    (
      {% for c in raw_column_constraints + raw_model_constraints -%}
        {{ c }}{{ "," if not loop.last }}
      {% endfor %}
    )
{% endmacro %}

{% macro build_model_constraints(relation) %}
    {{ return(adapter.dispatch('build_model_constraints', 'dbt')(relation)) }}
{% endmacro %}

{% macro sqlserver__build_model_constraints(relation) %}
  {#- Named model-level constraints, applied once the build has swapped the new
      table into place and dropped the old one: SQL Server scopes constraint
      names per schema, so the name is only free to reuse after the table that
      held it is gone.

      Each ADD is guarded on the name already being present on this table, so
      the macro is safe to call on every build path, including the ones that
      keep the existing table (a plain incremental run, a DML refresh). That
      makes a constraint added to an existing model land on the next run rather
      than waiting for a full refresh.

      What the guard cannot see is a constraint whose *definition* changed under
      an unchanged name: unlike an index name (a hash of its definition), a
      constraint name says nothing about what the constraint does. Redefining
      one needs --full-refresh, which rebuilds the table and so applies the new
      definition to a table that carries none. This is documented in the README.

      Emitted as a single batch: one round trip regardless of how many
      constraints the model declares. -#}
  {%- set contract_config = config.get('contract') -%}
  {%- if not contract_config or not contract_config.enforced -%}
    {{ return('') }}
  {%- endif -%}

  {%- set constraints = adapter.render_raw_model_alter_constraints(
      raw_constraints=model.get('constraints') or []) -%}
  {%- if not constraints -%}
    {{ return('') }}
  {%- endif -%}

  {%- set object_id_literal = escape_single_quotes(relation.include(database=False)) -%}
  {%- set alter_sql -%}
    {{ get_use_database_sql(relation.database) }}
    {%- for constraint in constraints %}
    if not exists (
      select 1
      from sys.objects {{ information_schema_hints() }}
      where name = '{{ escape_single_quotes(constraint['name']) }}'
        and parent_object_id = OBJECT_ID('{{ object_id_literal }}')
    )
    begin
      alter table {{ relation.include(database=False) }} {{ constraint['clause'] }};
    end
    {%- endfor %}
  {%- endset %}

  {#- auto_begin=False: this runs after the materialization's adapter.commit(),
      so opening the ambient transaction here would leave one dangling. -#}
  {% call statement('alter_table_add_constraints', auto_begin=False) -%}
    {{ alter_sql }}
  {%- endcall %}
{% endmacro %}
