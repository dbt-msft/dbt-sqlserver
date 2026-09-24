{#
    SQL Server has no `CREATE SCHEMA IF NOT EXISTS`, and check-then-act races under
    concurrency (Msg 2714), so an applock serializes the check and the create.
    The lock is session-scoped: a transaction-scoped one would be held until dbt
    commits, after the test query in `get_test_sql`. `XACT_ABORT` skips a trailing
    release when the create fails, so the `CATCH` releases and rethrows. A timed-out
    request falls through to the bare check.
#}
{% macro create_schema_if_not_exists(schema, authorization=none) -%}
  {%- set lock_resource = 'dbt_create_schema_' ~ schema -%}
  DECLARE @dbt_schema_lock int;
  EXEC @dbt_schema_lock = sp_getapplock
    @Resource = '{{ escape_single_quotes(lock_resource) }}',
    @LockMode = 'Exclusive',
    @LockOwner = 'Session',
    @LockTimeout = 30000;
  BEGIN TRY
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{{ escape_single_quotes(schema) }}')
    BEGIN
      EXEC('CREATE SCHEMA {{ escape_single_quotes(adapter.quote(schema)) }}{% if authorization is not none %} AUTHORIZATION {{ escape_single_quotes(adapter.quote(authorization)) }}{% endif %}')
    END
  END TRY
  BEGIN CATCH
    IF @dbt_schema_lock >= 0
      EXEC sp_releaseapplock @Resource = '{{ escape_single_quotes(lock_resource) }}', @LockOwner = 'Session';
    THROW;
  END CATCH
  IF @dbt_schema_lock >= 0
    EXEC sp_releaseapplock @Resource = '{{ escape_single_quotes(lock_resource) }}', @LockOwner = 'Session';
{%- endmacro %}

{% macro sqlserver__create_schema(relation) -%}
  {% call statement('create_schema') -%}
    {{ get_use_database_sql(relation.database) }}
    {{ create_schema_if_not_exists(relation.schema) }}
  {% endcall %}
{% endmacro %}

{% macro sqlserver__create_schema_with_authorization(relation, schema_authorization) -%}
  {% call statement('create_schema') -%}
    {{ get_use_database_sql(relation.database) }}
    {{ create_schema_if_not_exists(relation.schema, schema_authorization) }}
  {% endcall %}
{% endmacro %}

{% macro sqlserver__drop_schema(relation) -%}
  {%- set relations_in_schema = list_relations_without_caching(relation) %}

  {% for row in relations_in_schema %}
    {%- set schema_relation = api.Relation.create(database=relation.database,
                                               schema=relation.schema,
                                               identifier=row[1],
                                               type=row[3]
                                               ) -%}
    {% do adapter.drop_relation(schema_relation) %}
  {%- endfor %}

  {% call statement('drop_schema') -%}
    {{ get_use_database_sql(relation.database) }}
    EXEC('DROP SCHEMA IF EXISTS {{ relation.schema }}')
  {% endcall %}
{% endmacro %}

{% macro sqlserver__drop_schema_named(schema_name) %}
  {% set schema_relation = api.Relation.create(schema=schema_name, database=target.database) %}
  {{ adapter.drop_schema(schema_relation) }}
{% endmacro %}

{#
    Generates a schema name for a model.

    By default, this delegates to dbt-core's `default__generate_schema_name`, which
    concatenates `target.schema` + `_` + `custom_schema_name`.

    When the `dbt_sqlserver_use_default_schema_concat` flag is disabled, the legacy
    adapter behaviour is used instead:
      - `target.schema`              when no custom schema is set
      - `custom_schema_name` (trim)  when a custom schema is set

    The legacy behaviour is deprecated and this flag will be removed in a future
    release. To opt back into it in the meantime, set the flag in `dbt_project.yml`:
      flags:
        dbt_sqlserver_use_default_schema_concat: false

#}
{% macro sqlserver__generate_schema_name(custom_schema_name, node) -%}
    {%- if adapter.behavior.dbt_sqlserver_use_default_schema_concat -%}
        {{ default__generate_schema_name(custom_schema_name, node) }}
    {%- elif var('dbt_sqlserver_use_default_schema_concat', false) -%}
        {{ exceptions.warn(
            "DEPRECATED: Using `vars.dbt_sqlserver_use_default_schema_concat` is deprecated. "
            "Use `flags.dbt_sqlserver_use_default_schema_concat` in dbt_project.yml instead. "
            "Support for the `var` fallback will be removed in a future release."
        ) }}
        {{ default__generate_schema_name(custom_schema_name, node) }}
    {%- else -%}
        {%- set default_schema = target.schema -%}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
