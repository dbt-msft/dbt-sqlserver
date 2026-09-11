{#
    Concurrency-safe `CREATE SCHEMA`, emitted as a statement fragment: the caller
    supplies its own `USE <database>` prefix, since which database the schema
    belongs to is the caller's to decide.

    `IF NOT EXISTS (...) BEGIN CREATE SCHEMA ... END` on its own is check-then-act.
    With `threads > 1` - or two runs against one database, which is what CI does
    when it builds a schema per pull request - several sessions pass the check
    together and all but one fail the create with `Msg 2714, There is already an
    object named '<schema>' in the database`. The run dies on a schema that, by
    the time the error is raised, exists and is perfectly usable.

    SQL Server has no `CREATE SCHEMA IF NOT EXISTS`, and catching the error is not
    an option here: every connection runs `SET XACT_ABORT ON` (see #718), under
    which the failed create dooms the enclosing transaction - `XACT_STATE()`
    returns -1 and the transaction is already rolled back by the time the batch
    reaches its `COMMIT`. A `TRY`/`CATCH` swallowing 2714 would therefore trade a
    clear error for a silently discarded transaction, which is worse.

    So serialize instead. A database-scoped application lock makes the check and
    the create atomic with respect to every other session on that database,
    whatever process it belongs to, and is held only for the microseconds the
    create takes.

    `@LockOwner = 'Session'` rather than `'Transaction'`: callers run both inside
    dbt's transaction and in autocommit, and the transaction-scoped owner errors
    when there is no transaction. A lock request that times out returns a negative
    value and falls through to the bare check - the pre-existing behaviour, no
    worse than before - and releases nothing it did not take.
#}
{% macro create_schema_if_not_exists(schema, authorization=none) -%}
  {%- set lock_resource = 'dbt_create_schema_' ~ schema -%}
  DECLARE @dbt_schema_lock int;
  EXEC @dbt_schema_lock = sp_getapplock
    @Resource = '{{ escape_single_quotes(lock_resource) }}',
    @LockMode = 'Exclusive',
    @LockOwner = 'Session',
    @LockTimeout = 30000;
  IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{{ escape_single_quotes(schema) }}')
  BEGIN
    EXEC('CREATE SCHEMA {{ escape_single_quotes(adapter.quote(schema)) }}{% if authorization is not none %} AUTHORIZATION {{ escape_single_quotes(adapter.quote(authorization)) }}{% endif %}')
  END
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
