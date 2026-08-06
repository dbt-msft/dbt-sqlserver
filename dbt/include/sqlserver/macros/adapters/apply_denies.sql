{#-
    Object-level DENY permissions, modelled on apply_grants / apply_masks.

    Post-materialization step: (re)apply the object-level DENYs a model declares
    so they survive dbt's drop-and-recreate on every build. A DENY is stored in
    sys.database_permissions against the relation's object_id, so a rebuild
    (which mints a new object_id) silently discards it — the schema-GRANT it was
    carved out of survives, leaving a fail-open posture. This reads the live DENY
    state, diffs it against the resolved config, and emits only the changes:
    DENY for configured-not-present, REVOKE for present-not-configured (REVOKE
    removes a DENY as well as a GRANT).

    A no-op when nothing is configured and on adapters other than SQL Server.
    Unlike apply_masks there is NO relation.type guard — a view is a valid
    securable and, being recreated on every run, is where a DENY is lost most
    often.

    Config surface (normalised + validated in adapter.resolve_denies):
      * model-level `denies` dict, shaped exactly like `grants`.
-#}

{% macro apply_denies(relation, deny_config, should_revoke=True) %}
    {{ return(adapter.dispatch('apply_denies', 'dbt')(relation, deny_config, should_revoke)) }}
{% endmacro %}

{#- Non-SQL-Server adapters are unaffected. -#}
{% macro default__apply_denies(relation, deny_config, should_revoke=True) %}{% endmacro %}


{% macro get_show_deny_sql(relation) %}
    {{ return(adapter.dispatch('get_show_deny_sql', 'dbt')(relation)) }}
{% endmacro %}

{% macro default__get_show_deny_sql(relation) %}
    {{ return('') }}
{% endmacro %}

{#- Live object-level (minor_id = 0), DENY-state permissions on the relation. -#}
{% macro sqlserver__get_show_deny_sql(relation) %}
    select
        pr.name as grantee,
        dp.permission_name as privilege_type
    from sys.database_permissions dp {{ information_schema_hints() }}
    join sys.database_principals pr {{ information_schema_hints() }}
        on pr.principal_id = dp.grantee_principal_id
    where dp.class = 1
      and dp.major_id = OBJECT_ID('{{ escape_single_quotes(relation.include(database=False)) }}')
      and dp.minor_id = 0
      and dp.state_desc = 'DENY'
{% endmacro %}


{#- Lower-cased names of every database principal, for the existence guard. -#}
{% macro sqlserver__get_existing_principals() %}
    {% call statement('get_existing_principals', fetch_result=True) %}
        select name from sys.database_principals {{ information_schema_hints() }}
    {% endcall %}
    {% set result = [] %}
    {% for row in load_result('get_existing_principals').table.rows %}
        {% do result.append(row[0] | trim | lower) %}
    {% endfor %}
    {{ return(result) }}
{% endmacro %}


{% macro sqlserver__apply_denies(relation, deny_config, should_revoke=True) %}
    {#-- If deny_config is {} or None, this is a no-op (mirrors apply_grants). --#}
    {% if not deny_config %}
        {{ return(none) }}
    {% endif %}

    {#-- Reconcile against live state. On a fresh build there is nothing to
         revoke, so skip the round-trip and treat every configured deny as an
         add — exactly apply_grants' should_revoke shortcut. --#}
    {% if should_revoke %}
        {% set existing_denies = run_query(get_show_deny_sql(relation)) %}
    {% else %}
        {% set existing_denies = none %}
    {% endif %}

    {% set changes = adapter.deny_changes(existing_denies, deny_config) %}

    {#-- A DENY to a non-existent principal errors, and the principal set differs
         across dev / CI / prod, so warn-and-skip rather than fail — one config
         then runs unchanged everywhere. Revoked denies reference principals that
         necessarily still exist. --#}
    {% set existing_principals = sqlserver__get_existing_principals() %}

    {#-- Revoke first, then deny (order is immaterial — a (privilege, principal)
         pair is only ever on one side of the diff). --#}
    {% set statements = [] %}
    {% for privilege, principal in changes['revokes'] %}
        {% do statements.append(
            "revoke " ~ privilege ~ " on " ~ relation ~ " from ["
            ~ (principal | replace(']', ']]')) ~ "]") %}
    {% endfor %}
    {% for privilege, principal in changes['denies'] %}
        {% if (principal | trim | lower) in existing_principals %}
            {% do statements.append(
                "deny " ~ privilege ~ " on " ~ relation ~ " to ["
                ~ (principal | replace(']', ']]')) ~ "]") %}
        {% else %}
            {% do exceptions.warn("apply_denies on " ~ relation ~ ": database principal '"
                ~ principal ~ "' does not exist; skipping DENY " ~ privilege ~ ". "
                ~ "Create the principal or remove it from `denies`.") %}
        {% endif %}
    {% endfor %}

    {% if statements %}
        {% do run_query(statements | join(";\n")) %}
        {% do log("Applied " ~ statements | length ~ " deny change(s) on "
                  ~ relation, info=true) %}
    {% else %}
        {% do log("On " ~ relation ~ ": all denies are in place, no changes needed.") %}
    {% endif %}
{% endmacro %}
