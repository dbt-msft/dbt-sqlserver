{% macro sqlserver__get_show_grant_sql(relation) %}
    select
        GRANTEE as grantee,
        PRIVILEGE_TYPE as privilege_type
    from INFORMATION_SCHEMA.TABLE_PRIVILEGES
    where TABLE_CATALOG = '{{ relation.database }}'
      and TABLE_SCHEMA = '{{ relation.schema }}'
      and TABLE_NAME = '{{ relation.identifier }}'
{% endmacro %}


{%- macro sqlserver__get_dcl_statement_list(relation, grant_config, get_dcl_macro) -%}
    {#
      -- Unpack grant_config into specific privileges and the set of users who need them granted/revoked.
      -- Depending on whether this database supports multiple grantees per statement, pass in the list of
      -- all grantees per privilege, or (if not) template one statement per privilege-grantee pair.
      -- `get_dcl_macro` will be either `get_grant_sql` or `get_revoke_sql`
    #}
    {%- set dcl_statements = [] -%}
    {%- for privilege, grantees in grant_config.items() %}
        {%- set grantees_safe = [] -%}
        {%- for grantee in grantees -%}
            {#
                Grantees are wrapped in [] to avoid issues with spaces or other special chars in their names.
            #}
            {%- set grantee_safe = "[" ~ grantee ~ "]" -%}
            {%- do grantees_safe.append(grantee_safe) -%}
        {%- endfor -%}
        {%- if support_multiple_grantees_per_dcl_statement() and grantees_safe -%}
          {%- set dcl = get_dcl_macro(relation, privilege, grantees_safe) -%}
          {%- do dcl_statements.append(dcl) -%}
        {%- else -%}
          {%- for grantee in grantees_safe -%}
              {% set dcl = get_dcl_macro(relation, privilege, [grantee]) %}
              {%- do dcl_statements.append(dcl) -%}
          {% endfor -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(dcl_statements) }}
{%- endmacro %}


{% macro get_provision_sql(relation, privilege, grantees) %}
    {% for grantee in grantees %}
        if not exists(select name from sys.database_principals where name = '{{ grantee }}')
        create user {{ grantee }} from external provider;
    {% endfor %}
{% endmacro %}


{% macro sqlserver__apply_grants(relation, grant_config, should_revoke=True) %}
    {#-- If grant_config is {} or None, this is a no-op --#}
    {% if grant_config %}
        {% if should_revoke %}
            {#-- We think previous grants may have carried over --#}
            {#-- Show current grants and calculate diffs --#}
            {% set current_grants_table = run_query(get_show_grant_sql(relation)) %}
            {% set current_grants_dict = adapter.standardize_grants_dict(current_grants_table) %}
            {% set needs_granting = diff_of_two_dicts(grant_config, current_grants_dict) %}
            {% set needs_revoking = diff_of_two_dicts(current_grants_dict, grant_config) %}
            {% if not (needs_granting or needs_revoking) %}
                {{ log('On ' ~ relation ~': All grants are in place, no revocation or granting needed.')}}
            {% endif %}
        {% else %}
            {#-- We don't think there's any chance of previous grants having carried over. --#}
            {#-- Jump straight to granting what the user has configured. --#}
            {% set needs_revoking = {} %}
            {% set needs_granting = grant_config %}
        {% endif %}
        {% if needs_granting or needs_revoking %}
            {% set revoke_statement_list = get_dcl_statement_list(relation, needs_revoking, get_revoke_sql) %}

            {% if target.auto_provision_aad_principals is not none and target.auto_provision_aad_principals %}
                {% set provision_statement_list = get_dcl_statement_list(relation, needs_granting, get_provision_sql) %}
            {% else %}
                {% set provision_statement_list = [] %}
            {% endif %}

            {% set grant_statement_list = get_dcl_statement_list(relation, needs_granting, get_grant_sql) %}
            {% set dcl_statement_list = revoke_statement_list + provision_statement_list + grant_statement_list %}
            {% if dcl_statement_list %}
                {{ call_dcl_statements(dcl_statement_list) }}
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %}
