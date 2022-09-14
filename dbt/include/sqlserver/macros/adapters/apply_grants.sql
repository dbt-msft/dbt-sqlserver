{% macro sqlserver__get_show_grant_sql(relation) %}
    select
        GRANTEE as grantee,
        PRIVILEGE_TYPE as privilege_type
    from INFORMATION_SCHEMA.TABLE_PRIVILEGES
    where TABLE_CATALOG = '{{ relation.database }}'
      and TABLE_SCHEMA = '{{ relation.schema }}'
      and TABLE_NAME = '{{ relation.identifier }}'
{% endmacro %}


{%- macro sqlserver_get_grant_sql(relation, privilege, grantees) -%}
    {%- set grantees_safe = [] -%}
    {%- for grantee in grantees -%}
        {%- set grantee_safe = "[" ~ grantee ~ "]" -%}
        {%- do grantees_safe.append(grantee_safe) -%}

        {%- if target.auto_provision_aad_principals is not none and target.auto_provision_aad_principals -%}
            if not exists(select name from sys.database_principals where name = '{{ grantee_safe }}')
            create user {{ grantee_safe }} from external provider;
        {%- endif -%}

    {%- endfor -%}

    grant {{ privilege }} on {{ relation }} to {{ grantees_safe | join(', ') }};
{%- endmacro -%}
