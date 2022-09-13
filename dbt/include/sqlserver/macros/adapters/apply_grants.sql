{% macro sqlserver__get_show_grant_sql(relation) %}
    select
        GRANTEE as grantee,
        PRIVILEGE_TYPE as privilege_type
    from INFORMATION_SCHEMA.TABLE_PRIVILEGES
    where TABLE_CATALOG = '{{ relation.database }}'
      and TABLE_SCHEMA = '{{ relation.schema }}'
      and TABLE_NAME = '{{ relation.identifier }}'
    {% endmacro %}
