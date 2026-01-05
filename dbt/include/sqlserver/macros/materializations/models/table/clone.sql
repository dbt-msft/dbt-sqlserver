{% macro sqlserver__create_or_replace_clone(target_relation, defer_relation) %}
    CREATE TABLE {{target_relation}}
    AS CLONE OF {{defer_relation}}
{% endmacro %}