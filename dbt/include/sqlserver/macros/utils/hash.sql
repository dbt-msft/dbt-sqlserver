{% macro sqlserver__hash(field) %}
    convert(varchar(50), hashbytes('md5', {{field}}), 2)
{% endmacro %}
