{% macro sqlserver__snapshot_hash_arguments(args) %}
    CONVERT(VARCHAR(32), HashBytes('MD5', {% for arg in args %}
        coalesce(CONVERT(varchar, {{ arg }}, 121), '') {% if not loop.last %} + '|' + {% endif %}
    {% endfor %}), 2)
{% endmacro %}

