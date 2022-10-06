{% macro sqlserver__split_part(string_text, delimiter_text, part_number) %}

    (select value from string_split({{ string_text }}, {{ delimiter_text }}, 1) where ordinal = {{ part_number }})

{% endmacro %}
