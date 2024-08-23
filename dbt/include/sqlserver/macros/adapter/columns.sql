{% macro sqlserver__alter_column_type(relation, column_name, new_column_type) %}

    {%- set tmp_column = column_name + "__dbt_alter" -%}
    {% set alter_column_type %}
        alter {{ relation.type }} {{ relation }} add "{{ tmp_column }}" {{ new_column_type }};
    {%- endset %}

    {% set update_column %}
        update {{ relation }} set "{{ tmp_column }}" = "{{ column_name }}";
    {%- endset %}

    {% set drop_column %}
        alter {{ relation.type }} {{ relation }} drop column "{{ column_name }}";
    {%- endset %}

    {% set rename_column %}
        exec sp_rename '{{ relation | replace('"', '') }}.{{ tmp_column }}', '{{ column_name }}', 'column'
    {%- endset %}

    {% do run_query(alter_column_type) %}
    {% do run_query(update_column) %}
    {% do run_query(drop_column) %}
    {% do run_query(rename_column) %}

{% endmacro %}
