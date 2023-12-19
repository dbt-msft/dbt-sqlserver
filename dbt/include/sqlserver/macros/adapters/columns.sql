{% macro sqlserver__alter_column_type(relation, column_name, new_column_type) %}

  {%- set tmp_column = column_name + "__dbt_alter" -%}

  {% call statement('alter_column_type') -%}
    alter {{ relation.type }} {{ relation }} add "{{ tmp_column }}" {{ new_column_type }};
  {%- endcall -%}
  {% call statement('alter_column_type') -%}
    update {{ relation }} set "{{ tmp_column }}" = "{{ column_name }}";
  {%- endcall -%}
  {% call statement('alter_column_type') -%}
    alter {{ relation.type }} {{ relation }} drop column "{{ column_name }}";
  {%- endcall -%}
  {% call statement('alter_column_type') -%}
    exec sp_rename '{{ relation | replace('"', '') }}.{{ tmp_column }}', '{{ column_name }}', 'column'
  {%- endcall -%}

{% endmacro %}
