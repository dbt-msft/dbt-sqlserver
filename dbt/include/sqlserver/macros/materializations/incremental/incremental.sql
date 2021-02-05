{% macro dbt__incremental_delete(target_relation, tmp_relation) -%}
  {%- set unique_key = config.require('unique_key') -%}

  delete
  from {{ target_relation }}
  where ({{ unique_key }}) in (
    select ({{ unique_key }})
    from {{ tmp_relation.include(schema=False, database=False) }}
  );

{%- endmacro %}