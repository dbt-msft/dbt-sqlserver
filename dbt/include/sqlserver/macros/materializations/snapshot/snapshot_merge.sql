{% macro sqlserver__snapshot_merge_sql(target, source, insert_cols) -%}
      {%- set insert_cols_csv = insert_cols | join(', ') -%}
      
      EXEC('update {{ target }}
          set dbt_valid_to = TMP.dbt_valid_to
          from {{ source }} TMP
          where {{ target }}.dbt_scd_id = TMP.dbt_scd_id
            and TMP.dbt_change_type = ''update''
            and {{ target }}.dbt_valid_to is null;

            insert into {{ target }} (
                  {{ insert_cols_csv }}
                  )
            select {{ insert_cols_csv }}
            from {{ source }} 
            where dbt_change_type = ''insert'' ; ');

{% endmacro %}
