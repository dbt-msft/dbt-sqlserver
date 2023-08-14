{% macro fabric__make_temp_relation(base_relation, suffix) %}
    {%- set temp_identifier = base_relation.identifier ~ suffix -%}
    {%- set temp_relation = base_relation.incorporate(
                                path={"identifier": temp_identifier}) -%}

    {{ return(temp_relation) }}
{% endmacro %}

{% macro fabric__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    {{ fabric__drop_relation_script(relation) }}
  {%- endcall %}
{% endmacro %}

{% macro fabric__drop_relation_script(relation) -%}

    {% if relation.type == 'view' -%}
      {% call statement('find_references', fetch_result=true) %}
      {{ use_database_hint() }}
      select
          sch.name as schema_name,
          obj.name as view_name
      from sys.sql_expression_dependencies refs
      inner join sys.objects obj
      on refs.referencing_id = obj.object_id
      inner join sys.schemas sch
      on obj.schema_id = sch.schema_id
      where refs.referenced_database_name = '{{ relation.database }}'
      and refs.referenced_schema_name = '{{ relation.schema }}'
      and refs.referenced_entity_name = '{{ relation.identifier }}'
      and refs.referencing_class = 1
      and obj.type = 'V'
      {% endcall %}
      {% set references = load_result('find_references')['data'] %}
      {% for reference in references -%}
      -- dropping referenced view {{ reference[0] }}.{{ reference[1] }}
      {{ fabric__drop_relation_script(relation.incorporate(
          type="view",
          path={"schema": reference[0], "identifier": reference[1]})) }}
      {% endfor %}
      {% elif relation.type == 'table'%}
      {%- else -%}
          {{ exceptions.raise_not_implemented('Invalid relation being dropped: ' ~ relation) }}
      {% endif %}

    {{ use_database_hint() }}
    EXEC('DROP {{ relation.type }} IF EXISTS {{ relation.include(database=False) }};');

{% endmacro %}

{% macro fabric__rename_relation(from_relation, to_relation) -%}
  {% if to_relation.type == 'view' %}
    {% call statement('get_view_definition', fetch_result=True) %}
        SELECT m.[definition] AS VIEW_DEFINITION
        FROM sys.objects o
        INNER JOIN sys.sql_modules m
            ON m.[object_id] = o.[object_id]
        INNER JOIN sys.views v
            ON o.[object_id] = v.[object_id]
        INNER JOIN sys.schemas s
            ON o.schema_id = s.schema_id
            AND s.schema_id = v.schema_id
        WHERE s.name = '{{ from_relation.schema }}'
            AND v.name = '{{ from_relation.identifier }}'
            AND o.[type] = 'V';
    {% endcall %}

    {% set view_def_full = load_result('get_view_definition')['data'][0][0] %}
    {# Jinja does not allow bitwise operators and we need re.I | re.M here. So calculated manually this becomes 10. #}
    {% set final_view_sql = modules.re.sub("create\s+view\s+.*?\s+as\s+","",view_def_full, 10) %}

    {% call statement('create_new_view') %}
        {{ create_view_as(to_relation, final_view_sql) }}
    {% endcall %}
    {% call statement('drop_old_view') %}
        EXEC('DROP VIEW IF EXISTS {{ from_relation.include(database=False) }};');
    {% endcall %}
  {% endif %}
  {% if to_relation.type == 'table' %}
      {% call statement('rename_relation') %}
        EXEC('create table {{ to_relation.include(database=False) }} as select * from {{ from_relation.include(database=False) }}');
      {%- endcall %}
      -- Getting constraints from the old table
      {% call statement('get_table_constraints', fetch_result=True) %}
        SELECT DISTINCT Contraint_statement FROM
        (
          SELECT DISTINCT
          CASE
              WHEN tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                  THEN 'ALTER TABLE <<REPLACE TABLE>> ADD CONSTRAINT PK_<<CONSTRAINT NAME>>_'+ccu.COLUMN_NAME+' PRIMARY KEY NONCLUSTERED('+ccu.COLUMN_NAME+') NOT ENFORCED'
              WHEN tc.CONSTRAINT_TYPE = 'UNIQUE'
                  THEN 'ALTER TABLE <<REPLACE TABLE>> ADD CONSTRAINT UK_<<CONSTRAINT NAME>>_'+ccu.COLUMN_NAME+' UNIQUE NONCLUSTERED('+ccu.COLUMN_NAME+') NOT ENFORCED'
              END AS Contraint_statement
          FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc INNER JOIN
              INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
                  ON tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
          WHERE tc.TABLE_NAME = '{{ from_relation.identifier }}' and tc.TABLE_SCHEMA = '{{ from_relation.schema }}'
          UNION ALL
          SELECT
            'ALTER TABLE <<REPLACE TABLE>> ADD CONSTRAINT FK_<<CONSTRAINT NAME>>_'+CU.COLUMN_NAME+' FOREIGN KEY('+CU.COLUMN_NAME+') references '+PK.TABLE_SCHEMA+'.'+PK.TABLE_NAME+' ('+PT.COLUMN_NAME+') not enforced'   AS Contraint_statement
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS C
            INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK ON C.CONSTRAINT_NAME = FK.CONSTRAINT_NAME
            INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK ON C.UNIQUE_CONSTRAINT_NAME=PK.CONSTRAINT_NAME
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CU ON C.CONSTRAINT_NAME = CU.CONSTRAINT_NAME
            INNER JOIN (
                SELECT i1.TABLE_NAME, i2.COLUMN_NAME, i1.TABLE_SCHEMA, i2.TABLE_SCHEMA AS CU_TableSchema
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS i1
                    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE i2 ON i1.CONSTRAINT_NAME =i2.CONSTRAINT_NAME
                WHERE i1.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ) PT ON PT.TABLE_NAME = PK.TABLE_NAME AND PT.TABLE_SCHEMA = PK.TABLE_SCHEMA AND PT.CU_TableSchema = PK.TABLE_SCHEMA
          WHERE FK.TABLE_NAME = '{{ from_relation.identifier }}' and FK.TABLE_SCHEMA = '{{ from_relation.schema }}'
          and PK.TABLE_SCHEMA = '{{ from_relation.schema }}' and PT.TABLE_SCHEMA = '{{ from_relation.schema }}'
        ) T WHERE Contraint_statement IS NOT NULL
      {% endcall %}

      {%call statement('drop_table_constraints', fetch_result= True)%}
        SELECT drop_constraint_statement FROM
        (
          SELECT 'ALTER TABLE ['+TABLE_SCHEMA+'].['+TABLE_NAME+'] DROP CONSTRAINT ' + CONSTRAINT_NAME AS drop_constraint_statement
          FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
          WHERE TABLE_NAME = '{{ from_relation.identifier }}' and TABLE_SCHEMA = '{{ from_relation.schema }}'
        ) T WHERE drop_constraint_statement IS NOT NULL

      {% endcall %}

      {% set references = load_result('get_table_constraints')['data'] %}
      {% set drop_references = load_result('drop_table_constraints')['data'] %}

      {% for reference in drop_references -%}
        {% set drop_constraint = reference[0]%}

        {% call statement('Drop_Constraints') %}
          {{ log("Constraints to drop: "~reference[0], info=True) }}
          EXEC('{{drop_constraint}}');
        {% endcall %}
      {% endfor %}

      {% set targetTableNameConstraint = to_relation.include(database=False)%}
      {% set targetTableNameConstraint = (targetTableNameConstraint|string).strip().replace("\"","").replace(".","_")%}
      {% set targetTableName = to_relation.include(database=False) %}

      {% for reference in references -%}
        {% set constraint_name = reference[0].replace("<<CONSTRAINT NAME>>",targetTableNameConstraint)%}
        {% set alter_create_table_constraint_script = constraint_name.replace("<<REPLACE TABLE>>", (targetTableName|string).strip()) %}
        {{ log("Constraints to create: "~alter_create_table_constraint_script, info=True) }}
        {% call statement('Drop_Create_Constraints') %}
          EXEC('{{alter_create_table_constraint_script}}');
        {% endcall %}
      {% endfor %}

      {{ fabric__drop_relation(from_relation) }}
  {% endif %}
{% endmacro %}

-- DROP fabric__truncate_relation when TRUNCATE TABLE is supported
{% macro fabric__truncate_relation(relation) -%}

  {% set tempTableName %}
    {{ relation.include(database=False).identifier.replace("#", "") }}_{{ range(21000, 109000) | random }}
  {% endset %}

  {% call statement('truncate_relation') -%}
    CREATE TABLE {{ tempTableName }} AS SELECT * FROM {{ relation }} WHERE 1=2
    EXEC('DROP TABLE IF EXISTS {{ relation.include(database=False) }};');
    EXEC('CREATE TABLE {{ relation.include(database=False) }} AS SELECT * FROM {{ tempTableName }};');
    EXEC('DROP TABLE IF EXISTS {{ tempTableName }};');
  {%- endcall %}

{% endmacro %}
