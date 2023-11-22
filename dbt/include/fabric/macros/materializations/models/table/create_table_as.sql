{% macro fabric__create_table_as(temporary, relation, sql) -%}

   {% set tmp_relation = relation.incorporate(
   path={"identifier": relation.identifier.replace("#", "") ~ '_temp_view'},
   type='view')-%}
   {% do run_query(fabric__drop_relation_script(tmp_relation)) %}
   {% do run_query(fabric__drop_relation_script(relation)) %}

   {% set contract_config = config.get('contract') %}

    {{ fabric__create_view_as(tmp_relation, sql) }}
    {% if contract_config.enforced %}

        CREATE TABLE [{{relation.database}}].[{{relation.schema}}].[{{relation.identifier}}]
        {{ fabric__table_columns_and_constraints(relation) }}
        {{ get_assert_columns_equivalent(sql)  }}

        {% set listColumns %}
            {% for column in model['columns'] %}
                {{ "["~column~"]" }}{{ ", " if not loop.last }}
            {% endfor %}
        {%endset%}

        INSERT INTO [{{relation.database}}].[{{relation.schema}}].[{{relation.identifier}}]
        ({{listColumns}}) SELECT {{listColumns}} FROM [{{tmp_relation.database}}].[{{tmp_relation.schema}}].[{{tmp_relation.identifier}}];

    {%- else %}
      EXEC('CREATE TABLE [{{relation.database}}].[{{relation.schema}}].[{{relation.identifier}}] AS (SELECT * FROM [{{tmp_relation.database}}].[{{tmp_relation.schema}}].[{{tmp_relation.identifier}}]);');
    {% endif %}

    {{ fabric__drop_relation_script(tmp_relation) }}

{% endmacro %}
