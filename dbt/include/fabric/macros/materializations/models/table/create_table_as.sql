{% macro fabric__create_table_as(temporary, relation, sql) -%}

   {% set tmp_relation = relation.incorporate(
   path={"identifier": relation.identifier.replace("#", "") ~ '_temp_view'},
   type='view')-%}
   {% do run_query(fabric__drop_relation_script(tmp_relation)) %}
   {% do run_query(fabric__drop_relation_script(relation)) %}

   {% set contract_config = config.get('contract') %}

    {{ fabric__create_view_as(tmp_relation, sql) }}
    {% if contract_config.enforced %}

        CREATE TABLE {{ relation.include(database=False) }}
        {{ fabric__table_columns_and_constraints(relation.include(database=False)) }}
        {{ get_assert_columns_equivalent(sql)  }}

        {% set listColumns %}
            {% for column in model['columns'] %}
                {{ "["~column~"]" }}{{ ", " if not loop.last }}
            {% endfor %}
        {%endset%}

        INSERT INTO {{ relation.include(database=False) }}
        ({{listColumns}}) SELECT {{listColumns}} FROM {{ tmp_relation.include(database=False) }};

    {%- else %}
      EXEC('CREATE TABLE {{ relation.include(database=False) }} AS (SELECT * FROM {{ tmp_relation.include(database=False) }});');
    {% endif %}

    {{ fabric__drop_relation_script(tmp_relation) }}

{% endmacro %}
