{% macro sqlserver__create_table_as(temporary, relation, sql) -%}
    {%- set query_label = apply_label() -%}
    {%- set tmp_relation = relation.incorporate(path={"identifier": relation.identifier ~ '__dbt_tmp_vw'}, type='view') -%}

    {%- do adapter.drop_relation(tmp_relation) -%}
    USE [{{ relation.database }}];
    {{ get_create_view_as_sql(tmp_relation, sql) }}

    {%- set table_name -%}
        {{ relation }}
    {%- endset -%}


    {%- set contract_config = config.get('contract') -%}
    {%- set query -%}
        {% if contract_config.enforced and (not temporary) %}
            CREATE TABLE {{table_name}}
            {{ get_assert_columns_equivalent(sql)  }}
            {{ build_columns_constraints(relation) }}
            {% set listColumns %}
                {% for column in model['columns'] %}
                    {{ "["~column~"]" }}{{ ", " if not loop.last }}
                {% endfor %}
            {%endset%}
            INSERT INTO {{relation}} ({{listColumns}})
            SELECT {{listColumns}} FROM {{tmp_relation}} {{ query_label }}

        {% else %}
            SELECT * INTO {{ table_name }} FROM {{ tmp_relation }} {{ query_label }}
        {% endif %}
    {%- endset -%}

    EXEC('{{- escape_single_quotes(query) -}}')

    {# For some reason drop_relation is not firing. This solves the issue for now. #}
    EXEC('DROP VIEW IF EXISTS {{tmp_relation.schema}}.{{tmp_relation.identifier}}')



    {% set as_columnstore = config.get('as_columnstore', default=true) %}
    {% if not temporary and as_columnstore -%}
        {#-
        add columnstore index
        this creates with dbt_temp as its coming from a temporary relation before renaming
        could alter relation to drop the dbt_temp portion if needed
        -#}
        {{ sqlserver__create_clustered_columnstore_index(relation) }}
   {% endif %}

{% endmacro %}
