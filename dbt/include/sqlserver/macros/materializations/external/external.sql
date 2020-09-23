{% macro create_external_table(source_node) %}

    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}
    {%- set partitions = external.partitions -%}


    {# https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_EXTERNAL_TABLE.html #}
    {# This assumes you have already created an external schema #}

    {# SET ANSI_NULLS ON
    GO
    SET QUOTED_IDENTIFIER ON
    GO #}

    create external table {{source(source_node.source_name, source_node.name)}} (
        {% for column in columns %}
            {{ log('any tests?' ~ columns.tests) }}
            {%- set nullity = 'NULL' if 'not_null' in columns.tests else 'NOT NULL'-%}
            {{adapter.quote(column.name)}} {{column.data_type}} {{nullity}}
            {{- ',' if not loop.last -}}
        {% endfor %}
    )
    WITH (
        {% set dict = {'DATA_SOURCE': external.data_source,
                       'LOCATION' : external.location, 
                       'FILE_FORMAT' : external.file_format, 
                       'REJECT_TYPE' : external.reject_type, 
                       'REJECT_VALUE' : external.reject_value} -%}
        {%- for key, value in dict.items() %}
            {{key}} = {% if key == "LOCATION" -%} '{{value}}' {%- else -%} {{value}} {%- endif -%}
            {{- ',' if not loop.last -}}
            {%- endfor -%}
    )
{% endmacro %}

{% macro get_external_build_plan(source_node) %}

    {% set build_plan = [] %}
    
    {%- set partitions = source_node.external.get('partitions', none) -%}
    {% set create_or_replace = (var('ext_full_refresh', false)) %}
    

        {% set build_plan = [
                dropif(source_node),
                create_external_table(source_node)]%}
    
    {% do return(build_plan) %}

{% endmacro %}

{% macro dropif(node) %}
    
    {% set ddl %}
      if object_id ('{{source(node.source_name, node.name)}}') is not null
        begin
        drop external table {{source(node.source_name, node.name)}}
        end
    {% endset %}
    
    {{return(ddl)}}

{% endmacro %}

{% macro stage_external_sources(select=none) %}

    {% set sources_to_stage = [] %}
    
    {% for node in graph.nodes.values() %}
        
        {% if node.resource_type == 'source' and node.external.location != none %}

            {% do create_schema(node.databse, node.schema) %}
            
            {% if select %}
            
                {% for src in select.split(' ') %}
                
                    {% if '.' in src %}
                        {% set src_s = src.split('.') %}
                        {% if src_s[0] == node.source_name and src_s[1] == node.name %}
                            {% do sources_to_stage.append(node) %}
                        {% endif %}
                    {% else %}
                        {% if src == node.source_name %}
                            {% do sources_to_stage.append(node) %}
                        {% endif %}
                    {% endif %}
                    
                {% endfor %}
                        
            {% else %}
            
                {% do sources_to_stage.append(node) %}
                
            {% endif %}
            
        {% endif %}
        
    {% endfor %}
            
    {% for node in sources_to_stage %}
        {{ log ("what is this node" ~ node )}}

        {% set loop_label = loop.index ~ ' of ' ~ loop.length %}

        {% do dbt_utils.log_info(loop_label ~ ' START external source ' ~ node.schema ~ '.' ~ node.identifier) -%}
        
        {% set run_queue = get_external_build_plan(node) %}
        
        {% do dbt_utils.log_info(loop_label ~ ' SKIP') if run_queue == [] %}
        
        {% do dbt_external_tables.exit_transaction() %}
        
        {% for q in run_queue %}
        
            {% set q_msg = q|trim %}
            {% set q_log = q_msg[:50] ~ '...  ' if q_msg|length > 50 else q_msg %}
        
            {% do dbt_utils.log_info(loop_label ~ ' (' ~ loop.index ~ ') ' ~ q_log) %}
        
            {% call statement('runner', fetch_result = True, auto_begin = False) %}
                {{ q }}
            {% endcall %}
            
            {% set status = load_result('runner')['status'] %}
            {% do dbt_utils.log_info(loop_label ~ ' (' ~ loop.index ~ ') ' ~ status) %}
            
        {% endfor %}
        
    {% endfor %}
    
{% endmacro %}
