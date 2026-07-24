{% macro sqlserver__scalar_function_sql(target_relation) %}
    {{ sqlserver__scalar_function_create_replace_signature_sql(target_relation) }}
    {{ sqlserver__scalar_function_body_sql() }};
{% endmacro %}

{% macro sqlserver__scalar_function_create_replace_signature_sql(target_relation) %}
    CREATE OR ALTER FUNCTION {{ target_relation.include(database=False) }} ({{ sqlserver__formatted_scalar_function_args_sql() }})
    RETURNS {{ model.returns.data_type }}
    {{ sqlserver__scalar_function_volatility_sql() }}
    AS
{% endmacro %}

{% macro sqlserver__formatted_scalar_function_args_sql() %}
    {% set args = [] %}
    {% for arg in model.arguments -%}
        {%- set arg_def = '@' ~ arg.name ~ ' ' ~ arg.data_type -%}
        {%- set default_val = arg.default_value | default(none) -%}
        {%- if default_val is not none and default_val | string | trim != '' and default_val | string | lower != 'none' -%}
            {%- set arg_def = arg_def ~ ' = ' ~ default_val -%}
        {%- endif -%}
        {%- do args.append(arg_def) -%}
    {%- endfor %}
    {{ return(args | join(', ')) }}
{% endmacro %}

{% macro sqlserver__scalar_function_body_sql() %}
    BEGIN
        RETURN {{ model.compiled_code }}
    END
{% endmacro %}

{% macro sqlserver__scalar_function_volatility_sql() %}
    {% set volatility = model.config.get('volatility') %}
    {% if volatility != none %}
        {% do sqlserver__unsupported_volatility_warning(volatility) %}
    {% endif %}
{% endmacro %}

{% macro sqlserver__unsupported_volatility_warning(volatility) %}
    {% set msg = "Found `" ~ volatility ~ "` volatility specified on function `" ~ model.name ~ "`. SQL Server does not support volatility specifiers; this will be ignored." %}
    {% do exceptions.warn(msg) %}
{% endmacro %}
