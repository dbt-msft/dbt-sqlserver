{% macro sqlserver__openquery(server_name, remote_sql) -%}
    {#- Validation runs only at execution time, not during manifest parsing:
        a parse-time raise_compiler_error aborts the whole invocation on the
        first bad model and hides sibling errors. Execution-phase validation
        lets one dbt run/compile surface every bad model while still emitting
        the OPENQUERY fragment for good models. -#}
    {%- set openquery_max_length = 8000 -%}
    {%- set cleaned_sql = remote_sql | replace("\r", "") | replace("'", "''") -%}
    {%- if execute -%}
        {%- if server_name is none or server_name | trim == '' -%}
            {{ exceptions.raise_compiler_error("openquery: server_name must not be empty, got: '" ~ server_name ~ "'") }}
        {%- endif -%}
        {%- if remote_sql is none or remote_sql | trim == '' -%}
            {{ exceptions.raise_compiler_error("openquery: remote_sql must not be empty") }}
        {%- endif -%}
        {%- if cleaned_sql | length > openquery_max_length -%}
            {{ exceptions.raise_compiler_error("openquery: query exceeds SQL Server OPENQUERY 8 KB limit (got " ~ (cleaned_sql | length) ~ " characters after escaping, max 8000). Use EXEC('...') AT <server> or a remote view/OPENROWSET for longer queries.") }}
        {%- endif -%}
    {%- endif -%}
    OPENQUERY({{ adapter.quote(server_name) }}, '{{ cleaned_sql }}')
{%- endmacro %}
