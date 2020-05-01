{% macro sqlserver__snapshot_hash_arguments(args) %}
    CONVERT(VARCHAR(32), HashBytes('MD5', {% for arg in args %}
        coalesce(cast({{ arg }} as varchar ), '') {% if not loop.last %} + '|' + {% endif %}
    {% endfor %}), 2)
{% endmacro %}

{% macro sqlserver__snapshot_check_strategy(node, snapshotted_rel, current_rel, config, target_exists) %}
    {% set check_cols_config = config['check_cols'] %}
    {% set primary_key = config['unique_key'] %}
    {% set updated_at = snapshot_get_time() %}

    {% if check_cols_config == 'all' %}
        {% set check_cols = get_columns_in_query(node['injected_sql']) %}
    {% elif check_cols_config is iterable and (check_cols_config | length) > 0 %}
        {% set check_cols = check_cols_config %}
    {% else %}
        {% do exceptions.raise_compiler_error("Invalid value for 'check_cols': " ~ check_cols_config) %}
    {% endif %}

    {% set row_changed_expr -%}
        (
        {% for col in check_cols %}
            {{ snapshotted_rel }}.{{ col }} != {{ current_rel }}.{{ col }}
            or
            ({{ snapshotted_rel }}.{{ col }} is null) and not ({{ current_rel }}.{{ col }} is null)
            {%- if not loop.last %} or {% endif %}

        {% endfor %}
        )
    {%- endset %}

    {% set scd_id_expr = snapshot_hash_arguments([primary_key, updated_at]) %}

    {% do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr
    }) %}
{% endmacro %}