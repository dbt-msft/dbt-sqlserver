{% macro run_hooks(hooks, inside_transaction=True) %}
  {% for hook in hooks | selectattr('transaction', 'equalto', inside_transaction)  %}
    {% if not inside_transaction and loop.first %}
      {% call statement(auto_begin=inside_transaction) %}
        {% if not adapter.behavior.dbt_sqlserver_use_dbt_transactions %}
          if @@trancount > 0 commit; -- post hooks after fictitious transaction work as expected
        {% else %}
          commit; -- align transaction=False hook behavior with dbt-core transaction semantics.
        {% endif %}
      {% endcall %}
    {% endif %}
    {% set rendered = render(hook.get('sql')) | trim %}
    {% if (rendered | length) > 0 %}
      {% call statement(auto_begin=inside_transaction) %}
        {{ rendered }}
      {% endcall %}
    {% endif %}
  {% endfor %}
{% endmacro %}
