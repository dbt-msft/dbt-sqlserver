{% macro run_hooks(hooks, inside_transaction=True) %}
  {% for hook in hooks | selectattr('transaction', 'equalto', inside_transaction)  %}
    {% if not inside_transaction and loop.first %}
      {% call statement(auto_begin=inside_transaction) %}
        {% if not adapter.behavior.dbt_sqlserver_use_dbt_transactions %}
          if @@trancount > 0 commit; -- post hooks after fictitious transaction work as expected
        {% else %}
          {#- guarded, not a bare COMMIT: nothing guarantees a transaction is
              open here. A bare one appeared safe only because some earlier
              statement had auto-begun one (find_references, until relation.sql
              stopped doing that); with @@TRANCOUNT = 0 it raises Msg 3902. -#}
          if @@trancount > 0 commit; -- align transaction=False hook behavior with dbt-core transaction semantics.
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


{% macro sqlserver__pre_hook_transaction_scope() -%}
  {#-
    Resolve the pre_hook_transaction_scope model config: 'load' (default) or
    'build'. See docs/transaction_scope.md.

    'load'  - schema resolution (the tmp view and the empty CREATE) runs before
              the in-transaction pre-hooks and autocommits, so its Sch-M lock
              is released in an instant. The transaction then covers the
              pre-hooks, the load, the cutover and the in-transaction
              post-hooks, so a transaction: true pre-hook still rolls back
              with a failed load. The load takes an X table lock, never Sch-M,
              so it blocks no metadata reader in any other session (#819).
              Requires the model SQL to bind before the pre-hooks run: a
              transaction: true pre-hook that creates an object the model
              reads fails at the stage with Msg 208; declare that hook
              transaction: false (those run before the stage) or set 'build'.

    'build' - the pre-hooks, the create and the load share one transaction, so
              the new object's Sch-M is held for the whole load. Today's
              behaviour, kept as the opt-out for the case above.
  -#}
  {%- set scope = config.get('pre_hook_transaction_scope', 'load') -%}
  {%- if scope not in ['load', 'build'] -%}
    {{ exceptions.raise_compiler_error(
      "Invalid pre_hook_transaction_scope '" ~ scope ~ "'. "
      "Valid values are: 'load' (default), 'build'."
    ) }}
  {%- endif -%}
  {{ return(scope) }}
{%- endmacro %}
