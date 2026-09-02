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
    Resolve pre_hook_transaction_scope: where schema resolution (the tmp view
    and the empty CREATE) sits relative to the in-transaction pre-hooks.
    Shared by table, incremental and snapshot. Full detail in
    docs/transaction_scope.md.

      load (default)                         build
      ------------------------------------   ------------------------------------
      stage            autocommit            BEGIN
      BEGIN                                  |- in-tx pre-hooks
      |- in-tx pre-hooks                     |- stage         Sch-M on new object
      |- load          X table lock only     |- load          ... held to COMMIT
      |- cutover, masks, in-tx post-hooks    |- cutover, masks, in-tx post-hooks
      COMMIT                                 COMMIT
      |- view drops, indexes, grants, docs   |- view drops, indexes, grants, docs

    Both keep a transaction: true pre-hook atomic with the load. load fixes
    #819 (Sch-M conflicts with the Sch-S every metadata reader takes; an X
    table lock does not) but needs the model SQL to bind before the hooks run: a
    transaction: true pre-hook that creates an object the model reads fails
    at the stage with Msg 208. Remedies: transaction: false on that hook
    (outside-tx hooks run before the stage), or build for that model.
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
