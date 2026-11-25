{#- The second key a column needs because = misses a change to it, as a
    format string over the column, or none. = pads trailing spaces (and 0x00)
    even under BIN2, and compares datetimeoffset by instant, ignoring the
    offset. -#}
{% macro sqlserver__delta_extra_key(column, types) %}
  {%- set dtype = types[column.name | lower] -%}
  {%- if dtype in ['varchar', 'nvarchar', 'varbinary', 'sysname'] -%}
    {{- return('DATALENGTH(%s)') -}}
  {%- elif dtype == 'datetimeoffset' -%}
    {{- return('DATEPART(TZOFFSET, %s)') -}}
  {%- endif -%}
  {{- return(none) -}}
{% endmacro %}


{#- Comparison keys for the delta swap: every column under its own collation,
    plus its sqlserver__delta_extra_key. types maps each lower-cased column
    name to its base type (sqlserver__delta_probe). -#}
{% macro sqlserver__delta_keys(columns, types, alias, named=true) %}
  {%- set keys = [] -%}
  {%- for col in columns -%}
    {%- set extra = sqlserver__delta_extra_key(col, types) -%}
    {%- do keys.append(alias ~ '.' ~ col.quoted) -%}
    {%- if extra -%}
      {%- do keys.append(extra % (alias ~ '.' ~ col.quoted) ~ (' AS [__dbt_x_' ~ loop.index ~ ']' if named else '')) -%}
    {%- endif -%}
  {%- endfor -%}
  {{- return(keys | join(', ')) -}}
{% endmacro %}


{% macro sqlserver__delta_key_names(columns, types, alias=none) %}
  {%- set prefix = alias ~ '.' if alias else '' -%}
  {%- set names = [] -%}
  {%- for col in columns -%}
    {%- do names.append(prefix ~ col.quoted) -%}
    {%- if sqlserver__delta_extra_key(col, types) -%}
      {%- do names.append(prefix ~ '[__dbt_x_' ~ loop.index ~ ']') -%}
    {%- endif -%}
  {%- endfor -%}
  {{- return(names | join(', ')) -}}
{% endmacro %}


{% macro sqlserver__delta_key_columns(columns, relation) %}
  {%- set configured = config.get('delta_key') -%}
  {%- if not configured -%}
    {{- return([]) -}}
  {%- endif -%}
  {%- if configured is string -%}
    {%- set configured = [configured] -%}
  {%- endif -%}
  {%- set by_name = {} -%}
  {%- for col in columns -%}
    {%- do by_name.update({col.name | lower: col.quoted}) -%}
  {%- endfor -%}
  {%- set resolved = [] -%}
  {%- for name in configured -%}
    {%- if name | lower not in by_name -%}
      {{ exceptions.raise_compiler_error(
        "delta_key column '" ~ name ~ "' is not a column of " ~ relation
        ~ ". Columns: " ~ columns | map(attribute='name') | join(', ')
      ) }}
    {%- endif -%}
    {%- do resolved.append(by_name[name | lower]) -%}
  {%- endfor -%}
  {{- return(resolved) -}}
{% endmacro %}


{#- Sets @dbt_dupes > 0 when two scratch rows are equal on every delta key,
    which set operations would collapse.

    With delta_key, a key unique in scratch proves no two rows are equal on
    every column, so the full-width aggregate is skipped. A non-unique key
    proves nothing (the other columns may differ), so it falls through to the
    exact check.

    The exact check hash-aggregates every column, which sizes its memory grant
    by row width, so it runs only over rows whose CHECKSUM collides with
    another's. Rows equal under the GROUP BY (collation, extra key) always
    share a CHECKSUM, so no duplicate escapes the narrowing; the converse does
    not hold, hence the exact pass. -#}
{% macro sqlserver__delta_dupes_sql(columns, types, relation, key_columns, query_label) %}
  {%- set keys = sqlserver__delta_keys(columns, types, 's', named=false) -%}
  {%- set keys_c = sqlserver__delta_keys(columns, types, 'c', named=false) -%}
  DECLARE @dbt_dupes bigint = 1;
  {% if key_columns %}
  SELECT @dbt_dupes = COUNT_BIG(*) FROM (
    SELECT 1 AS x FROM {{ relation }} s
    GROUP BY {{ key_columns | join(', ') }} HAVING COUNT_BIG(*) > 1) d
  {{ query_label }}
  {% endif %}
  IF @dbt_dupes > 0
  BEGIN
    SELECT @dbt_dupes = COUNT_BIG(*) FROM (
      SELECT 1 AS x FROM {{ relation }} s
      WHERE CHECKSUM({{ keys }}) IN (
        SELECT CHECKSUM({{ keys_c }}) FROM {{ relation }} c
        GROUP BY CHECKSUM({{ keys_c }}) HAVING COUNT_BIG(*) > 1)
      GROUP BY {{ keys }} HAVING COUNT_BIG(*) > 1) d
    {{ query_label }}
  END;
{% endmacro %}


{#- Why the delta cannot compare the table, or none.
      unsupported type  EXCEPT, INTERSECT or CHECKSUM cannot compare it, or
                        it is sql_variant, whose = ignores trailing spaces and
                        base-type changes
      collation         differs between the table and the new build: EXCEPT
                        between them raises Msg 468
      too many columns  the gone table, a column per key, passes SQL
                        Server's 1024 -#}
{% macro sqlserver__delta_blocker(columns, types, conflicts) %}
  {%- set unsupported = [] -%}
  {%- for col in columns -%}
    {%- if types[col.name | lower] in ['xml', 'text', 'ntext', 'image', 'geography', 'geometry', 'sql_variant', 'json', 'vector'] -%}
      {%- do unsupported.append(col.name) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if unsupported -%}
    {{- return("column(s) " ~ unsupported | join(', ') ~ " (unsupported type)") -}}
  {%- elif conflicts -%}
    {{- return("column(s) " ~ conflicts | join(', ') ~ " (collation differs from the new build)") -}}
  {%- endif -%}
  {%- set ns = namespace(key_count=columns | length) -%}
  {%- for col in columns if sqlserver__delta_extra_key(col, types) -%}
    {%- set ns.key_count = ns.key_count + 1 -%}
  {%- endfor -%}
  {%- set key_count = ns.key_count -%}
  {%- if key_count + 1 > 1024 -%}
    {{- return(key_count ~ " comparison keys (more than the 1023 a table can hold)") -}}
  {%- endif -%}
  {{- return(none) -}}
{% endmacro %}


{#- Each target column's type, lower-cased, with an alias type resolved to
    its system type, and the columns whose collation differs in the new
    build. Only aliases are resolved: other types can share a system_type_id
    (vector's is varbinary's, the CLR types share one).
    Read-only probe, so auto_begin=False (#819). -#}
{% macro sqlserver__delta_probe(target_relation, refresh_relation) %}
  {% call statement('delta_probe', fetch_result=True, auto_begin=False) -%}
    {{ get_use_database_sql(target_relation.database) }}
    SELECT LOWER(t.name) COLLATE database_default AS column_name,
           LOWER(IIF(ty.is_user_defined = 1 AND ty.is_assembly_type = 0,
                      TYPE_NAME(t.system_type_id), ty.name)) AS base_type,
           IIF(s.collation_name <> t.collation_name, 1, 0) AS collation_conflict,
           t.name AS original_name
    FROM sys.columns t
    INNER JOIN sys.types ty ON ty.user_type_id = t.user_type_id
    LEFT JOIN sys.columns s
      ON s.object_id = OBJECT_ID('{{ refresh_relation }}') AND s.name = t.name
    WHERE t.object_id = OBJECT_ID('{{ target_relation }}')
    ORDER BY t.column_id;
  {%- endcall %}
  {%- set types = {} -%}
  {%- set conflicts = [] -%}
  {%- for row in load_result('delta_probe')['table'].rows -%}
    {%- do types.update({row[0]: row[1]}) -%}
    {%- if row[2] == 1 -%}
      {%- do conflicts.append(row[3]) -%}
    {%- endif -%}
  {%- endfor -%}
  {{- return({'types': types, 'conflicts': conflicts}) -}}
{% endmacro %}


{#- The delta_max_churn_percent config: past this share of the build's rows
    deleted, the delta takes the full swap. -#}
{% macro sqlserver__delta_max_churn_percent() %}
  {%- set percent = config.get('delta_max_churn_percent', 15) -%}
  {%- if percent is not number or percent is sameas true or percent is sameas false
        or percent < 0 or percent > 100 -%}
    {{ exceptions.raise_compiler_error(
      "Invalid delta_max_churn_percent '" ~ percent ~ "'. Use a number from 0 to 100 (default 15)."
    ) }}
  {%- endif -%}
  {{- return(percent) -}}
{% endmacro %}


{% macro sqlserver__table_dml_refresh_stage(target_relation, sql, refresh_method='dml') %}
  {#-
    Schema resolution for the dml refresh: drop leftovers by name, create
    the tmp view over the model SQL, create the scratch table EMPTY. Every
    statement passes auto_begin=False. table.sql calls this before the in-tx
    pre-hooks under load (nothing open: each statement autocommits, so the
    scratch table's Sch-M ends with its create) and after them under build.
    Returns the relations the load and the tail need, and the delta setup.
  -#}
  {%- set churn_percent = sqlserver__delta_max_churn_percent() if refresh_method == 'delta' else none -%}
  {%- set refresh_relation = target_relation.incorporate(
      path={"identifier": target_relation.identifier ~ '__dbt_refresh'}
  ) -%}
  {%- set tmp_vw_relation = refresh_relation.incorporate(
      path={"identifier": refresh_relation.identifier ~ '__dbt_tmp_vw'}, type='view'
  ) -%}

  {%- set gone_relation = target_relation.incorporate(
      path={"identifier": target_relation.identifier ~ '__dbt_gone'}
  ) -%}

  {% call statement('dml_refresh_cleanup_pre', auto_begin=False) -%}
    {{ get_use_database_sql(refresh_relation.database) }}
    DROP VIEW IF EXISTS {{ tmp_vw_relation.include(database=False) }};
    DROP TABLE IF EXISTS {{ refresh_relation }};
    DROP TABLE IF EXISTS {{ gone_relation }};
  {%- endcall %}

  {# Build new data into scratch table via temp view (handles CTEs in model SQL) #}
  {% call statement('dml_refresh_create_view', auto_begin=False) -%}
    {{ get_create_view_as_sql(tmp_vw_relation, sql) }}
  {%- endcall %}

  {#- Create the scratch table empty. It is never contract-enforced: contracts
      describe the model's target, and this table exists only to stage rows
      for the swap, which then inserts into the real (already contracted)
      target. -#}
  {% call statement('dml_refresh_create_scratch', auto_begin=False) -%}
    {{ sqlserver__get_create_table_empty_sql(refresh_relation, tmp_vw_relation, sql, false) }}
  {%- endcall %}

  {#- delta: the table of rows to delete, created empty here for the same
      reason as the scratch table (its Sch-M must not join the swap's
      transaction), and shaped from the target's keys. Skipped on a schema
      change, which takes the rename-swap: the target's columns, delta_key
      and the comparability checks describe the table being replaced. -#}
  {%- set use_delta = false -%}
  {%- set delta_key = [] -%}
  {%- set delta_types = {} -%}
  {%- set schema = check_for_schema_changes(refresh_relation, target_relation) if refresh_method == 'delta' else none -%}
  {% if schema and not schema['schema_changed'] %}
    {%- set target_columns = schema['target_columns'] -%}
    {%- set delta_key = sqlserver__delta_key_columns(target_columns, target_relation) -%}
    {%- set probe = sqlserver__delta_probe(target_relation, refresh_relation) -%}
    {%- set delta_types = probe['types'] -%}
    {%- set blocker = sqlserver__delta_blocker(target_columns, delta_types, probe['conflicts']) -%}
    {% if blocker %}
      {% do exceptions.warn(
        "table_refresh_method 'delta' cannot compare " ~ target_relation ~ ": " ~ blocker
        ~ "; refreshing with a full DELETE + INSERT instead."
      ) %}
    {% else %}
      {%- set use_delta = true -%}
      {% call statement('dml_refresh_create_gone', auto_begin=False) -%}
        SELECT TOP 0 {{ sqlserver__delta_keys(target_columns, delta_types, 'l') }}, CAST(0 AS int) AS [__dbt_ck]
        INTO {{ gone_relation }}
        FROM {{ target_relation }} l;
      {%- endcall %}
    {% endif %}
  {% endif %}

  {{ return({
    'refresh_relation': refresh_relation,
    'tmp_vw_relation': tmp_vw_relation,
    'gone_relation': gone_relation,
    'use_delta': use_delta,
    'delta_key': delta_key,
    'delta_types': delta_types,
    'churn_percent': churn_percent
  }) }}
{% endmacro %}


{% macro sqlserver__table_dml_refresh(target_relation, sql, stage) %}
  {#-
    XACT_ABORT. The DELETE + INSERT swap below is only safe because every
    connection runs SET XACT_ABORT ON at session level (#718): without it a
    run-time error in the INSERT aborts that statement only, and the DELETE
    can still commit, silently emptying the target. Do not add a per-macro
    SET, and do not fold the swap back into an unguarded batch.

    Flow (#819). Sch-M conflicts with the Sch-S every metadata reader takes,
    so no statement that holds one may share a transaction with the load.

      stage (table.sql, before the in-tx hooks)            autocommit
      |- DROP leftovers, CREATE VIEW, SELECT TOP 0 * INTO   Sch-M ends per statement
      BEGIN                        first in-tx pre-hook, else the swap
      |- INSERT scratch WITH (TABLOCK)                      X table lock only
      |- schema compare                                     read-only probes
      |- DELETE target + INSERT target                      X on the target
      |- in-tx post-hooks                                   atomic with the swap
      COMMIT                       table.sql
      |- DROP VIEW, reconcile indexes, masks, DROP scratch, grants, docs

    RCSI keeps concurrent readers on the old rows until COMMIT. On a schema
    change the swap is skipped and the scratch table is rebuilt and renamed
    into place instead (below). The scratch table is a real table with a
    __dbt_refresh suffix, not a global temp table, so it is visible for the
    schema compare and droppable by name on the next run.
  -#}

  {%- set refresh_relation = stage['refresh_relation'] -%}
  {%- set tmp_vw_relation = stage['tmp_vw_relation'] -%}

  {#- Query hint for the grant-taking data-movement statements below (the scratch
      load and the swap INSERT; not the empty create, which moves no rows).
      get_query_options() emits the OPTION (...) clause and terminates it with
      ';', matching how create_table_as appends it. -#}
  {%- set query_label = get_query_options(parse_options=True) -%}

  {#- Named 'main' because dbt requires a statement('main') call in every
      materialization, and this is the statement worth having there: it is the
      one that moves the rows, so adapter_response still reports a meaningful
      row count. -#}
  {% call statement('main', auto_begin=False) -%}
    {{ sqlserver__get_tablock_insert_sql(refresh_relation, tmp_vw_relation, query_label, false) }}
  {%- endcall %}

  {# Compare schemas: if columns differ, fall back to rename-swap #}
  {%- set schema_changes = check_for_schema_changes(refresh_relation, target_relation) -%}
  {%- set schema_match = not schema_changes['schema_changed'] -%}

  {#- Resolve the mask map once; applied per-branch below at the right point
      relative to index creation (see apply_masks). -#}
  {%- set mask_config = adapter.resolve_masks(model, config.get('masks')) -%}

  {% if schema_match %}
    {# Use the target's physical column order for both INSERT and SELECT. #}
    {# The scratch table has the same columns but possibly in a different order, #}
    {# so naming columns explicitly makes the swap order-independent. #}
    {%- set target_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set column_list = target_columns | map(attribute='quoted') | join(', ') -%}

    {#- The swap. With dbt-managed transactions off, the in-batch
        BEGIN/COMMIT makes it atomic; with them on (default) the transaction
        is opened here unless a pre-hook already did (by the dml statement's
        auto_begin; explicitly for delta, whose statement fetches a result
        and so declines auto_begin), and table.sql commits it after the
        in-tx post-hooks. -#}
    {% if stage['use_delta'] %}
      {%- set gone_relation = stage['gone_relation'] -%}
      {%- set types = stage['delta_types'] -%}
      {%- set gone_names = sqlserver__delta_key_names(target_columns, types) -%}
      {#- delta: delete only the target rows the new build lacks and insert
          only the rows the target lacks. Falls back to the full swap, in the
          same transaction, when:
            duplicates  scratch has rows equal on every key: set operations
                        would collapse them (exact check, see
                        sqlserver__delta_dupes_sql)
            churn       more than delta_max_churn_percent of rows differ:
                        past that the full swap is cheaper; TOP stops the
                        EXCEPT once it is clear
            count       target row count differs after the delta: it held
                        duplicates, or a CHECKSUM missed an equal row
          The CHECKSUM equality gives the delete a hashable join; INTERSECT
          is the exact (null-safe) check. Without it the delete is a
          correlated, unhashable semi-join, one gone scan per target row.
          NOCOUNT makes the path the batch's only result set. -#}
      {% do adapter.begin_if_closed() %}
      {% call statement('dml_refresh_swap', fetch_result=True, auto_begin=False) -%}
        SET NOCOUNT ON;
        {% if not adapter.behavior.dbt_sqlserver_use_dbt_transactions %}
        BEGIN TRANSACTION;
        {% endif %}
        DECLARE @dbt_rows bigint = (SELECT COUNT_BIG(*) FROM {{ refresh_relation }});
        DECLARE @dbt_path varchar(16) = 'delta';
        DECLARE @dbt_cap bigint = CAST(@dbt_rows * {{ stage['churn_percent'] }} / 100.0 AS bigint);

        {{ sqlserver__delta_dupes_sql(target_columns, types, refresh_relation, stage['delta_key'], query_label) }}
        IF @dbt_dupes > 0
          SET @dbt_path = 'duplicates';

        IF @dbt_path = 'delta'
        BEGIN
          INSERT TOP (@dbt_cap + 1) INTO {{ gone_relation }} WITH (TABLOCK) ({{ gone_names }}, [__dbt_ck])
          SELECT {{ sqlserver__delta_key_names(target_columns, types, 'd') }}, CHECKSUM({{ sqlserver__delta_key_names(target_columns, types, 'd') }})
          FROM (SELECT {{ sqlserver__delta_keys(target_columns, types, 'l') }} FROM {{ target_relation }} l WITH (TABLOCKX)
                EXCEPT
                SELECT {{ sqlserver__delta_keys(target_columns, types, 's') }} FROM {{ refresh_relation }} s) d
          {{ query_label }}
          IF @@ROWCOUNT > @dbt_cap
            SET @dbt_path = 'churn';
        END;

        IF @dbt_path = 'delta'
        BEGIN
          DELETE l FROM {{ target_relation }} l
          WHERE EXISTS (
            SELECT 1 FROM {{ gone_relation }} g
            WHERE g.[__dbt_ck] = CHECKSUM({{ sqlserver__delta_keys(target_columns, types, 'l', named=false) }})
              AND EXISTS (SELECT {{ sqlserver__delta_key_names(target_columns, types, 'g') }}
                          INTERSECT
                          SELECT {{ sqlserver__delta_keys(target_columns, types, 'l', named=false) }}))
          {{ query_label }}

          INSERT INTO {{ target_relation }} ({{ column_list }})
          SELECT {{ column_list }} FROM (
            SELECT {{ sqlserver__delta_keys(target_columns, types, 's') }} FROM {{ refresh_relation }} s
            EXCEPT
            SELECT {{ sqlserver__delta_keys(target_columns, types, 'l') }} FROM {{ target_relation }} l) d
          {{ query_label }}

          IF (SELECT COUNT_BIG(*) FROM {{ target_relation }}) <> @dbt_rows
            SET @dbt_path = 'count';
        END;

        IF @dbt_path <> 'delta'
        BEGIN
          DELETE FROM {{ target_relation }};
          INSERT INTO {{ target_relation }} ({{ column_list }})
            SELECT {{ column_list }} FROM {{ refresh_relation }} {{ query_label }}
        END;
        {% if not adapter.behavior.dbt_sqlserver_use_dbt_transactions %}
        COMMIT TRANSACTION;
        {% endif %}
        SET NOCOUNT OFF;
        SELECT @dbt_path AS refresh_path;
      {%- endcall %}
      {%- set refresh_path = load_result('dml_refresh_swap')['table'].rows[0][0] -%}
      {% if refresh_path != 'delta' %}
        {{ log("delta refresh of " ~ target_relation ~ " fell back to the full swap: " ~ refresh_path, info=true) }}
      {% endif %}
    {% else %}
    {% call statement('dml_refresh_swap') -%}
      {% if not adapter.behavior.dbt_sqlserver_use_dbt_transactions %}
      BEGIN TRANSACTION;
      {% endif %}
      DELETE FROM {{ target_relation }};
      INSERT INTO {{ target_relation }} ({{ column_list }})
        SELECT {{ column_list }} FROM {{ refresh_relation }} {{ query_label }}
      {% if not adapter.behavior.dbt_sqlserver_use_dbt_transactions %}
      COMMIT TRANSACTION;
      {% endif %}
    {%- endcall %}
    {% endif %}

    {#- Deliberately left open: table.sql commits after the in-tx post-hooks,
        so a transaction: true post-hook is atomic with the swap. Index and
        mask reconciliation run on the tail, outside; if they fail the new
        rows are committed with indexes not yet converged, and the next run
        converges them (both reconcile against config, not a delta). -#}

  {% else %}
    {# Schema changed — fall back to rename-swap for this run #}
    {{ log("Schema change detected for " ~ target_relation ~ " — falling back to rename-swap", info=true) }}

    {#- The scratch load joined no transaction of its own; open one for this
        branch's renames and drops, which stay transactional. -#}
    {% do adapter.begin_if_closed() %}

    {%- set backup_relation_type = target_relation.type -%}
    {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
    {{ drop_relation_if_exists(backup_relation) }}

    {#- The scratch table above came from the empty-create load (SELECT TOP 0
        INTO plus the TABLOCK insert), which is the right shape for the schema
        probe and the wrong one for the object that is about to be renamed into
        position: it copies no constraint and no index, and takes nullability
        from the query rather than from a contract. Left as-is it silently
        strips the model of its clustered columnstore index - create_indexes
        only builds what the `indexes`
        config names, never the as_columnstore CCI - and, under a contract, of
        its NOT NULLs and inline constraints too. None of it came back on a
        later run, because every later run matched the new schema and took the
        DELETE+INSERT path above.

        So rebuild it the way this adapter builds every other table. The
        rebuild belongs on this branch alone: doing it up front would build,
        and then throw away, a full columnstore index on every steady-state
        refresh, which on a large table dominates the run. A schema change is
        rare, so one extra build here is much the cheaper trade.

        It is not free, though: the model's SQL runs a second time here, the
        scratch load above having already run it once as the schema probe.
        Any side effect in that SQL therefore happens twice, and the two runs
        are not interchangeable - the schema decision came from the first, the
        table renamed into position comes from the second. A model whose column
        shape can differ between them lands the second shape unchecked, unless
        a contract is enforced and create_table_as re-asserts it. Probing the
        tmp view instead of the materialized scratch would collapse the two
        back into one, at the cost of changing how the probe behaves - a
        separate change, not this one.

        create_table_as builds and drops its own __dbt_tmp_vw - the same name
        as the stage's view, which it replaces. -#}
    {{ drop_relation_if_exists(refresh_relation) }}
    {% call statement('dml_refresh_rebuild') -%}
      {{ get_create_table_as_sql(False, refresh_relation, sql) }}
    {%- endcall %}

    {# Rename scratch table into position #}
    {% set existing_relation = load_cached_relation(target_relation) %}
    {% if existing_relation is not none %}
      {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {% endif %}

    {{ adapter.rename_relation(refresh_relation, target_relation) }}

    {#- Freshly rebuilt above (no masks carried), so apply masks before
        create_indexes — a mask cannot be added to a column an index depends
        on (documented for all SQL Server versions). Applied here, inside the
        cutover transaction, rather than on table.sql's tail: this table is new
        and unmasked, so a mask failure after the cutover committed would leave
        it live with the columns exposed. create_indexes runs on the tail
        instead; index_strategy='create' keeps the mask-then-index order. -#}
    {% do apply_masks(target_relation, mask_config) %}

    {{ drop_relation_if_exists(backup_relation) }}

    {# scratch table is now the target, nothing to drop #}
  {% endif %}

  {#- schema_match picks the tail's index strategy: reconcile on the swap
      path, create on the fallback (masked above, mask-then-index order).
      refresh_relation is the scratch table for the tail to drop after the
      commit; none on the fallback, where it was renamed into the target. -#}
  {{ return({
    'schema_match': schema_match,
    'refresh_relation': refresh_relation if schema_match else none
  }) }}
{% endmacro %}
