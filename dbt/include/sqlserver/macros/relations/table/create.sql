{% macro sqlserver__get_create_table_empty_sql(relation, tmp_relation, sql, contract_enforced) -%}
    {#-
      SQL that creates `relation` empty, with no rows loaded. Pair it with
      sqlserver__get_tablock_insert_sql to build a table in two statements
      instead of one fused `SELECT * INTO`.

      Why the split matters: a fused `SELECT ... INTO` both creates the object
      and loads it, so SQL Server holds the object's Sch-M lock from the moment
      the statement starts until it finishes. Sch-M is the one mode
      incompatible with the Sch-S lock every metadata reader takes, so a slow
      load blocks metadata readers in *other* sessions for its whole duration
      (dbt-msft/dbt-sqlserver#819). Creating the object empty takes Sch-M for
      an instant; the load that follows takes no Sch-M at all.

      Splitting only pays off when the two statements do not share an open
      transaction - locks are held to commit, not to end-of-statement - so
      callers must either run the pair in autocommit or commit between them.
      See each call site for how it does that.

      Returns SQL; it executes nothing, wraps nothing in EXEC() and manages no
      transaction, so callers keep control of batching and lock boundaries.

      `contract_enforced` is a parameter rather than a config lookup on
      purpose: sqlserver__create_table_as suppresses contracts for temporary
      relations, so the call sites do not agree on how to derive it. Deriving
      it here would silently change behaviour for temp builds.

      Only valid inside a model materialization: the contract branch reads the
      ambient `model` context var via get_assert_columns_equivalent, which also
      raises on a column mismatch. Keep that assertion in this macro only, so
      it fires exactly once per build.
    -#}
    {%- if contract_enforced -%}
        CREATE TABLE {{ relation }}
        {{ get_assert_columns_equivalent(sql) }}
        {{ build_columns_constraints(relation) }}
    {%- else -%}
        SELECT TOP 0 * INTO {{ relation }} FROM {{ tmp_relation }}
    {%- endif -%}
{%- endmacro %}


{% macro sqlserver__get_tablock_insert_sql(relation, tmp_relation, query_label, contract_enforced) -%}
    {#-
      SQL that bulk-loads `relation` from `tmp_relation`. The companion to
      sqlserver__get_create_table_empty_sql above; see it for why the create
      and the load are separate statements.

      WITH (TABLOCK) is what keeps this minimally logged under the simple and
      bulk-logged recovery models, matching the `SELECT * INTO` it replaces:
      minimal logging needs a table lock on a heap with no nonclustered
      indexes. Do not drop the hint to "reduce blocking" - it costs log volume,
      and an X table lock is compatible with Sch-S anyway, so it never blocks
      the metadata readers this split exists to protect.

      `query_label` is the OPTION (...) clause from
      get_query_options(parse_options=True). It rides the data-movement
      statement, not the empty create, because that is the statement whose plan
      takes a memory grant and whose label people search for.

      Returns SQL; executes nothing. `contract_enforced` is a parameter for the
      reason given on the create macro.

      Only valid inside a model materialization: the contract branch reads the
      ambient `model` context var for its column list.
    -#}
    {%- if contract_enforced -%}
        {%- set list_columns -%}
            {%- for column in model['columns'] -%}
                {{ adapter.quote(column) }}{{ ", " if not loop.last }}
            {%- endfor -%}
        {%- endset -%}
        INSERT INTO {{ relation }} WITH (TABLOCK) ({{ list_columns }})
        SELECT {{ list_columns }} FROM {{ tmp_relation }} {{ query_label }}
    {%- else -%}
        INSERT INTO {{ relation }} WITH (TABLOCK)
        SELECT * FROM {{ tmp_relation }} {{ query_label }}
    {%- endif -%}
{%- endmacro %}


{% macro sqlserver__get_create_table_stage_sql(temporary, relation, sql) -%}
    {#-
      First half of a table build: everything up to and including creating the
      object, loading no rows. Pair with sqlserver__get_create_table_load_sql.

      The split exists so a caller can put a transaction boundary between
      creating the object and loading it. Locks are held to commit, not to
      end-of-statement, so an empty CREATE that shares a transaction with the
      load holds the new object's Sch-M for the whole load - blocking every
      metadata reader in every other session (#819). Committing after this half
      releases it in an instant, since SELECT TOP 0 moves no rows.

      Callers that want one batch call sqlserver__create_table_as, which is
      exactly this half followed by the load half; it emits the same statements
      in the same order, so snapshots and the incremental temp build are
      unaffected by the split.

      Note the render-time `adapter.drop_relation` below: it is a side effect,
      not emitted SQL, and lives here so it fires exactly once per build
      whether the halves are rendered separately or through create_table_as.
    -#}
    {%- set full_refresh_build = config.get('full_refresh_build', 'heap_then_index') -%}
    {%- if full_refresh_build not in ['heap_then_index', 'prebuilt'] -%}
      {{ exceptions.raise_compiler_error(
        "Invalid full_refresh_build '" ~ full_refresh_build ~ "'. "
        "Valid values are: 'heap_then_index' (default), 'prebuilt'."
      ) }}
    {%- endif -%}
    {%- set tmp_relation = relation.incorporate(path={"identifier": relation.identifier ~ '__dbt_tmp_vw'}, type='view') -%}

    {#- Now that the incremental temp build commits standalone (see
        incremental.sql), a crash can leave a throwaway table behind and the
        SELECT * INTO below would hit Msg 2714. Drop it first, but only for
        adapter-generated throwaways: `temporary` covers the incremental
        __dbt_temp build, the suffix covers the full-refresh / table-refresh
        __dbt_tmp intermediate. Suffix match is exact, never substring, so a
        user model named stg__dbt_tmp_x is untouched.
        Never guard a fresh-create of the real target: dbt has decided that
        table does not exist, so 2714 must still surface rather than silently
        destroying an object dbt does not know about. -#}
    {%- set _ident = relation.identifier -%}
    {%- set build_into_temp = temporary or _ident.endswith('__dbt_tmp') or _ident.endswith('__dbt_tmp_vw') -%}

    {%- do adapter.drop_relation(tmp_relation) -%}
    {{ get_use_database_sql(relation.database) }}
    {{ get_create_view_as_sql(tmp_relation, sql) }}

    {%- set contract_config = config.get('contract') -%}
    {#- not plain `contract_config.enforced`: contracts are suppressed for temp
        builds, and the shared macros take the resolved flag as a parameter -#}
    {%- set contract_enforced = contract_config.enforced and (not temporary) -%}

    {#- EXEC(), not a bare statement: CREATE VIEW above must be the first
        statement in its batch, and a bare create referencing that just-made
        view would fail compilation in the same batch. Deferring through EXEC
        is what lets both live in one batch when the halves are concatenated. -#}
    {%- set query -%}
        {#- the drop guard is deliberately not applied on the contract branch,
            which has never had one -#}
        {%- if build_into_temp and not contract_enforced -%}
        IF OBJECT_ID('{{ escape_single_quotes(relation.include(database=False)) }}', 'U') IS NOT NULL
            EXEC('DROP TABLE {{ relation }}');
        {%- endif -%}
        {{ sqlserver__get_create_table_empty_sql(relation, tmp_relation, sql, contract_enforced) }}
    {%- endset -%}

    EXEC('{{- escape_single_quotes(query) -}}')
{%- endmacro %}


{% macro sqlserver__get_create_table_load_sql(temporary, relation, sql, drop_tmp_view=True) -%}
    {#-
      Second half of a table build: load the object the stage half created,
      then clean up and add the clustered columnstore index.

      The INSERT takes an X table lock, never Sch-M, so it cannot block the
      metadata readers #819 is about - which is why it is safe for this half to
      run long, inside a transaction or not.

      The tmp view drop lives here, not with the create: the INSERT reads that
      view, so dropping it in the stage half would break a split build. It
      trails the INSERT in the same batch by default. A caller whose load runs
      inside a pre-hook's transaction passes drop_tmp_view=False and drops the
      view after that transaction commits: an uncommitted DROP VIEW holds Sch-M
      on the view, and a database-wide catalog scan blocks on that just as it
      does on a table (#819). The stage half re-creates the view with
      CREATE OR ALTER after a render-time drop, so a view left behind by a
      failed run is harmless.
    -#}
    {%- set query_label = get_query_options(parse_options=True) -%}
    {%- set tmp_relation = relation.incorporate(path={"identifier": relation.identifier ~ '__dbt_tmp_vw'}, type='view') -%}

    {%- set contract_config = config.get('contract') -%}
    {%- set contract_enforced = contract_config.enforced and (not temporary) -%}

    {#- EXEC() for the same batching reason as the stage half: concatenated,
        this statement still follows CREATE VIEW inside one batch. -#}
    {%- set query -%}
        {{ sqlserver__get_tablock_insert_sql(relation, tmp_relation, query_label, contract_enforced) }}
    {%- endset -%}

    EXEC('{{- escape_single_quotes(query) -}}')

    {% if drop_tmp_view %}
    {# For some reason drop_relation is not firing. This solves the issue for now. #}
    EXEC('DROP VIEW IF EXISTS {{ tmp_relation.include(database=False) }}')
    {% endif %}

    {% set as_columnstore = config.get('as_columnstore', default=true) %}
    {% if not temporary and as_columnstore -%}
        {#-
        add columnstore index
        this creates with dbt_temp as its coming from a temporary relation before renaming
        could alter relation to drop the dbt_temp portion if needed
        -#}
        {{ sqlserver__create_clustered_columnstore_index(relation) }}
   {% endif %}
{%- endmacro %}


{% macro sqlserver__create_table_as(temporary, relation, sql) -%}
    {#-
      One-batch table build: the stage and load halves back to back.

      The halves are the source of truth; this is their concatenation, so
      callers that run the whole thing as a single statement (snapshots,
      the incremental temp build) get the same statements in the same order
      as before the split. Callers that need a transaction boundary between
      creating the object and loading it call the halves directly - see
      sqlserver__get_create_table_stage_sql for why that matters (#819).
    -#}
    {{ sqlserver__get_create_table_stage_sql(temporary, relation, sql) }}
    {{ sqlserver__get_create_table_load_sql(temporary, relation, sql) }}
{% endmacro %}


{% macro sqlserver__create_table_as_prebuilt(relation, sql) -%}
    {#-
      In-place build for full_refresh_build=prebuilt: create the table empty
      under its final name with its clustered design (the as_columnstore CCI
      or the clustered entry from the indexes config), then bulk-load it via
      INSERT WITH (TABLOCK). The create is idempotent: it drops any physical
      target first (see the OBJECT_ID guard below), so it is safe even when
      dbt's relation cache is stale.
      See the CHANGELOG for the trade-offs of this build mode.
    -#}
    {%- set query_label = get_query_options(parse_options=True) -%}
    {%- set as_columnstore = config.get('as_columnstore', default=true) -%}
    {%- set contract_config = config.get('contract') -%}
    {%- set contract_enforced = contract_config.enforced -%}
    {%- set tmp_relation = relation.incorporate(path={"identifier": relation.identifier ~ '__dbt_tmp_vw'}, type='view') -%}

    {#- find the clustered entry (validate_indexes guarantees at most one);
        without one the table loads in place as a heap -#}
    {%- set prebuilt_ns = namespace(clustered_dict=none) -%}
    {%- if not as_columnstore -%}
        {%- for raw_index in config.get('indexes', default=[]) -%}
            {%- set parsed = adapter.parse_index(raw_index) -%}
            {%- if parsed and parsed.type == 'clustered' and prebuilt_ns.clustered_dict is none -%}
                {%- set prebuilt_ns.clustered_dict = raw_index -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {%- set has_clustered_design = as_columnstore or prebuilt_ns.clustered_dict is not none -%}
    {%- if has_clustered_design -%}
        {{ log("Rebuilding " ~ relation ~ " in place with full_refresh_build=prebuilt", info=true) }}
    {%- else -%}
        {% do log("full_refresh_build=prebuilt on " ~ relation ~ " has no clustered index in the indexes config; loading in place as a heap") %}
    {%- endif -%}

    {%- do adapter.drop_relation(tmp_relation) -%}

    {#- Setup runs as its own statement (named 'main', so dbt's compiled-SQL
        artifacts and adapter_response still resolve normally), separate from
        the load below: the extended-property marker set here exists to
        survive a failed load, so it must not share a transaction with it.
        With dbt_sqlserver_use_dbt_transactions on (default), the
        materialization's whole build - including this statement - runs
        inside one continuous ambient transaction; without the split below, a
        later failure during the load would roll the marker back right along
        with it, defeating the point. -#}
    {%- set setup_sql -%}
        {{ get_use_database_sql(relation.database) }}
        {{ get_create_view_as_sql(tmp_relation, sql) }}

        {#- drop any physical target before the bare CREATE / SELECT ... INTO
            below. OBJECT_ID reads the database directly, so this is robust to
            dbt's relation cache being stale (reporting none while the table
            exists) - which would otherwise collide with Msg 2714 (object
            already exists) -#}
        IF OBJECT_ID('{{ escape_single_quotes(relation.include(database=False)) }}', 'U') IS NOT NULL
            EXEC('DROP TABLE {{ relation }}');

        {#- escape_single_quotes now covers both branches: the empty create is
            built from rendered relation names, and an identifier carrying a
            single quote would otherwise break out of the EXEC literal -#}
        {%- set ddl_query = sqlserver__get_create_table_empty_sql(relation, tmp_relation, sql, contract_enforced) -%}
        EXEC('{{- escape_single_quotes(ddl_query) -}}')

        {# mark the rebuild in progress; removed atomically with the load below #}
        EXEC sp_addextendedproperty @name = N'dbt_full_refresh_incomplete', @value = '1',
            @level0type = N'SCHEMA', @level0name = N'{{ escape_single_quotes(relation.schema) }}',
            @level1type = N'TABLE', @level1name = N'{{ escape_single_quotes(relation.identifier) }}'
    {%- endset %}
    {% call statement('main') -%}
        {{ setup_sql }}
    {%- endcall %}

    {#- Commit the marker onto its own. 'main' above always opens a
        transaction (default auto_begin), so this always actually commits
        here - and with it any transaction: true pre-hook, which is why 'build'
        scope cannot deliver rollback on this path (docs/transaction_scope.md).
        The load below then runs AUTOCOMMITTED (auto_begin=False, nothing
        open): the clustered design is created on the empty table and its
        Sch-M released as that statement ends, and the INSERT holds only an X
        table lock. Reopened inside the ambient transaction instead, that
        Sch-M sits on the LIVE name until the materialization's trailing
        commit - the whole load, plus masks and post-hooks (#819). The in-batch
        BEGIN/COMMIT around the INSERT and the marker drop is a real
        transaction under autocommit, which is exactly what their atomicity
        needs. begin_if_closed afterwards leaves later code (masks,
        post-hooks, the trailing adapter.commit()) a transaction to run in. -#}
    {{ adapter.commit_if_open() }}

    {%- set load_sql -%}
    {% if as_columnstore %}
        {{ sqlserver__create_clustered_columnstore_index(relation) }}
    {% elif prebuilt_ns.clustered_dict is not none %}
        {{ sqlserver__get_create_index_sql(relation, prebuilt_ns.clustered_dict) }}
    {% endif %}

    {%- set insert_statement = sqlserver__get_tablock_insert_sql(relation, tmp_relation, query_label, contract_enforced) %}
    {#- load and unmark atomically: any failure rolls back and keeps the marker.
        Relies on the session-level SET XACT_ABORT ON applied at connection
        open (see #718) so a run-time error here rolls back instead of
        falling through to COMMIT; EXEC('...') runs in the same session, so
        that setting still applies inside this dynamic SQL sub-batch. -#}
    {%- set insert_query -%}
        BEGIN TRANSACTION;
        {{ insert_statement }};
        EXEC sp_dropextendedproperty @name = N'dbt_full_refresh_incomplete',
            @level0type = N'SCHEMA', @level0name = N'{{ escape_single_quotes(relation.schema) }}',
            @level1type = N'TABLE', @level1name = N'{{ escape_single_quotes(relation.identifier) }}';
        COMMIT TRANSACTION;
    {%- endset %}
    EXEC('{{- escape_single_quotes(insert_query) -}}')

    EXEC('DROP VIEW IF EXISTS {{ tmp_relation.include(database=False) }}')
    {%- endset %}
    {% call statement('create_table_as_prebuilt_load', auto_begin=False) -%}
        {{ load_sql }}
    {%- endcall %}
    {{ adapter.begin_if_closed() }}
{% endmacro %}


{% macro sqlserver__mark_full_refresh_incomplete(relation) -%}
    {#- mark a table as having a full refresh in flight; the marker rides the
        object, so a successful swap (old table dropped) or in-place rebuild
        (table dropped) clears it naturally -#}
    {% do run_query(
        "if not exists (select 1 from sys.extended_properties where major_id = OBJECT_ID('"
        ~ relation.schema ~ "." ~ relation.identifier
        ~ "') and name = 'dbt_full_refresh_incomplete')"
        ~ " EXEC sp_addextendedproperty @name = N'dbt_full_refresh_incomplete', @value = '1',"
        ~ " @level0type = N'SCHEMA', @level0name = N'" ~ relation.schema ~ "',"
        ~ " @level1type = N'TABLE', @level1name = N'" ~ relation.identifier ~ "'"
    ) %}
    {#- This marker exists to survive a failed rebuild, so it must not ride on
        the same transaction as the rebuild it's guarding: if a prior
        statement this run (e.g. a pre-hook) already opened the ambient
        dbt-managed transaction, a later failure would roll the marker back
        right along with it, defeating the point. Commit it - a no-op when
        run_query above ran standalone, i.e. the common case of this being the
        first statement of the run. Deliberately NOT reopened: the load that
        follows must autocommit so the new table's Sch-M is not held to the
        materialization's trailing commit (#819); every statement after it
        that needs a transaction opens its own (default auto_begin), and the
        materialization's tail states its precondition with begin_if_closed
        before adapter.commit(). -#}
    {{ adapter.commit_if_open() }}
{%- endmacro %}


{% macro sqlserver__assert_no_incomplete_full_refresh(relation) -%}
    {%- set marker = run_query(
        "select count(*) as marker_count from sys.extended_properties where major_id = OBJECT_ID('"
        ~ relation.schema ~ "." ~ relation.identifier
        ~ "') and name = 'dbt_full_refresh_incomplete'"
    ) -%}
    {%- if marker.rows[0][0] > 0 -%}
        {{ exceptions.raise_compiler_error(
            "A previous full refresh of " ~ relation
            ~ " did not complete; the table may be stale, empty or partially "
            "loaded. Rerun with --full-refresh."
        ) }}
    {%- endif -%}
{%- endmacro %}


{% macro sqlserver__assert_no_unguarded_self_reference(relation, compiled_sql) -%}
    {#- a self-reference that survives full-refresh compilation (where
        is_incremental() is false) would run against a table prebuilt has
        already dropped: fail fast while the data is intact -#}
    {%- if relation.render() | lower in compiled_sql | lower -%}
        {{ exceptions.raise_compiler_error(
            "Model " ~ relation ~ " contains an unguarded self-reference "
            "({{ this }}) and full_refresh_build=prebuilt drops the table "
            "before rebuilding. Guard the reference with is_incremental(), "
            "or keep the default heap_then_index."
        ) }}
    {%- endif -%}
{%- endmacro %}
