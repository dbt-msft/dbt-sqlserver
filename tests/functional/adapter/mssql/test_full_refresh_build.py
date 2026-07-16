import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture

models__invalid_value_sql = """
{{
  config(
    materialized = "table",
    full_refresh_build = "bogus"
  )
}}

select 1 as column_a

"""


class TestFullRefreshBuildInvalid:
    @pytest.fixture(scope="class")
    def models(self):
        return {"invalid_value.sql": models__invalid_value_sql}

    def test_invalid_value(self, project):
        _, output = run_dbt_and_capture(["run", "--models", "invalid_value"], expect_pass=False)
        assert "Invalid full_refresh_build" in output
        assert "bogus" in output
        assert "heap_then_index" in output
        assert "prebuilt" in output


models__cci_prebuilt_sql = """
{{
  config(
    materialized = "table",
    full_refresh_build = "prebuilt"
  )
}}

select *
from (
  select 1 as column_a, 2 as column_b
  union all
  select 3, 4
) ordered_inner
order by column_a
offset 0 rows

"""


def get_cci_indexes(project, unique_schema, table_name):
    sql = f"""
    select i.[name], i.type_desc
    from sys.indexes i
    where i.object_id = OBJECT_ID('{unique_schema}.{table_name}')
      and i.index_id > 0
    """
    return project.run_sql(sql, fetch="all")


class TestFullRefreshBuildColumnstore:
    @pytest.fixture(scope="class")
    def models(self):
        return {"cci_prebuilt.sql": models__cci_prebuilt_sql}

    def test_cci_prebuilt_lifecycle(self, project, unique_schema):
        # tiny row counts land in delta rowgroups, so assert physical
        # design rather than rowgroup state
        _, output = run_dbt_and_capture(["run", "--models", "cci_prebuilt", "--full-refresh"])
        assert "full_refresh_build=prebuilt" in output

        indexes = get_cci_indexes(project, unique_schema, "cci_prebuilt")
        assert len(indexes) == 1
        assert indexes[0][1] == "CLUSTERED COLUMNSTORE"
        assert "dbt_tmp" not in indexes[0][0]
        first_name = indexes[0][0]

        # data made it through the two-step load
        rows = project.run_sql(f"select count(*) from {unique_schema}.cci_prebuilt", fetch="one")
        assert rows[0] == 2

        # second full refresh: one CCI, stable name, no leftovers
        _, output = run_dbt_and_capture(["run", "--models", "cci_prebuilt", "--full-refresh"])
        assert "full_refresh_build=prebuilt" in output
        indexes = get_cci_indexes(project, unique_schema, "cci_prebuilt")
        assert len(indexes) == 1
        assert indexes[0][0] == first_name

        # normal run: default swap build, no prebuilt
        _, output = run_dbt_and_capture(["run", "--models", "cci_prebuilt"])
        assert "full_refresh_build=prebuilt" not in output
        assert len(get_cci_indexes(project, unique_schema, "cci_prebuilt")) == 1
        leftovers = project.run_sql(
            f"""select count(*) from sys.tables t
                join sys.schemas s on s.schema_id = t.schema_id
                where s.name = '{unique_schema}'
                and (t.name like '%__dbt_tmp%' or t.name like '%__dbt_backup%')""",
            fetch="one",
        )
        assert leftovers[0] == 0


models__rowstore_prebuilt_sql = """
{{
  config(
    materialized = "table",
    as_columnstore = False,
    full_refresh_build = "prebuilt",
    indexes=[
      {'columns': ['column_b'], 'type': 'clustered', 'data_compression': 'page'},
      {'columns': ['column_a'], 'type': 'nonclustered'},
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""

models__fallback_no_clustered_sql = """
{{
  config(
    materialized = "table",
    as_columnstore = False,
    full_refresh_build = "prebuilt",
    indexes=[
      {'columns': ['column_a'], 'type': 'nonclustered'},
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""

models__prebuilt_default_cci_clustered_sql = """
{{
  config(
    materialized = "table",
    full_refresh_build = "prebuilt",
    indexes=[
      {'columns': ['column_a'], 'type': 'clustered'},
    ]
  )
}}

select 1 as column_a

"""


def get_rowstore_indexes(project, unique_schema, table_name):
    sql = f"""
    select i.[name], i.type_desc, isnull(max(p.data_compression_desc), '') as compression
    from sys.indexes i
    left join sys.partitions p
      on p.object_id = i.object_id and p.index_id = i.index_id
    where i.object_id = OBJECT_ID('{unique_schema}.{table_name}')
      and i.index_id > 0
    group by i.[name], i.type_desc
    """
    return {row[1]: (row[0], row[2]) for row in project.run_sql(sql, fetch="all")}


class TestFullRefreshBuildRowstore:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "rowstore_prebuilt.sql": models__rowstore_prebuilt_sql,
            "fallback_no_clustered.sql": models__fallback_no_clustered_sql,
            "prebuilt_default_cci_clustered.sql": models__prebuilt_default_cci_clustered_sql,
        }

    def test_rowstore_prebuilt(self, project, unique_schema):
        _, output = run_dbt_and_capture(["run", "--models", "rowstore_prebuilt", "--full-refresh"])
        assert "full_refresh_build=prebuilt" in output

        by_type = get_rowstore_indexes(project, unique_schema, "rowstore_prebuilt")
        assert set(by_type) == {"CLUSTERED", "NONCLUSTERED"}

        clustered_name, clustered_compression = by_type["CLUSTERED"]
        assert clustered_name.startswith("dbt_idx_")
        # compress-on-insert
        assert clustered_compression == "PAGE"
        assert by_type["NONCLUSTERED"][0].startswith("dbt_idx_")

        rows = project.run_sql(
            f"select count(*) from {unique_schema}.rowstore_prebuilt", fetch="one"
        )
        assert rows[0] == 1

        # Full-refresh rebuild: stable names, still exactly one clustered.
        run_dbt(["run", "--models", "rowstore_prebuilt", "--full-refresh"])
        second = get_rowstore_indexes(project, unique_schema, "rowstore_prebuilt")
        assert second == by_type

        # Normal run: default swap path, no prebuilt.
        _, output = run_dbt_and_capture(["run", "--models", "rowstore_prebuilt"])
        assert "full_refresh_build=prebuilt" not in output

    def test_fallback_without_clustered(self, project, unique_schema):
        # no clustered index: loads as a heap with no console warning
        _, output = run_dbt_and_capture(
            ["run", "--models", "fallback_no_clustered", "--full-refresh"]
        )
        # the heap-fallback trace is debug level: never on the console
        assert "loading in place as a heap" not in output

        by_type = get_rowstore_indexes(project, unique_schema, "fallback_no_clustered")
        assert set(by_type) == {"NONCLUSTERED"}

        # rerun: still quiet, still heap + NCI
        _, output = run_dbt_and_capture(
            ["run", "--models", "fallback_no_clustered", "--full-refresh"]
        )
        assert "loading in place as a heap" not in output

    def test_prebuilt_clustered_with_default_columnstore_errors(self, project):
        # as_columnstore defaults true: the existing cross-config validation
        # must reject the clustered rowstore entry with guidance.
        _, output = run_dbt_and_capture(
            ["run", "--models", "prebuilt_default_cci_clustered"], expect_pass=False
        )
        assert "as_columnstore" in output


models__incr_prebuilt_sql = """
{{
  config(
    materialized = "incremental",
    as_columnstore = False,
    full_refresh_build = "prebuilt",
    indexes=[
      {'columns': ['column_a'], 'type': 'clustered'},
    ]
  )
}}

select *
from (
  select 1 as column_a, 2 as column_b
) t

{% if is_incremental() %}
    where column_a > (select max(column_a) from {{this}})
{% endif %}

"""

models__contract_prebuilt_sql = """
{{
  config(
    materialized = "table",
    as_columnstore = False,
    full_refresh_build = "prebuilt",
    contract = {"enforced": True},
    indexes=[
      {'columns': ['column_a'], 'type': 'clustered'},
    ]
  )
}}

select 1 as column_a, cast('x' as varchar(10)) as column_b

"""

models__contract_prebuilt_yml = """
version: 2
models:
  - name: contract_prebuilt
    config:
      contract:
        enforced: true
    columns:
      - name: column_a
        data_type: int
      - name: column_b
        data_type: varchar(10)
"""

models__dml_prebuilt_sql = """
{{
  config(
    materialized = "table",
    as_columnstore = False,
    table_refresh_method = "dml",
    full_refresh_build = "prebuilt",
    indexes=[
      {'columns': ['column_a'], 'type': 'clustered'},
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""


class TestFullRefreshBuildIncrementalAndContract:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incr_prebuilt.sql": models__incr_prebuilt_sql,
            "contract_prebuilt.sql": models__contract_prebuilt_sql,
            "contract_prebuilt.yml": models__contract_prebuilt_yml,
            "dml_prebuilt.sql": models__dml_prebuilt_sql,
        }

    def test_incremental_lifecycle(self, project, unique_schema):
        # first build: prebuilt applies (no existing table)
        _, output = run_dbt_and_capture(["run", "--models", "incr_prebuilt"])
        assert "full_refresh_build=prebuilt" in output
        first = get_rowstore_indexes(project, unique_schema, "incr_prebuilt")
        assert set(first) == {"CLUSTERED"}
        assert first["CLUSTERED"][0].startswith("dbt_idx_")

        # plain incremental run: no prebuilt
        _, output = run_dbt_and_capture(["run", "--models", "incr_prebuilt"])
        assert "full_refresh_build=prebuilt" not in output
        assert get_rowstore_indexes(project, unique_schema, "incr_prebuilt") == first

        # full refresh: prebuilt, stable index name
        _, output = run_dbt_and_capture(["run", "--models", "incr_prebuilt", "--full-refresh"])
        assert "full_refresh_build=prebuilt" in output
        assert get_rowstore_indexes(project, unique_schema, "incr_prebuilt") == first

    def test_contract_enforced_prebuilt(self, project, unique_schema):
        _, output = run_dbt_and_capture(["run", "--models", "contract_prebuilt", "--full-refresh"])
        assert "full_refresh_build=prebuilt" in output
        by_type = get_rowstore_indexes(project, unique_schema, "contract_prebuilt")
        assert set(by_type) == {"CLUSTERED"}
        assert by_type["CLUSTERED"][0].startswith("dbt_idx_")

        # stable on full-refresh rerun; normal run keeps the default path
        run_dbt(["run", "--models", "contract_prebuilt", "--full-refresh"])
        assert get_rowstore_indexes(project, unique_schema, "contract_prebuilt") == by_type
        _, output = run_dbt_and_capture(["run", "--models", "contract_prebuilt"])
        assert "full_refresh_build=prebuilt" not in output

    def test_dml_refresh_prebuilt_on_rebuild_boundaries(self, project, unique_schema):
        # first build: prebuilt applies (opted in, indices from birth);
        # dml governs only the steady-state refreshes in between
        _, output = run_dbt_and_capture(["run", "--models", "dml_prebuilt"])
        assert "full_refresh_build=prebuilt" in output
        _, output = run_dbt_and_capture(["run", "--models", "dml_prebuilt"])
        assert "full_refresh_build=prebuilt" not in output
        by_type = get_rowstore_indexes(project, unique_schema, "dml_prebuilt")
        assert set(by_type) == {"CLUSTERED"}
        first_name = by_type["CLUSTERED"][0]

        # --full-refresh is a rebuild boundary: prebuilt wins over dml,
        # the table is dropped and rebuilt in place (a fully-logged
        # whole-table DELETE+INSERT is the wrong tool for a rebuild)
        _, output = run_dbt_and_capture(["run", "--models", "dml_prebuilt", "--full-refresh"])
        assert "full_refresh_build=prebuilt" in output
        by_type = get_rowstore_indexes(project, unique_schema, "dml_prebuilt")
        assert set(by_type) == {"CLUSTERED"}
        assert by_type["CLUSTERED"][0] == first_name
        rows = project.run_sql(f"select count(*) from {unique_schema}.dml_prebuilt", fetch="one")
        assert rows[0] == 1


models__guard_model_sql = """
{{
  config(
    materialized = "incremental",
    as_columnstore = False,
    full_refresh_build = "prebuilt",
    indexes = var('guard_indexes', [{'columns': ['column_a'], 'type': 'clustered'}])
  )
}}

select *
from (
  select 1 as column_a, 2 as column_b
) t

{% if is_incremental() %}
    where column_a > (select max(column_a) from {{this}})
{% endif %}

"""


class TestFullRefreshBuildSafety:
    @pytest.fixture(scope="class")
    def models(self):
        return {"guard_model.sql": models__guard_model_sql}

    def row_count(self, project, unique_schema):
        return project.run_sql(f"select count(*) from {unique_schema}.guard_model", fetch="one")[0]

    def test_invalid_config_validated_before_drop(self, project, unique_schema):
        run_dbt(["run", "--models", "guard_model"])
        # an out-of-band row the model cannot regenerate: proves the OLD
        # table survived rather than being dropped and rebuilt
        project.run_sql(f"insert into {unique_schema}.guard_model values (99, 99)")
        assert self.row_count(project, unique_schema) == 2

        # An invalid index set (two clustered entries) must fail BEFORE the
        # old table is dropped.
        bad = (
            "[{'columns': ['column_a'], 'type': 'clustered'},"
            " {'columns': ['column_b'], 'type': 'clustered'}]"
        )
        run_dbt_and_capture(
            [
                "run",
                "--models",
                "guard_model",
                "--full-refresh",
                "--vars",
                f"guard_indexes: {bad}",
            ],
            expect_pass=False,
        )
        assert self.row_count(project, unique_schema) == 2

    def test_failed_rebuild_blocks_normal_incremental_run(self, project, unique_schema):
        run_dbt(["run", "--models", "guard_model"])

        # Simulate a prebuilt rebuild that died mid-load: empty table carrying
        # the in-progress marker.
        project.run_sql(f"truncate table {unique_schema}.guard_model")
        project.run_sql(f"""EXEC sp_addextendedproperty @name = N'dbt_full_refresh_incomplete',
                @value = '1', @level0type = N'SCHEMA',
                @level0name = '{unique_schema}',
                @level1type = N'TABLE', @level1name = 'guard_model'""")

        # A normal incremental run must ERROR, not silently append.
        _, output = run_dbt_and_capture(["run", "--models", "guard_model"], expect_pass=False)
        assert "did not complete" in output
        assert "--full-refresh" in output

        # --full-refresh recovers and clears the marker.
        run_dbt(["run", "--models", "guard_model", "--full-refresh"])
        assert self.row_count(project, unique_schema) == 1
        marker = project.run_sql(
            f"""select count(*) from sys.extended_properties
                where major_id = OBJECT_ID('{unique_schema}.guard_model')
                and name = 'dbt_full_refresh_incomplete'""",
            fetch="one",
        )
        assert marker[0] == 0


models__swap_guard_sql = """
{{
  config(
    materialized = "incremental",
    as_columnstore = False
  )
}}

select 1 as column_a {% if var('break_model', false) %}, 1/0 as boom {% endif %}

"""


class TestFullRefreshBuildSwapGuard:
    """The incomplete-rebuild marker also protects DEFAULT (swap) full
    refreshes: a rebuild that dies mid-intermediate-build must block normal
    incremental runs instead of silently appending onto the stale table."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "guard_model.sql": models__guard_model_sql,
            "swap_guard.sql": models__swap_guard_sql,
        }

    def count(self, project, unique_schema, table):
        return project.run_sql(f"select count(*) from {unique_schema}.{table}", fetch="one")[0]

    def marker_count(self, project, unique_schema, table):
        return project.run_sql(
            f"""select count(*) from sys.extended_properties
                where major_id = OBJECT_ID('{unique_schema}.{table}')
                and name = 'dbt_full_refresh_incomplete'""",
            fetch="one",
        )[0]

    def test_failed_swap_full_refresh_blocks_normal_runs(self, project, unique_schema):
        run_dbt(["run", "--models", "swap_guard"])
        project.run_sql(f"insert into {unique_schema}.swap_guard values (99)")
        assert self.count(project, unique_schema, "swap_guard") == 2

        # full refresh dies mid-intermediate-build (runtime divide-by-zero):
        # old table stays live but is now marked incomplete
        run_dbt_and_capture(
            ["run", "--models", "swap_guard", "--full-refresh", "--vars", "break_model: true"],
            expect_pass=False,
        )
        assert self.count(project, unique_schema, "swap_guard") == 2
        assert self.marker_count(project, unique_schema, "swap_guard") == 1

        # a normal run must ERROR, not append onto the stale table
        _, output = run_dbt_and_capture(["run", "--models", "swap_guard"], expect_pass=False)
        assert "did not complete" in output
        assert self.count(project, unique_schema, "swap_guard") == 2

        # a successful full refresh recovers; the marker leaves with the
        # old table when it is swapped out
        run_dbt(["run", "--models", "swap_guard", "--full-refresh"])
        assert self.count(project, unique_schema, "swap_guard") == 1
        assert self.marker_count(project, unique_schema, "swap_guard") == 0

    def test_real_failure_during_prebuilt_rebuild_sets_marker(self, project, unique_schema):
        run_dbt(["run", "--models", "guard_model"])

        # an index on a column the model doesn't produce passes config
        # validation but fails at the engine AFTER the drop: the marker on
        # the new empty table must be present
        bad = "[{'columns': ['no_such_column'], 'type': 'clustered'}]"
        run_dbt_and_capture(
            [
                "run",
                "--models",
                "guard_model",
                "--full-refresh",
                "--vars",
                f"guard_indexes: {bad}",
            ],
            expect_pass=False,
        )
        assert self.marker_count(project, unique_schema, "guard_model") == 1

        _, output = run_dbt_and_capture(["run", "--models", "guard_model"], expect_pass=False)
        assert "did not complete" in output

        run_dbt(["run", "--models", "guard_model", "--full-refresh"])
        assert self.count(project, unique_schema, "guard_model") == 1
        assert self.marker_count(project, unique_schema, "guard_model") == 0


models__selfref_model_sql = """
{{
  config(
    materialized = "incremental",
    as_columnstore = False,
    full_refresh_build = "prebuilt",
    indexes = [{'columns': ['column_a'], 'type': 'clustered'}]
  )
}}

select 1 as column_a
{% if var('selfref', false) %}
union all
select column_a from {{ this }} where 1 = 0
{% endif %}

"""


class TestFullRefreshBuildSelfReference:
    @pytest.fixture(scope="class")
    def models(self):
        return {"selfref_model.sql": models__selfref_model_sql}

    def test_unguarded_self_reference_fails_before_drop(self, project, unique_schema):
        run_dbt(["run", "--models", "selfref_model"])
        project.run_sql(f"insert into {unique_schema}.selfref_model values (99)")

        # an unguarded {{ this }} must fail fast: BEFORE the drop (data
        # intact) and BEFORE the marker (normal runs not poisoned)
        _, output = run_dbt_and_capture(
            ["run", "--models", "selfref_model", "--full-refresh", "--vars", "selfref: true"],
            expect_pass=False,
        )
        assert "self-reference" in output
        rows = project.run_sql(f"select count(*) from {unique_schema}.selfref_model", fetch="one")
        assert rows[0] == 2

        # no marker was set, so a normal run still works
        run_dbt(["run", "--models", "selfref_model"])
