"""Functional tests for column-level Dynamic Data Masking (DDM).

Exercises the two config surfaces (column-level ``masked_with:`` and the
model-level ``masks`` dict), precedence/opt-out/validation, the introspection
against ``sys.masked_columns``, that masks survive a full-refresh rebuild, and
that an un-privileged (no ``UNMASK``) principal actually sees masked values.

Requires SQL Server 2016+ (DDM). The CI/test server is 2022.
"""

import pytest

from dbt.tests.util import get_connection, run_dbt, run_dbt_and_capture

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def masked_columns(project, table_name):
    """Return {column_name: masking_function} from sys.masked_columns."""
    sql = f"""
        select c.name, c.masking_function
        from sys.masked_columns c
        where c.object_id = OBJECT_ID('{project.test_schema}.{table_name}')
    """
    with get_connection(project.adapter):
        _, table = project.adapter.execute(sql, fetch=True)
    return {row[0]: row[1] for row in table.rows}


def select_as_unprivileged(project, table_name, columns):
    """Read `columns` from the table as a freshly-created user that holds
    SELECT but not UNMASK, so masking is in effect. Returns the first row."""
    cols = ", ".join(columns)
    user = "dbt_mask_reader"
    sql = f"""
        if database_principal_id('{user}') is null
            create user {user} without login;
        grant select on schema::{project.test_schema} to {user};
        execute as user = '{user}';
        select {cols} from {project.test_schema}.{table_name};
        revert;
    """
    with get_connection(project.adapter):
        _, table = project.adapter.execute(sql, fetch=True)
    return table.rows[0]


# ---------------------------------------------------------------------------
# model fixtures
# ---------------------------------------------------------------------------

# column-level `masked_with:` surface
column_property_model_sql = """
{{ config(materialized="table") }}
select
    1 as id,
    cast('Smith' as varchar(50)) as surname,
    cast('1234567890' as varchar(10)) as nhs_number
"""

column_property_yml = """
version: 2
models:
  - name: masked_model
    columns:
      - name: surname
        masked_with: "default()"
      - name: nhs_number
        masked_with: 'partial(0,"XXXXXXXXXX",0)'
"""

# model-level `masks` dict surface
model_level_masks_sql = """
{{ config(
    materialized="table",
    masks={"surname": "default()", "nhs_number": "email()"}
) }}
select
    1 as id,
    cast('Smith' as varchar(50)) as surname,
    cast('a@b.com' as varchar(50)) as nhs_number
"""


# ---------------------------------------------------------------------------
# column-level `masked_with:` surface
# ---------------------------------------------------------------------------


class TestColumnPropertyMasks:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "masked_model.sql": column_property_model_sql,
            "masked_model.yml": column_property_yml,
        }

    def test_masks_applied_and_survive_full_refresh(self, project):
        run_dbt(["run"])
        masks = masked_columns(project, "masked_model")
        assert masks.get("surname") == "default()"
        assert masks.get("nhs_number") == 'partial(0, "XXXXXXXXXX", 0)'
        assert "id" not in masks

        # a full refresh drops & recreates the table — the whole point is that
        # the adapter re-applies the mask so it is not lost
        run_dbt(["run", "--full-refresh"])
        masks = masked_columns(project, "masked_model")
        assert masks.get("surname") == "default()"
        assert masks.get("nhs_number") == 'partial(0, "XXXXXXXXXX", 0)'

    def test_unprivileged_user_sees_masked_values(self, project):
        run_dbt(["run"])
        surname, nhs = select_as_unprivileged(project, "masked_model", ["surname", "nhs_number"])
        # default() masks a string to 'xxxx'; partial(0,"XXXXXXXXXX",0) keeps
        # only the padding
        assert surname == "xxxx"
        assert nhs == "XXXXXXXXXX"


# ---------------------------------------------------------------------------
# model-level `masks` surface
# ---------------------------------------------------------------------------


class TestModelLevelMasks:
    @pytest.fixture(scope="class")
    def models(self):
        return {"masked_model.sql": model_level_masks_sql}

    def test_model_level_masks_applied(self, project):
        run_dbt(["run"])
        masks = masked_columns(project, "masked_model")
        assert masks.get("surname") == "default()"
        assert masks.get("nhs_number") == "email()"


# ---------------------------------------------------------------------------
# snapshot materialization
# ---------------------------------------------------------------------------

snapshot_source_model_sql = """
{{ config(materialized="table") }}
select 1 as id, cast('Smith' as varchar(50)) as surname
"""

masked_snapshot_sql = """
{% snapshot masked_snapshot %}
{{ config(
    unique_key='id',
    strategy='check',
    check_cols=['surname'],
    masks={'surname': 'default()'}
) }}
select * from {{ ref('snap_source') }}
{% endsnapshot %}
"""


class TestSnapshotMasks:
    @pytest.fixture(scope="class")
    def models(self):
        return {"snap_source.sql": snapshot_source_model_sql}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"masked_snapshot.sql": masked_snapshot_sql}

    def test_masks_applied_to_snapshot_table(self, project):
        run_dbt(["run"])
        run_dbt(["snapshot"])
        masks = masked_columns(project, "masked_snapshot")
        assert masks.get("surname") == "default()"
        # dbt-maintained snapshot metadata columns are left unmasked
        assert "id" not in masks

        # a second snapshot run persists the table — mask stays, no error
        run_dbt(["snapshot"])
        assert masked_columns(project, "masked_snapshot").get("surname") == "default()"


# ---------------------------------------------------------------------------
# idempotency / change / drop
# ---------------------------------------------------------------------------


incremental_masked_sql = """
{{ config(materialized="incremental", masks={"surname": "default()"}) }}
select 1 as id, cast('Smith' as varchar(50)) as surname
{% if is_incremental() %} where 1 = 0 {% endif %}
"""


class TestMaskIdempotentOnPersistedTable:
    """On a persisted (incremental, non-full-refresh) table the mask survives
    the run, so re-applying is a true no-op that emits zero mask DDL."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"masked_model.sql": incremental_masked_sql}

    def test_rerun_emits_no_mask_ddl(self, project):
        _, first = run_dbt_and_capture(["run"])
        assert "data-mask change" in first  # applied on first build
        assert masked_columns(project, "masked_model").get("surname") == "default()"

        # second (incremental, persisted) run: mask already in place → no DDL
        _, second = run_dbt_and_capture(["run"])
        assert "data-mask change" not in second
        assert masked_columns(project, "masked_model").get("surname") == "default()"


class TestMaskLifecycle:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "masked_model.sql": column_property_model_sql,
            "masked_model.yml": column_property_yml,
        }

    def test_change_function_updates(self, project):
        run_dbt(["run"])
        # swap surname's function
        changed_yml = column_property_yml.replace(
            'masked_with: "default()"', 'masked_with: "email()"'
        )
        write_model(project, "masked_model.yml", changed_yml)
        run_dbt(["run"])
        assert masked_columns(project, "masked_model").get("surname") == "email()"

    def test_removing_config_drops_mask(self, project):
        run_dbt(["run"])
        assert "surname" in masked_columns(project, "masked_model")
        # drop the yml entirely — no masks configured anymore
        write_model(project, "masked_model.yml", "version: 2\nmodels: []\n")
        run_dbt(["run"])
        assert masked_columns(project, "masked_model") == {}


# ---------------------------------------------------------------------------
# precedence, opt-out, validation
# ---------------------------------------------------------------------------

both_surfaces_sql = """
{{ config(materialized="table", masks={"surname": "default()"}) }}
select 1 as id, cast('Smith' as varchar(50)) as surname
"""

both_surfaces_yml = """
version: 2
models:
  - name: masked_model
    columns:
      - name: surname
        masked_with: "email()"
"""


class TestBothSurfacesColumnWins:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "masked_model.sql": both_surfaces_sql,
            "masked_model.yml": both_surfaces_yml,
        }

    def test_column_level_wins_and_warns(self, project):
        _, logs = run_dbt_and_capture(["run"])
        # column-level function applied, not the model-level one
        assert masked_columns(project, "masked_model").get("surname") == "email()"
        # a conflict warning was surfaced naming the model and both functions
        assert "masked_model" in logs
        assert "email()" in logs and "default()" in logs
        assert "column-level" in logs


opt_out_sql = """
{{ config(materialized="table", masks={"surname": "default()", "nhs_number": "default()"}) }}
select 1 as id, cast('Smith' as varchar(50)) as surname,
    cast('x' as varchar(50)) as nhs_number
"""

opt_out_yml = """
version: 2
models:
  - name: masked_model
    columns:
      - name: nhs_number
        masked_with: null
"""


class TestColumnLevelOptOut:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "masked_model.sql": opt_out_sql,
            "masked_model.yml": opt_out_yml,
        }

    def test_null_opts_out_of_inherited_mask(self, project):
        run_dbt(["run"])
        masks = masked_columns(project, "masked_model")
        # surname keeps the model-level default; nhs_number opted out
        assert masks.get("surname") == "default()"
        assert "nhs_number" not in masks


invalid_key_sql = """
{{ config(materialized="table", masks={"surnam": "default()"}) }}
select 1 as id, cast('Smith' as varchar(50)) as surname
"""


class TestInvalidModelMaskKey:
    @pytest.fixture(scope="class")
    def models(self):
        return {"masked_model.sql": invalid_key_sql}

    def test_unknown_column_warns_and_is_skipped(self, project):
        _, logs = run_dbt_and_capture(["run"])
        # run succeeds, no mask applied, and a warning names the bad column
        assert masked_columns(project, "masked_model") == {}
        assert "surnam" in logs


# helper defined at module end so it is importable above via closure at call time
def write_model(project, filename, contents):
    import os

    path = os.path.join(project.project_root, "models", filename)
    with open(path, "w") as f:
        f.write(contents)
