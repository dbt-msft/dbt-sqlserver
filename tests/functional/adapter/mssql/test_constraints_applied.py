"""Constraints declared in yaml must reach the database.

The dbt-owned tests in tests/functional/adapter/dbt/test_constraints.py assert
the *generated SQL*; these assert what actually exists in the catalog after a
run, which is what #579 was about - the DDL was simply never emitted.
"""

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture, write_file

# A model-level constraint carrying `name:` is applied by ALTER TABLE after the
# build swaps the new table in; an unnamed one rides the CREATE TABLE column
# list and is named by SQL Server. Both shapes appear here.
#
# Every model below selects the same two columns and differs only in its config,
# so they are built from one body.
MODEL_BODY = "select 1 as id, 'blue' as color"


def model_sql(**config):
    settings = ["materialized='table'"]
    settings += [f"{key}={value!r}" for key, value in config.items()]
    return "{{ config(" + ", ".join(settings) + ") }}\n" + MODEL_BODY


named_model_sql = model_sql()

named_schema_yml = """
version: 2
models:
  - name: named_model
    config:
      contract:
        enforced: true
    constraints:
      - type: primary_key
        name: PK_named_model
        columns: [id]
      - type: unique
        name: UQ_named_model_color
        columns: [color]
      - type: check
        name: CK_named_model_id
        expression: id > 0
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
      - name: color
        data_type: varchar(100)
"""

anonymous_model_sql = model_sql()

anonymous_schema_yml = """
version: 2
models:
  - name: anonymous_model
    config:
      contract:
        enforced: true
    constraints:
      - type: unique
        columns: [color]
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: id > 0
      - name: color
        data_type: varchar(100)
"""

# as_columnstore=False leaves the clustered slot free, so `expression: clustered`
# has something to claim.
clustered_model_sql = model_sql(as_columnstore=False)

clustered_schema_yml = """
version: 2
models:
  - name: clustered_model
    config:
      contract:
        enforced: true
    constraints:
      - type: primary_key
        name: PK_clustered_model
        columns: [id]
        expression: clustered
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
      - name: color
        data_type: varchar(100)
"""

incremental_model_sql = """
{{ config(materialized='incremental', unique_key='id', on_schema_change='append_new_columns') }}
select 1 as id, 'blue' as color
"""

incremental_schema_without_pk_yml = """
version: 2
models:
  - name: incremental_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
      - name: color
        data_type: varchar(100)
"""

# The same model with a primary key, so a test can add the constraint to a model
# that already exists in the database.
incremental_schema_yml = incremental_schema_without_pk_yml.replace(
    "    columns:",
    """    constraints:
      - type: primary_key
        name: PK_incremental_model
        columns: [id]
    columns:""",
)

named_column_constraint_schema_yml = """
version: 2
models:
  - name: anonymous_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
          - type: primary_key
            name: PK_you_cannot_have_this
      - name: color
        data_type: varchar(100)
"""


fk_parent_sql = """
{{ config(materialized='table', as_columnstore=False) }}
select 1 as id
"""

fk_child_sql = """
{{ config(materialized='table', as_columnstore=False) }}

-- depends_on: {{ ref('fk_parent') }}

select 1 as parent_id, 'blue' as color
"""

fk_schema_yml = """
version: 2
models:
  - name: fk_parent
    config:
      contract:
        enforced: true
    constraints:
      - type: primary_key
        name: PK_fk_parent
        columns: [id]
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
  - name: fk_child
    config:
      contract:
        enforced: true
    constraints:
      - type: foreign_key
        name: FK_fk_child_parent
        columns: [parent_id]
        to: ref('fk_parent')
        to_columns: [id]
    columns:
      - name: parent_id
        data_type: int
        constraints:
          - type: not_null
      - name: color
        data_type: varchar(100)
"""


def _constraints(project, table):
    """Every constraint object on a table, as {type: [names]}."""
    rows = project.run_sql(
        f"""
        select o.type_desc, o.name
        from sys.objects o
        where o.parent_object_id = OBJECT_ID('{project.test_schema}.{table}')
          and o.type in ('PK', 'UQ', 'C', 'F')
        """,
        fetch="all",
    )
    grouped: dict = {}
    for type_desc, name in rows:
        grouped.setdefault(type_desc, []).append(name)
    return grouped


def _indexes(project, table):
    """Every index on a table, as {name: type_desc}. A clustered columnstore
    index has no name of its own worth asserting on, so it keys on None."""
    rows = project.run_sql(
        f"""
        select i.name, i.type_desc
        from sys.indexes i
        where i.object_id = OBJECT_ID('{project.test_schema}.{table}')
        """,
        fetch="all",
    )
    return {name: type_desc for name, type_desc in rows}


def _index_type(project, table, index_name):
    return _indexes(project, table).get(index_name)


class TestNamedModelConstraints:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "named_model.sql": named_model_sql,
            "schema.yml": named_schema_yml,
        }

    def test_named_constraints_are_created_with_their_names(self, project):
        run_dbt(["run"])

        constraints = _constraints(project, "named_model")
        assert constraints.get("PRIMARY_KEY_CONSTRAINT") == ["PK_named_model"]
        assert constraints.get("UNIQUE_CONSTRAINT") == ["UQ_named_model_color"]
        assert constraints.get("CHECK_CONSTRAINT") == ["CK_named_model_id"]

        # The default as_columnstore build carries a clustered columnstore index,
        # so the key constraints must be nonclustered to coexist with it.
        assert _index_type(project, "named_model", "PK_named_model") == "NONCLUSTERED"

    def test_rebuild_reuses_the_same_constraint_names(self, project):
        """The names only survive a rebuild because they are applied after the
        old table (which held them) is dropped - the point of the ALTER path."""
        run_dbt(["run"])
        run_dbt(["run", "--full-refresh"])

        constraints = _constraints(project, "named_model")
        assert constraints.get("PRIMARY_KEY_CONSTRAINT") == ["PK_named_model"]
        assert constraints.get("CHECK_CONSTRAINT") == ["CK_named_model_id"]


class TestAnonymousConstraints:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "anonymous_model.sql": anonymous_model_sql,
            "schema.yml": anonymous_schema_yml,
        }

    def test_unnamed_constraints_are_created_inline(self, project):
        run_dbt(["run"])

        constraints = _constraints(project, "anonymous_model")
        # One column-level primary key and check, one model-level unique. SQL
        # Server names them itself, so assert the shape, not the names.
        assert len(constraints.get("PRIMARY_KEY_CONSTRAINT", [])) == 1
        assert len(constraints.get("UNIQUE_CONSTRAINT", [])) == 1
        assert len(constraints.get("CHECK_CONSTRAINT", [])) == 1

    def test_rebuild_does_not_collide(self, project):
        run_dbt(["run"])
        run_dbt(["run", "--full-refresh"])

        constraints = _constraints(project, "anonymous_model")
        assert len(constraints.get("PRIMARY_KEY_CONSTRAINT", [])) == 1


class TestNamedColumnConstraintWarns:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "anonymous_model.sql": anonymous_model_sql,
            "schema.yml": named_column_constraint_schema_yml,
        }

    def test_column_level_name_is_ignored_with_a_warning(self, project):
        _, log_output = run_dbt_and_capture(["run"])

        assert "PK_you_cannot_have_this" in log_output
        assert "constraints:" in log_output

        constraints = _constraints(project, "anonymous_model")
        # The constraint is still created, just not under the requested name.
        created = constraints.get("PRIMARY_KEY_CONSTRAINT", [])
        assert len(created) == 1
        assert created[0] != "PK_you_cannot_have_this"


class TestClusteredOverride:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "clustered_model.sql": clustered_model_sql,
            "schema.yml": clustered_schema_yml,
        }

    def test_expression_selects_the_clustering(self, project):
        run_dbt(["run"])

        assert _index_type(project, "clustered_model", "PK_clustered_model") == "CLUSTERED"


class TestIncrementalConstraints:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_model.sql": incremental_model_sql,
            "schema.yml": incremental_schema_yml,
        }

    def test_constraint_survives_a_second_run(self, project):
        """A named constraint is added on the build that creates the table and
        must not be re-added on the next incremental run - doing so fails with
        Msg 2714 (there is already an object named ...)."""
        run_dbt(["run"])
        assert _constraints(project, "incremental_model").get("PRIMARY_KEY_CONSTRAINT") == [
            "PK_incremental_model"
        ]

        run_dbt(["run"])
        assert _constraints(project, "incremental_model").get("PRIMARY_KEY_CONSTRAINT") == [
            "PK_incremental_model"
        ]

    def test_constraint_is_reapplied_on_full_refresh(self, project):
        run_dbt(["run"])
        run_dbt(["run", "--full-refresh"])

        assert _constraints(project, "incremental_model").get("PRIMARY_KEY_CONSTRAINT") == [
            "PK_incremental_model"
        ]


class TestConstraintAddedToAnExistingModel:
    """A constraint added after the model already exists must land on the next
    run. Only a build that creates the table applies constraints, so without
    the existence-guarded ADD this was a silent no-op until --full-refresh.

    One test per class: these rewrite models/schema.yml and the `project`
    fixture is class-scoped, so a second test here would inherit both the
    rewritten file and the constraint left behind on the database.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_model.sql": incremental_model_sql,
            "schema.yml": incremental_schema_without_pk_yml,
        }

    def test_it_lands_on_the_next_incremental_run(self, project):
        run_dbt(["run"])
        assert _constraints(project, "incremental_model").get("PRIMARY_KEY_CONSTRAINT") is None

        write_file(incremental_schema_yml, "models", "schema.yml")

        # A plain incremental run, no --full-refresh.
        run_dbt(["run"])
        assert _constraints(project, "incremental_model").get("PRIMARY_KEY_CONSTRAINT") == [
            "PK_incremental_model"
        ]

        # And a further run must not try to add it a second time (Msg 2714).
        run_dbt(["run"])
        assert _constraints(project, "incremental_model").get("PRIMARY_KEY_CONSTRAINT") == [
            "PK_incremental_model"
        ]


class TestForeignKeyToRef:
    """`to: ref(...)` is the form dbt-core actually produces, and it resolves to
    a fully rendered relation - database included. T-SQL's REFERENCES grammar
    takes [schema.]table only, so the database qualifier has to come off."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fk_parent.sql": fk_parent_sql,
            "fk_child.sql": fk_child_sql,
            "schema.yml": fk_schema_yml,
        }

    def test_the_foreign_key_is_created(self, project):
        run_dbt(["run"])

        assert _constraints(project, "fk_child").get("FOREIGN_KEY_CONSTRAINT") == [
            "FK_fk_child_parent"
        ]

        referenced = project.run_sql(
            f"""
            select OBJECT_NAME(fk.referenced_object_id)
            from sys.foreign_keys fk
            where fk.name = 'FK_fk_child_parent'
              and fk.parent_object_id = OBJECT_ID('{project.test_schema}.fk_child')
            """,
            fetch="all",
        )
        assert referenced[0][0] == "fk_parent"


masked_model_sql = model_sql(as_columnstore=False)

masked_schema_yml = """
version: 2
models:
  - name: masked_model
    config:
      contract:
        enforced: true
      masks:
        id: "default()"
    constraints:
      - type: primary_key
        name: PK_masked_model
        columns: [id]
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
      - name: color
        data_type: varchar(100)
"""


class TestNamedConstraintOnAMaskedColumn:
    """A named constraint is applied after apply_masks, so its index lands on an
    already-masked column. An unnamed one rides the CREATE TABLE and its index
    exists before the masks do, which apply_masks refuses - naming it is the
    documented way out."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "masked_model.sql": masked_model_sql,
            "schema.yml": masked_schema_yml,
        }

    def test_the_mask_and_the_constraint_coexist(self, project):
        run_dbt(["run"])

        assert _constraints(project, "masked_model").get("PRIMARY_KEY_CONSTRAINT") == [
            "PK_masked_model"
        ]

        masked = project.run_sql(
            f"""
            select c.name
            from sys.masked_columns c
            where c.object_id = OBJECT_ID('{project.test_schema}.masked_model')
              and c.is_masked = 1
            """,
            fetch="all",
        )
        assert [row[0] for row in masked] == ["id"]
