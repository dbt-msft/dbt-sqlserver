"""Step-by-step, end-to-end scenarios for constraint handling (#579).

Each class walks one model through a sequence of runs and yaml edits and
asserts the catalog after every step. The steps are numbered and match the
scenarios in docs/constraints.md, so a failure names the step that broke.

One test method per class: every step rewrites files under models/ and the
`project` fixture is class-scoped, so a second method would inherit the
previous one's files and database objects.

tests/functional/adapter/dbt/test_constraints.py asserts the generated SQL and
tests/functional/adapter/mssql/test_constraints_applied.py asserts single
builds; this module is about what happens *across* runs.
"""

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture, write_file


def _constraints(project, table):
    """Every constraint object on a table, as {type_desc: sorted [names]}."""
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
    return {key: sorted(names) for key, names in grouped.items()}


def _index_types(project, table):
    """{index name: type_desc}; the columnstore index keys on None."""
    rows = project.run_sql(
        f"""
        select i.name, i.type_desc
        from sys.indexes i
        where i.object_id = OBJECT_ID('{project.test_schema}.{table}')
        """,
        fetch="all",
    )
    return {name: type_desc for name, type_desc in rows}


def _not_null_columns(project, table):
    rows = project.run_sql(
        f"""
        select c.name
        from sys.columns c
        where c.object_id = OBJECT_ID('{project.test_schema}.{table}')
          and c.is_nullable = 0
        """,
        fetch="all",
    )
    return sorted(row[0] for row in rows)


def _row_count(project, table):
    return project.run_sql(f"select count(*) from {project.test_schema}.{table}", fetch="one")[0]


def _leftovers(project):
    """Build-time scratch objects that a finished run must not leave behind."""
    rows = project.run_sql(
        f"""
        select name
        from sys.tables
        where schema_id = SCHEMA_ID('{project.test_schema}')
          and (name like '%[_][_]dbt[_]tmp%' or name like '%[_][_]dbt[_]backup%')
        """,
        fetch="all",
    )
    return sorted(row[0] for row in rows)


def _exists(project, table):
    return (
        project.run_sql(f"select OBJECT_ID('{project.test_schema}.{table}', 'U')", fetch="one")[0]
        is not None
    )


# --------------------------------------------------------------------------- #
# Scenario 1: a table model, from contract-off through both failure modes      #
# --------------------------------------------------------------------------- #

LIFECYCLE_MODEL = "{{ config(materialized='table') }}\nselect 1 as id, 'blue' as color"

LIFECYCLE_SCHEMA = """
version: 2
models:
  - name: lifecycle_model
    config:
      contract:
        enforced: {enforced}
    constraints:
      - type: primary_key
        name: PK_lifecycle_model
        columns: [id]
      - type: unique
        columns: [color]
      - type: check
        expression: id > 0
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
      - name: color
        data_type: varchar(100)
"""


class TestTableModelLifecycle:
    """Steps 1-7 of the table-model scenario in docs/constraints.md."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "lifecycle_model.sql": LIFECYCLE_MODEL,
            "schema.yml": LIFECYCLE_SCHEMA.format(enforced="false"),
        }

    def test_lifecycle(self, project):
        table = "lifecycle_model"

        # Step 1: contract off. The constraints are declared but, as on every
        # adapter, not emitted - nothing reaches the catalog.
        run_dbt(["run"])
        assert _constraints(project, table) == {}

        # Step 2: contract on. Every declared constraint lands: the named PK
        # via ALTER after the swap, the unnamed UNIQUE and CHECK inline.
        write_file(LIFECYCLE_SCHEMA.format(enforced="true"), "models", "schema.yml")
        run_dbt(["run"])
        after_enforce = _constraints(project, table)
        assert after_enforce["PRIMARY_KEY_CONSTRAINT"] == ["PK_lifecycle_model"]
        assert len(after_enforce["UNIQUE_CONSTRAINT"]) == 1
        assert len(after_enforce["CHECK_CONSTRAINT"]) == 1
        assert _not_null_columns(project, table) == ["id"]
        indexes = _index_types(project, table)
        assert indexes["PK_lifecycle_model"] == "NONCLUSTERED"
        assert "CLUSTERED COLUMNSTORE" in indexes.values()

        # Step 3: a rebuild. The named constraint keeps its name (it was
        # applied after the outgoing table, which held it, was dropped) and
        # nothing is duplicated or left behind.
        run_dbt(["run"])
        rebuilt = _constraints(project, table)
        assert rebuilt["PRIMARY_KEY_CONSTRAINT"] == ["PK_lifecycle_model"]
        assert len(rebuilt["UNIQUE_CONSTRAINT"]) == 1
        assert len(rebuilt["CHECK_CONSTRAINT"]) == 1
        assert _leftovers(project) == []

        # Step 4: the data violates an *unnamed* constraint. It is validated
        # while the new table is loaded, so the run fails before the swap and
        # the previous table - data and constraints - is untouched.
        write_file(LIFECYCLE_MODEL.replace("1 as id", "0 as id"), "models", table + ".sql")
        _, output = run_dbt_and_capture(["run"], expect_pass=False)
        assert "CHECK constraint" in output
        assert _row_count(project, table) == 1
        assert (
            project.run_sql(f"select id from {project.test_schema}.{table}", fetch="one")[0] == 1
        )
        assert _constraints(project, table)["PRIMARY_KEY_CONSTRAINT"] == ["PK_lifecycle_model"]

        # Step 5: fixing the data recovers cleanly - whatever the failed load
        # left behind is cleared by the next successful build.
        write_file(LIFECYCLE_MODEL, "models", table + ".sql")
        run_dbt(["run"])
        assert _constraints(project, table)["PRIMARY_KEY_CONSTRAINT"] == ["PK_lifecycle_model"]
        assert _leftovers(project) == []

        # Step 6: the data violates a *named* constraint. That one is applied
        # after the model has committed, so the documented trade-off holds:
        # the run fails, the new table is in place, and it carries everything
        # except the constraint that failed.
        write_file(
            LIFECYCLE_MODEL + "\nunion all select 1 as id, 'red' as color",
            "models",
            table + ".sql",
        )
        _, output = run_dbt_and_capture(["run"], expect_pass=False)
        assert "PK_lifecycle_model" in output
        assert _row_count(project, table) == 2
        unconstrained = _constraints(project, table)
        assert "PRIMARY_KEY_CONSTRAINT" not in unconstrained
        assert len(unconstrained["UNIQUE_CONSTRAINT"]) == 1
        assert len(unconstrained["CHECK_CONSTRAINT"]) == 1

        # Step 7: the next good build puts the named constraint back.
        write_file(LIFECYCLE_MODEL, "models", table + ".sql")
        run_dbt(["run"])
        assert _row_count(project, table) == 1
        assert _constraints(project, table)["PRIMARY_KEY_CONSTRAINT"] == ["PK_lifecycle_model"]
        assert _leftovers(project) == []


# --------------------------------------------------------------------------- #
# Scenario 2: named constraints on a table that persists across runs           #
# --------------------------------------------------------------------------- #

INCREMENTAL_MODEL = """
{{ config(materialized='incremental', unique_key='id', on_schema_change='append_new_columns') }}
select 1 as id, 'blue' as color
"""


def incremental_schema(pk_name, check_name, with_unique=False):
    unique = (
        """
      - type: unique
        name: UQ_incr_color
        columns: [color]"""
        if with_unique
        else ""
    )
    return f"""
version: 2
models:
  - name: incr_model
    config:
      contract:
        enforced: true
    constraints:
      - type: primary_key
        name: {pk_name}
        columns: [id]
      - type: check
        name: {check_name}
        expression: id > 0{unique}
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
      - name: color
        data_type: varchar(100)
"""


class TestIncrementalConstraintChanges:
    """Every bullet of the README's "Changing a constraint after the first
    build", in order, on one incremental model whose table is never rebuilt
    until the final --full-refresh."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incr_model.sql": INCREMENTAL_MODEL,
            "schema.yml": incremental_schema("PK_incr_v1", "CK_incr_v1"),
        }

    def test_change_matrix(self, project):
        table = "incr_model"

        # Step 1: the build that creates the table applies the named ones.
        run_dbt(["run"])
        assert _constraints(project, table) == {
            "PRIMARY_KEY_CONSTRAINT": ["PK_incr_v1"],
            "CHECK_CONSTRAINT": ["CK_incr_v1"],
        }

        # Step 2: a plain incremental run. The existence guard makes the ALTER
        # a no-op, so nothing is re-added (Msg 2714) and nothing is doubled.
        run_dbt(["run"])
        assert _constraints(project, table) == {
            "PRIMARY_KEY_CONSTRAINT": ["PK_incr_v1"],
            "CHECK_CONSTRAINT": ["CK_incr_v1"],
        }

        # Step 3: *adding* a named constraint lands on the next incremental
        # run, with no --full-refresh.
        write_file(
            incremental_schema("PK_incr_v1", "CK_incr_v1", with_unique=True),
            "models",
            "schema.yml",
        )
        run_dbt(["run"])
        assert _constraints(project, table)["UNIQUE_CONSTRAINT"] == ["UQ_incr_color"]

        # Step 4: *removing* it from the yaml does not drop it.
        write_file(incremental_schema("PK_incr_v1", "CK_incr_v1"), "models", "schema.yml")
        run_dbt(["run"])
        assert _constraints(project, table)["UNIQUE_CONSTRAINT"] == ["UQ_incr_color"]

        # Step 5: *renaming* a check adds the new name beside the old one.
        write_file(incremental_schema("PK_incr_v1", "CK_incr_v2"), "models", "schema.yml")
        run_dbt(["run"])
        assert _constraints(project, table)["CHECK_CONSTRAINT"] == ["CK_incr_v1", "CK_incr_v2"]

        # Step 6: *renaming* the primary key cannot add a second one: the run
        # fails with Msg 1779 and the old key stays.
        write_file(incremental_schema("PK_incr_v2", "CK_incr_v2"), "models", "schema.yml")
        _, output = run_dbt_and_capture(["run"], expect_pass=False)
        assert "already has a primary key" in output
        assert _constraints(project, table)["PRIMARY_KEY_CONSTRAINT"] == ["PK_incr_v1"]

        # Step 7: --full-refresh rebuilds the table, so only what the yaml
        # declares now exists - the renamed key, the renamed check, no unique.
        run_dbt(["run", "--full-refresh"])
        assert _constraints(project, table) == {
            "PRIMARY_KEY_CONSTRAINT": ["PK_incr_v2"],
            "CHECK_CONSTRAINT": ["CK_incr_v2"],
        }
        assert _leftovers(project) == []


# --------------------------------------------------------------------------- #
# Scenario 3: a foreign key pointing at a model, and the parent's rebuild      #
# --------------------------------------------------------------------------- #

FK_PARENT = "{{ config(materialized='table', as_columnstore=False) }}\nselect 1 as id"

FK_PARENT_WITH_PRE_HOOK = FK_PARENT.replace(
    "as_columnstore=False", 'as_columnstore=False, pre_hook="{{ drop_fk_constraints() }}"'
)

FK_CHILD = """
{{ config(materialized='table', as_columnstore=False) }}

-- depends_on: {{ ref('fk_parent') }}

select 1 as parent_id
"""

FK_SCHEMA = """
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
"""


def _fk_target(project, child, fk_name):
    """The object a foreign key points at right now - it follows a rename."""
    return project.run_sql(
        f"""
        select OBJECT_NAME(fk.referenced_object_id)
        from sys.foreign_keys fk
        where fk.name = '{fk_name}'
          and fk.parent_object_id = OBJECT_ID('{project.test_schema}.{child}')
        """,
        fetch="one",
    )[0]


class TestForeignKeyParentRebuild:
    """The README's foreign-key section, step by step: why the parent's
    rebuild fails, what state that leaves, and how the shipped pre_hook is
    the way out."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fk_parent.sql": FK_PARENT,
            "fk_child.sql": FK_CHILD,
            "schema.yml": FK_SCHEMA,
        }

    def test_parent_rebuild(self, project):
        # Step 1: both build; the key exists and points at the parent.
        run_dbt(["run"])
        assert _constraints(project, "fk_child")["FOREIGN_KEY_CONSTRAINT"] == [
            "FK_fk_child_parent"
        ]
        assert _fk_target(project, "fk_child", "FK_fk_child_parent") == "fk_parent"

        # Step 2: rebuilding the parent alone fails at the backup drop (Msg
        # 3726). The swap itself has already happened: the new parent is in
        # place, the child's key follows the *renamed* old table, and - since
        # named constraints are applied after that drop - the new parent never
        # received its own key.
        _, output = run_dbt_and_capture(["run", "--select", "fk_parent"], expect_pass=False)
        assert "FOREIGN KEY constraint" in output
        assert _exists(project, "fk_parent")
        assert _fk_target(project, "fk_child", "FK_fk_child_parent") == "fk_parent__dbt_backup"
        assert "PRIMARY_KEY_CONSTRAINT" not in _constraints(project, "fk_parent")

        # Step 3: a plain run does not recover: the parent trips over the same
        # backup before it builds anything, and the child is skipped behind it.
        results = run_dbt(["run"], expect_pass=False)
        statuses = {result.node.name: result.status for result in results}
        assert statuses == {"fk_parent": "error", "fk_child": "skipped"}
        assert _fk_target(project, "fk_child", "FK_fk_child_parent") == "fk_parent__dbt_backup"

        # Step 4: rebuilding the child alone fails too - it has no parent key
        # to reference - but its swap drops the old child, and the stale key
        # with it. That is what unblocks the parent; a plain run then rebuilds
        # both in order. (Dropping the child's key by hand does the same.)
        _, output = run_dbt_and_capture(["run", "--select", "fk_child"], expect_pass=False)
        assert "no primary or candidate keys" in output
        assert "FOREIGN_KEY_CONSTRAINT" not in _constraints(project, "fk_child")
        run_dbt(["run"])
        assert _constraints(project, "fk_parent")["PRIMARY_KEY_CONSTRAINT"] == ["PK_fk_parent"]
        assert _fk_target(project, "fk_child", "FK_fk_child_parent") == "fk_parent"
        assert _leftovers(project) == []

        # Step 5: the shipped pre_hook on the parent drops the inbound key
        # before the build, so the parent rebuilds on its own. The documented
        # cost: the child's key is gone until the child is next built.
        write_file(FK_PARENT_WITH_PRE_HOOK, "models", "fk_parent.sql")
        run_dbt(["run", "--select", "fk_parent"])
        assert _constraints(project, "fk_parent")["PRIMARY_KEY_CONSTRAINT"] == ["PK_fk_parent"]
        assert "FOREIGN_KEY_CONSTRAINT" not in _constraints(project, "fk_child")
        assert _leftovers(project) == []

        # Step 6: the child's next build puts it back.
        run_dbt(["run", "--select", "fk_child"])
        assert _fk_target(project, "fk_child", "FK_fk_child_parent") == "fk_parent"

        # Step 7: a full run, in dependency order, ends with the key in place.
        run_dbt(["run"])
        assert _fk_target(project, "fk_child", "FK_fk_child_parent") == "fk_parent"
        assert _leftovers(project) == []


# --------------------------------------------------------------------------- #
# Scenario 4: the `expression` clustering override                             #
# --------------------------------------------------------------------------- #

# Not a format string: str.format would collapse the Jinja braces.
KEYED_MODEL = "{{ config(materialized='table', as_columnstore=COLUMNSTORE) }}\nselect 1 as id"

KEYED_SCHEMA = """
version: 2
models:
  - name: keyed_model
    config:
      contract:
        enforced: true
    constraints:
      - type: primary_key
        name: PK_keyed_model
        columns: [id]
        expression: "{expression}"
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
"""


class TestClusteringExpression:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "keyed_model.sql": KEYED_MODEL.replace("COLUMNSTORE", "True"),
            "schema.yml": KEYED_SCHEMA.format(expression="with (fillfactor = 90)"),
        }

    def test_expression_values(self, project):
        table = "keyed_model"

        # Step 1: anything but the two keywords is rejected before the build,
        # even for a named key whose ALTER would only run after the commit.
        _, output = run_dbt_and_capture(["run"], expect_pass=False)
        assert "Invalid expression" in output
        assert "fillfactor" in output
        assert not _exists(project, table)

        # Step 2: the keywords are matched case-insensitively.
        write_file(KEYED_SCHEMA.format(expression="NONCLUSTERED"), "models", "schema.yml")
        run_dbt(["run"])
        assert _index_types(project, table)["PK_keyed_model"] == "NONCLUSTERED"

        # Step 3: `clustered` under the default as_columnstore has no slot to
        # take - SQL Server refuses a second clustered index.
        write_file(KEYED_SCHEMA.format(expression="clustered"), "models", "schema.yml")
        _, output = run_dbt_and_capture(["run"], expect_pass=False)
        assert "more than one clustered index" in output

        # Step 4: with the columnstore index off it is honoured.
        write_file(KEYED_MODEL.replace("COLUMNSTORE", "False"), "models", table + ".sql")
        run_dbt(["run"])
        indexes = _index_types(project, table)
        assert indexes["PK_keyed_model"] == "CLUSTERED"
        assert "CLUSTERED COLUMNSTORE" not in indexes.values()


# --------------------------------------------------------------------------- #
# Scenario 5: the prebuilt build path                                          #
# --------------------------------------------------------------------------- #

PREBUILT_MODEL = """
{{ config(materialized='table', full_refresh_build='prebuilt') }}
select 1 as id, 'blue' as color
"""

PREBUILT_SCHEMA = """
version: 2
models:
  - name: prebuilt_model
    config:
      contract:
        enforced: true
    constraints:
      - type: primary_key
        name: PK_prebuilt_model
        columns: [id]
      - type: check
        expression: id > 0
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
      - name: color
        data_type: varchar(100)
"""


class TestPrebuiltBuildCarriesConstraints:
    """`prebuilt` builds the table in place under its final name (first build
    and --full-refresh) and falls back to the rename-swap in between. Named
    and unnamed constraints have to come through all three."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"prebuilt_model.sql": PREBUILT_MODEL, "schema.yml": PREBUILT_SCHEMA}

    def test_every_build_boundary(self, project):
        table = "prebuilt_model"
        expected = {"PRIMARY_KEY_CONSTRAINT": ["PK_prebuilt_model"]}

        # Step 1: first build, in place.
        _, output = run_dbt_and_capture(["run"])
        assert "full_refresh_build=prebuilt" in output
        first = _constraints(project, table)
        assert first["PRIMARY_KEY_CONSTRAINT"] == expected["PRIMARY_KEY_CONSTRAINT"]
        assert len(first["CHECK_CONSTRAINT"]) == 1
        assert _not_null_columns(project, table) == ["id"]
        assert "CLUSTERED COLUMNSTORE" in _index_types(project, table).values()

        # Step 2: a steady-state run takes the rename-swap.
        run_dbt(["run"])
        swapped = _constraints(project, table)
        assert swapped["PRIMARY_KEY_CONSTRAINT"] == expected["PRIMARY_KEY_CONSTRAINT"]
        assert len(swapped["CHECK_CONSTRAINT"]) == 1

        # Step 3: --full-refresh drops and rebuilds in place again.
        run_dbt(["run", "--full-refresh"])
        refreshed = _constraints(project, table)
        assert refreshed["PRIMARY_KEY_CONSTRAINT"] == expected["PRIMARY_KEY_CONSTRAINT"]
        assert len(refreshed["CHECK_CONSTRAINT"]) == 1
        assert _leftovers(project) == []


# --------------------------------------------------------------------------- #
# Scenario 6: several CHECKs on one column                                     #
# --------------------------------------------------------------------------- #

CHECKS_MODEL = "{{ config(materialized='table') }}\nselect 5 as id, 'blue' as color"

CHECKS_SCHEMA = """
version: 2
models:
  - name: checks_model
    config:
      contract:
        enforced: true
    constraints:
      - type: check
        expression: color <> 'red'
    columns:
      - name: id
        data_type: int
        constraints:
          - type: not_null
          - type: check
            expression: id > 0
          - type: check
            expression: id < 10
      - name: color
        data_type: varchar(100)
"""


class TestSeveralChecksOnOneColumn:
    """SQL Server allows one column-level CHECK per column, so the adapter
    hoists them to table level. Both of the column's checks, and the model's
    own, must exist and be enforced."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"checks_model.sql": CHECKS_MODEL, "schema.yml": CHECKS_SCHEMA}

    def test_all_checks_exist_and_bite(self, project):
        table = "checks_model"

        # Step 1: three CHECK objects, with the three predicates.
        run_dbt(["run"])
        assert len(_constraints(project, table)["CHECK_CONSTRAINT"]) == 3
        definitions = [
            row[0]
            for row in project.run_sql(
                f"""
                select definition from sys.check_constraints
                where parent_object_id = OBJECT_ID('{project.test_schema}.{table}')
                """,
                fetch="all",
            )
        ]
        joined = " ".join(definitions)
        assert "[id]>(0)" in joined.replace(" ", "")
        assert "[id]<(10)" in joined.replace(" ", "")
        assert "red" in joined

        # Step 2: the second check on the column is enforced, not just present.
        write_file(CHECKS_MODEL.replace("5 as id", "50 as id"), "models", table + ".sql")
        _, output = run_dbt_and_capture(["run"], expect_pass=False)
        assert "CHECK constraint" in output
        assert _row_count(project, table) == 1


# --------------------------------------------------------------------------- #
# Scenario 7: dbt unit tests on a contract with keys                           #
# --------------------------------------------------------------------------- #

UT_UPSTREAM = "{{ config(materialized='table') }}\nselect 1 as id, 'blue' as color"

UT_MODEL = "{{ config(materialized='table') }}\nselect id, color from {{ ref('ut_upstream') }}"

UT_SCHEMA = """
version: 2
models:
  - name: ut_upstream
  - name: ut_model
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
      - name: color
        data_type: varchar(100)

unit_tests:
  - name: ut_model_passes_rows_through
    model: ut_model
    given:
      - input: ref('ut_upstream')
        rows:
          - {id: 1, color: blue}
          - {id: 1, color: blue}
    expect:
      rows:
        - {id: 1, color: blue}
        - {id: 1, color: blue}
"""


class TestUnitTestFixturesIgnoreKeys:
    """A unit test's rows are stand-ins, not data that satisfies the contract's
    keys: duplicate ids in the fixture must not fail the test on a PRIMARY KEY
    or UNIQUE copied off the contract, while the real build keeps both."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ut_upstream.sql": UT_UPSTREAM,
            "ut_model.sql": UT_MODEL,
            "schema.yml": UT_SCHEMA,
        }

    def test_fixture_rows_may_violate_the_keys(self, project):
        # Step 1: the real build carries the keys.
        run_dbt(["run"])
        built = _constraints(project, "ut_model")
        assert len(built["PRIMARY_KEY_CONSTRAINT"]) == 1
        assert len(built["UNIQUE_CONSTRAINT"]) == 1

        # Step 2: the unit test passes with rows that violate both.
        results = run_dbt(["test", "--select", "test_type:unit"])
        assert len(results) == 1
        assert results[0].status == "pass"
