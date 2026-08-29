from unittest import mock

import pytest
from dbt_common.contracts.constraints import (
    ColumnLevelConstraint,
    ConstraintType,
    ModelLevelConstraint,
)
from dbt_common.exceptions import DbtValidationError

from dbt.adapters.sqlserver import sqlserver_adapter
from dbt.adapters.sqlserver.sqlserver_adapter import SQLServerAdapter


def render_column(**kwargs):
    return SQLServerAdapter.render_column_constraint(ColumnLevelConstraint(**kwargs))


def render_model(**kwargs):
    return SQLServerAdapter.render_model_constraint(ModelLevelConstraint(**kwargs))


class TestRenderColumnConstraint:
    @pytest.mark.parametrize(
        "constraint,expected",
        [
            ({"type": ConstraintType.not_null}, "not null"),
            ({"type": ConstraintType.check, "expression": "id > 0"}, "check (id > 0)"),
            ({"type": ConstraintType.unique}, "unique nonclustered"),
            ({"type": ConstraintType.primary_key}, "primary key nonclustered"),
            ({"type": ConstraintType.custom, "expression": "default 0"}, "default 0"),
            # A check with no predicate has nothing to render.
            ({"type": ConstraintType.check}, None),
        ],
    )
    def test_renders_inline(self, constraint, expected):
        assert render_column(**constraint) == expected

    @pytest.mark.parametrize("clustering", ["clustered", "nonclustered", "  CLUSTERED  "])
    def test_expression_chooses_the_clustering(self, clustering):
        """PRIMARY KEY / UNIQUE default to NONCLUSTERED so they can coexist with
        the clustered columnstore index, but dbt's own `expression` overrides."""
        assert (
            render_column(type=ConstraintType.primary_key, expression=clustering)
            == f"primary key {clustering.strip()}"
        )

    @pytest.mark.parametrize("expression", ["clustered_thing", "with (fillfactor = 90)"])
    def test_anything_but_the_two_keywords_is_rejected(self, expression):
        """Those two are the only thing T-SQL accepts between the keyword and
        the column list, so a typo fails at compile rather than as a syntax
        error inside an EXEC() string."""
        with pytest.raises(DbtValidationError, match="Invalid expression"):
            render_column(type=ConstraintType.primary_key, expression=expression)

    def test_foreign_key_from_to_and_to_columns(self):
        """dbt-core renders `to: ref(...)` to a fully qualified relation, which
        for this adapter carries the database. SQL Server accepts a three-part
        REFERENCES target naming the current database, so it is passed through
        unchanged."""
        assert (
            render_column(
                type=ConstraintType.foreign_key,
                to='"mydb"."dbo"."dim"',
                to_columns=["id"],
            )
            == 'references "mydb"."dbo"."dim" ("id")'
        )

    def test_a_name_on_not_null_is_ignored_with_a_warning(self):
        """not_null is a column attribute with no name of its own; the README
        promises a warning for a name on any column-level constraint.

        AdapterLogger goes through dbt's event system rather than stdlib
        logging, so the logger is patched instead of capturing output.
        """
        with mock.patch.object(sqlserver_adapter, "logger") as patched:
            assert render_column(type=ConstraintType.not_null, name="NN_id") == "not null"
        patched.warning.assert_called_once()
        assert "NN_id" in patched.warning.call_args.args[0]

    def test_foreign_key_from_expression(self):
        assert (
            render_column(type=ConstraintType.foreign_key, expression="dbo.dim (id)")
            == "references dbo.dim (id)"
        )

    def test_foreign_key_without_a_target_renders_nothing(self):
        assert render_column(type=ConstraintType.foreign_key) is None

    def test_a_name_is_ignored_but_the_constraint_is_still_rendered(self):
        """Column-level constraints are always anonymous: the build creates the
        table alongside the one it replaces, and constraint names are unique per
        schema, so a name here would collide on the next build."""
        assert (
            render_column(type=ConstraintType.primary_key, name="PK_x")
            == "primary key nonclustered"
        )


class TestRenderRawColumnsConstraints:
    raw_columns = {
        "id": {
            "name": "id",
            "data_type": "int",
            "constraints": [
                {"type": "not_null"},
                {"type": "primary_key"},
                {"type": "check", "expression": "id > 0"},
            ],
        },
        "color": {"name": "color", "data_type": "varchar(100)", "constraints": []},
    }

    def test_renders_every_constraint(self):
        assert SQLServerAdapter.render_raw_columns_constraints(self.raw_columns) == [
            "id int not null primary key nonclustered",
            "color varchar(100)",
            # CHECK is hoisted to a table-level clause: SQL Server allows only
            # one column-level CHECK per column.
            "check (id > 0)",
        ]

    def test_not_null_only_drops_the_rest(self):
        """Unit-test fixture tables take this path: their rows are stand-ins, so
        a UNIQUE or FOREIGN KEY off the real contract would fail on data that was
        never meant to satisfy it."""
        assert SQLServerAdapter.render_raw_columns_constraints(
            self.raw_columns, only_not_null=True
        ) == ["id int not null", "color varchar(100)"]


class TestRenderModelConstraint:
    def test_unnamed_constraints_render_inline(self):
        assert (
            render_model(type=ConstraintType.primary_key, columns=["id", "region"])
            == 'primary key nonclustered ("id", "region")'
        )

    def test_named_constraints_render_nothing_inline(self):
        """They are applied by ALTER after the swap instead."""
        assert render_model(type=ConstraintType.primary_key, name="PK_x", columns=["id"]) is None


class TestRenderRawModelAlterConstraints:
    def test_only_named_constraints_are_altered_in(self):
        clauses = SQLServerAdapter.render_raw_model_alter_constraints(
            [
                {"type": "primary_key", "columns": ["id"]},
                {"type": "primary_key", "name": "PK_m", "columns": ["id", "region"]},
                {"type": "unique", "name": "UQ_m", "columns": ["email"]},
                {"type": "check", "name": "CK_m", "expression": "id > 0", "columns": []},
                {
                    "type": "foreign_key",
                    "name": "FK_m",
                    "columns": ["dim_id"],
                    "to": "dbo.dim",
                    "to_columns": ["id"],
                },
                {
                    "type": "foreign_key",
                    "name": "FK_legacy",
                    "columns": ["dim_id"],
                    "expression": "dbo.dim (id)",
                },
            ]
        )
        assert clauses == [
            {
                "name": "PK_m",
                "clause": 'add constraint "PK_m" primary key nonclustered ("id", "region")',
            },
            {"name": "UQ_m", "clause": 'add constraint "UQ_m" unique nonclustered ("email")'},
            {"name": "CK_m", "clause": 'add constraint "CK_m" check (id > 0)'},
            {
                "name": "FK_m",
                "clause": 'add constraint "FK_m" foreign key ("dim_id") references dbo.dim ("id")',
            },
            {
                "name": "FK_legacy",
                "clause": (
                    'add constraint "FK_legacy" foreign key ("dim_id") references dbo.dim (id)'
                ),
            },
        ]

    def test_the_bare_name_is_returned_for_the_existence_guard(self):
        """The macro tests sys.objects for the name before adding it, so it
        needs the name unquoted as well as inside the clause."""
        clauses = SQLServerAdapter.render_raw_model_alter_constraints(
            [{"type": "primary_key", "name": "PK_m", "columns": ["id"]}]
        )
        assert clauses[0]["name"] == "PK_m"
        assert clauses[0]["clause"].startswith('add constraint "PK_m" ')

    def test_clustering_override_applies_to_the_alter_form_too(self):
        assert SQLServerAdapter.render_raw_model_alter_constraints(
            [{"type": "primary_key", "name": "PK_m", "columns": ["id"], "expression": "clustered"}]
        ) == [{"name": "PK_m", "clause": 'add constraint "PK_m" primary key clustered ("id")'}]

    def test_no_constraints_renders_nothing(self):
        assert SQLServerAdapter.render_raw_model_alter_constraints([]) == []
