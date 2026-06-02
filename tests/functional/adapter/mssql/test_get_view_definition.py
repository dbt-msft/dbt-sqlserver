import pytest

from dbt.tests.util import run_dbt_and_capture

# Builds a relation for the given schema/identifier and runs the adapter's
# get_view_definition_sql against it, logging whether a definition row came
# back. Lets the tests assert the macro's behaviour directly instead of going
# through the full view materialization.
VALIDATE_GET_VIEW_DEFINITION_MACRO = """
{% macro validate_get_view_definition_sql(schema, identifier) -%}
    {% set relation = api.Relation.create(
        database=target.database, schema=schema, identifier=identifier, type='view'
    ) %}
    {% set result = run_query(get_view_definition_sql(relation)) %}
    {% if result is not none and result.rows | length > 0 and result.rows[0][0] is not none %}
        {{ log("view_definition_found: true") }}
    {% else %}
        {{ log("view_definition_found: false") }}
    {% endif %}
{% endmacro %}
"""


class TestGetViewDefinitionSql:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"validate_get_view_definition_sql.sql": VALIDATE_GET_VIEW_DEFINITION_MACRO}

    def _resolve(self, schema, identifier):
        kwargs = {"schema": schema, "identifier": identifier}
        _, log_output = run_dbt_and_capture(
            [
                "--debug",
                "run-operation",
                "validate_get_view_definition_sql",
                "--args",
                str(kwargs),
            ]
        )
        return log_output

    def test_resolves_identifier_containing_right_bracket(self, project):
        """A legal identifier containing ``]`` must still resolve. quotename()
        doubles the ``]`` so OBJECT_ID gets a valid object name; the old manual
        bracket-quoting produced a malformed name and returned NULL."""
        identifier = "weird]name"
        # ``]`` is escaped by doubling it inside a bracketed identifier.
        project.run_sql(f"create view {project.test_schema}.[weird]]name] as select 1 as id")

        log_output = self._resolve(project.test_schema, identifier)
        assert "view_definition_found: true" in log_output

    def test_missing_view_returns_no_rows(self, project):
        """A view that does not exist yields zero rows, which the view
        materialization relies on to fall through to a create."""
        log_output = self._resolve(project.test_schema, "does_not_exist")
        assert "view_definition_found: false" in log_output
