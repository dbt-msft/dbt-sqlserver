"""
Unit tests for the sqlserver__generate_schema_name macro.

These tests use Jinja2 directly - no database connection required.

Behaviour matrix:
  flag=False (default/legacy) + no custom_schema  -> target.schema
  flag=False (default/legacy) + "reporting"       -> "reporting"  (NO prefix)
  flag=True  (dbt-core concat) + no custom_schema -> target.schema
  flag=True  (dbt-core concat) + "reporting"      -> "target_schema_reporting"
"""

import jinja2
import pytest

# Inline minimal templates (only the two macros under test)
_DEFAULT_GENERATE_SCHEMA_NAME = """
{% macro default__generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
"""

_SQLSERVER_GENERATE_SCHEMA_NAME = """
{% macro sqlserver__generate_schema_name(custom_schema_name, node) -%}
    {%- if adapter.behavior.dbt_sqlserver_use_default_schema_concat -%}
        {{ default__generate_schema_name(custom_schema_name, node) }}
    {%- elif var('dbt_sqlserver_use_default_schema_concat', false) -%}
        {{ default__generate_schema_name(custom_schema_name, node) }}
    {%- else -%}
        {%- set default_schema = target.schema -%}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
"""


def _render(
    custom_schema_name,
    target_schema="my_target_schema",
    use_default_concat=False,
    use_legacy_var=False,
):
    """Render sqlserver__generate_schema_name with a minimal Jinja2 env."""

    class BehaviorFlag:
        def __init__(self, enabled):
            self.enabled = enabled

        def __bool__(self):
            return self.enabled

        @property
        def no_warn(self):
            return self.enabled

    class Behavior:
        def __init__(self, enabled):
            self.dbt_sqlserver_use_default_schema_concat = BehaviorFlag(enabled)

    env = jinja2.Environment(
        trim_blocks=True,
        lstrip_blocks=True,
        extensions=["jinja2.ext.do"],
    )
    template_src = (
        _DEFAULT_GENERATE_SCHEMA_NAME
        + "\n"
        + _SQLSERVER_GENERATE_SCHEMA_NAME
        + "\n"
        + "{{ sqlserver__generate_schema_name(custom_schema_name, node) }}"
    )
    tmpl = env.from_string(template_src)
    ctx = {
        "custom_schema_name": custom_schema_name,
        "node": None,
        "target": type("Target", (), {"schema": target_schema})(),
        "var": lambda key, default=None: (
            use_legacy_var if key == "dbt_sqlserver_use_default_schema_concat" else default
        ),
        "adapter": type("Adapter", (), {"behavior": Behavior(use_default_concat)})(),
    }
    result = tmpl.render(**ctx).strip()
    return result


# ---------------------------------------------------------------------------
# Tests - flag=False  (legacy behaviour, the default)
# ---------------------------------------------------------------------------


class TestLegacyBehaviour:
    """When flag is False (or absent), uses legacy adapter behaviour."""

    def test_no_custom_schema_returns_target_schema(self):
        """Without a custom schema, the target schema is returned unchanged."""
        assert _render(None, target_schema="dbt_dev", use_default_concat=False) == "dbt_dev"

    def test_custom_schema_returned_directly_without_prefix(self):
        """
        Key difference from dbt-core: custom_schema_name is NOT prefixed
        with target.schema.  "reporting" stays "reporting".
        """
        assert (
            _render("reporting", target_schema="dbt_dev", use_default_concat=False) == "reporting"
        )

    def test_custom_schema_whitespace_is_trimmed(self):
        assert (
            _render("  analytics  ", target_schema="dbt_dev", use_default_concat=False)
            == "analytics"
        )

    def test_flag_absent_defaults_to_legacy(self):
        """var() returning its default (False) gives the same legacy result."""
        assert _render("sales", target_schema="prod", use_default_concat=False) == "sales"

    def test_vars_enable_default_concat_when_flag_absent(self):
        assert (
            _render(
                "reporting",
                target_schema="dbt_dev",
                use_default_concat=False,
                use_legacy_var=True,
            )
            == "dbt_dev_reporting"
        )


# ---------------------------------------------------------------------------
# Tests - flag=True  (dbt-core default concatenation)
# ---------------------------------------------------------------------------


class TestDefaultConcatBehaviour:
    """When flag is True, delegates to default__generate_schema_name."""

    def test_no_custom_schema_returns_target_schema(self):
        assert _render(None, target_schema="dbt_dev", use_default_concat=True) == "dbt_dev"

    def test_custom_schema_is_prefixed_with_target_schema(self):
        """dbt-core: "dbt_dev" + "_" + "reporting" -> "dbt_dev_reporting" """
        assert (
            _render("reporting", target_schema="dbt_dev", use_default_concat=True)
            == "dbt_dev_reporting"
        )

    def test_custom_schema_concatenation_uses_underscore_separator(self):
        assert (
            _render("finance", target_schema="analytics", use_default_concat=True)
            == "analytics_finance"
        )


# ---------------------------------------------------------------------------
# Parametrised matrix test
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "custom_schema_name, target_schema, use_default_concat, expected",
    [
        (None, "dbt_dev", False, "dbt_dev"),
        ("reporting", "dbt_dev", False, "reporting"),
        ("  trimmed  ", "dbt_dev", False, "trimmed"),
        (None, "dbt_dev", True, "dbt_dev"),
        ("reporting", "dbt_dev", True, "dbt_dev_reporting"),
        ("finance", "analytics", True, "analytics_finance"),
    ],
    ids=[
        "legacy-no_custom",
        "legacy-custom_direct",
        "legacy-custom_trimmed",
        "concat-no_custom",
        "concat-custom_prefixed",
        "concat-different_target",
    ],
)
def test_schema_name_generation(custom_schema_name, target_schema, use_default_concat, expected):
    assert _render(custom_schema_name, target_schema, use_default_concat) == expected
