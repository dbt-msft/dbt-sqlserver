import pytest

from dbt.tests.util import run_dbt

upstream_key_sql = """
{% macro upstream_key(field_list) %}
    {%- set fields = [] -%}
    {%- for field in field_list -%}
        {%- do fields.append(
            "coalesce(cast(" ~ field ~ " as " ~ dbt.type_string() ~ "),"
            ~ " '_dbt_utils_surrogate_key_null_')"
        ) -%}
        {%- if not loop.last %}{%- do fields.append("'-'") -%}{%- endif -%}
    {%- endfor -%}
    {{ dbt.hash(dbt.concat(fields)) }}
{% endmacro %}
"""

surrogate_key_cases_sql = """
{% set long_a = "replicate(cast('a' as varchar(max)), 9000) + 'x'" %}
{% set long_b = "replicate(cast('a' as varchar(max)), 9000) + 'y'" %}
{% set half = "replicate(cast('b' as varchar(max)), 5000)" %}
{% set cases = [
    ("int and string", ["cast(42 as int)", "'abc'"]),
    ("single field", ["cast(42 as int)"]),
    ("null", ["cast(null as int)", "'abc'"]),
    ("date", ["cast('2024-01-07' as date)", "cast(1.5 as decimal(10, 2))"]),
    ("nvarchar", ["N'café 中'", "cast(null as nvarchar(10))"]),
    ("uniqueidentifier", ["cast('6F9619FF-8B86-D011-B42D-00C04FC964FF' as uniqueidentifier)"]),
    ("long field a", [long_a]),
    ("long field b", [long_b]),
    ("long combined", [half, half]),
] %}
{% for label, fields in cases %}
select
    '{{ label }}' as label,
    {{ sqlserver__generate_surrogate_key(fields) }} as actual,
    {{ upstream_key(fields) }} as expected
{% if not loop.last %}union all{% endif %}
{% endfor %}
"""


class TestGenerateSurrogateKey:
    @pytest.fixture(scope="class")
    def models(self):
        return {"surrogate_key_cases.sql": surrogate_key_cases_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"upstream_key.sql": upstream_key_sql}

    def test_matches_upstream_hash(self, project):
        run_dbt(["run"])

        rows = project.run_sql(
            "select label, actual, expected from {schema}.surrogate_key_cases",
            fetch="all",
        )
        assert len(rows) == 9
        assert [r for r in rows if r[1] is None or r[1] != r[2]] == []
