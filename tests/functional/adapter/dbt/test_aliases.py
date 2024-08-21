import pytest
from dbt.tests.adapter.aliases import fixtures
from dbt.tests.adapter.aliases.test_aliases import (
    BaseAliasErrors,
    BaseAliases,
    BaseSameAliasDifferentDatabases,
    BaseSameAliasDifferentSchemas,
)

# we override the default as the SQLServer adapter uses CAST instead of :: for type casting
MACROS__CAST_SQL_SQLServer = """


{% macro string_literal(s) -%}
  {{ adapter.dispatch('string_literal', macro_namespace='test')(s) }}
{%- endmacro %}

{% macro default__string_literal(s) %}
    CAST('{{ s }}' AS VARCHAR(8000))
{% endmacro %}

"""


class TestAliases(BaseAliases):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "cast.sql": MACROS__CAST_SQL_SQLServer,
            "expect_value.sql": fixtures.MACROS__EXPECT_VALUE_SQL,
        }


class TestAliasesError(BaseAliasErrors):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "cast.sql": MACROS__CAST_SQL_SQLServer,
            "expect_value.sql": fixtures.MACROS__EXPECT_VALUE_SQL,
        }


class TestSameAliasDifferentSchemas(BaseSameAliasDifferentSchemas):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "cast.sql": MACROS__CAST_SQL_SQLServer,
            "expect_value.sql": fixtures.MACROS__EXPECT_VALUE_SQL,
        }


class TestSameAliasDifferentDatabases(BaseSameAliasDifferentDatabases):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "cast.sql": MACROS__CAST_SQL_SQLServer,
            "expect_value.sql": fixtures.MACROS__EXPECT_VALUE_SQL,
        }
