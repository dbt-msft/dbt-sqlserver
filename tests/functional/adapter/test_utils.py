import pytest
from dbt.tests.adapter.utils.fixture_cast_bool_to_text import models__test_cast_bool_to_text_yml
from dbt.tests.adapter.utils.fixture_listagg import (
    models__test_listagg_yml,
    seeds__data_listagg_csv,
)
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_array_append import BaseArrayAppend
from dbt.tests.adapter.utils.test_array_concat import BaseArrayConcat
from dbt.tests.adapter.utils.test_array_construct import BaseArrayConstruct
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_current_timestamp import BaseCurrentTimestampNaive
from dbt.tests.adapter.utils.test_date_trunc import BaseDateTrunc
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff
from dbt.tests.adapter.utils.test_escape_single_quotes import BaseEscapeSingleQuotesQuote
from dbt.tests.adapter.utils.test_except import BaseExcept
from dbt.tests.adapter.utils.test_hash import BaseHash
from dbt.tests.adapter.utils.test_intersect import BaseIntersect
from dbt.tests.adapter.utils.test_last_day import BaseLastDay
from dbt.tests.adapter.utils.test_length import BaseLength
from dbt.tests.adapter.utils.test_listagg import BaseListagg
from dbt.tests.adapter.utils.test_position import BasePosition
from dbt.tests.adapter.utils.test_replace import BaseReplace
from dbt.tests.adapter.utils.test_right import BaseRight
from dbt.tests.adapter.utils.test_safe_cast import BaseSafeCast
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart
from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral


class BaseFixedMacro:
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "test_assert_equal.sql": """
    {% test assert_equal(model, actual, expected) %}
    select * from {{ model }}
    where {{ actual }} != {{ expected }}
    or ({{ actual }} is null and {{ expected }} is not null)
    or ({{ expected }} is null and {{ actual }} is not null)
    {% endtest %}
    """
        }


class TestAnyValueFabric(BaseFixedMacro, BaseAnyValue):
    pass


@pytest.mark.skip("bool_or not supported in this adapter")
class TestBoolOrFabric(BaseFixedMacro, BaseBoolOr):
    pass


class TestCastBoolToTextFabric(BaseFixedMacro, BaseCastBoolToText):
    @pytest.fixture(scope="class")
    def models(self):
        models__test_cast_bool_to_text_sql = """
        with data as (

            select 0 as input, 'false' as expected union all
            select 1 as input, 'true' as expected union all
            select null as input, null as expected

        )

        select

            {{ cast_bool_to_text("input") }} as actual,
            expected

        from data
        """

        return {
            "test_cast_bool_to_text.yml": models__test_cast_bool_to_text_yml,
            "test_cast_bool_to_text.sql": self.interpolate_macro_namespace(
                models__test_cast_bool_to_text_sql, "cast_bool_to_text"
            ),
        }


class TestConcatFabric(BaseFixedMacro, BaseConcat):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_concat.csv": """input_1,input_2,output
a,b,ab
a,,a
,b,b
"""
        }


class TestDateTruncFabric(BaseFixedMacro, BaseDateTrunc):
    pass


seeds__data_hash_csv = """input_1,output
ab,187ef4436122d1cc2f40dc2b92f0eba0
a,0cc175b9c0f1b6a831c399e269772661
1,c4ca4238a0b923820dcc509a6f75849b
,d41d8cd98f00b204e9800998ecf8427e"""


class TestHashFabric(BaseFixedMacro, BaseHash):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_hash.csv": seeds__data_hash_csv}


class TestStringLiteralFabric(BaseFixedMacro, BaseStringLiteral):
    pass


class TestSplitPartFabric(BaseFixedMacro, BaseSplitPart):
    pass


class TestDateDiffFabric(BaseFixedMacro, BaseDateDiff):
    pass


class TestEscapeSingleQuotesFabric(BaseFixedMacro, BaseEscapeSingleQuotesQuote):
    pass


class TestIntersectFabric(BaseFixedMacro, BaseIntersect):
    pass


class TestLastDayFabric(BaseFixedMacro, BaseLastDay):
    pass


class TestLengthFabric(BaseFixedMacro, BaseLength):
    pass


class TestListaggFabric(BaseFixedMacro, BaseListagg):
    #  Only supported in SQL Server 2017 and later or cloud versions
    #  DISTINCT not supported
    #  limit not supported
    @pytest.fixture(scope="class")
    def seeds(self):
        seeds__data_listagg_output_csv = """group_col,expected,version
1,"a_|_b_|_c",bottom_ordered
2,"1_|_a_|_p",bottom_ordered
3,"g_|_g_|_g",bottom_ordered
3,"g, g, g",comma_whitespace_unordered
3,"g,g,g",no_params
        """

        return {
            "data_listagg.csv": seeds__data_listagg_csv,
            "data_listagg_output.csv": seeds__data_listagg_output_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        models__test_listagg_sql = """
with data as (

    select * from {{ ref('data_listagg') }}

),

data_output as (

    select * from {{ ref('data_listagg_output') }}

),

calculate as (

    select
        group_col,
        {{ listagg('string_text', "'_|_'", "order by order_col") }} as actual,
        'bottom_ordered' as version
    from data
    group by group_col

    union all

    select
        group_col,
        {{ listagg('string_text', "', '") }} as actual,
        'comma_whitespace_unordered' as version
    from data
    where group_col = 3
    group by group_col

    union all

    select
        group_col,
        {{ listagg('string_text') }} as actual,
        'no_params' as version
    from data
    where group_col = 3
    group by group_col

)

select
    calculate.actual,
    data_output.expected
from calculate
left join data_output
on calculate.group_col = data_output.group_col
and calculate.version = data_output.version
"""

        return {
            "test_listagg.yml": models__test_listagg_yml,
            "test_listagg.sql": self.interpolate_macro_namespace(
                models__test_listagg_sql, "listagg"
            ),
        }


class TestRightFabric(BaseFixedMacro, BaseRight):
    pass


class TestSafeCastFabric(BaseFixedMacro, BaseSafeCast):
    pass


class TestDateAddFabric(BaseFixedMacro, BaseDateAdd):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test",
            "seeds": {
                "test": {
                    "data_dateadd": {
                        "+column_types": {
                            "from_time": "datetimeoffset",
                            "result": "datetimeoffset",
                        },
                    },
                },
            },
        }


class TestExceptFabric(BaseFixedMacro, BaseExcept):
    pass


class TestPositionFabric(BaseFixedMacro, BasePosition):
    pass


class TestReplaceFabric(BaseFixedMacro, BaseReplace):
    pass


class TestCurrentTimestampFabric(BaseCurrentTimestampNaive):
    pass


@pytest.mark.skip(reason="arrays not supported")
class TestArrayAppendFabric(BaseFixedMacro, BaseArrayAppend):
    pass


@pytest.mark.skip(reason="arrays not supporteTd")
class TestArrayConcatFabric(BaseFixedMacro, BaseArrayConcat):
    pass


@pytest.mark.skip(reason="arrays not supported")
class TestArrayConstructFabric(BaseFixedMacro, BaseArrayConstruct):
    pass
