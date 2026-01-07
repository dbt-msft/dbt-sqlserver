import pytest
from dbt.tests.adapter.utils import fixture_cast_bool_to_text, fixture_dateadd, fixture_listagg
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_array_append import BaseArrayAppend
from dbt.tests.adapter.utils.test_array_concat import BaseArrayConcat
from dbt.tests.adapter.utils.test_array_construct import BaseArrayConstruct
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr
from dbt.tests.adapter.utils.test_cast import BaseCast
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_current_timestamp import (
    BaseCurrentTimestampAware,
    BaseCurrentTimestampNaive,
)
from dbt.tests.adapter.utils.test_date import BaseDate
from dbt.tests.adapter.utils.test_date_spine import BaseDateSpine
from dbt.tests.adapter.utils.test_date_trunc import BaseDateTrunc
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff
from dbt.tests.adapter.utils.test_equals import BaseEquals
from dbt.tests.adapter.utils.test_escape_single_quotes import (
    BaseEscapeSingleQuotesBackslash,
    BaseEscapeSingleQuotesQuote,
)
from dbt.tests.adapter.utils.test_except import BaseExcept
from dbt.tests.adapter.utils.test_generate_series import BaseGenerateSeries
from dbt.tests.adapter.utils.test_get_intervals_between import BaseGetIntervalsBetween
from dbt.tests.adapter.utils.test_get_powers_of_two import BaseGetPowersOfTwo
from dbt.tests.adapter.utils.test_hash import BaseHash
from dbt.tests.adapter.utils.test_intersect import BaseIntersect
from dbt.tests.adapter.utils.test_last_day import BaseLastDay
from dbt.tests.adapter.utils.test_length import BaseLength
from dbt.tests.adapter.utils.test_listagg import BaseListagg
from dbt.tests.adapter.utils.test_null_compare import BaseMixedNullCompare, BaseNullCompare
from dbt.tests.adapter.utils.test_position import BasePosition
from dbt.tests.adapter.utils.test_replace import BaseReplace
from dbt.tests.adapter.utils.test_right import BaseRight
from dbt.tests.adapter.utils.test_safe_cast import BaseSafeCast
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart
from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral
from dbt.tests.adapter.utils.test_timestamps import BaseCurrentTimestamps
from dbt.tests.adapter.utils.test_validate_sql import BaseValidateSqlMethod

# flake8: noqa: E501


class TestAnyValue(BaseAnyValue):
    pass


@pytest.mark.skip(reason="Not supported/Not implemented")
class TestArrayAppend(BaseArrayAppend):
    pass


@pytest.mark.skip(reason="Not supported/Not implemented")
class TestArrayConcat(BaseArrayConcat):
    pass


@pytest.mark.skip(reason="Not supported/Not implemented")
class TestArrayConstruct(BaseArrayConstruct):
    pass


@pytest.mark.skip(reason="Not supported/Not implemented")
class TestBoolOr(BaseBoolOr):
    pass


class TestCast(BaseCast):
    pass


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


class TestCastBoolToText(BaseCastBoolToText):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_cast_bool_to_text.yml": fixture_cast_bool_to_text.models__test_cast_bool_to_text_yml,  # noqa: E501
            "test_cast_bool_to_text.sql": self.interpolate_macro_namespace(
                models__test_cast_bool_to_text_sql, "cast_bool_to_text"
            ),
        }


class TestConcat(BaseConcat):
    pass


@pytest.mark.skip(reason="Only should implement Aware or Naive. Opted for Naive.")
class TestCurrentTimestampAware(BaseCurrentTimestampAware):
    pass


class TestCurrentTimestampNaive(BaseCurrentTimestampNaive):
    pass


@pytest.mark.skip(reason="Date spine relies on recursive CTES which are not supported.")
class TestDate(BaseDate):
    pass


@pytest.mark.skip(reason="Date spine relies on recursive CTES which are not supported.")
class TestDateSpine(BaseDateSpine):
    pass


class TestDateTrunc(BaseDateTrunc):
    pass


class TestDateAdd(BaseDateAdd):
    models__test_dateadd_sql = """
    with data as (

        select * from {{ ref('data_dateadd') }}

    )

    select
        case
            when datepart = 'hour' then cast({{ dateadd('hour', 'interval_length', 'from_time') }} as {{ api.Column.translate_type('timestamp') }})
            when datepart = 'day' then cast({{ dateadd('day', 'interval_length', 'from_time') }} as {{ api.Column.translate_type('timestamp') }})
            when datepart = 'month' then cast({{ dateadd('month', 'interval_length', 'from_time') }} as {{ api.Column.translate_type('timestamp') }})
            when datepart = 'year' then cast({{ dateadd('year', 'interval_length', 'from_time') }} as {{ api.Column.translate_type('timestamp') }})
            else null
        end as actual,
        result as expected

    from data
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test",
            # this is only needed for BigQuery, right?
            # no harm having it here until/unless there's an adapter that doesn't support the 'timestamp' type
            "seeds": {
                "test": {
                    "data_dateadd": {
                        "+column_types": {
                            "from_time": "datetime2(6)",
                            "result": "datetime2(6)",
                        },
                    },
                },
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_dateadd.csv": fixture_dateadd.seeds__data_dateadd_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_dateadd.yml": fixture_dateadd.models__test_dateadd_yml,
            "test_dateadd.sql": self.interpolate_macro_namespace(
                self.models__test_dateadd_sql, "dateadd"
            ),
        }


class TestDateDiff(BaseDateDiff):
    pass


class TestEquals(BaseEquals):
    pass


class TestEscapeSingleQuotesQuote(BaseEscapeSingleQuotesQuote):
    pass


@pytest.mark.skip(reason="SQLServer applies escaping with double of values")
class TestEscapeSingleQuotesBackslash(BaseEscapeSingleQuotesBackslash):
    pass


class TestExcept(BaseExcept):
    pass


@pytest.mark.skip(
    reason="Only newer versions of SQLServer support Generate Series. Skipping for back compat"
)
class TestGenerateSeries(BaseGenerateSeries):
    pass


class TestGetIntervalsBetween(BaseGetIntervalsBetween):
    pass


class TestGetPowersOfTwo(BaseGetPowersOfTwo):
    pass


class TestHash(BaseHash):
    pass


class TestIntersect(BaseIntersect):
    pass


class TestLastDay(BaseLastDay):
    pass


class TestLength(BaseLength):
    pass


seeds__data_listagg_output_csv = """group_col,expected,version
1,"a_|_b_|_c",bottom_ordered
2,"1_|_a_|_p",bottom_ordered
3,"g_|_g_|_g",bottom_ordered
1,"c_|_b_|_a",reverse_order
2,"p_|_a_|_1",reverse_order
3,"g_|_g_|_g",reverse_order
3,"g, g, g",comma_whitespace_unordered
"""


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
        {{ listagg('string_text', "'_|_'", "order by order_col desc", 2) }} as actual,
        'reverse_order' as version
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

)

select
    calculate.actual,
    data_output.expected
from calculate
left join data_output
on calculate.group_col = data_output.group_col
and calculate.version = data_output.version
"""


class TestListagg(BaseListagg):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_listagg.csv": fixture_listagg.seeds__data_listagg_csv,
            "data_listagg_output.csv": seeds__data_listagg_output_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_listagg.yml": fixture_listagg.models__test_listagg_yml,
            "test_listagg.sql": self.interpolate_macro_namespace(
                models__test_listagg_sql, "listagg"
            ),
        }


class TestMixedNullCompare(BaseMixedNullCompare):
    pass


class TestNullCompare(BaseNullCompare):
    pass


class TestPosition(BasePosition):
    pass


class TestReplace(BaseReplace):
    pass


class TestRight(BaseRight):
    pass


class TestSafeCast(BaseSafeCast):
    pass


class TestSplitPart(BaseSplitPart):
    pass


class TestStringLiteral(BaseStringLiteral):
    pass


@pytest.mark.skip(
    reason="""
                  comment here about why this is skipped.
                  https://github.com/dbt-labs/dbt-adapters/blob/f1987d4313cc94bac9906963dff1337ee0bffbc6/dbt/include/global_project/macros/adapters/timestamps.sql#L39
                  """
)
class TestCurrentTimestamps(BaseCurrentTimestamps):
    pass


class TestValidateSqlMethod(BaseValidateSqlMethod):
    pass
