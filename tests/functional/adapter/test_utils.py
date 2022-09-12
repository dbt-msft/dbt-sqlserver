import pytest
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_concat import BaseConcat
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


class TestAnyValueSQLServer(BaseAnyValue):
    pass


@pytest.mark.skip("Not supported in this adapter")
class TestBoolOrSQLServer(BaseBoolOr):
    pass


class TestCastBoolToTextSQLServer(BaseCastBoolToText):
    pass


class TestConcatSQLServer(BaseConcat):
    pass


class TestDateTruncSQLServer(BaseDateTrunc):
    pass


class TestHashSQLServer(BaseHash):
    pass


class TestStringLiteralSQLServer(BaseStringLiteral):
    pass


class TestSplitPartSQLServer(BaseSplitPart):
    pass


class TestDateDiffSQLServer(BaseDateDiff):
    pass


class TestEscapeSingleQuotesSQLServer(BaseEscapeSingleQuotesQuote):
    pass


class TestIntersectSQLServer(BaseIntersect):
    pass


class TestLastDaySQLServer(BaseLastDay):
    pass


class TestLengthSQLServer(BaseLength):
    pass


class TestListaggSQLServer(BaseListagg):
    pass


class TestRightSQLServer(BaseRight):
    pass


class TestSafeCastSQLServer(BaseSafeCast):
    pass


class TestDateAddSQLServer(BaseDateAdd):
    pass


class TestExceptSQLServer(BaseExcept):
    pass


class TestPositionSQLServer(BasePosition):
    pass


class TestReplaceSQLServer(BaseReplace):
    pass
