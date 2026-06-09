import pytest

from dbt.adapters.sqlserver.sqlserver_column import SQLServerColumn


def col_kwargs(dtype, char_size=None, numeric_precision=0, numeric_scale=0):
    return {
        "column": "c",
        "dtype": dtype,
        "char_size": char_size,
        "numeric_precision": numeric_precision,
        "numeric_scale": numeric_scale,
    }


@pytest.mark.parametrize(
    "src_kwargs,tgt_kwargs,expect_with_flag,expect_without_flag",
    [
        # String same-family expansions always work
        (col_kwargs("varchar", char_size=10), col_kwargs("varchar", char_size=100), True, True),
        (col_kwargs("char", char_size=5), col_kwargs("char", char_size=20), True, True),
        (col_kwargs("nvarchar", char_size=50), col_kwargs("nvarchar", char_size=200), True, True),
        (col_kwargs("nchar", char_size=10), col_kwargs("nchar", char_size=30), True, True),
        # String same-size does not expand
        (col_kwargs("varchar", char_size=100), col_kwargs("varchar", char_size=100), False, False),
        # String smaller target does not expand
        (col_kwargs("varchar", char_size=100), col_kwargs("varchar", char_size=50), False, False),
        # String cross-family (VARCHAR -> NVARCHAR) requires flag
        (col_kwargs("varchar", char_size=10), col_kwargs("nvarchar", char_size=10), True, False),
        (col_kwargs("char", char_size=5), col_kwargs("nchar", char_size=5), True, False),
        # String cross-family reverse (NVARCHAR -> VARCHAR) never works
        (col_kwargs("nvarchar", char_size=10), col_kwargs("varchar", char_size=10), False, False),
        # Integer family promotions require the feature flag
        (col_kwargs("int"), col_kwargs("bigint"), True, False),
        (col_kwargs("bit"), col_kwargs("tinyint"), True, False),
        # Integer -> numeric widening requires the feature flag
        (col_kwargs("int"), col_kwargs("numeric", numeric_precision=10), True, False),
        # Numeric/decimal promotions: precision/scale must increase; flag required
        (
            col_kwargs("numeric", numeric_precision=10, numeric_scale=2),
            col_kwargs("numeric", numeric_precision=12, numeric_scale=4),
            True,
            False,
        ),
        (
            col_kwargs("numeric", numeric_precision=10, numeric_scale=2),
            col_kwargs("numeric", numeric_precision=12, numeric_scale=1),
            False,
            False,
        ),
        # Fixed-money types (MONEY/SMALLMONEY)
        (
            col_kwargs("smallmoney", numeric_precision=10, numeric_scale=4),
            col_kwargs("money", numeric_precision=19, numeric_scale=4),
            True,
            False,
        ),
        (
            col_kwargs("money", numeric_precision=19, numeric_scale=4),
            col_kwargs("numeric", numeric_precision=20, numeric_scale=4),
            True,
            False,
        ),
        # MONEY -> NUMERIC with dtype change and equal specs
        (
            col_kwargs("money", numeric_precision=19, numeric_scale=4),
            col_kwargs("numeric", numeric_precision=19, numeric_scale=4),
            True,
            False,
        ),
        # NUMERIC -> MONEY that would shrink precision should not be allowed
        (
            col_kwargs("numeric", numeric_precision=20, numeric_scale=4),
            col_kwargs("money", numeric_precision=19, numeric_scale=4),
            False,
            False,
        ),
    ],
)
def test_can_expand_parametrized(src_kwargs, tgt_kwargs, expect_with_flag, expect_without_flag):
    src = SQLServerColumn(**src_kwargs)
    tgt = SQLServerColumn(**tgt_kwargs)

    assert src.can_expand_to(tgt) is expect_without_flag
    assert (src.can_expand_to(tgt) or src.can_expand_safe(tgt)) is expect_with_flag
