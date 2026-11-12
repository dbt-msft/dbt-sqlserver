"""date_trunc cases BaseDateTrunc leaves out: it checks day and month on one datetime."""

import pytest

from dbt.tests.util import run_dbt

# 2024-01-07 is a Sunday; weeks run Monday to Sunday. 1753-01-01 and 1900-01-01 are the
# earliest datetime and smalldatetime. A datetimeoffset truncates in its local time, as day
# does. Strings read as datetime use the T separator or none, which every language setting
# reads alike.
date_trunc_cases_sql = """
{% set ts = "cast('2024-01-07 15:30:45.1234567' as datetime2)" %}
{% set tz = "cast('2024-01-07 20:30:45 -05:00' as datetimeoffset)" %}
{% set cases = [
    ("week", ts, "2024-01-01"),
    ("week", "cast('2024-01-07' as date)", "2024-01-01"),
    ("week", "cast('2024-01-07T15:30:45' as datetime)", "2024-01-01"),
    ("week", "cast('2024-01-08' as date)", "2024-01-08"),
    ("week", "cast('2024-01-13 23:59:59.9999999' as datetime2)", "2024-01-08"),
    ("week", "cast('17530101' as datetime)", "1753-01-01"),
    ("week", "cast('19000101' as smalldatetime)", "1900-01-01"),
    ("week", "cast('1700-01-01' as date)", "1699-12-28"),
    ("week", "'2024-01-03T15:30:45'", "2024-01-01"),
    ("wk", ts, "2024-01-01"),
    ("hour", ts, "2024-01-07 15:00:00"),
    ("hh", ts, "2024-01-07 15:00:00"),
    ("minute", ts, "2024-01-07 15:30:00"),
    ("second", ts, "2024-01-07 15:30:45"),
    ("millisecond", ts, "2024-01-07 15:30:45.123"),
    ("second", "cast('9999-12-31 23:59:59.9999999' as datetime2)", "9999-12-31 23:59:59"),
    ("hour", tz, "2024-01-07 20:00:00"),
    ("day", tz, "2024-01-07"),
    ("week", tz, "2024-01-01"),
    ("year", ts, "2024-01-01"),
] %}
{% for datepart, value, expected in cases %}
select
    '{{ datepart }}' as datepart,
    '{{ value | replace("'", "''") }}' as value,
    cast({{ dbt.date_trunc(datepart, value) }} as datetime2) as actual,
    cast('{{ expected }}' as datetime2) as expected
{% if not loop.last %}union all{% endif %}
{% endfor %}
"""

# An empty seed column is typed int, so int input must still build.
date_trunc_types_sql = """
{{ config(materialized="table") }}
{% set ts = "cast('2024-01-07 15:30:45' as datetime2)" %}
select
    {{ dbt.date_trunc("day", ts) }} as day_col,
    {{ dbt.date_trunc("week", ts) }} as week_col,
    {{ dbt.date_trunc("hour", ts) }} as hour_col,
    {{ dbt.date_trunc("week", "cast(null as int)") }} as week_of_int,
    {{ dbt.date_trunc("hour", "cast(null as int)") }} as hour_of_int
"""


class TestDateTruncDateparts:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "date_trunc_cases.sql": date_trunc_cases_sql,
            "date_trunc_types.sql": date_trunc_types_sql,
        }

    def test_date_trunc(self, project):
        run_dbt(["run"])

        wrong = project.run_sql(
            "select datepart, value, actual, expected from {schema}.date_trunc_cases"
            " where actual is null or actual <> expected",
            fetch="all",
        )
        assert wrong == []

        # Day and coarser return date; finer dateparts return datetime2 to hold the time.
        types = project.run_sql(
            "select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS"
            " where TABLE_SCHEMA = '{schema}' and TABLE_NAME = 'date_trunc_types'",
            fetch="all",
        )
        assert dict(types) == {
            "day_col": "date",
            "week_col": "date",
            "hour_col": "datetime2",
            "week_of_int": "date",
            "hour_of_int": "datetime2",
        }
