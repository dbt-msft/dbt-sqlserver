"""Functional tests for varchar(max) / nvarchar(max) column type expansion
via expand_target_column_types().

These tests verify physical adapter behaviour against real SQL Server catalog
metadata — complementing the unit-level comparison/rendering tests already in
test_can_expand_to.py and test_sqlserver_column.py.
"""

import pytest

from dbt.adapters.sqlserver.sqlserver_relation import (
    SQLServerRelation,
    SQLServerRelationType,
)
from dbt.tests.util import run_dbt

CURRENT_BOUNDED = """
    {{ config(materialized='table') }}
    select
        cast('abc' as varchar(100)) as varchar_col,
        cast(N'abc' as nvarchar(100)) as nvarchar_col
"""

GOAL_MAX = """
    {{ config(materialized='table') }}
    select
        cast('abc' as varchar(max)) as varchar_col,
        cast(N'abc' as nvarchar(max)) as nvarchar_col
"""

CURRENT_MAX = """
    {{ config(materialized='table') }}
    select
        cast('abc' as varchar(max)) as varchar_col,
        cast(N'abc' as nvarchar(max)) as nvarchar_col
"""

GOAL_BOUNDED = """
    {{ config(materialized='table') }}
    select
        cast('abc' as varchar(100)) as varchar_col,
        cast(N'abc' as nvarchar(100)) as nvarchar_col
"""


def _table_relation(adapter, name: str):
    """Build a table-typed relation from a model name."""
    credentials = adapter.config.credentials
    return SQLServerRelation.create(
        database=credentials.database,
        schema=adapter.config.credentials.schema,
        identifier=name,
        type=SQLServerRelationType.Table,
    )


class TestSQLServerMaxStringTypeExpansion:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "current_bounded.sql": CURRENT_BOUNDED,
            "goal_max.sql": GOAL_MAX,
            "current_max.sql": CURRENT_MAX,
            "goal_bounded.sql": GOAL_BOUNDED,
        }

    @staticmethod
    def _columns_by_name(adapter, relation):
        return {col.name.lower(): col for col in adapter.get_columns_in_relation(relation)}

    @staticmethod
    def _assert_max_string_columns(columns):
        varchar_col = columns["varchar_col"]
        assert varchar_col.dtype.lower() == "varchar"
        assert int(varchar_col.char_size) == -1

        nvarchar_col = columns["nvarchar_col"]
        assert nvarchar_col.dtype.lower() == "nvarchar"
        assert int(nvarchar_col.char_size) == -1

    def test_bounded_strings_expand_to_max(self, project):
        """bounded varchar(100)/nvarchar(100) expand to varchar(max)/nvarchar(max)."""
        run_dbt(["run", "--select", "current_bounded"])
        run_dbt(["run", "--select", "goal_max"])

        adapter = project.adapter
        current_relation = _table_relation(adapter, "current_bounded")
        goal_relation = _table_relation(adapter, "goal_max")

        with adapter.connection_named("__test"):
            adapter.expand_target_column_types(goal_relation, current_relation)
            columns = self._columns_by_name(adapter, current_relation)

        self._assert_max_string_columns(columns)

    def test_max_strings_do_not_shrink_to_bounded(self, project):
        """varchar(max)/nvarchar(max) columns are not narrowed to bounded."""
        run_dbt(["run", "--select", "current_max"])
        run_dbt(["run", "--select", "goal_bounded"])

        adapter = project.adapter
        current_relation = _table_relation(adapter, "current_max")
        goal_relation = _table_relation(adapter, "goal_bounded")

        with adapter.connection_named("__test"):
            adapter.expand_target_column_types(goal_relation, current_relation)
            columns = self._columns_by_name(adapter, current_relation)

        self._assert_max_string_columns(columns)
