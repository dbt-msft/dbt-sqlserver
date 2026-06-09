from unittest.mock import MagicMock, patch

import pytest

from dbt.adapters.sqlserver.sqlserver_adapter import SQLServerAdapter
from dbt.adapters.sqlserver.sqlserver_relation import SQLServerRelation


@pytest.fixture
def adapter():
    config = MagicMock()
    config.flags = {}
    config.project_name = "test"
    config.credentials.type = "sqlserver"
    mp_context = MagicMock()
    adapter = SQLServerAdapter(config, mp_context)
    adapter._get_row_count = MagicMock(return_value=0)
    adapter.get_columns_in_relation = MagicMock(return_value=[])
    adapter.alter_column_type = MagicMock()
    adapter.behavior = MagicMock()
    adapter.behavior.dbt_sqlserver_enable_safe_type_expansion = True
    return adapter


def make_rel(name="t"):
    rel = MagicMock(spec=SQLServerRelation)
    rel.__str__ = lambda s: f"test_schema.{name}"
    return rel


class TestExpandColumnTypes:
    def test_skips_row_count_when_max_rows_is_negative_one(self, adapter):
        adapter.expand_column_types(make_rel("goal"), make_rel("current"), max_rows=-1)
        adapter._get_row_count.assert_not_called()

    def test_blocks_safe_expansion_when_max_rows_is_zero(self, adapter):
        adapter._get_row_count.return_value = 0
        adapter.get_columns_in_relation = MagicMock(return_value=[])
        adapter.alter_column_type = MagicMock()

        goal = make_rel("goal")
        goal_col = MagicMock()
        goal_col.name = "c"
        goal_col.dtype = "nvarchar"
        goal_col.is_string = MagicMock(return_value=True)
        goal_col.is_number = MagicMock(return_value=True)
        goal_col.string_size = MagicMock(return_value=20)
        goal_col.string_type_instance = MagicMock(return_value="nvarchar(20)")
        goal_col.data_type = "nvarchar(20)"

        current = make_rel("current")
        current_col = MagicMock()
        current_col.name = "c"
        current_col.dtype = "varchar"
        current_col.is_string = MagicMock(return_value=True)
        current_col.is_number = MagicMock(return_value=True)
        current_col.can_expand_to = MagicMock(return_value=False)
        current_col.can_expand_safe = MagicMock(return_value=True)

        adapter.get_columns_in_relation.side_effect = lambda r: (
            [goal_col] if r is goal else [current_col]
        )

        with patch("dbt.adapters.sqlserver.sqlserver_adapter.logger"):
            adapter.expand_column_types(goal, current, max_rows=0)

        adapter._get_row_count.assert_not_called()
        adapter.alter_column_type.assert_not_called()

    def test_reads_row_count_when_within_limit(self, adapter):
        adapter._get_row_count.return_value = 50
        adapter.expand_column_types(make_rel("goal"), make_rel("current"), max_rows=100)
        adapter._get_row_count.assert_called_once()

    def test_emits_warning_when_row_count_exceeds_max(self, adapter):
        adapter._get_row_count.return_value = 200
        with patch("dbt.adapters.sqlserver.sqlserver_adapter.logger") as logger:
            adapter.expand_column_types(make_rel("goal"), make_rel("current"), max_rows=100)
        adapter._get_row_count.assert_called_once()
        logger.warning.assert_called_once()

    def test_expand_target_column_types_forwards_max_rows(self, adapter):
        adapter._get_row_count.return_value = 0
        adapter.get_columns_in_relation = MagicMock(return_value=[])
        adapter.alter_column_type = MagicMock()

        goal = make_rel("goal")
        current = make_rel("current")
        max_rows = 500

        with patch.object(adapter, "expand_column_types") as mock_expand:
            adapter.expand_target_column_types(goal, current, max_rows=max_rows)

        mock_expand.assert_called_once_with(goal, current, max_rows)
