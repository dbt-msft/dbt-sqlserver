import os
from unittest import mock

import freezegun
import pytest

from dbt.tests.adapter.sample_mode.test_sample_mode import BaseSampleModeTest
from dbt.tests.util import run_dbt

# BaseSampleModeTest.test_sample_mode uses @freezegun.freeze_time("2025-01-03T02:03:0Z").
# Align static dates so the "1 day" sample window
# [2025-01-02 02:03:00, 2025-01-03 02:03:00) selects exactly two rows.
_input_model_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, cast('2025-01-01 02:03:00' as datetime2) as event_time
UNION ALL
select 2 as id, cast('2025-01-02 14:03:00' as datetime2) as event_time
UNION ALL
select 3 as id, cast('2025-01-03 02:02:59' as datetime2) as event_time
"""


class TestSQLServerSampleMode(BaseSampleModeTest):
    @pytest.fixture(scope="class")
    def input_model_sql(self) -> str:
        return _input_model_sql

    @mock.patch.dict(os.environ, {"DBT_EXPERIMENTAL_SAMPLE_MODE": "True"})
    @freezegun.freeze_time("2025-01-03T02:03:0Z")
    def test_sample_mode(self, project) -> None:
        _ = run_dbt(["run"])
        self.assert_row_count(
            project=project,
            relation_name="model_that_samples_input_sql",
            expected_row_count=3,
        )

        _ = run_dbt(["run", "--sample=1 day"])
        self.assert_row_count(
            project=project,
            relation_name="model_that_samples_input_sql",
            expected_row_count=2,
        )
