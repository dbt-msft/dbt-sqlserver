"""
Tests for snapshots with multiple unique keys.
Tests issue #615 fix - snapshots incorrectly handling multiple unique_key values.
"""

import pytest
from dbt.tests.util import run_dbt

# Seed data for testing
seed_csv = """order_id,customer_id,order_date,amount,updated_at
1,100,2024-01-01,50.00,2024-01-01 10:00:00
2,101,2024-01-01,75.00,2024-01-01 11:00:00
3,100,2024-01-02,120.00,2024-01-02 09:00:00
4,102,2024-01-02,30.00,2024-01-02 10:00:00
5,101,2024-01-03,90.00,2024-01-03 11:00:00
""".lstrip()


# Snapshot with multiple unique keys
snapshot_sql = """
{% snapshot orders_snapshot %}
    {{
        config(
            target_schema=schema,
            unique_key=['order_id', 'customer_id'],
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}
    select * from {{ ref('orders_seed') }}
{% endsnapshot %}
""".lstrip()


class TestSnapshotMultipleUniqueKeys:
    """Test snapshots with multiple unique keys - fixes issue #615"""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "orders_seed.csv": seed_csv,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "orders_snapshot.sql": snapshot_sql,
        }

    def test_snapshot_with_multiple_unique_keys(self, project):
        """
        Test that snapshots with multiple unique keys work correctly.
        This is the core test for issue #615.

        Before the fix:
        - 2nd execution: Added duplicate dbt_unique_key_1, dbt_unique_key_2 columns
        - 3rd execution: Generated invalid SQL with select * and duplicate aliases

        After the fix:
        - All executions work correctly
        - No duplicate columns are created
        - SQL remains valid across multiple runs
        """
        # Run seed
        results = run_dbt(["seed"])
        assert len(results) == 1

        # Run snapshot first time - should create the snapshot table
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # Get the schema to query with
        test_schema = project.test_schema

        # Verify snapshot table has correct columns
        # Query to get column names
        column_query = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'orders_snapshot'
            AND TABLE_SCHEMA = '{test_schema}'
            ORDER BY ORDINAL_POSITION
        """
        column_results = project.run_sql(column_query, fetch="all")
        column_names = [col[0].lower() for col in column_results]

        # Check for expected columns
        assert "order_id" in column_names
        assert "customer_id" in column_names
        assert "dbt_scd_id" in column_names
        assert "dbt_updated_at" in column_names
        assert "dbt_valid_from" in column_names
        assert "dbt_valid_to" in column_names

        # IMPORTANT: dbt_unique_key_1 and dbt_unique_key_2 should NOT be in the final table
        # They are only used internally in CTEs. The bug (issue #615) was that they were
        # incorrectly being added to the table.
        assert "dbt_unique_key_1" not in column_names
        assert "dbt_unique_key_2" not in column_names

        # Count initial records - should be 5
        results = project.run_sql(
            f"select count(*) as cnt from {test_schema}.orders_snapshot", fetch="one"
        )
        assert results[0] == 5

        # Run snapshot second time - should not create duplicate columns
        # This is where issue #615 manifested - duplicate columns were added
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # Verify no unwanted columns were created (this was the bug)
        column_results_after = project.run_sql(column_query, fetch="all")
        column_names_after = [col[0].lower() for col in column_results_after]

        # These columns should still NOT be in the table
        assert "dbt_unique_key_1" not in column_names_after
        assert "dbt_unique_key_2" not in column_names_after

        # Run snapshot third time to ensure it's stable
        # Issue #615 caused invalid SQL on the 3rd run due to accumulated duplicates
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # Verify still no unwanted columns after third run
        column_results_final = project.run_sql(column_query, fetch="all")
        column_names_final = [col[0].lower() for col in column_results_final]
        assert "dbt_unique_key_1" not in column_names_final
        assert "dbt_unique_key_2" not in column_names_final

        # Verify record count is still 5 (no duplicates created)
        results = project.run_sql(
            f"select count(*) as cnt from {test_schema}.orders_snapshot", fetch="one"
        )
        assert results[0] == 5
