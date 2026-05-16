import os

import pytest
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.util import run_dbt


class TestSimpleMaterializations(BaseSimpleMaterializations):

    def test_existing_view_materialization(self, project, models):
        """Test that materializing an existing view works correctly."""
        # Create a temporary model file directly in the project
        model_path = os.path.join(project.project_root, "models", "view_model_exists.sql")

        # Write the initial model without the value column
        with open(model_path, "w") as f:
            f.write(
                """
                {{ config(materialized='view') }}
                select
                1 as id
                {% if var('include_value_column', false) %}
                , 2 as value
                {% endif %}
            """
            )

        # First run to create the view without the extra column
        results = run_dbt(["run", "-m", "view_model_exists"])
        assert len(results) == 1

        # Generate catalog to get column information
        catalog = run_dbt(["docs", "generate"])

        # Check columns in the catalog
        node_id = "model.base.view_model_exists"
        assert node_id in catalog.nodes

        # Get columns from the catalog
        columns = catalog.nodes[node_id].columns
        column_names = [name.lower() for name in columns.keys()]

        # Verify only the id column exists
        assert "id" in column_names
        assert "value" not in column_names

        # Second run with a variable to include the extra column
        results = run_dbt(
            ["run", "-m", "view_model_exists", "--vars", '{"include_value_column": true}']
        )
        assert len(results) == 1

        # Generate catalog again to get updated column information
        catalog = run_dbt(["docs", "generate"])

        # Get updated columns from the catalog
        columns = catalog.nodes[node_id].columns
        column_names = [name.lower() for name in columns.keys()]

        # Verify both columns exist now
        assert "id" in column_names
        assert "value" in column_names


class TestSingularTests(BaseSingularTests):
    pass


@pytest.mark.skip(reason="SQLServer doesn't support nested CTE")
class TestSingularTestsEphemeral(BaseSingularTestsEphemeral):
    pass


class TestEmpty(BaseEmpty):
    pass


@pytest.mark.skip(reason="SQLServer doesn't support nested CTE")
class TestEphemeral(BaseEphemeral):
    pass


class TestIncremental(BaseIncremental):
    pass


class TestGenericTests(BaseGenericTests):
    pass


class TestSnapshotCheckCols(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestamp(BaseSnapshotTimestamp):
    pass


class TestBaseAdapterMethod(BaseAdapterMethod):
    pass
