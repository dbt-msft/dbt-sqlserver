import pytest
from dbt.tests.adapter.materialized_view.basic import MaterializedViewBasic


@pytest.mark.skip(reason="Materialized views are not supported in SQLServer")
class TestMaterializedViews(MaterializedViewBasic):
    pass
