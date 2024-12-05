from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from dbt.adapters.exceptions import IndexConfigError, IndexConfigNotDictError
from dbt.exceptions import DbtRuntimeError
from dbt_common.utils import encoding as dbt_encoding

from dbt.adapters.sqlserver.relation_configs.index import SQLServerIndexConfig, SQLServerIndexType


def test_sqlserver_index_type_default():
    assert SQLServerIndexType.default() == SQLServerIndexType.nonclustered


def test_sqlserver_index_type_valid_types():
    valid_types = SQLServerIndexType.valid_types()
    assert isinstance(valid_types, tuple)
    assert len(valid_types) > 0


def test_sqlserver_index_config_creation():
    config = SQLServerIndexConfig(
        columns=("col1", "col2"),
        unique=True,
        type=SQLServerIndexType.nonclustered,
        included_columns=frozenset(["col3", "col4"]),
    )
    assert config.columns == ("col1", "col2")
    assert config.unique is True
    assert config.type == SQLServerIndexType.nonclustered
    assert config.included_columns == frozenset(["col3", "col4"])


def test_sqlserver_index_config_from_dict():
    config_dict = {
        "columns": ["col1", "col2"],
        "unique": True,
        "type": "nonclustered",
        "included_columns": ["col3", "col4"],
    }
    config = SQLServerIndexConfig.from_dict(config_dict)
    assert config.columns == ("col1", "col2")
    assert config.unique is True
    assert config.type == SQLServerIndexType.nonclustered
    assert config.included_columns == frozenset(["col3", "col4"])


def test_sqlserver_index_config_validation_rules():
    # Test valid configuration
    valid_config = SQLServerIndexConfig(
        columns=("col1", "col2"),
        unique=True,
        type=SQLServerIndexType.nonclustered,
        included_columns=frozenset(["col3", "col4"]),
    )
    assert len(valid_config.validation_rules) == 4
    for rule in valid_config.validation_rules:
        assert rule.validation_check is True

    # Test invalid configurations
    with pytest.raises(DbtRuntimeError, match="'columns' is a required property"):
        SQLServerIndexConfig(columns=())

    with pytest.raises(
        DbtRuntimeError,
        match="Non-clustered indexes are the only index types that can include extra columns",
    ):
        SQLServerIndexConfig(
            columns=("col1",),
            type=SQLServerIndexType.clustered,
            included_columns=frozenset(["col2"]),
        )

    with pytest.raises(
        DbtRuntimeError,
        match="Clustered and nonclustered indexes are the only types that can be unique",
    ):
        SQLServerIndexConfig(columns=("col1",), unique=True, type=SQLServerIndexType.columnstore)


def test_sqlserver_index_config_parse_model_node():
    model_node_entry = {
        "columns": ["col1", "col2"],
        "unique": True,
        "type": "nonclustered",
        "included_columns": ["col3", "col4"],
    }
    parsed_dict = SQLServerIndexConfig.parse_model_node(model_node_entry)
    assert parsed_dict == {
        "columns": ("col1", "col2"),
        "unique": True,
        "type": "nonclustered",
        "included_columns": frozenset(["col3", "col4"]),
    }


def test_sqlserver_index_config_parse_relation_results():
    relation_results_entry = {
        "name": "index_name",
        "columns": "col1,col2",
        "unique": True,
        "type": "nonclustered",
        "included_columns": "col3,col4",
    }
    parsed_dict = SQLServerIndexConfig.parse_relation_results(relation_results_entry)
    assert parsed_dict == {
        "name": "index_name",
        "columns": ("col1", "col2"),
        "unique": True,
        "type": "nonclustered",
        "included_columns": {"col3", "col4"},
    }


def test_sqlserver_index_config_as_node_config():
    config = SQLServerIndexConfig(
        columns=("col1", "col2"),
        unique=True,
        type=SQLServerIndexType.nonclustered,
        included_columns=frozenset(["col3", "col4"]),
    )
    node_config = config.as_node_config
    assert node_config == {
        "columns": ("col1", "col2"),
        "unique": True,
        "type": "nonclustered",
        "included_columns": frozenset(["col3", "col4"]),
    }


FAKE_NOW = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def patch_datetime_now():
    with patch("dbt.adapters.sqlserver.relation_configs.index.datetime_now") as mocked_datetime:
        mocked_datetime.return_value = FAKE_NOW
        yield mocked_datetime


def test_sqlserver_index_config_render():
    config = SQLServerIndexConfig(
        columns=("col1", "col2"), unique=True, type=SQLServerIndexType.nonclustered
    )
    relation = MagicMock()
    relation.render.return_value = "test_relation"

    result = config.render(relation)

    expected_string = "col1_col2_test_relation_True_nonclustered_2023-01-01T00:00:00+00:00"

    print(f"Expected string: {expected_string}")
    print(f"Actual result (MD5): {result}")
    print(f"Expected result (MD5): {dbt_encoding.md5(expected_string)}")

    assert result == dbt_encoding.md5(expected_string)


def test_sqlserver_index_config_parse():
    valid_raw_index = {"columns": ["col1", "col2"], "unique": True, "type": "nonclustered"}
    result = SQLServerIndexConfig.parse(valid_raw_index)
    assert isinstance(result, SQLServerIndexConfig)
    assert result.columns == ("col1", "col2")
    assert result.unique is True
    assert result.type == SQLServerIndexType.nonclustered

    assert SQLServerIndexConfig.parse(None) is None

    with pytest.raises(IndexConfigError):
        SQLServerIndexConfig.parse({"invalid": "config"})

    with pytest.raises(IndexConfigNotDictError):
        SQLServerIndexConfig.parse("not a dict")
