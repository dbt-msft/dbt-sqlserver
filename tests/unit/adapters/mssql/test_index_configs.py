from unittest.mock import MagicMock

import pytest
from dbt_common.utils import encoding as dbt_encoding

from dbt.adapters.exceptions import IndexConfigError, IndexConfigNotDictError
from dbt.adapters.sqlserver.relation_configs.index import SQLServerIndexConfig, SQLServerIndexType
from dbt.exceptions import DbtRuntimeError


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
    assert len(valid_config.validation_rules) == 7
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
        "data_compression": None,
        "sort_in_tempdb": False,
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
        "data_compression": None,
    }


def test_parse_relation_results_strips_whitespace():
    # The sys.indexes introspection aggregates columns as "col1, col2" -
    # entries must be stripped or reconciliation comparisons never match.
    parsed = SQLServerIndexConfig.parse_relation_results(
        {
            "name": "ix",
            "columns": "col1, col2",
            "unique": True,
            "type": "nonclustered",
            "included_columns": "col3, col4",
        }
    )
    assert parsed["columns"] == ("col1", "col2")
    assert parsed["included_columns"] == {"col3", "col4"}


def test_parse_relation_results_empty_and_null_includes():
    parsed = SQLServerIndexConfig.parse_relation_results(
        {"name": "ix", "columns": "col1", "unique": False, "type": "clustered"}
    )
    assert parsed["columns"] == ("col1",)
    # absent/NULL included_columns must parse to empty, not {""}
    assert parsed["included_columns"] == set()

    parsed = SQLServerIndexConfig.parse_relation_results(
        {
            "name": "ix",
            "columns": "col1",
            "unique": False,
            "type": "clustered",
            "included_columns": "",
        }
    )
    assert parsed["included_columns"] == set()


def test_sqlserver_index_config_as_node_config():
    config = SQLServerIndexConfig(
        columns=("col1", "col2"),
        unique=True,
        type=SQLServerIndexType.nonclustered,
        included_columns=frozenset(["col3", "col4"]),
    )
    node_config = config.as_node_config
    assert node_config == {
        "columns": ["col1", "col2"],
        "unique": True,
        "type": "nonclustered",
        "included_columns": ["col3", "col4"],
        "data_compression": None,
        "sort_in_tempdb": False,
    }


def make_relation(rendered="test_relation"):
    relation = MagicMock()
    relation.render.return_value = rendered
    return relation


def test_sqlserver_index_config_render():
    config = SQLServerIndexConfig(
        columns=("col1", "col2"),
        unique=True,
        type=SQLServerIndexType.nonclustered,
        included_columns=frozenset(["col4", "col3"]),
    )

    result = config.render(make_relation())

    # Deterministic full-definition hash: ordered columns, sorted includes,
    # relation identity, unique flag, type. NO timestamp.
    expected_string = "col1_col2_col3_col4_test_relation_True_nonclustered"
    assert result == "dbt_idx_" + dbt_encoding.md5(expected_string)


def test_render_is_deterministic():
    def build():
        return SQLServerIndexConfig(
            columns=("col1", "col2"),
            unique=False,
            type=SQLServerIndexType.clustered,
        )

    assert build().render(make_relation()) == build().render(make_relation())


def test_render_differs_on_definition():
    base = SQLServerIndexConfig(columns=("col1", "col2"))
    variants = [
        SQLServerIndexConfig(columns=("col1",)),
        SQLServerIndexConfig(columns=("col2", "col1")),  # order matters
        SQLServerIndexConfig(columns=("col1", "col2"), unique=True),
        SQLServerIndexConfig(columns=("col1", "col2"), type=SQLServerIndexType.clustered),
        SQLServerIndexConfig(columns=("col1", "col2"), included_columns=frozenset(["col3"])),
    ]
    relation = make_relation()
    base_name = base.render(relation)
    names = [variant.render(relation) for variant in variants]
    assert base_name not in names
    assert len(set(names)) == len(names)

    # Same definition against a different relation gets a different name.
    assert base_name != base.render(make_relation("other_relation"))


def test_render_managed_prefix():
    config = SQLServerIndexConfig(columns=("col1",))
    assert config.render(make_relation()).startswith("dbt_idx_")


def test_render_no_stdout(capsys):
    SQLServerIndexConfig(columns=("col1",)).render(make_relation())
    assert capsys.readouterr().out == ""


def test_index_config_data_compression_round_trip():
    config = SQLServerIndexConfig(columns=("col1",), data_compression="page")
    assert config.data_compression == "page"
    assert config.sort_in_tempdb is False

    config = SQLServerIndexConfig.from_dict(
        {"columns": ["col1"], "data_compression": "row", "sort_in_tempdb": True}
    )
    assert config.data_compression == "row"
    assert config.sort_in_tempdb is True


def test_parse_model_node_new_fields():
    parsed = SQLServerIndexConfig.parse_model_node(
        {"columns": ["col1"], "data_compression": "page", "sort_in_tempdb": True}
    )
    assert parsed["data_compression"] == "page"
    assert parsed["sort_in_tempdb"] is True


def test_as_node_config_new_fields():
    config = SQLServerIndexConfig(columns=("col1",), data_compression="page", sort_in_tempdb=True)
    node_config = config.as_node_config
    assert node_config["data_compression"] == "page"
    assert node_config["sort_in_tempdb"] is True


def test_data_compression_invalid_value():
    with pytest.raises(DbtRuntimeError, match="none.*row.*page"):
        SQLServerIndexConfig(columns=("col1",), data_compression="gzip")


def test_data_compression_invalid_for_columnstore():
    with pytest.raises(DbtRuntimeError, match="data_compression"):
        SQLServerIndexConfig(
            columns=("col1",),
            type=SQLServerIndexType.columnstore,
            data_compression="page",
        )


def test_sort_in_tempdb_invalid_for_columnstore():
    with pytest.raises(DbtRuntimeError, match="sort_in_tempdb"):
        SQLServerIndexConfig(
            columns=("col1",),
            type=SQLServerIndexType.columnstore,
            sort_in_tempdb=True,
        )


def test_render_differs_on_data_compression():
    relation = make_relation()
    plain = SQLServerIndexConfig(columns=("col1",))
    compressed = SQLServerIndexConfig(columns=("col1",), data_compression="page")
    assert plain.render(relation) != compressed.render(relation)


def test_render_and_equality_ignore_sort_in_tempdb():
    # sort_in_tempdb is a build-time option: not introspectable from sys.indexes
    # and doesn't change the resulting index. If it participated in the name or
    # equality, reconciliation would drop/recreate on every run.
    relation = make_relation()
    without = SQLServerIndexConfig(columns=("col1",))
    with_tempdb = SQLServerIndexConfig(columns=("col1",), sort_in_tempdb=True)
    assert without.render(relation) == with_tempdb.render(relation)
    assert without == with_tempdb


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


def test_as_node_config_round_trips_through_parse():
    # as_node_config output is fed back into get_create_index_sql ->
    # adapter.parse_index -> jsonschema validation, which requires
    # JSON-compatible types (arrays, not tuples/frozensets).
    config = SQLServerIndexConfig(
        columns=("col1", "col2"),
        unique=True,
        included_columns=frozenset(["col3"]),
        data_compression="page",
    )
    reparsed = SQLServerIndexConfig.parse(config.as_node_config)
    assert reparsed == config
