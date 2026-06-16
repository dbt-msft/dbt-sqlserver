from unittest.mock import MagicMock

import pytest

from dbt.adapters.sqlserver.relation_configs.index import (
    SQLServerIndexConfig,
    create_needs_own_batch,
    index_config_changes,
)
from dbt.exceptions import DbtRuntimeError


def make_relation(rendered="test_relation"):
    relation = MagicMock()
    relation.render.return_value = rendered
    return relation


def existing_row(
    name,
    columns="col1",
    type="nonclustered",
    unique=False,
    included_columns="",
    data_compression="NONE",
    is_primary_key=False,
    is_unique_constraint=False,
):
    return {
        "name": name,
        "columns": columns,
        "type": type,
        "unique": unique,
        "included_columns": included_columns,
        "data_compression": data_compression,
        "is_primary_key": is_primary_key,
        "is_unique_constraint": is_unique_constraint,
    }


def managed_row(config, relation, **overrides):
    """An existing-index row as describe_indexes would return it for a
    previously-created managed index."""
    return existing_row(
        name=config.render(relation),
        columns=", ".join(config.columns),
        type=str(config.type),
        unique=config.unique,
        included_columns=", ".join(sorted(config.included_columns)),
        **overrides,
    )


RELATION = make_relation()
CFG_A = SQLServerIndexConfig(columns=("col1",))
CFG_B = SQLServerIndexConfig(columns=("col2",), unique=True)
CFG_CLUSTERED = SQLServerIndexConfig(columns=("col3",), type="clustered")


def actions(changes):
    return [(str(change.action), change.context.name or None) for change in changes]


def test_diff_create_missing():
    changes, warnings = index_config_changes([], [CFG_A], RELATION, "false")
    assert warnings == []
    assert len(changes) == 1
    assert str(changes[0].action) == "create"
    assert changes[0].context == CFG_A


def test_diff_no_change_when_equal():
    existing = [managed_row(CFG_A, RELATION), managed_row(CFG_B, RELATION)]
    changes, warnings = index_config_changes(existing, [CFG_A, CFG_B], RELATION, "false")
    assert changes == []
    assert warnings == []


def test_diff_drop_managed_not_expected():
    existing = [managed_row(CFG_A, RELATION), managed_row(CFG_B, RELATION)]
    changes, warnings = index_config_changes(existing, [CFG_A], RELATION, "false")
    assert len(changes) == 1
    assert str(changes[0].action) == "drop"
    assert changes[0].context.name == CFG_B.render(RELATION)
    assert warnings == []


def test_diff_drops_ordered_before_creates():
    existing = [managed_row(CFG_A, RELATION)]
    changes, _ = index_config_changes(existing, [CFG_B], RELATION, "false")
    assert [str(change.action) for change in changes] == ["drop", "create"]


def test_diff_ignores_legacy_prefixes():
    legacy = [
        existing_row("clustered_abc123", type="clustered"),
        existing_row("nonclustered_def456"),
    ]
    for mode in ("false", "warn", "true"):
        changes, _ = index_config_changes(legacy, [], RELATION, mode)
        assert all(str(change.action) != "drop" for change in changes)


def test_diff_unmanaged_untouched_under_false():
    existing = [existing_row("ix_dba_tuning")]
    changes, warnings = index_config_changes(existing, [], RELATION, "false")
    assert changes == []
    assert warnings == []


def test_diff_unmanaged_warn():
    existing = [existing_row("ix_dba_tuning")]
    changes, warnings = index_config_changes(existing, [], RELATION, "warn")
    assert changes == []
    assert len(warnings) == 1
    assert "ix_dba_tuning" in warnings[0]


def test_diff_unmanaged_dropped_under_true_with_exclusions():
    existing = [
        existing_row("ix_dba_tuning"),
        existing_row("PK__t__abc", is_primary_key=True),
        existing_row("UQ__t__def", is_unique_constraint=True),
        existing_row("clustered_legacy1", type="clustered"),
    ]
    changes, warnings = index_config_changes(existing, [], RELATION, "true")
    dropped = [change.context.name for change in changes if str(change.action) == "drop"]
    assert dropped == ["ix_dba_tuning"]
    assert warnings == []


def test_diff_normalizes_bool_modes():
    existing = [existing_row("ix_dba_tuning")]
    # YAML gives python bools for false/true
    changes_false, _ = index_config_changes(existing, [], RELATION, False)
    assert changes_false == []
    changes_true, _ = index_config_changes(existing, [], RELATION, True)
    assert [change.context.name for change in changes_true] == ["ix_dba_tuning"]


def test_diff_invalid_mode_raises():
    with pytest.raises(DbtRuntimeError, match="drop_unmanaged_indexes"):
        index_config_changes([], [], RELATION, "yes please")


def test_diff_clustered_collision_with_protected_raises():
    # A clustered PK backing index we will never drop blocks an expected
    # clustered index: fail fast with the blocking index named.
    existing = [existing_row("PK__t__abc", type="clustered", is_primary_key=True)]
    with pytest.raises(DbtRuntimeError, match="PK__t__abc"):
        index_config_changes(existing, [CFG_CLUSTERED], RELATION, "false")


def test_diff_clustered_collision_with_legacy_raises():
    existing = [existing_row("clustered_legacy1", type="clustered")]
    with pytest.raises(DbtRuntimeError, match="clustered_legacy1"):
        index_config_changes(existing, [CFG_CLUSTERED], RELATION, "true")


def test_diff_clustered_collision_with_unmanaged_depends_on_mode():
    existing = [existing_row("ix_dba_clustered", type="clustered")]
    # Not droppable under false/warn -> guard raises
    with pytest.raises(DbtRuntimeError, match="ix_dba_clustered"):
        index_config_changes(existing, [CFG_CLUSTERED], RELATION, "false")
    # Droppable under true -> drop then create
    changes, _ = index_config_changes(existing, [CFG_CLUSTERED], RELATION, "true")
    assert [str(change.action) for change in changes] == ["drop", "create"]


def test_diff_clustered_replacement_of_managed_is_fine():
    old_clustered = SQLServerIndexConfig(columns=("col9",), type="clustered")
    existing = [managed_row(old_clustered, RELATION)]
    changes, _ = index_config_changes(existing, [CFG_CLUSTERED], RELATION, "false")
    assert [str(change.action) for change in changes] == ["drop", "create"]


def test_diff_clustered_no_guard_when_expected_exists():
    existing = [managed_row(CFG_CLUSTERED, RELATION)]
    changes, warnings = index_config_changes(existing, [CFG_CLUSTERED], RELATION, "false")
    assert changes == []
    assert warnings == []


def test_diff_never_drops_clustered_columnstore():
    # as_columnstore=true tables carry a CCI the adapter created outside the
    # indexes config; sweeping it would silently convert the table to a heap.
    existing = [existing_row("someschema_sometable_cci", type="clustered columnstore")]
    for mode in ("false", "warn", "true"):
        changes, _ = index_config_changes(existing, [], RELATION, mode)
        assert changes == []


def test_diff_clustered_columnstore_blocks_expected_clustered():
    existing = [existing_row("someschema_sometable_cci", type="clustered columnstore")]
    with pytest.raises(DbtRuntimeError, match="sometable_cci"):
        index_config_changes(existing, [CFG_CLUSTERED], RELATION, "true")


def test_change_never_requires_full_refresh():
    changes, _ = index_config_changes([], [CFG_A], RELATION, "false")
    assert changes[0].requires_full_refresh is False


def test_diff_raises_when_two_configs_collide_on_name():
    # ("a_b",) and ("a", "b") flatten to the same name-hash input; without the
    # collision guard one would silently overwrite the other in the diff.
    one_col = SQLServerIndexConfig(columns=("a_b",))
    two_col = SQLServerIndexConfig(columns=("a", "b"))
    assert one_col.render(RELATION) == two_col.render(RELATION)
    with pytest.raises(DbtRuntimeError, match="same managed name"):
        index_config_changes([], [one_col, two_col], RELATION, "false")


def test_diff_dedupes_identical_configs():
    # Genuinely identical entries collide on name but must not raise.
    changes, _ = index_config_changes([], [CFG_A, CFG_A], RELATION, "false")
    assert len(changes) == 1


@pytest.mark.parametrize(
    "build_options,expected",
    [
        (None, False),
        ({}, False),
        ({"maxdop": 4}, False),
        ({"online": True}, True),
        ({"resumable": True}, True),
        ({"online": False}, False),
        ({"maxdop": 4, "resumable": True}, True),
    ],
)
def test_create_needs_own_batch(build_options, expected):
    assert create_needs_own_batch(build_options) is expected
