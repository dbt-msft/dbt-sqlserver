"""Unit tests for the pure mask resolution + diff logic.

These exercise the surface-merge / precedence / opt-out rules and the
desired-vs-current diff without needing a database connection, mirroring
tests/unit/adapters/mssql/test_index_diff.py.
"""

from dbt.adapters.sqlserver.sqlserver_mask import (
    ColumnMask,
    mask_changes,
    resolve_masks,
)

# ---------------------------------------------------------------------------
# resolve_masks: merge the two config surfaces into one per-column map
# ---------------------------------------------------------------------------


def col(name, value, present=True):
    return ColumnMask(name=name, masked_with_present=present, masked_with=value)


def test_resolve_model_level_only():
    mask_map, warnings = resolve_masks(
        column_masks=[],
        model_masks={"surname": "default()", "nhs_number": 'partial(0,"X",0)'},
        model_name="core_patients",
    )
    assert mask_map == {"surname": "default()", "nhs_number": 'partial(0,"X",0)'}
    assert warnings == []


def test_resolve_column_level_only():
    mask_map, warnings = resolve_masks(
        column_masks=[col("surname", "default()")],
        model_masks={},
        model_name="core_patients",
    )
    assert mask_map == {"surname": "default()"}
    assert warnings == []


def test_resolve_column_wins_over_model_and_warns():
    mask_map, warnings = resolve_masks(
        column_masks=[col("surname", "email()")],
        model_masks={"surname": "default()"},
        model_name="core_patients",
    )
    assert mask_map == {"surname": "email()"}
    assert len(warnings) == 1
    # warning names the model, column, both functions
    w = warnings[0]
    assert "core_patients" in w
    assert "surname" in w
    assert "email()" in w and "default()" in w


def test_resolve_conflict_warns_even_when_functions_agree():
    _, warnings = resolve_masks(
        column_masks=[col("surname", "default()")],
        model_masks={"surname": "default()"},
        model_name="core_patients",
    )
    assert len(warnings) == 1


def test_resolve_null_opts_out_of_inherited_model_default():
    mask_map, warnings = resolve_masks(
        column_masks=[col("nhs_number", None)],
        model_masks={"nhs_number": "default()", "surname": "default()"},
        model_name="core_patients",
    )
    assert mask_map == {"surname": "default()"}
    assert warnings == []


def test_resolve_null_opt_out_with_no_inherited_mask_is_noop():
    mask_map, warnings = resolve_masks(
        column_masks=[col("surname", None)],
        model_masks={},
        model_name="core_patients",
    )
    assert mask_map == {}
    assert warnings == []


def test_resolve_is_case_insensitive_for_precedence_and_opt_out():
    # column-level wins even when the model-level key differs only in case
    mask_map, warnings = resolve_masks(
        column_masks=[col("NHS_Number", "email()")],
        model_masks={"nhs_number": "default()"},
        model_name="core_patients",
    )
    assert list(mask_map.values()) == ["email()"]
    assert len(mask_map) == 1
    assert len(warnings) == 1


# ---------------------------------------------------------------------------
# mask_changes: diff desired map against sys.masked_columns state
# ---------------------------------------------------------------------------


def existing(name, fn):
    return {"name": name, "masking_function": fn}


def test_diff_add_new_mask():
    result = mask_changes(
        existing_masks=[],
        desired={"surname": "default()"},
        index_key_columns=set(),
    )
    assert result["adds"] == [("surname", "default()")]
    assert result["changes"] == []
    assert result["drops"] == []
    assert result["errors"] == []


def test_diff_noop_when_unchanged():
    result = mask_changes(
        existing_masks=[existing("surname", "default()")],
        desired={"surname": "default()"},
        index_key_columns=set(),
    )
    assert result["adds"] == []
    assert result["changes"] == []
    assert result["drops"] == []


def test_diff_change_function():
    result = mask_changes(
        existing_masks=[existing("surname", "default()")],
        desired={"surname": "email()"},
        index_key_columns=set(),
    )
    assert result["changes"] == [("surname", "email()")]
    assert result["adds"] == []
    assert result["drops"] == []


def test_diff_drop_when_removed_from_config():
    result = mask_changes(
        existing_masks=[existing("surname", "default()")],
        desired={},
        index_key_columns=set(),
    )
    assert result["drops"] == ["surname"]
    assert result["adds"] == []
    assert result["changes"] == []


def test_diff_is_case_insensitive_on_column_names():
    result = mask_changes(
        existing_masks=[existing("Surname", "default()")],
        desired={"surname": "default()"},
        index_key_columns=set(),
    )
    assert result["adds"] == []
    assert result["changes"] == []
    assert result["drops"] == []


def test_diff_function_comparison_ignores_whitespace():
    # sys.masked_columns may store partial() with reformatted spacing
    result = mask_changes(
        existing_masks=[existing("nhs_number", 'partial(0, "XXXXXXXXXX", 0)')],
        desired={"nhs_number": 'partial(0,"XXXXXXXXXX",0)'},
        index_key_columns=set(),
    )
    assert result["changes"] == []
    assert result["adds"] == []


def test_diff_add_to_index_key_column_is_an_error_not_a_ddl():
    result = mask_changes(
        existing_masks=[],
        desired={"nhs_number": "default()"},
        index_key_columns={"nhs_number"},
    )
    assert result["adds"] == []
    assert len(result["errors"]) == 1
    assert "nhs_number" in result["errors"][0]


def test_diff_changing_existing_mask_on_index_key_is_allowed():
    # the column is already masked, so no ADD dependency problem — a plain
    # MASKED WITH change does not hit the index dependency error
    result = mask_changes(
        existing_masks=[existing("nhs_number", "default()")],
        desired={"nhs_number": "email()"},
        index_key_columns={"nhs_number"},
    )
    assert result["changes"] == [("nhs_number", "email()")]
    assert result["errors"] == []


# ---------------------------------------------------------------------------
# mask_changes: existence validation against the built relation's columns
# ---------------------------------------------------------------------------


def test_diff_skips_and_warns_on_column_absent_from_relation():
    result = mask_changes(
        existing_masks=[],
        desired={"surnam": "default()"},  # typo — no such column
        index_key_columns=set(),
        existing_columns=["id", "surname"],
    )
    assert result["adds"] == []
    assert result["changes"] == []
    assert len(result["skipped"]) == 1
    assert "surnam" in result["skipped"][0]


def test_diff_existence_check_is_case_insensitive():
    result = mask_changes(
        existing_masks=[],
        desired={"Surname": "default()"},
        index_key_columns=set(),
        existing_columns=["id", "surname"],
    )
    assert result["adds"] == [("Surname", "default()")]
    assert result["skipped"] == []


def test_diff_without_existing_columns_skips_no_validation():
    # existing_columns=None (default) preserves the un-validated diff behaviour
    result = mask_changes(
        existing_masks=[],
        desired={"anything": "default()"},
        index_key_columns=set(),
    )
    assert result["adds"] == [("anything", "default()")]
    assert result["skipped"] == []
