"""Unit tests for the pure deny resolution + diff logic.

These exercise the normalise / dedupe / grant-conflict rules and the
desired-vs-current diff without needing a database connection, mirroring
tests/unit/adapters/mssql/test_mask.py.
"""

from dbt.adapters.sqlserver.sqlserver_deny import (
    SUPPORTED_PRIVILEGES,
    deny_changes,
    resolve_denies,
)

# ---------------------------------------------------------------------------
# resolve_denies: normalise the config into one {privilege: [principals]} map
# ---------------------------------------------------------------------------


def test_resolve_basic():
    resolved, warnings, unsupported = resolve_denies(
        {"select": ["Restricted_Read_Only"]},
        grant_config=None,
        model_name="stg_patient",
    )
    assert resolved == {"select": ["Restricted_Read_Only"]}
    assert warnings == []
    assert unsupported == []


def test_resolve_empty_config():
    resolved, warnings, unsupported = resolve_denies(None, None, "m")
    assert resolved == {}
    assert warnings == []
    assert unsupported == []


def test_resolve_lowercases_privilege():
    resolved, _, _ = resolve_denies({"SELECT": ["A"]}, None, "m")
    assert resolved == {"select": ["A"]}


def test_resolve_dedupes_principals_case_insensitively():
    resolved, _, _ = resolve_denies({"select": ["Reader", "reader", "READER"]}, None, "m")
    # first spelling preserved, duplicates dropped
    assert resolved == {"select": ["Reader"]}


def test_resolve_merges_case_variant_privilege_keys():
    resolved, _, _ = resolve_denies({"select": ["A"], "SELECT": ["B"]}, None, "m")
    assert resolved == {"select": ["A", "B"]}


def test_resolve_drops_empty_principal_list():
    resolved, _, _ = resolve_denies({"select": []}, None, "m")
    assert resolved == {}


def test_resolve_unsupported_privilege_collected_not_skipped_silently():
    resolved, _, unsupported = resolve_denies(
        {"execute": ["A"], "control": ["B"], "select": ["C"]}, None, "m"
    )
    assert resolved == {"select": ["C"]}
    assert sorted(unsupported) == ["control", "execute"]


def test_resolve_all_supported_privileges_accepted():
    cfg = {priv: ["A"] for priv in SUPPORTED_PRIVILEGES}
    resolved, _, unsupported = resolve_denies(cfg, None, "m")
    assert set(resolved) == set(SUPPORTED_PRIVILEGES)
    assert unsupported == []


def test_resolve_warns_on_grant_deny_conflict():
    _, warnings, _ = resolve_denies(
        {"select": ["Reader"]},
        grant_config={"select": ["reader"]},  # case-insensitive match
        model_name="core_patients",
    )
    assert len(warnings) == 1
    w = warnings[0]
    assert "core_patients" in w and "Reader" in w and "select" in w


def test_resolve_no_conflict_when_different_privilege():
    _, warnings, _ = resolve_denies(
        {"select": ["Reader"]},
        grant_config={"insert": ["Reader"]},
        model_name="m",
    )
    assert warnings == []


# ---------------------------------------------------------------------------
# deny_changes: diff desired against live sys.database_permissions state
# ---------------------------------------------------------------------------


def rows(*pairs):
    return [{"privilege_type": p, "grantee": g} for p, g in pairs]


def test_changes_first_build_all_denies():
    ch = deny_changes([], {"select": ["Reader"], "insert": ["Writer"]})
    assert sorted(ch["denies"]) == [("insert", "Writer"), ("select", "Reader")]
    assert ch["revokes"] == []


def test_changes_converged_is_noop():
    existing = rows(("SELECT", "Reader"))
    ch = deny_changes(existing, {"select": ["Reader"]})
    assert ch["denies"] == []
    assert ch["revokes"] == []


def test_changes_converged_case_insensitive():
    existing = rows(("SELECT", "READER"))
    ch = deny_changes(existing, {"SELECT": ["reader"]})
    assert ch["denies"] == []
    assert ch["revokes"] == []


def test_changes_revoke_when_removed_from_config():
    existing = rows(("SELECT", "Reader"), ("INSERT", "OldUser"))
    ch = deny_changes(existing, {"select": ["Reader"]})
    assert ch["denies"] == []
    # revoke keeps the DB's spelling
    assert ch["revokes"] == [("INSERT", "OldUser")]


def test_changes_add_and_revoke_together():
    existing = rows(("SELECT", "Reader"))
    ch = deny_changes(existing, {"update": ["NewUser"]})
    assert ch["denies"] == [("update", "NewUser")]
    assert ch["revokes"] == [("SELECT", "Reader")]


def test_changes_deny_keeps_config_spelling():
    # nothing existing; the emitted DENY uses the principal exactly as configured
    ch = deny_changes([], {"select": ["DOMAIN\\Report_Readers"]})
    assert ch["denies"] == [("select", "DOMAIN\\Report_Readers")]
