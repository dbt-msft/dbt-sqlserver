"""Read-only catalog probes must not open the ambient transaction (#819).

A `{% call statement(...) %}` defaults to ``auto_begin=True``, so a probe that
omits the flag OPENS the dbt-managed transaction when none is running. That is
harmless in isolation, but the materialization tail (masks, index
reconciliation, grants, persist_docs) runs *after* the cutover commits, and any
statement it issues with ``auto_begin=False`` merely JOINS whatever is open. So
a single probe reopening a transaction drags every mask ALTER and index build
that follows back inside it, held to the trailing ``adapter.commit()`` - which
is the exact Sch-M window on the live target that #819 is about.

Probes are pure reads and never need a transaction of their own, so the rule is
simply that they all pass ``auto_begin=False``. This test is a source-level
guard: it fails when a new probe is added without the flag, which a runtime
assertion in a functional test would only catch on the paths it happens to
exercise.
"""

import re
from pathlib import Path

import pytest

MACRO_ROOT = Path(__file__).parents[4] / "dbt" / "include" / "sqlserver" / "macros"

# Probes that may still open a transaction, with the reason. These run during
# cache population or `dbt docs generate`, never inside a materialization tail,
# so they cannot drag mask/index DDL into a transaction. Revisit as a #819
# follow-up rather than widening this list.
KNOWN_EXCEPTIONS = {
    "list_relations_without_caching": "cache population, not a materialization tail",
    "get_relation_without_caching": "cache population, not a materialization tail",
    "last_modified": "source freshness, not a materialization tail",
    "catalog": "dbt docs generate, not a materialization tail",
}

# Guards against the regex silently matching nothing if the macro style changes.
MINIMUM_PROBES_EXPECTED = 15

CALL_STATEMENT = re.compile(r"\{%-?\s*call\s+statement\(\s*(?P<args>[^)]*)\)")
NAME = re.compile(r"""^\s*['"](?P<name>[^'"]+)['"]""")


def _probes():
    """Yield (path, lineno, name, args) for every fetch_result call statement."""
    for path in sorted(MACRO_ROOT.rglob("*.sql")):
        for lineno, line in enumerate(path.read_text().splitlines(), start=1):
            match = CALL_STATEMENT.search(line)
            if not match:
                continue
            args = match.group("args")
            if "fetch_result" not in args.lower():
                continue
            name_match = NAME.match(args)
            name = name_match.group("name") if name_match else "<unnamed>"
            yield path, lineno, name, args


ALL_PROBES = list(_probes())


def test_probe_scan_found_statements():
    """The scan itself must keep working if macro formatting changes."""
    assert len(ALL_PROBES) >= MINIMUM_PROBES_EXPECTED, (
        f"only found {len(ALL_PROBES)} fetch_result probes under {MACRO_ROOT}; "
        "the call-statement regex has probably gone stale"
    )


@pytest.mark.parametrize(
    "path,lineno,name,args",
    ALL_PROBES,
    ids=[f"{p.name}:{ln}:{n}" for p, ln, n, _ in ALL_PROBES],
)
def test_probe_declines_the_ambient_transaction(path, lineno, name, args):
    if name in KNOWN_EXCEPTIONS:
        pytest.skip(f"{name}: {KNOWN_EXCEPTIONS[name]}")
    assert re.search(r"auto_begin\s*=\s*[Ff]alse", args), (
        f"{path.relative_to(MACRO_ROOT.parents[3])}:{lineno} statement "
        f"'{name}' is a read-only probe but does not pass auto_begin=False, so "
        "it opens the ambient transaction and the materialization tail's "
        "mask/index DDL will join it and hold it to commit (#819). Add "
        "auto_begin=False, or add the statement to KNOWN_EXCEPTIONS with the "
        "reason it can never run in a materialization tail."
    )
