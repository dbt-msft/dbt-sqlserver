"""A statement that drops a relation by name must select its database first.

SQL Server resolves an unqualified `DROP VIEW [schema].[name]` against whatever
database the *connection* is currently on, and `USE` persists for the session -
so a two-part drop lands wherever the previous statement left the connection.
For a model in another database that is the wrong one: `IF EXISTS` makes the
drop a silent no-op, leaving the object behind, and a same-named object in the
current database is dropped instead.

Every drop the adapter emits therefore selects the database first, as
`sqlserver__get_drop_sql` does. This is a source-level guard because the
failure needs a cross-database model AND a connection left on another database
to show up at all: it is latent today, which is exactly why nothing at runtime
would catch a new drop that forgets the `USE`.

Scope is deliberately narrow - `{% call statement(...) %}` blocks, the ones
that are dispatched as their own batch. A drop concatenated into a larger
rendered batch (`create.sql` wraps its tmp-view drops in `EXEC()` after the
stage half has already emitted a `USE`) inherits that batch's database and is
not matched here.
"""

import re
from pathlib import Path

import pytest

MACRO_ROOT = Path(__file__).parents[4] / "dbt" / "include" / "sqlserver" / "macros"

# Guards against the regex silently matching nothing if the macro style changes.
MINIMUM_DROPS_EXPECTED = 6

CALL_BLOCK = re.compile(
    r"\{%-?\s*call\s+statement\((?P<args>[^)]*)\)\s*-?%\}(?P<body>.*?)\{%-?\s*endcall\s*-?%\}",
    re.S,
)
DROP = re.compile(r"\bdrop\s+(view|table)\b", re.I)
NAME = re.compile(r"""^\s*['"](?P<name>[^'"]+)['"]""")


def _dropping_statements():
    """Yield (path, lineno, name, body) for every call statement that drops."""
    for path in sorted(MACRO_ROOT.rglob("*.sql")):
        text = path.read_text()
        for match in CALL_BLOCK.finditer(text):
            body = match.group("body")
            if not DROP.search(body):
                continue
            lineno = text[: match.start()].count("\n") + 1
            name_match = NAME.match(match.group("args"))
            yield (
                path,
                lineno,
                name_match.group("name") if name_match else "<unnamed>",
                body,
            )


ALL_DROPS = list(_dropping_statements())


def test_drop_scan_found_statements():
    """The scan itself must keep working if macro formatting changes."""
    assert len(ALL_DROPS) >= MINIMUM_DROPS_EXPECTED, (
        f"only found {len(ALL_DROPS)} dropping call statements under {MACRO_ROOT}; "
        "the call-block regex has probably gone stale"
    )


@pytest.mark.parametrize(
    "path,lineno,name,body",
    ALL_DROPS,
    ids=[f"{p.name}:{ln}:{n}" for p, ln, n, _ in ALL_DROPS],
)
def test_drop_selects_its_database(path, lineno, name, body):
    assert "get_use_database_sql" in body, (
        f"{path.relative_to(MACRO_ROOT.parents[3])}:{lineno} statement '{name}' "
        "drops a relation without selecting its database first. Emit "
        "{{ get_use_database_sql(<relation>.database) }} ahead of the drop, or "
        "the statement targets whichever database the connection was left on."
    )
