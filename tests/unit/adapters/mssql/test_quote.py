"""Identifier quoting (#785).

Macros route identifiers through ``adapter.quote()`` instead of hand-formatting
them, so the escaping has to live in ``quote()`` itself.
"""

import re
from pathlib import Path

import pytest

from dbt.adapters.sqlserver.sqlserver_adapter import SQLServerAdapter
from dbt.adapters.sqlserver.sqlserver_relation import SQLServerRelation


def _relation(**kwargs):
    return SQLServerRelation.create(type="table", **kwargs)


@pytest.mark.parametrize(
    "identifier,expected",
    [
        ("id", '"id"'),
        ("my column", '"my column"'),
        ("MODEL", '"MODEL"'),  # case preserved; it matters on a CS collation
        ("weird]name", '"weird]name"'),  # brackets are ordinary characters here
        ('ab"cd', '"ab""cd"'),  # the delimiter is escaped by doubling
        ('"', '""""'),
        ('x" from sys.tables--', '"x"" from sys.tables--"'),
    ],
)
def test_quote_escapes_embedded_delimiters(identifier, expected):
    assert SQLServerAdapter.quote(identifier) == expected


def test_relation_rendering_escapes_like_adapter_quote():
    """Both quoting paths must agree: identifiers built inside string literals
    (OBJECT_ID, sp_rename) are rendered, not passed through adapter.quote()."""
    assert _relation(database="d", schema='s"c', identifier="t").render() == '"d"."s""c"."t"'
    for part in ("plain", 'ab"cd', "MODEL"):
        rendered = _relation(database="d", schema="s", identifier=part).render()
        assert rendered.split(".")[-1] == SQLServerAdapter.quote(part)


def test_relation_render_unchanged_for_ordinary_names():
    relation = _relation(database="TestDB", schema="dbo", identifier="my_model")
    assert relation.render() == '"TestDB"."dbo"."my_model"'


MACRO_ROOT = Path(__file__).parents[4] / "dbt" / "include" / "sqlserver" / "macros"

# Jinja-interpolated identifiers wrapped by hand. Each drifts from what relation
# rendering emits, and skips the escaping in quote().
HAND_QUOTING = {
    "[{{ ... }}]": re.compile(r"\[\s*\{\{"),
    "'[' ~ concat": re.compile(r"""["']\[["']\s*[~+]"""),
    'join("], [")': re.compile(r"""join\(\s*["']\]\s*,\s*\[["']\s*\)"""),
    "'\"' ~ concat": re.compile(r"""'"'\s*[~+]|"\\""\s*[~+]"""),
}

# Brackets that are not identifier quoting: split_part's XQuery predicate.
NOT_QUOTING = (re.compile(r"AS XML\)\.value\("),)


@pytest.mark.parametrize("macro_file", sorted(MACRO_ROOT.rglob("*.sql")), ids=lambda p: p.name)
def test_macros_do_not_hand_format_identifiers(macro_file):
    offenders = []
    for lineno, line in enumerate(macro_file.read_text().splitlines(), start=1):
        if any(x.search(line) for x in NOT_QUOTING):
            continue
        for label, pattern in HAND_QUOTING.items():
            if pattern.search(line):
                offenders.append(f"  {macro_file.name}:{lineno} ({label}): {line.strip()}")

    assert not offenders, (
        "use adapter.quote() or the relation object so generated SQL keeps one "
        "quoting style (#785):\n" + "\n".join(offenders)
    )
