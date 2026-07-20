"""
Unit tests for the SQL Server index macros.

Mirrors the style of test_generate_schema_name.py: inline Jinja, no DB.

Coverage:
  * mssql__quote_ident         - bracket escaping
  * mssql__qualified_relation  - 3-part name assembly
  * mssql__strip_dbt_suffix    - __dbt_tmp / __dbt_backup stripping
  * mssql__index_name          - determinism, 116-char cap, hashing,
                                 immunity to __dbt_tmp suffix
  * sqlserver__index_exists    - emitted SQL shape (OBJECT_ID scoped)
  * create_clustered_index     - quoted columns, IF NOT EXISTS wrapper
  * create_nonclustered_index  - INCLUDE columns, quoting, idempotence
"""

import hashlib
import re
from pathlib import Path

import jinja2
import pytest
from jinja2.runtime import Macro as _Jinja2Macro


class _MacroReturn(BaseException):
    def __init__(self, value):
        self.value = value


_orig_macro_call = _Jinja2Macro.__call__


def _patched_macro_call(self, *args, **kwargs):
    try:
        return _orig_macro_call(self, *args, **kwargs)
    except _MacroReturn as exc:
        return exc.value


@pytest.fixture(scope="module", autouse=True)
def _patch_jinja2_macro_return():
    _Jinja2Macro.__call__ = _patched_macro_call
    yield
    _Jinja2Macro.__call__ = _orig_macro_call


MACRO_PATH = (
    Path(__file__).resolve().parents[4]
    / "dbt"
    / "include"
    / "sqlserver"
    / "macros"
    / "adapters"
    / "indexes.sql"
)

MACRO_SRC = MACRO_PATH.read_text(encoding="utf-8")


class _FakeRelation:
    """Minimal stand-in for dbt's Relation object."""

    def __init__(self, database, schema, identifier):
        self.database = database
        self.schema = schema
        self.identifier = identifier

    def __str__(self):
        return f"[{self.database}].[{self.schema}].[{self.identifier}]"


def _env():
    env = jinja2.Environment(
        trim_blocks=True,
        lstrip_blocks=True,
        extensions=["jinja2.ext.do"],
    )

    def local_md5(s):
        return hashlib.md5(s.encode("utf-8")).hexdigest()

    env.globals.update(
        {
            "local_md5": local_md5,
            "information_schema_hints": lambda: "",
            "log": lambda *a, **kw: "",
            "get_use_database_sql": lambda db: f"USE [{db}];",
            "this": _FakeRelation("mydb", "myschema", "my_model"),
            "return": lambda v: (_ for _ in ()).throw(_MacroReturn(v)),
        }
    )
    return env


def _render(call_expr, **ctx):
    env = _env()
    template = env.from_string(MACRO_SRC + "\n" + "{{ " + call_expr + " }}")
    return template.render(**ctx).strip()


def _normalize_ws(s):
    return re.sub(r"\s+", " ", s).strip()


class TestQuoteIdent:
    def test_simple_name(self):
        assert _render("mssql__quote_ident('foo')") == "[foo]"

    def test_name_with_space(self):
        assert _render("mssql__quote_ident('weird name')") == "[weird name]"

    def test_name_with_close_bracket_is_escaped(self):
        assert _render("mssql__quote_ident('we][ird')") == "[we]][ird]"

    def test_name_with_dot_is_not_split(self):
        assert _render("mssql__quote_ident('a.b')") == "[a.b]"


class TestQualifiedRelation:
    def test_three_part_name(self):
        rel = _FakeRelation("mydb", "myschema", "my_model")
        out = _render("mssql__qualified_relation(rel)", rel=rel)
        assert out == "[mydb].[myschema].[my_model]"

    def test_escapes_brackets_in_every_part(self):
        rel = _FakeRelation("d]b", "sch]ema", "id]ent")
        out = _render("mssql__qualified_relation(rel)", rel=rel)
        assert out == "[d]]b].[sch]]ema].[id]]ent]"


class TestStripDbtSuffix:
    @pytest.mark.parametrize(
        "identifier, expected",
        [
            ("my_model", "my_model"),
            ("my_model__dbt_tmp", "my_model"),
            ("my_model__dbt_backup", "my_model"),
            ("my_model__dbt_tmp_vw", "my_model"),
            ("my__dbt_tmp_model", "my__dbt_tmp_model"),  # __dbt_tmp in middle, not a suffix
        ],
    )
    def test_strip(self, identifier, expected):
        assert _render(f"mssql__strip_dbt_suffix('{identifier}')") == expected


class TestIndexName:
    def test_deterministic(self):
        rel = _FakeRelation("d", "s", "t")
        a = _render("mssql__index_name(rel, 'nidx', ['x','y'])", rel=rel)
        b = _render("mssql__index_name(rel, 'nidx', ['x','y'])", rel=rel)
        assert a == b

    def test_column_order_changes_name(self):
        rel = _FakeRelation("d", "s", "t")
        a = _render("mssql__index_name(rel, 'nidx', ['x','y'])", rel=rel)
        b = _render("mssql__index_name(rel, 'nidx', ['y','x'])", rel=rel)
        assert a != b

    def test_unique_flag_changes_name(self):
        rel = _FakeRelation("d", "s", "t")
        a = _render("mssql__index_name(rel, 'cidx', ['x'], unique=False)", rel=rel)
        b = _render("mssql__index_name(rel, 'cidx', ['x'], unique=True)", rel=rel)
        assert a != b

    def test_includes_changes_name(self):
        rel = _FakeRelation("d", "s", "t")
        a = _render("mssql__index_name(rel, 'nidx', ['x'])", rel=rel)
        b = _render("mssql__index_name(rel, 'nidx', ['x'], includes=['y'])", rel=rel)
        assert a != b

    def test_accepts_string_columns(self):
        rel = _FakeRelation("d", "s", "t")
        name = _render("mssql__index_name(rel, 'nidx', 'x')", rel=rel)
        assert name.startswith("nidx_t_")

    def test_false_includes_are_treated_as_empty(self):
        rel = _FakeRelation("d", "s", "t")
        a = _render("mssql__index_name(rel, 'nidx', ['x'], includes=false)", rel=rel)
        b = _render("mssql__index_name(rel, 'nidx', ['x'])", rel=rel)
        assert a == b

    def test_dbt_tmp_suffix_does_not_affect_name(self):
        a = _render(
            "mssql__index_name(rel, 'cci', ['__all__'])",
            rel=_FakeRelation("d", "s", "my_model"),
        )
        b = _render(
            "mssql__index_name(rel, 'cci', ['__all__'])",
            rel=_FakeRelation("d", "s", "my_model__dbt_tmp"),
        )
        c = _render(
            "mssql__index_name(rel, 'cci', ['__all__'])",
            rel=_FakeRelation("d", "s", "my_model__dbt_backup"),
        )
        assert a == b == c

    def test_length_capped_at_116_chars(self):
        rel = _FakeRelation("d", "s", "x" * 500)
        name = _render("mssql__index_name(rel, 'nidx', ['c'])", rel=rel)
        assert len(name) <= 116

    def test_long_name_still_unique_per_signature(self):
        rel = _FakeRelation("d", "s", "x" * 500)
        a = _render("mssql__index_name(rel, 'nidx', ['c1'])", rel=rel)
        b = _render("mssql__index_name(rel, 'nidx', ['c2'])", rel=rel)
        assert a != b
        assert len(a) <= 116
        assert len(b) <= 116

    def test_brackets_stripped_from_readable_part(self):
        rel = _FakeRelation("d", "s", "[weird]")
        name = _render("mssql__index_name(rel, 'nidx', ['c'])", rel=rel)
        assert "[" not in name and "]" not in name


class TestIndexExists:
    def test_emits_object_id_scoped_check(self):
        rel = _FakeRelation("mydb", "myschema", "my_model")
        sql = _render("sqlserver__index_exists(rel, 'idx_foo')", rel=rel)
        sql = _normalize_ws(sql)
        assert "EXISTS" in sql
        assert "sys.indexes" in sql
        assert "name = N'idx_foo'" in sql
        assert "OBJECT_ID(N'[mydb].[myschema].[my_model]')" in sql


class TestCreateClusteredIndex:
    def test_quotes_columns(self):
        rel = _FakeRelation("d", "s", "t")
        sql = _render("create_clustered_index(['id_col', 'data'], relation=rel)", rel=rel)
        sql = _normalize_ws(sql)
        assert "([id_col], [data])" in sql

    def test_wrapped_in_if_not_exists(self):
        rel = _FakeRelation("d", "s", "t")
        sql = _normalize_ws(_render("create_clustered_index(['c'], relation=rel)", rel=rel))
        assert "if not exists" in sql
        assert "begin create" in sql
        assert sql.endswith("end")

    def test_unique_flag_emits_unique_keyword(self):
        rel = _FakeRelation("d", "s", "t")
        sql = _normalize_ws(
            _render("create_clustered_index(['c'], unique=True, relation=rel)", rel=rel)
        )
        assert "unique clustered index" in sql

    def test_accepts_string_column(self):
        rel = _FakeRelation("d", "s", "t")
        sql = _normalize_ws(_render("create_clustered_index('id_col', relation=rel)", rel=rel))
        assert "([id_col])" in sql


class TestCreateNonclusteredIndex:
    def test_emits_nonclustered(self):
        rel = _FakeRelation("d", "s", "t")
        sql = _normalize_ws(_render("create_nonclustered_index(['c'], relation=rel)", rel=rel))
        assert "create nonclustered index" in sql

    def test_accepts_string_column(self):
        rel = _FakeRelation("d", "s", "t")
        sql = _normalize_ws(_render("create_nonclustered_index('c', relation=rel)", rel=rel))
        assert "([c])" in sql

    def test_accepts_string_include_column(self):
        rel = _FakeRelation("d", "s", "t")
        sql = _normalize_ws(
            _render("create_nonclustered_index(['c'], includes='inc1', relation=rel)", rel=rel)
        )
        assert "include ([inc1])" in sql

    def test_include_columns_quoted(self):
        rel = _FakeRelation("d", "s", "t")
        sql = _normalize_ws(
            _render(
                "create_nonclustered_index(['c'], includes=['inc1','inc2'], relation=rel)",
                rel=rel,
            )
        )
        assert "include ([inc1], [inc2])" in sql

    def test_no_include_block_when_no_includes(self):
        rel = _FakeRelation("d", "s", "t")
        sql = _normalize_ws(_render("create_nonclustered_index(['c'], relation=rel)", rel=rel))
        assert "include" not in sql

    def test_false_include_is_treated_as_empty(self):
        rel = _FakeRelation("d", "s", "t")
        sql = _normalize_ws(
            _render("create_nonclustered_index(['c'], includes=false, relation=rel)", rel=rel)
        )
        assert "include" not in sql

    def test_columns_with_brackets_are_escaped(self):
        rel = _FakeRelation("d", "s", "t")
        sql = _normalize_ws(
            _render("create_nonclustered_index(['we]ird'], relation=rel)", rel=rel)
        )
        assert "[we]]ird]" in sql

    def test_idempotent_wrapper(self):
        rel = _FakeRelation("d", "s", "t")
        sql = _normalize_ws(_render("create_nonclustered_index(['c'], relation=rel)", rel=rel))
        assert "if not exists" in sql and "begin" in sql and "end" in sql


class TestColumnstoreIndexName:
    def test_intermediate_relation_uses_target_name(self):
        intermediate = _FakeRelation("d", "s", "my_model__dbt_tmp")
        target = _FakeRelation("d", "s", "my_model")
        sql_int = _render(
            "sqlserver__create_clustered_columnstore_index(intermediate)",
            intermediate=intermediate,
        )
        sql_final = _render(
            "sqlserver__create_clustered_columnstore_index(target)",
            target=target,
        )
        name_re = re.compile(
            r"CREATE\s+CLUSTERED\s+COLUMNSTORE\s+INDEX\s+(\[[^\]]+\])", re.IGNORECASE
        )
        name_int = name_re.search(_normalize_ws(sql_int)).group(1)
        name_final = name_re.search(_normalize_ws(sql_final)).group(1)
        assert name_int == name_final, (
            f"intermediate CCI name {name_int} differs from final {name_final}; "
            "would be orphaned after rename"
        )

    def test_uses_qualified_target_relation_for_create(self):
        target = _FakeRelation("mydb", "myschema", "my_model")
        sql = _normalize_ws(
            _render(
                "sqlserver__create_clustered_columnstore_index(target)",
                target=target,
            )
        )
        assert "ON [mydb].[myschema].[my_model]" in sql
