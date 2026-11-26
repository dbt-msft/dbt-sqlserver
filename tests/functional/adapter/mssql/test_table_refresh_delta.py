import pytest

from dbt.tests.util import get_connection, run_dbt, run_dbt_and_capture

delta_model_sql = """
{{ config(materialized="table", table_refresh_method=var("method", "delta"),
          as_columnstore=False) }}
select id, val, ci_val, sys_val, at_tz, loaded_at from {{ target.schema }}.delta_src
"""

delta_variant_model_sql = """
{{ config(materialized="table", table_refresh_method="delta", as_columnstore=False) }}
select 1 as id, cast('a' as sql_variant) as doc{% if var("extra", false) %}, 2 as extra{% endif %}
"""


def execute(project, sql, fetch=False):
    with get_connection(project.adapter):
        _, table = project.adapter.execute(sql, fetch=fetch)
    return table.rows if fetch else None


def differences(project):
    """Rows whose value, byte length, offset or copy count differs between model and source."""
    keys = "id, val, DATALENGTH(val), ci_val, DATALENGTH(ci_val), sys_val, DATALENGTH(sys_val)"
    keys = f"{keys}, at_tz, DATEPART(TZOFFSET, at_tz), loaded_at".replace(
        "ci_val", "ci_val COLLATE Latin1_General_CI_AS"
    )
    names = "id, val, val_len, ci_val, ci_len, sys_val, sys_len, at_tz, tz, loaded_at"
    src, live = (
        f"SELECT {names}, COUNT(*) AS copies"
        f" FROM (SELECT {keys} FROM {project.test_schema}.{table}) t ({names}) GROUP BY {names}"
        for table in ("delta_src", "delta_model")
    )
    return execute(
        project,
        f"SELECT (SELECT COUNT(*) FROM ({src} EXCEPT {live}) a)"
        f" + (SELECT COUNT(*) FROM ({live} EXCEPT {src}) b) AS differences",
        fetch=True,
    )[0][0]


def leftovers(project):
    return execute(
        project,
        f"SELECT COUNT(*) AS leftovers FROM sys.tables"
        f" WHERE schema_id = SCHEMA_ID('{project.test_schema}') AND name LIKE '%[_][_]dbt[_]%'",
        fetch=True,
    )[0][0]


def refresh(project, expect_pass=True, model="delta_model", vars=None):
    """Run the model; return the reason it fell back to the full swap, or None."""
    args = ["run", "--select", model] + (["--vars", vars] if vars else [])
    _, log = run_dbt_and_capture(args, expect_pass=expect_pass)
    marker = "fell back to the full swap: "
    return next(
        (line.split(marker)[1].strip() for line in log.splitlines() if marker in line), None
    )


class TestDeltaRefresh:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "delta_model.sql": delta_model_sql,
            "delta_variant_model.sql": delta_variant_model_sql,
        }

    def test_delta_paths(self, project):
        s = project.test_schema

        def ci_val(row_id):
            return execute(
                project, f"SELECT ci_val FROM {s}.delta_model WHERE id = {row_id}", fetch=True
            )[0][0]

        # ci_val is case-insensitive under the test database's CS collation.
        execute(
            project,
            f"""
            CREATE TABLE {s}.delta_src (
              id int NOT NULL, val varchar(20) NULL,
              ci_val varchar(20) COLLATE Latin1_General_CI_AS NULL,
              sys_val sysname NOT NULL, at_tz datetimeoffset NOT NULL,
              loaded_at datetime2 NOT NULL);
            WITH n AS (SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
                       FROM sys.all_objects)
            INSERT {s}.delta_src
            SELECT n, IIF(n % 10 = 0, NULL, CONCAT('v', n)), 'abc', 's',
                   '2026-01-01 10:00 +00:00', '2026-01-01' FROM n;
            """,
        )
        run_dbt(["run", "--select", "delta_model"])  # creates the table; no swap yet

        assert refresh(project) is None
        assert differences(project) == 0

        # A few rows, a NULL becoming a value, and changes = misses: trailing spaces, and an
        # offset that keeps the instant.
        execute(
            project,
            f"""
            UPDATE {s}.delta_src SET loaded_at = '2026-01-02' WHERE id <= 5;
            UPDATE {s}.delta_src SET val = 'filled' WHERE id = 10;
            UPDATE {s}.delta_src SET val = 'v21 ' WHERE id = 21;
            UPDATE {s}.delta_src SET sys_val = 's ' WHERE id = 22;
            UPDATE {s}.delta_src SET at_tz = SWITCHOFFSET(at_tz, '+01:00') WHERE id = 23;
            """,
        )
        assert refresh(project) is None
        assert differences(project) == 0

        # Case-only change under CI: equal under the column's collation, so the delta keeps the
        # old value. Every full swap below then shows itself by writing the new one.
        execute(project, f"UPDATE {s}.delta_src SET ci_val = 'ABC' WHERE id = 30")
        assert refresh(project) is None
        assert ci_val(30) == "abc"

        execute(project, f"UPDATE {s}.delta_src SET loaded_at = '2026-01-03' WHERE id <= 50")
        assert refresh(project) == "churn"
        assert differences(project) == 0
        assert ci_val(30) == "ABC"

        # Duplicates in the build; then moved to another row, so the target's copies and the
        # build's cancel out in a row count; then left behind in the target alone.
        execute(project, f"INSERT {s}.delta_src SELECT * FROM {s}.delta_src WHERE id = 1")
        assert refresh(project) == "duplicates"
        assert differences(project) == 0
        execute(
            project,
            f"""
            DELETE TOP (1) FROM {s}.delta_src WHERE id = 1;
            INSERT {s}.delta_src SELECT * FROM {s}.delta_src WHERE id = 2;
            """,
        )
        assert refresh(project) == "duplicates"
        assert differences(project) == 0
        execute(project, f"DELETE TOP (1) FROM {s}.delta_src WHERE id = 2")
        assert refresh(project) == "count"
        assert differences(project) == 0

        # A swap that fails part-way rolls back whole, and the next run clears its leftovers,
        # even under another method.
        execute(
            project,
            f"""
            ALTER TABLE {s}.delta_model ADD CONSTRAINT delta_ck CHECK (id < 1000);
            DELETE {s}.delta_src WHERE id = 99;
            INSERT {s}.delta_src SELECT 1000, val, ci_val, sys_val, at_tz, loaded_at
            FROM {s}.delta_src WHERE id = 1;
            """,
        )
        refresh(project, expect_pass=False)
        assert differences(project) == 2
        execute(project, f"DELETE {s}.delta_src WHERE id = 1000")
        run_dbt(["run", "--select", "delta_model", "--vars", "method: dml"])
        assert differences(project) == 0
        assert leftovers(project) == 0

        # The build's collation no longer matches the target's: EXCEPT cannot compare them.
        execute(
            project,
            f"""
            ALTER TABLE {s}.delta_src
              ALTER COLUMN ci_val varchar(20) COLLATE Latin1_General_CS_AS NULL;
            UPDATE {s}.delta_src SET ci_val = 'Abc' WHERE id = 30;
            """,
        )
        _, log = run_dbt_and_capture(["run", "--select", "delta_model"])
        assert "column(s) ci_val (collation differs" in log
        assert ci_val(30) == "Abc"

        assert leftovers(project) == 0

    def test_unsupported_type_warns_and_swaps(self, project):
        run_dbt(["run", "--select", "delta_variant_model"])
        _, log = run_dbt_and_capture(["run", "--select", "delta_variant_model"])
        assert "column(s) doc (unsupported type)" in log
        # A schema change takes the rename-swap, so the delta has nothing to warn about.
        _, log = run_dbt_and_capture(
            ["run", "--select", "delta_variant_model", "--vars", "extra: true"]
        )
        assert "cannot compare" not in log
        assert (
            execute(
                project,
                f"SELECT CAST(doc AS varchar(10)) AS doc"
                f" FROM {project.test_schema}.delta_variant_model",
                fetch=True,
            )[0][0]
            == "a"
        )


class TestDeltaRefreshWithoutDbtTransactions(TestDeltaRefresh):
    """The swap batch carries its own BEGIN and COMMIT."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"dbt_sqlserver_use_dbt_transactions": False}}


delta_key_model_sql = """
{{ config(materialized="table", table_refresh_method="delta", delta_key="id", as_columnstore=False,
          query_options={"max_grant_percent": 50}) }}
select id, grp, val from {{ target.schema }}.key_src
"""

delta_composite_key_model_sql = """
{{ config(materialized="table", table_refresh_method="delta", delta_key=["ID", "grp"],
          as_columnstore=False) }}
select id, grp, val from {{ target.schema }}.key_src
"""

delta_new_key_model_sql = """
{{ config(materialized="table", table_refresh_method="delta", delta_key=var("key", "id"),
          as_columnstore=False) }}
select id{% if var("key", "id") == "new_id" %}, id * 10 as new_id{% endif %}
from {{ target.schema }}.new_key_src
"""

delta_bad_key_model_sql = """
{{ config(materialized="table", table_refresh_method="delta", delta_key=["id", "nope"],
          as_columnstore=False) }}
select id, grp, val from {{ target.schema }}.key_src
"""


class TestDeltaKey:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "key_model.sql": delta_key_model_sql,
            "key_composite_model.sql": delta_composite_key_model_sql,
            "key_bad_model.sql": delta_bad_key_model_sql,
            "key_new_model.sql": delta_new_key_model_sql,
        }

    def test_key_narrows_the_check_without_trusting_it(self, project):
        s = project.test_schema
        execute(
            project,
            f"""
            CREATE TABLE {s}.key_src (id int NOT NULL, grp int NOT NULL, val varchar(20) NULL);
            WITH n AS (SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
                       FROM sys.all_objects)
            INSERT {s}.key_src SELECT n, n % 5, CONCAT('v', n) FROM n;
            """,
        )
        for model in ("key_model", "key_composite_model"):
            run_dbt(["run", "--select", model])
            assert refresh(project, model=model) is None

        # Unique key, changed row.
        execute(project, f"UPDATE {s}.key_src SET val = 'z' WHERE id = 2")
        assert refresh(project, model="key_model") is None

        # Key repeats but the rows differ: the key proves nothing, the exact check passes.
        execute(project, f"INSERT {s}.key_src VALUES (1, 1, 'other')")
        assert refresh(project, model="key_model") is None
        assert refresh(project, model="key_composite_model") is None

        # Key repeats and so does the whole row: only the exact check can see it.
        execute(project, f"INSERT {s}.key_src VALUES (1, 1, 'other')")
        assert refresh(project, model="key_model") == "duplicates"
        assert refresh(project, model="key_composite_model") == "duplicates"
        assert (
            execute(project, f"SELECT COUNT(*) FROM {s}.key_model", fetch=True)[0][0]
            == execute(project, f"SELECT COUNT(*) FROM {s}.key_src", fetch=True)[0][0]
        )

    def test_key_added_with_its_column_takes_the_rename_swap(self, project):
        s = project.test_schema
        execute(project, f"SELECT 1 AS id INTO {s}.new_key_src")
        run_dbt(["run", "--select", "key_new_model"])
        # The run that adds the key column takes the rename-swap; the next uses the key.
        assert refresh(project, model="key_new_model", vars="key: new_id") is None
        execute(project, f"INSERT {s}.new_key_src VALUES (2)")
        assert refresh(project, model="key_new_model", vars="key: new_id") is None
        assert (
            execute(project, f"SELECT SUM(new_id) FROM {s}.key_new_model", fetch=True)[0][0] == 30
        )

    def test_unknown_key_column_is_named(self, project):
        run_dbt(["run", "--select", "key_bad_model"])
        results = run_dbt(["run", "--select", "key_bad_model"], expect_pass=False)
        assert "delta_key column 'nope'" in results[0].message


delta_ci_model_sql = """
{{ config(materialized="table", table_refresh_method="delta", as_columnstore=False) }}
select id, s, n, b, f from {{ target.schema }}.ci_src
"""


class TestDeltaDuplicateCheckEquality:
    """No delta_key: the checksum-narrowed check must flag exactly what the full GROUP BY does."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"ci_model.sql": delta_ci_model_sql}

    def test_checksum_agrees_with_group_by(self, project):
        s = project.test_schema
        # Rows the GROUP BY keys treat as equal must share a CHECKSUM, or stage 1 misses them.
        values = (
            "('abc'),('ABC'),('Abc'),('àbc'),('ÀBÇ'),('abç'),('abc '),('ABC '),('abç  '),"
            "('aa'),('Aa'),('áa'),('ae'),('æ'),('ß'),('ss'),('SS'),('o'),('ö'),('Ö'),(''),(' '),"
            "(NULL)"
        )
        violations = sum(
            execute(
                project,
                f"""
                SELECT COUNT(*) FROM (
                  SELECT 1 AS x FROM (SELECT CAST(s AS varchar(10)) COLLATE {coll} AS s
                                      FROM (VALUES {values}) v(s)) t
                  GROUP BY s, DATALENGTH(s)
                  HAVING COUNT(DISTINCT CHECKSUM(s, DATALENGTH(s))) > 1) q
                """,
                fetch=True,
            )[0][0]
            for coll in (
                "Latin1_General_CI_AI",
                "Latin1_General_CI_AS",
                "Latin1_General_CS_AS",
                "Latin1_General_BIN2",
            )
        )
        assert violations == 0

        execute(
            project,
            f"""
            CREATE TABLE {s}.ci_src (
              id int NOT NULL,
              s varchar(20) COLLATE Latin1_General_CI_AI NULL,
              n nvarchar(20) COLLATE Latin1_General_CI_AI NULL,
              b varbinary(8) NULL, f float NULL);
            INSERT {s}.ci_src VALUES
              (1, 'abc', N'abc', 0x01, 0), (2, 'abc ', N'abc ', 0x0100, 1),
              (3, NULL, NULL, NULL, NULL), (4, 'x', N'x', 0x02, 2);
            """,
        )
        run_dbt(["run", "--select", "ci_model"])
        assert refresh(project, model="ci_model") is None

        # Trailing spaces, varbinary zero padding and NULL-vs-value are distinct rows.
        execute(
            project,
            f"""
            INSERT {s}.ci_src VALUES
              (1, 'abc  ', N'abc', 0x01, 0), (1, 'abc', N'abc', 0x010000, 0),
              (1, NULL, N'abc', 0x01, 0);
            """,
        )
        assert refresh(project, model="ci_model") is None

        # Equal under CI_AI: case-only, accent-only, both.
        execute(project, f"INSERT {s}.ci_src VALUES (50, 'abc', N'abc', 0x01, 0)")
        assert refresh(project, model="ci_model") is None
        for dup in ("'ABC', N'abc'", "'ábc', N'abc'", "'abc', N'ÀBC'"):
            execute(project, f"INSERT {s}.ci_src VALUES (50, {dup}, 0x01, 0)")
            assert refresh(project, model="ci_model") == "duplicates", dup
            execute(project, f"DELETE TOP (1) FROM {s}.ci_src WHERE id = 50")
            assert refresh(project, model="ci_model") == "count", dup
            assert refresh(project, model="ci_model") is None, dup

        # NULLs compare equal to each other, and 0 equals -0.
        execute(project, f"INSERT {s}.ci_src VALUES (3, NULL, NULL, NULL, NULL)")
        assert refresh(project, model="ci_model") == "duplicates"


delta_alias_model_sql = """
{{ config(materialized="table", table_refresh_method="delta", as_columnstore=False) }}
select id, code, note from {{ target.schema }}.alias_src
"""

delta_text_alias_model_sql = """
{{ config(materialized="table", table_refresh_method="delta", as_columnstore=False) }}
select id, body from {{ target.schema }}.text_alias_src
"""

delta_json_vector_model_sql = """
{{ config(materialized="table", table_refresh_method="delta", as_columnstore=False) }}
select id, doc, vec from {{ target.schema }}.json_vector_src
"""

delta_cap_model_sql = """
{{ config(materialized="table", table_refresh_method="delta", as_columnstore=False,
          delta_max_churn_percent=var("cap", 15)) }}
select id, val from {{ target.schema }}.cap_src
"""

delta_wide_model_sql = """
{{ config(materialized="table", table_refresh_method="delta", as_columnstore=False) }}
select 1 as id{% for i in range(520) %}, cast('x' as varchar(5)) as c{{ i }}{% endfor %}
"""


class TestDeltaComparability:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "alias_model.sql": delta_alias_model_sql,
            "text_alias_model.sql": delta_text_alias_model_sql,
            "wide_model.sql": delta_wide_model_sql,
            "json_vector_model.sql": delta_json_vector_model_sql,
            "cap_model.sql": delta_cap_model_sql,
        }

    def test_alias_types_compare_as_their_base_type(self, project):
        s = project.test_schema
        execute(
            project,
            f"""
            CREATE TYPE {s}.code_t FROM varchar(10);
            CREATE TYPE {s}.text_t FROM text;
            """,
        )
        execute(
            project,
            f"""
            CREATE TABLE {s}.alias_src (id int, code {s}.code_t, note {s}.code_t);
            INSERT {s}.alias_src SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),
              'a', 'n' FROM sys.all_objects;
            CREATE TABLE {s}.text_alias_src (id int, body {s}.text_t);
            INSERT {s}.text_alias_src VALUES (1, 'a');
            """,
        )
        try:
            run_dbt(["run", "--select", "alias_model", "text_alias_model"])

            # A trailing-space change to a varchar alias is written.
            execute(project, f"UPDATE {s}.alias_src SET code = 'a ' WHERE id = 1")
            assert refresh(project, model="alias_model") is None
            assert (
                execute(
                    project, f"SELECT DATALENGTH(code) FROM {s}.alias_model WHERE id = 1", True
                )[0][0]
                == 2
            )

            # A text alias cannot be compared: warn and take the full swap.
            execute(project, f"UPDATE {s}.text_alias_src SET body = 'b'")
            _, log = run_dbt_and_capture(["run", "--select", "text_alias_model"])
            assert "column(s) body (unsupported type)" in log
            assert (
                execute(
                    project,
                    f"SELECT CAST(body AS varchar(10)) FROM {s}.text_alias_model",
                    True,
                )[0][0]
                == "b"
            )
        finally:
            execute(
                project,
                f"""
                DROP TABLE IF EXISTS {s}.alias_model, {s}.alias_src,
                  {s}.text_alias_model, {s}.text_alias_src;
                DROP TYPE IF EXISTS {s}.code_t;
                DROP TYPE IF EXISTS {s}.text_t;
                """,
            )

    def test_too_many_keys_for_the_gone_table_takes_the_full_swap(self, project):
        run_dbt(["run", "--select", "wide_model"])
        _, log = run_dbt_and_capture(["run", "--select", "wide_model"])
        assert "1041 comparison keys" in log

    def test_json_and_vector_warn_and_swap(self, project):
        # sql_variant comes back driver-encoded on ADBC; cast it server-side.
        version = "SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS int)"
        if execute(project, version, True)[0][0] < 17:
            pytest.skip("json and vector need SQL Server 2025")
        s = project.test_schema
        execute(
            project,
            f"""
            CREATE TABLE {s}.json_vector_src (id int, doc json, vec vector(3));
            INSERT {s}.json_vector_src VALUES (1, '{{"a":1}}', '[1,2,3]');
            """,
        )
        run_dbt(["run", "--select", "json_vector_model"])
        execute(
            project,
            f"UPDATE {s}.json_vector_src SET doc = '{{\"a\":2}}', vec = '[1,2,4]'",
        )
        _, log = run_dbt_and_capture(["run", "--select", "json_vector_model"])
        assert "column(s) doc, vec (unsupported type)" in log
        values = "SELECT CAST(doc AS nvarchar(20)), CAST(vec AS nvarchar(60)) FROM"
        assert (
            execute(project, f"{values} {s}.json_vector_model", True)[0]
            == execute(project, f"{values} {s}.json_vector_src", True)[0]
        )

    def test_churn_cap_is_configurable(self, project):
        s = project.test_schema
        execute(
            project,
            f"""
            CREATE TABLE {s}.cap_src (id int, val varchar(20));
            INSERT {s}.cap_src SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), 'a'
            FROM sys.all_objects;
            """,
        )
        run_dbt(["run", "--select", "cap_model"])

        def change(rows, val):
            execute(project, f"UPDATE {s}.cap_src SET val = '{val}' WHERE id <= {rows}")

        # (rows changed out of 100, cap, expected path); the cap rounds down to whole rows.
        for rows, cap, path in [
            (20, None, "churn"),
            (20, 25, None),
            (7, 7.5, None),
            (8, 7.5, "churn"),
            (1, 0, "churn"),
        ]:
            change(rows, f"{rows}-{cap}")
            cap_vars = None if cap is None else f"cap: {cap}"
            assert refresh(project, model="cap_model", vars=cap_vars) == path
        assert refresh(project, model="cap_model", vars="cap: 0") is None  # nothing changed

        results = run_dbt(
            ["run", "--select", "cap_model", "--vars", "cap: 101"], expect_pass=False
        )
        assert "Invalid delta_max_churn_percent '101'" in results[0].message
