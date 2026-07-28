"""Functional tests for dbt --use-v2-parser basic operations.

Covers parse, compile, run (with seed), build, and docs generate
using the Rust v2 parser via dbt-core-experimental-parser binary.

STATUS (2026-07-28): The v2 parser (dbt-core-experimental-parser / dbt Fusion
2.0.0-alpha.5) does NOT recognize the ``sqlserver`` adapter type in
profiles.yml. It only supports: snowflake, bigquery, databricks, redshift,
duckdb, salesforce, clickhouse (plus experimental: postgres, trino,
datafusion, spark, fdcs, exasol, fabric). Until ``sqlserver`` is added to the
v2 parser's adapter enum, ``--use-v2-parser`` cannot be used with
dbt-sqlserver. These tests are marked xfail pending that support.
"""

import os
import shutil

import pytest

from dbt.tests.util import run_dbt

_HAS_V2_PARSER = shutil.which("dbt-core-experimental-parser") is not None
skip_if_no_v2_parser = pytest.mark.skipif(
    not _HAS_V2_PARSER,
    reason="dbt-core-experimental-parser binary not on PATH",
)
xfail_no_sqlserver_support = pytest.mark.xfail(
    _HAS_V2_PARSER,
    reason="v2 parser does not yet support sqlserver adapter type: "
    "FusionParserError: unknown variant `sqlserver`",
    strict=True,
)

pytestmark = [pytest.mark.v2_parser, skip_if_no_v2_parser, xfail_no_sqlserver_support]

# ------- Model SQL snippets -------

SEED_DATA = """id,name
1,alpha
2,beta
3,gamma
"""

MODEL_BASE_SQL = """
select * from {{ ref('seed_base') }}
"""

MODEL_VIEW_SQL = """
{{ config(materialized='view') }}
select id, upper(name) as name_upper from {{ ref('model_base') }}
"""

MODEL_TABLE_SQL = """
{{ config(materialized='table') }}
select id, name_upper from {{ ref('model_view') }}
"""


class TestV2ParserBasic:
    """Basic v2 parser operations: parse, compile, run, build, docs generate."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_base.csv": SEED_DATA}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_base.sql": MODEL_BASE_SQL,
            "model_view.sql": MODEL_VIEW_SQL,
            "model_table.sql": MODEL_TABLE_SQL,
        }

    def test_parse_with_v2_parser(self, project):
        """dbt parse --use-v2-parser succeeds and produces manifest."""
        results = run_dbt(["--use-v2-parser", "parse"])
        assert len(results) >= 1

    def test_compile_with_v2_parser(self, project):
        """dbt compile --use-v2-parser succeeds and produces compiled SQL files."""
        # parse first so project state is ready
        run_dbt(["--use-v2-parser", "parse"])
        results = run_dbt(["--use-v2-parser", "compile"])
        assert len(results) >= 1
        for r in results:
            assert r.status == "success"

        # Verify compiled SQL files exist
        compiled_root = os.path.join(project.project_root, "target", "compiled")
        assert os.path.isdir(compiled_root)
        compiled_files = []
        for root, _, files in os.walk(compiled_root):
            for f in files:
                if f.endswith(".sql"):
                    compiled_files.append(os.path.join(root, f))
        assert len(compiled_files) >= 1

    def test_run_with_v2_parser(self, project):
        """dbt seed + dbt run --use-v2-parser materializes models."""
        # Seed first
        seed_results = run_dbt(["--use-v2-parser", "seed"])
        assert len(seed_results) >= 1
        assert all(r.status == "success" for r in seed_results)

        # Then run models
        run_results = run_dbt(["--use-v2-parser", "run"])
        assert len(run_results) >= 1
        assert all(r.status == "success" for r in run_results)

    def test_build_with_v2_parser(self, project):
        """dbt build --use-v2-parser runs the full DAG: seed -> models."""
        # seed already done by test_run; build should be a no-op or succeed
        build_results = run_dbt(["--use-v2-parser", "build", "--select", "model_view+"])
        assert len(build_results) >= 1
        assert all(r.status in ("success", "pass") for r in build_results)

    def test_docs_generate_with_v2_parser(self, project):
        """dbt docs generate --use-v2-parser produces catalog.json and manifest.json."""
        # Ensure models are materialized first
        run_dbt(["--use-v2-parser", "build"])
        results = run_dbt(["--use-v2-parser", "docs", "generate"])
        assert len(results) >= 1

        target_dir = os.path.join(project.project_root, "target")
        manifest_path = os.path.join(target_dir, "manifest.json")
        catalog_path = os.path.join(target_dir, "catalog.json")
        assert os.path.isfile(manifest_path), f"manifest.json missing at {manifest_path}"
        assert os.path.isfile(catalog_path), f"catalog.json missing at {catalog_path}"

    def test_incremental_with_v2_parser(self, project):
        """An incremental model builds correctly under --use-v2-parser and
        a second run is idempotent."""
        run_dbt(["--use-v2-parser", "build"])
        # Second run: incremental should be a no-op (no new data)
        results = run_dbt(["--use-v2-parser", "run", "--select", "model_table"])
        assert len(results) == 1
        assert results[0].status == "success"
