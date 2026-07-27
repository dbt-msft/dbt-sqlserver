# V2 Parser Support (Experimental)

## Current Status

**The v2 parser does NOT support the `sqlserver` adapter type.** As of dbt-core
1.12.0 and `dbt-core-experimental-parser` 2.0.0-alpha.5 (July 2026), the v2
parser only recognizes these adapter types:

- **Supported**: snowflake, bigquery, databricks, redshift, duckdb, salesforce,
  clickhouse
- **Experimental** (requires `DBT_ALLOW_EXPERIMENTAL_ADAPTERS=true`): postgres,
  trino, datafusion, spark, fdcs, exasol, fabric

`sqlserver` is not in either list. Passing `--use-v2-parser` with a
profiles.yml containing `type: sqlserver` produces:

```
[error] [InvalidConfig (dbt1005)]: Failed to parse profiles.yml:
unknown variant `sqlserver`, expected one of `redshift`, `snowflake`, ...
```

Until `sqlserver` is added to the v2 parser's adapter registry (which would
require either a Rust adapter implementation in upstream `dbt-labs/dbt-core` or
a change in the v2 parser to accept unknown adapter types during parse-only
mode), `--use-v2-parser` cannot be used with `dbt-sqlserver`.

**The test suite and CI job in this repo will automatically start passing when
that support is added** -- all tests are marked `xfail` with `strict=True` to
surface the transition immediately.

## References

- **dbt-core 1.12.0 release notes** -- `--use-v2-parser` flag ([#13029](https://github.com/dbt-labs/dbt-core/issues/13029)):
  > "Add `--use-v2-parser` to delegate parsing to the fusion parser, load its
  > `manifest.json` into a runtime Manifest, and bypass dbt-core's parser."

- **dbt-core CLI flag definition** (`dbt/cli/params.py` lines 805-818):
  ```python
  use_v2_parser = ("--use-v2-parser/--no-use-v2-parser",
      envvar="DBT_ENGINE_USE_V2_PARSER", default=False)
  v2_parser = ("--v2-parser", default="dbt-core-experimental-parser parse")
  ```

- **V2 parser handoff** (`dbt/parser/fusion.py`): `parse_with_fusion()` spawns
  `dbt-core-experimental-parser parse`, forwarding `--project-dir`,
  `--profiles-dir`, `--profile`, `--target`, then loads the resulting
  `manifest.json` into dbt-core's runtime `Manifest`.

- **V2 parser hint** (`dbt/hints.py` line 30-33):
  > "Your parse is taking a long time. You can speed up your parsing with the
  > new rust parser: [docs link]"

- **dbt docs** (incomplete as of July 2026): the [parsing global
  configs](https://docs.getdbt.com/reference/global-configs/parsing#opt-in-v2-parser)
  page has an "Opt-in v2 parser" heading with no content.

## What the V2 Parser Is

The v2 parser is a **Rust-based dbt engine** (dbt Fusion; distributed as the
`dbt-core-experimental-parser` pip package). When `--use-v2-parser` is
specified, dbt-core delegates the parse phase to this binary:

1. dbt-core spawns `dbt-core-experimental-parser parse` with the project's flags
2. The v2 parser reads all project files, validates them, and produces
   `manifest.json`
3. dbt-core loads that manifest and uses its own Python adapter for
   connection/runtime/materialization

This means the adapter code (`dbt/adapters/sqlserver/`) is **unchanged** -- only
the parse phase is replaced. However, the v2 parser validates `profiles.yml`
including the `type` field, so it must recognize the adapter type.

## Local Setup

The `dbt-core-experimental-parser` package is already in the `dev` dependency
group:

```shell
# After make dev or uv sync --group dev:
uv run dbt-core-experimental-parser --version
# dbt-core 2.0.0-alpha.5
```

Verify the flag is present:

```shell
uv run dbt parse --help | grep use-v2-parser
#   --use-v2-parser / --no-use-v2-parser
```

## Running the Tests

```shell
# Via make
make v2-parser-test

# Directly
uv run pytest -m v2_parser tests/functional/adapter/v2_parser -v
```

All 13 tests are marked `xfail` -- they will pass **automatically** when the v2
parser adds `sqlserver` support.  Requires a running SQL Server (`make server`)
and `test.env` configured.

## CI Integration

| Property | Value |
|---|---|
| Workflow | `.github/workflows/integration-tests-sqlserver.yml` |
| Job | `v2-parser-tests` |
| Trigger | push/PR to `master` or `v*` (no schedule) |
| Blocking | Yes |
| Matrix | py3.13 + pyodbc + SQL Server 2025 + ODBC 18 |

The CI job installs `dbt-core-experimental-parser` and runs the v2 parser test
suite. When tests are xfail, the job passes (expected failure). When support
lands and tests un-xfail, any regression is caught immediately.

## Manifest Comparison

`devops/scripts/compare_manifests.py` compares manifests produced by the default
Python parser vs the v2 parser. Currently only works with supported adapter
types (duckdb, etc.) since `sqlserver` is rejected by the v2 parser.

```shell
python devops/scripts/compare_manifests.py --project-dir <path>
```

Exit codes: `0` = equivalent, `1` = differences, `2` = error.

## Test Scenarios

| Module | Scenarios |
|---|---|
| `test_v2_parser_basic.py` | parse, compile, seed+run, build, docs generate, incremental |
| `test_v2_parser_adapter_features.py` | column types, native string types, indexes, multi-materialization |

## Next Steps for sqlserver Support

1. Track [dbt-core#13029](https://github.com/dbt-labs/dbt-core/issues/13029) for
   v2 parser adapter registration updates
2. When `sqlserver` is added to the adapter enum, remove `xfail` markers
3. Run the manifest comparison script to document any remaining differences
4. A future dbt Core 2 migration will require a Rust-based `dbt-sqlserver`
   adapter in `dbt-labs/dbt-core`
