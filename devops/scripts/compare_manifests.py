#!/usr/bin/env python3
"""Compare manifest.json from default dbt parser vs v2 (experimental) parser.

Exit codes:
  0 - manifests equivalent (or only known-diff fields differ)
  1 - meaningful differences found
  2 - error (binary missing, parse failed, etc.)
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

# Sections to compare within the manifest
COMPARE_SECTIONS: list[str] = [
    "nodes",
    "sources",
    "macros",
    "docs",
    "exposures",
    "metrics",
    "semantic_models",
    "selectors",
    "disabled",
]

# Fields known to differ between parses that should be ignored
DEFAULT_KNOWN_DIFF_FIELDS: list[str] = [
    "metadata.generated_at",
    "metadata.invocation_id",
    "metadata.dbt_schema_version",
    "metadata.dbt_version",
    "metadata.env",
    "metadata.project_id",
    "metadata.user_id",
    "metadata.adapter_type",  # may differ depending on parsing path
]


def _get_nested(obj: dict[str, Any], dotted_key: str) -> Any:
    """Get a nested value from dict using dot-notation key."""
    keys = dotted_key.split(".")
    current: Any = obj
    for key in keys:
        if isinstance(current, Mapping):
            current = current.get(key)
        else:
            return None
    return current


def _strip_known_diff_fields(data: dict[str, Any], known_fields: list[str]) -> None:
    """Remove known-diff fields from a manifest dict in-place."""
    for field in known_fields:
        parts = field.split(".")
        if len(parts) == 1:
            data.pop(parts[0], None)
            continue
        # Nested field -- walk to parent, delete leaf
        parent: Any = data
        for key in parts[:-1]:
            if isinstance(parent, Mapping):
                parent = parent.get(key)
            else:
                parent = None
                break
        if isinstance(parent, Mapping):
            parent.pop(parts[-1], None)


def _recursive_diff(
    v1: Any,
    v2: Any,
    path: str = "",
) -> list[dict[str, Any]]:
    """Recursively compare two values, returning list of difference entries."""
    diffs: list[dict[str, Any]] = []

    if isinstance(v1, Mapping) and isinstance(v2, Mapping):
        all_keys = set(v1.keys()) | set(v2.keys())
        for key in sorted(all_keys):
            child_path = f"{path}.{key}" if path else key
            if key not in v1:
                diffs.append({"path": child_path, "type": "missing_in_v1", "v2_value": v2[key]})
            elif key not in v2:
                diffs.append({"path": child_path, "type": "missing_in_v2", "v1_value": v1[key]})
            else:
                diffs.extend(_recursive_diff(v1[key], v2[key], child_path))
    elif isinstance(v1, list) and isinstance(v2, list):
        if len(v1) != len(v2):
            diffs.append(
                {
                    "path": path,
                    "type": "list_length_mismatch",
                    "v1_length": len(v1),
                    "v2_length": len(v2),
                }
            )
        max_len = max(len(v1), len(v2))
        for i in range(max_len):
            child_path = f"{path}[{i}]"
            if i >= len(v1):
                diffs.append(
                    {
                        "path": child_path,
                        "type": "missing_in_v1",
                        "v2_value": v2[i],
                    }
                )
            elif i >= len(v2):
                diffs.append(
                    {
                        "path": child_path,
                        "type": "missing_in_v2",
                        "v1_value": v1[i],
                    }
                )
            else:
                diffs.extend(_recursive_diff(v1[i], v2[i], child_path))
    elif v1 != v2:
        diffs.append(
            {
                "path": path,
                "type": "value_mismatch",
                "v1_value": v1,
                "v2_value": v2,
            }
        )

    return diffs


def _run_dbt_parse(
    project_dir: Path,
    profiles_dir: Path | None,
    target: str | None,
    use_v2: bool,
) -> Path:
    """Run dbt parse and return path to the resulting manifest.json."""
    cmd = ["dbt", "parse"]
    cmd.extend(["--project-dir", str(project_dir)])
    if profiles_dir:
        cmd.extend(["--profiles-dir", str(profiles_dir)])
    if target:
        cmd.extend(["--target", target])
    # Use --no-write-json to avoid conflicting with custom output handling?
    # Actually, dbt parse always writes manifest.json to target/.
    # For v2, we need --use-v2-parser.
    if use_v2:
        cmd.append("--use-v2-parser")
    else:
        # For v1, save the manifest before v2 overwrites it
        pass

    print(f"  Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(project_dir))

    if result.returncode != 0:
        print(f"  dbt parse failed with exit code {result.returncode}", file=sys.stderr)
        stderr_output = result.stderr or result.stdout
        if stderr_output:
            print(f"  Output:\n{stderr_output[:2000]}", file=sys.stderr)
        sys.exit(2)

    manifest_path = project_dir / "target" / "manifest.json"
    if not manifest_path.exists():
        print(f"  ERROR: manifest.json not found at {manifest_path}", file=sys.stderr)
        sys.exit(2)

    return manifest_path


def _section_counts(manifest: dict[str, Any]) -> dict[str, int]:
    """Return counts of items in each comparable section."""
    counts: dict[str, int] = {}
    for section in COMPARE_SECTIONS:
        data = manifest.get(section)
        if isinstance(data, Mapping):
            counts[section] = len(data)
        elif isinstance(data, list):
            counts[section] = len(data)
        else:
            counts[section] = 0
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare dbt manifest.json from default parser vs v2 parser."
    )
    parser.add_argument("--project-dir", required=True, help="Path to dbt project")
    parser.add_argument("--profiles-dir", default=None, help="Path to dbt profiles directory")
    parser.add_argument("--target", default=None, help="dbt target name")
    parser.add_argument(
        "--known-diff-fields",
        default="",
        help="Comma-separated extra fields to ignore (e.g. metadata.x, nodes.*.compiled)",
    )
    args = parser.parse_args()

    project_dir = Path(args.project_dir).resolve()
    if not project_dir.is_dir():
        print(f"ERROR: project-dir not found: {project_dir}", file=sys.stderr)
        sys.exit(2)

    profiles_dir = Path(args.profiles_dir).resolve() if args.profiles_dir else None
    if args.profiles_dir and not profiles_dir.is_dir():  # type: ignore[union-attr]
        print(f"ERROR: profiles-dir not found: {profiles_dir}", file=sys.stderr)
        sys.exit(2)

    # Build known-diff fields list
    known_diff_fields = list(DEFAULT_KNOWN_DIFF_FIELDS)
    if args.known_diff_fields:
        extra_fields = [f.strip() for f in args.known_diff_fields.split(",") if f.strip()]
        known_diff_fields.extend(extra_fields)

    # Check dbt is available
    if not shutil.which("dbt"):
        print("ERROR: dbt binary not found in PATH", file=sys.stderr)
        sys.exit(2)

    target_dir = project_dir / "target"
    target_dir.mkdir(parents=True, exist_ok=True)

    v1_manifest_json = target_dir / "manifest_v1.json"
    v2_manifest_json = target_dir / "manifest_v2.json"

    # Step 1: Run dbt parse (default parser) and save manifest
    print("=== Step 1: dbt parse (default Python parser) ===")
    manifest_path = _run_dbt_parse(project_dir, profiles_dir, args.target, use_v2=False)
    shutil.copy2(manifest_path, v1_manifest_json)
    print(f"  Saved: {v1_manifest_json}")

    # Step 2: Run dbt parse (v2 parser) and save manifest
    print("\n=== Step 2: dbt parse (v2 experimental parser) ===")
    manifest_path = _run_dbt_parse(project_dir, profiles_dir, args.target, use_v2=True)
    shutil.copy2(manifest_path, v2_manifest_json)
    print(f"  Saved: {v2_manifest_json}")

    # Step 3: Load both manifests
    print("\n=== Step 3: Loading manifests ===")
    with open(v1_manifest_json, encoding="utf-8") as f:
        v1: dict[str, Any] = json.load(f)
    with open(v2_manifest_json, encoding="utf-8") as f:
        v2: dict[str, Any] = json.load(f)

    # Step 4: Strip known-diff fields from both
    print(f"  Stripping known-diff fields: {known_diff_fields}")
    _strip_known_diff_fields(v1, known_diff_fields)
    _strip_known_diff_fields(v2, known_diff_fields)

    # Step 5: Compare sections
    print("\n=== Step 4: Comparing manifests ===")
    all_diffs: list[dict[str, Any]] = []

    # Section-level comparison
    section_summary: dict[str, Any] = {
        "v1_counts": _section_counts(v1),
        "v2_counts": _section_counts(v2),
    }
    count_diffs: dict[str, dict[str, int]] = {}
    for section in COMPARE_SECTIONS:
        c1 = section_summary["v1_counts"].get(section, 0)
        c2 = section_summary["v2_counts"].get(section, 0)
        if c1 != c2:
            count_diffs[section] = {"v1": c1, "v2": c2}

    section_summary["count_diffs"] = count_diffs

    # Deep diff
    print("  Performing deep comparison...")
    diffs = _recursive_diff(v1, v2)
    all_diffs.extend(diffs)

    # Build diff output
    diff_output: dict[str, Any] = {
        "summary": {
            "total_diffs": len(all_diffs),
            "section_counts": section_summary,
        },
        "known_diff_fields_stripped": known_diff_fields,
        "differences": all_diffs,
    }

    # Write diff
    diff_path = target_dir / "manifest_diff.json"
    with open(diff_path, "w", encoding="utf-8") as f:
        json.dump(diff_output, f, indent=2, default=str)

    # Step 6: Human-readable summary
    print("\n=== Summary ===\n")
    print(f"  Total differences found: {len(all_diffs)}")
    if count_diffs:
        print("  Section count diffs:")
        for section, counts in sorted(count_diffs.items()):
            print(f"    {section}: v1={counts['v1']} vs v2={counts['v2']}")
    else:
        print("  All section counts match.")

    if all_diffs:
        print("\n  Difference types:")
        diff_types: dict[str, int] = {}
        for d in all_diffs:
            t = d.get("type", "unknown")
            diff_types[t] = diff_types.get(t, 0) + 1
        for t, c in sorted(diff_types.items()):
            print(f"    {t}: {c}")

        # Show first N diffs as examples
        print("\n  First 10 differences:")
        for d in all_diffs[:10]:
            print(f"    {d['type']}: {d['path']}")
            if "v1_value" in d and "v2_value" in d:
                v1_str = str(d["v1_value"])[:80]
                v2_str = str(d["v2_value"])[:80]
                print(f"      v1: {v1_str}")
                print(f"      v2: {v2_str}")
    else:
        print("  No differences found -- manifests are equivalent!")

    print(f"\n  Diff written to: {diff_path}")

    exit_code = 1 if all_diffs else 0
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
