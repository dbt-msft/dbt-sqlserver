#!/usr/bin/env python3
"""Verify the packaged version matches the release git tag.

Usage:
    python .github/scripts/verify_version.py <tag>

The tag typically comes from ``$GITHUB_REF_NAME`` and may be prefixed with
``v`` (e.g. ``v1.9.1``). The leading ``v`` is stripped before comparison.

Exits 0 on match, 1 on mismatch, 2 on usage error.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
VERSION_FILE = REPO_ROOT / "dbt" / "adapters" / "sqlserver" / "__version__.py"
VERSION_RE = re.compile(r"""version\s*=\s*["'](?P<version>.+?)["']""")


def read_package_version(version_file: Path = VERSION_FILE) -> str:
    match = VERSION_RE.search(version_file.read_text())
    if not match:
        raise ValueError(f"could not find version in {version_file}")
    return match.group("version")


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: verify_version.py <tag>", file=sys.stderr)
        return 2
    tag_version = argv[1].lstrip("v")
    pkg_version = read_package_version()
    if tag_version != pkg_version:
        print(
            f"Git tag {tag_version!r} does not match "
            f"package version {pkg_version!r}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
