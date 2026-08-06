"""Pure resolution + diff logic for object-level ``DENY`` permissions.

Kept free of any database or dbt-context dependency so it can be unit tested
in isolation, mirroring ``sqlserver_mask.py``. The adapter's ``@available``
wrappers extract plain data from the model / ``sys.database_permissions`` and
delegate here; the Jinja ``sqlserver__apply_denies`` macro turns the diff into
DDL.

A SQL Server object-level ``DENY`` is stored against ``object_id``, so dbt
destroys it every time it drops and recreates the relation. The ``denies:``
config re-applies it after materialization, diffed against the live state, the
same way ``masks:`` re-applies Dynamic Data Masking.

One config surface feeds a ``{privilege: [principals]}`` map:

* the model-level ``denies`` dict, shaped exactly like ``grants``.

Privilege and principal identifiers are compared case-insensitively, matching
SQL Server's default collation. The user's original spelling is the one emitted
in DDL.
"""

from typing import Dict, List, Optional, Sequence, Tuple

# Object-level, table-scoped privileges that a DENY can carry. Column-level
# DENY (minor_id > 0) and database/server-scoped permissions are out of scope:
# the schema-grant-with-object-exceptions pattern only needs these.
SUPPORTED_PRIVILEGES = ("select", "insert", "update", "delete", "references")


def _normalize(name: str) -> str:
    return name.strip().lower()


def _dedupe_ci(principals: Sequence[str]) -> List[str]:
    """Drop case-insensitive duplicates, preserving first-seen spelling/order."""
    seen: Dict[str, str] = {}
    for principal in principals or []:
        key = _normalize(principal)
        if key and key not in seen:
            seen[key] = principal
    return list(seen.values())


def _normalize_grant_map(grant_config: Optional[Dict[str, Sequence[str]]]) -> Dict[str, set]:
    """``{privilege: [grantee]}`` -> ``{privilege_lower: {grantee_lower}}``."""
    result: Dict[str, set] = {}
    for privilege, grantees in (grant_config or {}).items():
        result.setdefault(_normalize(privilege), set()).update(
            _normalize(g) for g in (grantees or [])
        )
    return result


def resolve_denies(
    deny_config: Optional[Dict[str, Sequence[str]]],
    grant_config: Optional[Dict[str, Sequence[str]]],
    model_name: str,
) -> Tuple[Dict[str, List[str]], List[str], List[str]]:
    """Normalise the ``denies`` config into a clean ``{privilege: [principals]}``.

    ``deny_config`` is already surface-merged by dbt across ``dbt_project.yml`` /
    ``.yml`` / in-file ``config()``. This:

    * lower-cases privilege names and case-insensitively de-duplicates the
      principal list under each (preserving the first spelling seen);
    * collects any privilege that is **not** an object-level table privilege into
      ``unsupported`` so the caller can fail loudly — a silently-skipped DENY is
      the fail-open regression this feature exists to prevent;
    * warns when a ``(privilege, principal)`` pair appears in **both** ``denies``
      and ``grants``, which would emit contradictory DDL and is almost certainly
      a mistake.

    Returns ``(resolved, warnings, unsupported)``.
    """
    warnings: List[str] = []
    unsupported: List[str] = []
    resolved: Dict[str, List[str]] = {}

    for privilege, principals in (deny_config or {}).items():
        norm_priv = _normalize(privilege)
        if norm_priv not in SUPPORTED_PRIVILEGES:
            unsupported.append(privilege)
            continue
        deduped = _dedupe_ci(principals)
        if not deduped:
            continue
        # Merge case-variant privilege keys ("select" and "SELECT") into one.
        existing = resolved.get(norm_priv, [])
        resolved[norm_priv] = _dedupe_ci(existing + deduped)

    grant_map = _normalize_grant_map(grant_config)
    for privilege, principals in resolved.items():
        granted = grant_map.get(privilege, set())
        for principal in principals:
            if _normalize(principal) in granted:
                warnings.append(
                    f"On model '{model_name}', principal '{principal}' is both granted "
                    f"and denied '{privilege}'. A DENY overrides a GRANT in SQL Server; "
                    f"remove the principal from one of `grants` or `denies` to avoid "
                    f"contradictory permissions."
                )

    return resolved, warnings, unsupported


def deny_changes(
    existing_denies: Sequence[Dict[str, str]],
    desired: Dict[str, Sequence[str]],
) -> Dict[str, List[Tuple[str, str]]]:
    """Diff the desired deny map against current ``sys.database_permissions``.

    ``existing_denies`` is a sequence of ``{"grantee", "privilege_type"}`` rows
    (state ``DENY``, class 1, minor_id 0). ``desired`` is the resolved
    ``{privilege: [principals]}`` map.

    Returns lists of ``(privilege, principal)`` pairs keyed:

    * ``denies`` – configured but not yet present (emit ``DENY``), keeping the
      config's spelling;
    * ``revokes`` – present but no longer configured (emit ``REVOKE``, which
      removes a DENY as well as a GRANT), keeping the database's spelling.

    Both dimensions are compared case-insensitively on ``(privilege, principal)``.
    A converged relation returns two empty lists.
    """
    existing_pairs: Dict[Tuple[str, str], Tuple[str, str]] = {}
    for row in existing_denies:
        privilege = row["privilege_type"]
        principal = row["grantee"]
        existing_pairs[(_normalize(privilege), _normalize(principal))] = (privilege, principal)

    desired_pairs: Dict[Tuple[str, str], Tuple[str, str]] = {}
    for privilege, principals in (desired or {}).items():
        for principal in principals:
            desired_pairs[(_normalize(privilege), _normalize(principal))] = (privilege, principal)

    denies = [pair for key, pair in desired_pairs.items() if key not in existing_pairs]
    revokes = [pair for key, pair in existing_pairs.items() if key not in desired_pairs]

    return {"denies": denies, "revokes": revokes}
