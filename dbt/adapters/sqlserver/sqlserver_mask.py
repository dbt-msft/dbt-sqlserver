"""Pure resolution + diff logic for Dynamic Data Masking (DDM).

Kept free of any database or dbt-context dependency so it can be unit tested
in isolation, mirroring ``relation_configs/index.py``. The adapter's
``@available`` wrappers extract plain data from the model / ``sys.masked_columns``
and delegate here; the Jinja ``sqlserver__apply_masks`` macro turns the diff
into DDL.

Two config surfaces feed one per-column mask map:

* column-level ``masked_with:`` property (a value, or ``null`` to opt out), and
* the model-level ``masks`` dict.

Column-level wins over model-level, and any conflict is surfaced as a warning.
Column identifiers are compared case-insensitively, matching SQL Server's
default collation.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set, Tuple


@dataclass(frozen=True)
class ColumnMask:
    """A column's ``masked_with`` declaration, as parsed from schema YAML.

    ``masked_with_present`` distinguishes "the key was written" from "absent":
    an explicit ``masked_with: null`` (present with ``masked_with=None``) means
    "no mask here", overriding any inherited model-level ``masks`` entry.
    """

    name: str
    masked_with_present: bool
    masked_with: Optional[str]


def _normalize_name(name: str) -> str:
    return name.strip().lower()


def _pop_ci(mapping: Dict[str, str], name: str) -> Optional[str]:
    """Remove a key matching ``name`` case-insensitively; return its value."""
    target = _normalize_name(name)
    for key in list(mapping):
        if _normalize_name(key) == target:
            return mapping.pop(key)
    return None


def _find_ci(mapping: Dict[str, str], name: str) -> Optional[str]:
    """Return the existing key matching ``name`` case-insensitively, if any."""
    target = _normalize_name(name)
    for key in mapping:
        if _normalize_name(key) == target:
            return key
    return None


def resolve_masks(
    column_masks: Sequence[ColumnMask],
    model_masks: Optional[Dict[str, str]],
    model_name: str,
) -> Tuple[Dict[str, str], List[str]]:
    """Merge the two config surfaces into one ``{column: function}`` map.

    ``model_masks`` seeds the map (it is already surface-merged by dbt across
    ``dbt_project.yml`` / ``.yml`` / in-file ``config()``). Each column-level
    ``masked_with`` then overrides:

    * a function value wins over any model-level entry for the same column and,
      when both surfaces target that column, emits a conflict warning (even when
      the two functions agree — a duplicate declaration is worth surfacing);
    * an explicit ``null`` removes any inherited model-level entry ("opt out").

    Key *existence* is not validated here — that happens in the macro against
    the actually-built relation, which is authoritative and complete even when
    the YAML ``columns:`` block is partial.
    """
    mask_map: Dict[str, str] = dict(model_masks or {})
    warnings: List[str] = []

    for cm in column_masks:
        if not cm.masked_with_present:
            continue

        if cm.masked_with is None:
            # explicit opt-out: drop any inherited model-level mask
            _pop_ci(mask_map, cm.name)
            continue

        conflict_key = _find_ci(model_masks or {}, cm.name)
        if conflict_key is not None:
            warnings.append(
                f"On model '{model_name}', column '{cm.name}' is masked by both a "
                f"column-level `masked_with` ('{cm.masked_with}') and a model-level "
                f"`masks` entry ('{(model_masks or {})[conflict_key]}'). The column-level "
                f"function ('{cm.masked_with}') is applied (column-level wins)."
            )

        # column-level wins: drop any case-variant model-level key, then set
        _pop_ci(mask_map, cm.name)
        mask_map[cm.name] = cm.masked_with

    return mask_map, warnings


def _normalize_function(function: str) -> str:
    """Canonicalise a masking-function string for comparison only.

    ``sys.masked_columns.masking_function`` may store a function with different
    internal spacing than the user wrote (e.g. ``partial(0, "X", 0)`` vs
    ``partial(0,"X",0)``), so whitespace is stripped and case folded before
    comparing desired against current. The user's original string is always the
    one emitted in DDL.
    """
    return "".join(function.split()).lower()


def mask_changes(
    existing_masks: Sequence[Dict[str, str]],
    desired: Dict[str, str],
    index_key_columns: Set[str],
    existing_columns: Optional[Sequence[str]] = None,
) -> Dict[str, list]:
    """Diff the desired mask map against current ``sys.masked_columns`` state.

    ``existing_masks`` is a sequence of ``{"name", "masking_function"}`` rows.
    ``existing_columns``, when given, is every column of the built relation; any
    desired mask naming a column that is not present is skipped (a typo or a
    stale rename) and reported in ``skipped`` rather than emitted as DDL that
    would fail. Passing ``None`` disables that check.

    Returns lists keyed ``adds`` / ``changes`` / ``drops`` / ``skipped`` /
    ``errors``:

    * ``adds`` – ``[(column, function)]`` masked in config, not yet in the DB
      (``ALTER COLUMN ... ADD MASKED``);
    * ``changes`` – ``[(column, function)]`` masked in the DB with a different
      function (``ALTER COLUMN ... MASKED WITH``, no ``ADD``);
    * ``drops`` – ``[column]`` masked in the DB but no longer in config
      (``ALTER COLUMN ... DROP MASKED``);
    * ``skipped`` – warning strings for desired columns absent from the relation;
    * ``errors`` – ``ADD`` targeting a column that is currently an index key,
      which SQL Server rejects with a dependency error (documented for all
      versions 2016+). Only *adds* hit this: re-specifying the function on an
      already-masked index-key column is fine. On fresh builds masks are applied
      before indexes exist, so ``index_key_columns`` is empty there and this
      never triggers; it only guards the persisted path where an index already
      references the column.
    """
    existing_by_name = {
        _normalize_name(row["name"]): row["masking_function"] for row in existing_masks
    }
    desired_by_name = {_normalize_name(col): (col, fn) for col, fn in desired.items()}
    index_keys = {_normalize_name(c) for c in index_key_columns}
    known_columns = (
        {_normalize_name(c) for c in existing_columns} if existing_columns is not None else None
    )

    adds: List[Tuple[str, str]] = []
    changes: List[Tuple[str, str]] = []
    drops: List[str] = []
    skipped: List[str] = []
    errors: List[str] = []

    for norm, (col, fn) in desired_by_name.items():
        if known_columns is not None and norm not in known_columns:
            skipped.append(
                f"Column '{col}' is configured for masking but is not a column of "
                f"the built relation; skipping. Check for a typo or a renamed column."
            )
            continue
        if norm not in existing_by_name:
            if norm in index_keys:
                errors.append(
                    f"Column '{col}' is configured for masking but is also an index "
                    f"key column. SQL Server cannot add a mask to a column that an "
                    f"index depends on. Remove the mask, or remove the column from the "
                    f"index (drop the index, apply the mask, then recreate the index)."
                )
                continue
            adds.append((col, fn))
        elif _normalize_function(existing_by_name[norm]) != _normalize_function(fn):
            changes.append((col, fn))

    for norm, current_fn in existing_by_name.items():
        if norm not in desired_by_name:
            # preserve the DB's spelling of the column name for the DROP
            drops.append(
                next(row["name"] for row in existing_masks if _normalize_name(row["name"]) == norm)
            )

    return {
        "adds": adds,
        "changes": changes,
        "drops": drops,
        "skipped": skipped,
        "errors": errors,
    }
