"""Initial compilation pipeline for the experiment DSL."""

from __future__ import annotations

from collections import OrderedDict
from itertools import product
from random import Random
from typing import Any, Iterable, Sequence

from daydreaming_dagster.spec_dsl.errors import SpecDslError, SpecDslErrorCode
from daydreaming_dagster.spec_dsl.models import ExperimentSpec


def _dedupe(seq: Iterable[Any]) -> list[Any]:
    seen: set[Any] = set()
    ordered: list[Any] = []
    for item in seq:
        if item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def compile_design(
    spec: ExperimentSpec,
    *,
    seed: int | None = None,
) -> list[OrderedDict[str, Any]]:
    """Compile a spec into a deterministic list of rows.

    The initial implementation strips duplicate axis levels and emits the
    raw Cartesian product. Rule handling arrives in subsequent steps.
    """

    axis_order = list(spec.axes.keys())
    axis_levels = {name: _dedupe(spec.axes[name].levels) for name in axis_order}
    tie_back: dict[str, str] = {}
    pair_meta: dict[str, tuple[str, str]] = {}
    tuple_meta: dict[str, dict[str, Any]] = {}

    field_order = spec.output.get("field_order")

    for rule in spec.rules:
        _apply_rule(rule, axis_levels, axis_order, tie_back, pair_meta, tuple_meta)

    ordered_axes: list[Sequence[Any]] = [axis_levels[name] for name in axis_order]

    rows: list[OrderedDict[str, Any]] = []
    cartesian = list(product(*ordered_axes))
    if seed is not None:
        Random(seed).shuffle(cartesian)

    for combo in cartesian:
        base_row = OrderedDict(zip(axis_order, combo, strict=False))
        expanded_rows = _finalize_row(
            base_row,
            tie_back=tie_back,
            pair_meta=pair_meta,
            tuple_meta=tuple_meta,
            field_order=field_order,
        )
        rows.extend(expanded_rows)
    return rows
def _apply_rule(
    rule: Any,
    axes: dict[str, list[Any]],
    axis_order: list[str],
    tie_back: dict[str, str],
    pair_meta: dict[str, tuple[str, str]],
    tuple_meta: dict[str, dict[str, Any]],
) -> None:
    if not isinstance(rule, dict):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"error": "rules must be mapping"},
        )

    if "subset" in rule:
        _apply_subset(rule["subset"], axes)
    elif "tie" in rule:
        _apply_tie(rule["tie"], axes, axis_order, tie_back)
    elif "pair" in rule:
        _apply_pair(rule["pair"], axes, axis_order, tie_back, pair_meta)
    elif "tuple" in rule:
        _apply_tuple(rule["tuple"], axes, axis_order, tie_back, tuple_meta)
    elif rule:
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"error": f"unsupported rule {list(rule)}"},
        )


def _apply_subset(spec: Any, axes: dict[str, list[Any]]) -> None:
    if not isinstance(spec, dict):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"error": "subset rule must be mapping"},
        )

    axis = spec.get("axis")
    keep = spec.get("keep")

    if not isinstance(axis, str):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"error": "subset.axis must be string"},
        )
    if axis not in axes:
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"error": "subset axis missing", "axis": axis},
        )
    if not isinstance(keep, list):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"error": "subset.keep must be list", "axis": axis},
        )

    keep_set = set(keep)
    filtered = [value for value in axes[axis] if value in keep_set]
    if not filtered:
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"error": "subset removed all levels", "axis": axis},
        )
    axes[axis] = filtered


def _apply_tie(
    spec: Any,
    axes: dict[str, list[Any]],
    axis_order: list[str],
    tie_back: dict[str, str],
) -> None:
    if not isinstance(spec, dict):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"error": "tie rule must be mapping"},
        )

    names = spec.get("axes")
    if not isinstance(names, list) or not names:
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"error": "tie.axes must be non-empty list"},
        )

    for name in names:
        if not isinstance(name, str):
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"error": "tie axes must be strings"},
            )
        if name not in axes:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"error": "tie axis missing", "axis": name},
            )

    canonical = spec.get("to", names[0])
    if not isinstance(canonical, str):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"error": "tie.to must be string"},
        )

    base_levels = axes[names[0]]
    other_levels = [set(axes[name]) for name in names[1:]]
    if other_levels:
        intersection = [value for value in base_levels if all(value in s for s in other_levels)]
    else:
        intersection = list(base_levels)

    if not intersection:
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"error": "tie produced empty intersection", "axes": tuple(names)},
        )

    axes[canonical] = intersection

    first_index_candidates = [axis_order.index(name) for name in names if name in axis_order]
    first_index = min(first_index_candidates) if first_index_candidates else 0

    if canonical not in axis_order:
        axis_order.insert(first_index, canonical)

    tie_back.setdefault(canonical, canonical)

    for name in names:
        tie_back[name] = canonical
        if name == canonical:
            continue
        axes.pop(name, None)
        if name in axis_order:
            axis_order.remove(name)


def _apply_pair(
    spec: Any,
    axes: dict[str, list[Any]],
    axis_order: list[str],
    tie_back: dict[str, str],
    pair_meta: dict[str, tuple[str, str]],
) -> None:
    if not isinstance(spec, dict):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"error": "pair rule must be mapping"},
        )

    left_raw = spec.get("left")
    right_raw = spec.get("right")
    name = spec.get("name")
    allowed = spec.get("allowed")
    balance = spec.get("balance")

    if not all(isinstance(item, str) for item in (left_raw, right_raw, name)):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"error": "pair.left/right/name must be strings"},
        )

    if left_raw == right_raw:
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"error": "pair left/right must differ", "axis": left_raw},
        )

    if not isinstance(allowed, list) or not allowed:
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"error": "pair.allowed must be non-empty list"},
        )

    left_resolved = tie_back.get(left_raw, left_raw)
    right_resolved = tie_back.get(right_raw, right_raw)

    for resolved_name in (left_resolved, right_resolved):
        if resolved_name not in axes:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"error": "pair axis missing", "axis": resolved_name},
            )

    left_domain = set(axes[left_resolved])
    right_domain = set(axes[right_resolved])

    canonical_pairs: list[tuple[Any, Any]] = []
    for entry in allowed:
        if not isinstance(entry, (list, tuple)) or len(entry) != 2:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"error": "pair.allowed entries must be length-2 sequences"},
            )
        left_val, right_val = entry
        if isinstance(left_val, list):
            left_val = tuple(left_val)
        if isinstance(right_val, list):
            right_val = tuple(right_val)
        if left_val not in left_domain or right_val not in right_domain:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"error": "pair value outside domain", "pair": (left_val, right_val)},
            )
        if (left_val, right_val) not in canonical_pairs:
            canonical_pairs.append((left_val, right_val))

    if balance:
        if balance not in {"left", "right", "both"}:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"error": "pair.balance must be left/right/both"},
            )
        if balance in {"left", "both"}:
            _enforce_balance(canonical_pairs, side="left")
        if balance in {"right", "both"}:
            _enforce_balance(canonical_pairs, side="right")

    axes[name] = canonical_pairs
    pair_meta[name] = (left_raw, right_raw)

    left_index = axis_order.index(left_resolved)
    right_index = axis_order.index(right_resolved)
    insert_at = min(left_index, right_index)

    for resolved_name in {left_resolved, right_resolved}:
        axes.pop(resolved_name, None)
        if resolved_name in axis_order:
            axis_order.remove(resolved_name)

    axis_order.insert(insert_at, name)


def _enforce_balance(pairs: list[tuple[Any, Any]], *, side: str) -> None:
    counts: dict[Any, int] = {}
    idx = 0 if side == "left" else 1
    for pair in pairs:
        value = pair[idx]
        counts[value] = counts.get(value, 0) + 1

    if len(set(counts.values())) > 1:
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={
                "error": f"pair imbalance on {side}",
                "counts": tuple(sorted(counts.items())),
            },
        )


def _apply_tuple(
    spec: Any,
    axes: dict[str, list[Any]],
    axis_order: list[str],
    tie_back: dict[str, str],
    tuple_meta: dict[str, dict[str, Any]],
) -> None:
    if not isinstance(spec, dict):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"error": "tuple rule must be mapping"},
        )

    name = spec.get("name")
    axes_list = spec.get("axes")
    items = spec.get("items")

    if not isinstance(name, str):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"error": "tuple.name must be string"},
        )
    if not isinstance(axes_list, list) or not axes_list:
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"error": "tuple.axes must be non-empty list"},
        )
    if not isinstance(items, list) or not items:
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"error": "tuple.items must be non-empty list"},
        )
    if "expand" in spec:
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"error": "tuple.expand deprecated"},
        )

    for axis_name in axes_list:
        if not isinstance(axis_name, str):
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"error": "tuple axes must be strings"},
            )
        resolved = tie_back.get(axis_name, axis_name)
        if resolved not in axes:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"error": "tuple axis missing", "axis": axis_name},
            )

    domains = [set(axes[tie_back.get(a, a)]) for a in axes_list]

    canonical_items: list[tuple[Any, ...]] = []
    for entry in items:
        if not isinstance(entry, (list, tuple)) or len(entry) != len(axes_list):
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"error": "tuple items must match axes length", "item": entry},
            )
        tuple_entry = tuple(entry)
        for idx, value in enumerate(tuple_entry):
            if value not in domains[idx]:
                raise SpecDslError(
                    SpecDslErrorCode.INVALID_SPEC,
                    ctx={
                        "error": "tuple value outside domain",
                        "axis": axes_list[idx],
                        "value": value,
                    },
                )
        if tuple_entry not in canonical_items:
            canonical_items.append(tuple_entry)

    insert_at = min(axis_order.index(tie_back.get(a, a)) for a in axes_list)

    axes[name] = canonical_items
    axis_order.insert(insert_at, name)
    tuple_meta[name] = {"axes": list(axes_list)}

    for axis_name in axes_list:
        resolved = tie_back.get(axis_name, axis_name)
        axes.pop(resolved, None)
        if resolved in axis_order:
            axis_order.remove(resolved)


def _finalize_row(
    row: OrderedDict[str, Any],
    *,
    tie_back: dict[str, str],
    pair_meta: dict[str, tuple[str, str]],
    tuple_meta: dict[str, dict[str, Any]],
    field_order: Sequence[Any] | None,
) -> list[OrderedDict[str, Any]]:
    _expand_pairs(row, pair_meta)
    _expand_tuples(row, tuple_meta)
    _expand_ties(row, tie_back)

    rows = [row]

    if isinstance(field_order, list):
        for r in rows:
            ordered = OrderedDict()
            for field in field_order:
                if field in r:
                    ordered[field] = r[field]
            for key, value in r.items():
                if key not in ordered:
                    ordered[key] = value
            r.clear()
            r.update(ordered)
    return rows


def _expand_pairs(row: OrderedDict[str, Any], pair_meta: dict[str, tuple[str, str]]) -> None:
    if not pair_meta:
        return
    to_delete: list[str] = []
    for axis_name, (left_raw, right_raw) in pair_meta.items():
        if axis_name not in row:
            continue
        pair_value = row[axis_name]
        if not isinstance(pair_value, (list, tuple)) or len(pair_value) != 2:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"error": "pair axis value must be 2-tuple", "axis": axis_name},
            )
        left_val, right_val = pair_value
        row[left_raw] = left_val
        row[right_raw] = right_val
        to_delete.append(axis_name)
    for axis_name in to_delete:
        row.pop(axis_name, None)


def _expand_tuples(row: OrderedDict[str, Any], tuple_meta: dict[str, dict[str, Any]]) -> None:
    if not tuple_meta:
        return
    to_delete: list[str] = []
    for axis_name, meta in tuple_meta.items():
        if axis_name not in row:
            continue
        tuple_value = row[axis_name]
        if not isinstance(tuple_value, (list, tuple)) or len(tuple_value) != len(meta["axes"]):
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"error": "tuple axis value has wrong arity", "axis": axis_name},
            )
        for sub_axis, value in zip(meta["axes"], tuple_value, strict=False):
            row[sub_axis] = value
        to_delete.append(axis_name)
    for axis_name in to_delete:
        row.pop(axis_name, None)


def _expand_ties(row: OrderedDict[str, Any], tie_back: dict[str, str]) -> None:
    for original, canonical in tie_back.items():
        if original == canonical:
            continue
        if canonical in row and original not in row:
            row[original] = row[canonical]
