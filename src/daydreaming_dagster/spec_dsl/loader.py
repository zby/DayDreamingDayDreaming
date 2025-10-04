"""Spec loader for the experiment DSL."""

from __future__ import annotations

import csv
import io
import json
from collections import OrderedDict
from pathlib import Path
from typing import Any, Iterable, Mapping, MutableMapping, Sequence

from daydreaming_dagster.spec_dsl.errors import SpecDslError, SpecDslErrorCode
from daydreaming_dagster.spec_dsl.models import AxisSpec, ExperimentSpec

try:  # Optional dependency for YAML specs
    import yaml  # type: ignore
except Exception:  # pragma: no cover - optional import guard
    yaml = None


class _UnsupportedSpecFormat(SpecDslError):
    def __init__(self, path: Path) -> None:  # pragma: no cover - defensive
        super().__init__(SpecDslErrorCode.INVALID_SPEC, ctx={"path": str(path)})


def _load_mapping(data: Any, *, path: Path) -> Mapping[str, Any]:
    if isinstance(data, Mapping):
        return data
    raise SpecDslError(
        SpecDslErrorCode.INVALID_SPEC,
        ctx={"path": str(path), "error": "top-level must be mapping"},
    )


def _parse_file(path: Path) -> Mapping[str, Any]:
    suffix = path.suffix.lower()
    raw = path.read_text(encoding="utf-8")
    if suffix in {".yaml", ".yml"}:
        if not yaml:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(path), "error": "PyYAML missing"},
            )
        data = yaml.safe_load(raw)  # type: ignore[no-any-unimported]
        return _load_mapping(data or {}, path=path)
    if suffix == ".json":
        data = json.loads(raw)
        return _load_mapping(data, path=path)
    raise _UnsupportedSpecFormat(path)


def parse_spec_mapping(
    data: Mapping[str, Any],
    *,
    source: Path | str | None = None,
    base_dir: Path | None = None,
) -> ExperimentSpec:
    """Parse an already-loaded spec mapping into an :class:`ExperimentSpec`."""

    config_path = Path(source) if source is not None else Path("<memory>")
    mapping = _load_mapping(data, path=config_path)

    if base_dir is None:
        candidate = config_path if config_path.is_dir() else config_path.parent
        base_dir = candidate or Path(".")

    return _build_experiment_spec(mapping, config_path=config_path, base_dir=base_dir)


def load_spec(path: Path | str) -> ExperimentSpec:
    """Load a spec file into an :class:`ExperimentSpec`."""

    spec_path = Path(path)
    if spec_path.is_dir():
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"path": str(spec_path), "error": "spec directory bundles deprecated"},
        )

    data = _parse_file(spec_path)
    return _build_experiment_spec(
        data,
        config_path=spec_path,
        base_dir=spec_path if spec_path.is_dir() else spec_path.parent,
    )


def _build_experiment_spec(
    data: Mapping[str, Any],
    *,
    config_path: Path,
    base_dir: Path,
) -> ExperimentSpec:
    base_dir = Path(base_dir)
    axes_section = data.get("axes", {})
    if not isinstance(axes_section, Mapping):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"path": str(config_path), "error": "axes must be mapping"},
        )

    axes: OrderedDict[str, AxisSpec] = OrderedDict()
    derived_axes: set[str] = set()
    explicit_axes: set[str] = set(axes_section.keys())
    for name, axis_payload in axes_section.items():
        levels = _parse_axis_entry(
            name=name,
            payload=axis_payload,
            config_path=config_path,
            root_dir=base_dir,
        )
        axes[name] = AxisSpec(name=name, levels=tuple(levels))

    raw_rules = data.get("rules", {})
    rules = _parse_rules(
        raw_rules,
        config_path=config_path,
        base_dir=base_dir,
    )

    tuple_axis_levels = _derive_tuple_axes(
        rules,
        axes=axes,
        derived_axes=derived_axes,
        explicit_axes=explicit_axes,
        config_path=config_path,
    )

    for axis_name, levels in tuple_axis_levels.items():
        axes[axis_name] = AxisSpec(name=axis_name, levels=tuple(levels))

    for axis_name in derived_axes:
        if axis_name not in tuple_axis_levels:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={
                    "path": str(config_path),
                    "axis": axis_name,
                    "error": "axis marked as derived but not provided by tuple",
                },
            )

    output = data.get("output", {})
    if not isinstance(output, Mapping):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"path": str(config_path), "error": "output must be mapping"},
        )

    field_order = output.get("field_order")
    if not isinstance(field_order, list):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"path": str(config_path), "error": "output.field_order required"},
        )

    valid_fields = set(axes.keys())
    missing_fields = [field for field in field_order if field not in valid_fields]
    if missing_fields:
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={
                "path": str(config_path),
                "error": "output.field_order references undefined fields",
                "missing": tuple(missing_fields),
            },
        )

    return ExperimentSpec(
        axes=axes,
        rules=tuple(rules),
        output=dict(output),
    )
def _parse_axis_entry(
    *,
    name: str,
    payload: Any,
    config_path: Path,
    root_dir: Path,
) -> list[Any]:
    if isinstance(payload, str) and payload.startswith("@file:"):
        levels = _load_inline_payload((root_dir / payload.removeprefix("@file:")).resolve())
    elif isinstance(payload, list):
        levels = list(payload)
    else:
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={
                "path": str(config_path),
                "axis": name,
                "error": "axis must be list or '@file:' string",
            },
        )

    if not isinstance(levels, list):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={
                "path": str(config_path),
                "axis": name,
                "error": "axis file must produce list",
            },
        )

    return levels

    raise SpecDslError(
        SpecDslErrorCode.INVALID_SPEC,
        ctx={"path": str(config_path), "axis": name, "error": "axis entry must be list or mapping"},
    )


def _resolve_file_reference(value: Any, *, base_dir: Path) -> Any:
    if isinstance(value, str) and value.startswith("@file:"):
        path = (base_dir / value.removeprefix("@file:")).resolve()
        return _load_inline_payload(path)
    return value


def _load_inline_payload(path: Path) -> Any:
    suffix = path.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        if not yaml:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"error": "PyYAML missing for @file", "path": str(path)},
            )
        data = yaml.safe_load(path.read_text(encoding="utf-8"))  # type: ignore[no-any-unimported]
        return data or []
    if suffix == ".json":
        return json.loads(path.read_text(encoding="utf-8"))
    if suffix == ".csv":
        buffer = io.StringIO(path.read_text(encoding="utf-8"))
        reader = csv.DictReader(buffer)
        fieldnames = reader.fieldnames
        if not fieldnames:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(path), "error": "@file CSV requires header"},
            )
        header = [name.strip() for name in fieldnames if name and name.strip()]
        if not header:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(path), "error": "@file CSV header cannot be empty"},
            )

        buffer.seek(0)
        reader = csv.DictReader(buffer)

        if len(header) == 1:
            column = header[0]
            values: list[str] = []
            for row in reader:
                raw = row.get(column)
                value = str(raw).strip() if raw is not None else ""
                if value:
                    values.append(value)
            if not values:
                raise SpecDslError(
                    SpecDslErrorCode.INVALID_SPEC,
                    ctx={"path": str(path), "error": "@file CSV requires data rows"},
                )
            return values

        tuples: list[tuple[str, ...]] = []
        for line_idx, row in enumerate(reader, start=2):
            normalized: list[str] = []
            for name in header:
                raw = row.get(name)
                value = str(raw).strip() if raw is not None else ""
                if not value:
                    raise SpecDslError(
                        SpecDslErrorCode.INVALID_SPEC,
                        ctx={
                            "path": str(path),
                            "error": "@file CSV row missing value",
                            "column": name,
                            "row": line_idx,
                        },
                    )
                normalized.append(value)
            tuples.append(tuple(normalized))
        if not tuples:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(path), "error": "@file CSV requires data rows"},
            )
        return tuples
    if suffix == ".txt":
        return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    raise SpecDslError(
        SpecDslErrorCode.INVALID_SPEC,
        ctx={"error": "unsupported @file extension", "path": str(path)},
    )


def _parse_rules(
    section: Any,
    *,
    config_path: Path,
    base_dir: Path,
) -> list[Mapping[str, Any]]:
    if section in (None, {}):
        return []
    if not isinstance(section, Mapping):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"path": str(config_path), "error": "rules must be mapping"},
        )

    rules: list[Mapping[str, Any]] = []
    for key, value in section.items():
        if key == "subsets":
            rules.extend(
                _parse_subset_rules(value, config_path=config_path, base_dir=base_dir)
            )
        elif key == "ties":
            rules.extend(
                _parse_tie_rules(value, config_path=config_path, base_dir=base_dir)
            )
        elif key == "pairs":
            rules.extend(
                _parse_pair_rules(value, config_path=config_path, base_dir=base_dir)
            )
        elif key == "tuples":
            rules.extend(
                _parse_tuple_rules(value, config_path=config_path, base_dir=base_dir)
            )
        else:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "error": f"unsupported rule section '{key}'"},
            )
    return rules


def _parse_subset_rules(
    payload: Any,
    *,
    config_path: Path,
    base_dir: Path,
) -> list[Mapping[str, Any]]:
    if not isinstance(payload, Mapping):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"path": str(config_path), "error": "rules.subsets must be mapping"},
        )

    rules: list[Mapping[str, Any]] = []
    for axis, keep_values in payload.items():
        if not isinstance(axis, str):
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "error": "subset axis must be string"},
            )
        resolved = _resolve_file_reference(keep_values, base_dir=base_dir)
        if not isinstance(resolved, list):
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={
                    "path": str(config_path),
                    "axis": axis,
                    "error": "subset keep must be list",
                },
            )
        rules.append({"subset": {"axis": axis, "keep": resolved}})
    return rules


def _parse_tie_rules(
    payload: Any,
    *,
    config_path: Path,
    base_dir: Path,
) -> list[Mapping[str, Any]]:
    if not isinstance(payload, Mapping):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"path": str(config_path), "error": "rules.ties must be mapping"},
        )

    rules: list[Mapping[str, Any]] = []
    for canonical, spec in payload.items():
        if not isinstance(canonical, str) or not canonical:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "error": "tie name must be non-empty string"},
            )

        axes: Iterable[Any]
        if isinstance(spec, list):
            axes = spec
        elif isinstance(spec, Mapping):
            if "to" in spec:
                raise SpecDslError(
                    SpecDslErrorCode.INVALID_SPEC,
                    ctx={
                        "path": str(config_path),
                        "error": "tie payload must omit 'to'; use mapping key instead",
                    },
                )
            axes = spec.get("axes", [])
        else:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "error": "tie payload must be list or mapping"},
            )

        axes_list = list(axes)
        if not axes_list or not all(isinstance(item, str) for item in axes_list):
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={
                    "path": str(config_path),
                    "error": "tie axes must be list of strings",
                },
            )

        rules.append({"tie": {"axes": axes_list, "to": canonical}})
    return rules


def _parse_pair_rules(
    payload: Any,
    *,
    config_path: Path,
    base_dir: Path,
) -> list[Mapping[str, Any]]:
    if not isinstance(payload, Mapping):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"path": str(config_path), "error": "rules.pairs must be mapping"},
        )

    rules: list[Mapping[str, Any]] = []
    for name, spec in payload.items():
        if not isinstance(name, str) or not name:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "error": "pair name must be non-empty string"},
            )
        if not isinstance(spec, Mapping):
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "pair": name, "error": "pair payload must be mapping"},
            )

        left = spec.get("left")
        right = spec.get("right")
        allowed_raw = spec.get("allowed")
        balance = spec.get("balance")

        if not isinstance(left, str) or not isinstance(right, str):
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "pair": name, "error": "pair.left/right required"},
            )

        allowed_resolved = _resolve_file_reference(allowed_raw, base_dir=base_dir)
        if not isinstance(allowed_resolved, list):
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "pair": name, "error": "pair.allowed must be list"},
            )

        rule_payload: dict[str, Any] = {
            "left": left,
            "right": right,
            "name": name,
            "allowed": allowed_resolved,
        }

        if balance is not None:
            if not isinstance(balance, str):
                raise SpecDslError(
                    SpecDslErrorCode.INVALID_SPEC,
                    ctx={
                        "path": str(config_path),
                        "pair": name,
                        "error": "pair.balance must be string",
                    },
                )
            rule_payload["balance"] = balance

        rules.append({"pair": rule_payload})
    return rules


def _parse_tuple_rules(
    payload: Any,
    *,
    config_path: Path,
    base_dir: Path,
) -> list[Mapping[str, Any]]:
    if not isinstance(payload, Mapping):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"path": str(config_path), "error": "rules.tuples must be mapping"},
        )

    rules: list[Mapping[str, Any]] = []
    for name, spec in payload.items():
        if not isinstance(name, str) or not name:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "error": "tuple name must be non-empty string"},
            )
        if not isinstance(spec, Mapping):
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "tuple": name, "error": "tuple payload must be mapping"},
            )

        axes = spec.get("axes")
        items_raw = spec.get("items")

        if not isinstance(axes, list) or not all(isinstance(a, str) for a in axes):
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "tuple": name, "error": "tuple.axes must be list"},
            )

        if "expand" in spec:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "tuple": name, "error": "tuple.expand deprecated"},
            )

        items_resolved = _resolve_file_reference(items_raw, base_dir=base_dir)
        if not isinstance(items_resolved, list):
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "tuple": name, "error": "tuple.items must be list"},
            )

        rule_payload: dict[str, Any] = {
            "name": name,
            "axes": axes,
            "items": items_resolved,
        }

        rules.append({"tuple": rule_payload})
    return rules


def _derive_tuple_axes(
    rules: Sequence[Mapping[str, Any]],
    *,
    axes: MutableMapping[str, AxisSpec],
    derived_axes: set[str],
    explicit_axes: set[str],
    config_path: Path,
) -> dict[str, list[Any]]:
    if not rules:
        return {}

    tuple_axis_levels: dict[str, list[Any]] = {}

    for rule in rules:
        tuple_spec = rule.get("tuple")  # type: ignore[assignment]
        if not tuple_spec:
            continue

        axes_list = tuple_spec.get("axes")
        items = tuple_spec.get("items")

        if not isinstance(axes_list, list) or not all(isinstance(a, str) for a in axes_list):
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "error": "tuple axes must be list of strings"},
            )

        if not isinstance(items, list) or not items:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "error": "tuple items must be non-empty list"},
            )

        arity = len(axes_list)

        for axis_name in axes_list:
            if axis_name not in axes:
                axes[axis_name] = AxisSpec(name=axis_name, levels=())
                derived_axes.add(axis_name)
            if axis_name in explicit_axes:
                raise SpecDslError(
                    SpecDslErrorCode.INVALID_SPEC,
                    ctx={
                        "path": str(config_path),
                        "axis": axis_name,
                        "error": "tuple axis may not provide explicit levels",
                    },
                )

        for entry in items:
            if not isinstance(entry, (list, tuple)) or len(entry) != arity:
                raise SpecDslError(
                    SpecDslErrorCode.INVALID_SPEC,
                    ctx={
                        "path": str(config_path),
                        "error": "tuple item arity mismatch",
                        "tuple_axes": tuple(axes_list),
                        "item": entry,
                    },
                )
            for idx, axis_name in enumerate(axes_list):
                value = entry[idx]
                axis_values = tuple_axis_levels.setdefault(axis_name, [])
                if value not in axis_values:
                    axis_values.append(value)

    # Ensure every derived axis picked up levels
    for axis_name in derived_axes:
        levels = tuple_axis_levels.get(axis_name)
        if not levels:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={
                    "path": str(config_path),
                    "axis": axis_name,
                    "error": "tuple axis must provide at least one value",
                },
            )

    return tuple_axis_levels
