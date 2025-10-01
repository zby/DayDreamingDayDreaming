"""Spec loader for the experiment DSL."""

from __future__ import annotations

import csv
import json
from collections import OrderedDict
from pathlib import Path
from typing import Any, Mapping

from daydreaming_dagster.spec_dsl.errors import SpecDslError, SpecDslErrorCode
from daydreaming_dagster.spec_dsl.models import AxisSpec, ExperimentSpec, ReplicateSpec

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


def load_spec(path: Path | str) -> ExperimentSpec:
    """Load a spec file into an :class:`ExperimentSpec`."""

    spec_path = Path(path)
    if spec_path.is_dir():
        data = _load_directory(spec_path)
    else:
        data = _parse_file(spec_path)

    axes_section = data.get("axes", {})
    if not isinstance(axes_section, Mapping):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"path": str(spec_path), "error": "axes must be mapping"},
        )

    base_dir = spec_path if spec_path.is_dir() else spec_path.parent

    axes: OrderedDict[str, AxisSpec] = OrderedDict()
    for name, axis_payload in axes_section.items():
        levels, catalog_meta = _parse_axis_entry(
            name=name,
            payload=axis_payload,
            config_path=spec_path,
            root_dir=base_dir,
        )
        axes[name] = AxisSpec(name=name, levels=tuple(levels), catalog_lookup=catalog_meta)

    raw_rules = data.get("rules", [])
    if not isinstance(raw_rules, list):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"path": str(spec_path), "error": "rules must be list"},
        )
    rules: list[Mapping[str, Any]] = []
    for rule in raw_rules:
        if not isinstance(rule, Mapping):
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(spec_path), "error": "each rule must be mapping"},
            )
        rules.append(_maybe_load_file(rule, base_dir=base_dir))

    output = data.get("output", {})
    if not isinstance(output, Mapping):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"path": str(spec_path), "error": "output must be mapping"},
        )

    replicates_section = data.get("replicates", {})
    replicates = _parse_replicates(replicates_section, config_path=spec_path)

    return ExperimentSpec(
        axes=axes,
        rules=tuple(rules),
        output=dict(output),
        replicates=replicates,
    )


def _load_directory(root: Path) -> Mapping[str, Any]:
    config_file = root / "config.yaml"
    if not config_file.exists():
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"path": str(root), "error": "missing config.yaml"},
        )

    data = dict(_parse_file(config_file))

    inline_axes = data.setdefault("axes", {})
    rules = list(data.setdefault("rules", []))

    axes_dir = root / "axes"
    if axes_dir.exists():
        for axis_file in sorted(axes_dir.glob("*.txt")):
            axis_name = axis_file.stem
            levels = [line.strip() for line in axis_file.read_text(encoding="utf-8").splitlines() if line.strip()]
            inline_axes[axis_name] = {"levels": levels}

    rules_dir = root / "rules"
    if rules_dir.exists():
        for rule_file in sorted(rules_dir.glob("*.yaml")):
            rules.append(_parse_file(rule_file))

    data["rules"] = rules
    return data


def _parse_axis_entry(
    *,
    name: str,
    payload: Any,
    config_path: Path,
    root_dir: Path,
) -> tuple[list[Any], Mapping[str, Any] | None]:
    if isinstance(payload, list):
        return list(payload), None

    if isinstance(payload, dict):
        if "levels" not in payload:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "axis": name, "error": "axis missing levels"},
            )
        levels = payload["levels"]
        if isinstance(levels, str) and levels.startswith("@file:"):
            levels = _maybe_load_file(levels, base_dir=root_dir)
        elif isinstance(levels, list):
            levels = _maybe_load_file(levels, base_dir=root_dir)
        else:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "axis": name, "error": "levels must be list"},
            )

        if not isinstance(levels, list):
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "axis": name, "error": "levels file must yield list"},
            )

        catalog_meta = payload.get("catalog_lookup")
        if catalog_meta is not None and not isinstance(catalog_meta, dict):
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "axis": name, "error": "catalog_lookup must be mapping"},
            )
        return list(levels), catalog_meta

    raise SpecDslError(
        SpecDslErrorCode.INVALID_SPEC,
        ctx={"path": str(config_path), "axis": name, "error": "axis entry must be list or mapping"},
    )


def _parse_replicates(
    section: Any,
    *,
    config_path: Path,
) -> "OrderedDict[str, ReplicateSpec]":
    if section in (None, {}):
        return OrderedDict()
    if not isinstance(section, Mapping):
        raise SpecDslError(
            SpecDslErrorCode.INVALID_SPEC,
            ctx={"path": str(config_path), "error": "replicates must be mapping"},
        )

    replicates: "OrderedDict[str, ReplicateSpec]" = OrderedDict()
    for axis, payload in section.items():
        if not isinstance(axis, str):
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "error": "replicate axis must be string"},
            )
        if not isinstance(payload, Mapping):
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "axis": axis, "error": "replicate entry must be mapping"},
            )
        count = payload.get("count")
        if not isinstance(count, int) or count < 1:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "axis": axis, "error": "replicate count must be >=1"},
            )
        column = payload.get("column")
        if column is None:
            column = f"{axis}_replicate"
        if not isinstance(column, str) or not column:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"path": str(config_path), "axis": axis, "error": "replicate column must be non-empty string"},
            )
        replicates[axis] = ReplicateSpec(axis=axis, count=count, column=column)

    return replicates


def _maybe_load_file(value: Any, *, base_dir: Path) -> Any:
    if isinstance(value, str) and value.startswith("@file:"):
        path = (base_dir / value.removeprefix("@file:")).resolve()
        return _load_inline_payload(path)
    if isinstance(value, list):
        return [
            x if not (isinstance(x, str) and x.startswith("@file:"))
            else _maybe_load_file(x, base_dir=base_dir)
            for x in value
        ]
    if isinstance(value, Mapping):
        return {
            key: _maybe_load_file(val, base_dir=base_dir)
            for key, val in value.items()
        }
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
        rows: list[list[str]] = []
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            for row in reader:
                cleaned = [cell.strip() for cell in row if cell.strip()]
                if cleaned:
                    rows.append(cleaned)
        return rows
    if suffix == ".txt":
        return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    raise SpecDslError(
        SpecDslErrorCode.INVALID_SPEC,
        ctx={"error": "unsupported @file extension", "path": str(path)},
    )
