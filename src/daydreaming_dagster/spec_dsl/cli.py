"""Command-line entry point for compiling experiment specs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from daydreaming_dagster.data_layer.paths import Paths
from daydreaming_dagster.spec_dsl import compile_design, load_spec
from daydreaming_dagster.spec_dsl.errors import SpecDslError, SpecDslErrorCode


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compile experiment DSL specs")
    parser.add_argument("spec", help="Path to spec file or directory")
    parser.add_argument("--out", help="Optional output path (csv or jsonl)")
    parser.add_argument("--format", choices={"csv", "jsonl"}, help="Output format override")
    parser.add_argument("--seed", type=int, help="Optional shuffle seed for deterministic shuffling")
    parser.add_argument("--limit", type=int, help="Maximum rows to emit to stdout")
    parser.add_argument(
        "--catalog",
        action="append",
        default=None,
        help="Path to JSON file containing catalog mappings (may be repeated)",
    )
    parser.add_argument(
        "--catalog-csv",
        action="append",
        default=None,
        metavar="NAME=PATH[:COLUMN]",
        help="Load catalog levels from CSV for catalog NAME (default column 'id')",
    )
    parser.add_argument(
        "--data-root",
        help="Optional data root for resolving known catalog CSV shortcuts",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    spec = load_spec(args.spec)
    catalogs = _load_catalogs(args.catalog, args.catalog_csv, args.data_root)
    rows = compile_design(spec, catalogs=catalogs, seed=args.seed)

    if args.out:
        out_path = Path(args.out)
        fmt = args.format or out_path.suffix.lstrip(".") or "csv"
        _write_rows(rows, out_path, fmt)
    else:
        limit = args.limit or len(rows)
        for row in rows[:limit]:
            print(row)
        if limit < len(rows):
            print(f"... truncated {len(rows) - limit} rows")

    return 0


def _write_rows(rows, out: Path, fmt: str) -> None:
    if fmt == "jsonl":
        import json

        with out.open("w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row) + "\n")
        return

    if fmt == "csv":
        import csv

        fieldnames = sorted({field for row in rows for field in row.keys()})
        with out.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return

    raise ValueError(f"Unsupported format: {fmt}")


def _load_catalogs(
    json_paths: list[str] | None,
    csv_specs: list[str] | None,
    data_root: str | None,
):
    catalogs: dict[str, set[str]] = {}

    def ensure_catalog(name: str) -> set[str]:
        return catalogs.setdefault(name, set())

    if json_paths:
        for path_str in json_paths:
            path = Path(path_str)
            data = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                raise SpecDslError(
                    SpecDslErrorCode.INVALID_SPEC,
                    ctx={"error": "catalog file must contain object", "path": str(path)},
                )
            for key, value in data.items():
                if isinstance(value, dict):
                    ensure_catalog(key).update(str(v) for v in value.keys())
                elif isinstance(value, (list, tuple, set)):
                    ensure_catalog(key).update(str(v) for v in value)
                else:
                    raise SpecDslError(
                        SpecDslErrorCode.INVALID_SPEC,
                        ctx={"error": "catalog entries must be list/set or mapping", "catalog": key},
                    )

    paths_helper = Paths.from_str(data_root) if data_root else None

    if csv_specs:
        import csv

        for spec in csv_specs:
            if "=" not in spec:
                raise SpecDslError(
                    SpecDslErrorCode.INVALID_SPEC,
                    ctx={"error": "catalog-csv must be NAME=PATH[:COLUMN]", "value": spec},
                )
            name, remainder = spec.split("=", 1)
            if not name:
                raise SpecDslError(
                    SpecDslErrorCode.INVALID_SPEC,
                    ctx={"error": "catalog name missing", "value": spec},
                )
            if ":" in remainder:
                path_part, column = remainder.split(":", 1)
            else:
                path_part, column = remainder, "id"

            csv_path = _resolve_catalog_csv_path(path_part, paths_helper)
            with csv_path.open("r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                if column not in reader.fieldnames:
                    raise SpecDslError(
                        SpecDslErrorCode.INVALID_SPEC,
                        ctx={
                            "error": "catalog column missing",
                            "path": str(csv_path),
                            "column": column,
                        },
                    )
                values = [row[column] for row in reader if row.get(column)]
                ensure_catalog(name).update(values)

    if catalogs:
        return {k: sorted(v) for k, v in catalogs.items()}
    return None


def _resolve_catalog_csv_path(path_part: str, paths_helper: Paths | None) -> Path:
    if path_part.startswith("@"):
        if paths_helper is None:
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"error": "@syntax requires --data-root", "value": path_part},
            )
        attr = path_part[1:]
        if not hasattr(paths_helper, attr):
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"error": "unknown Paths attribute", "attribute": attr},
            )
        resolved = getattr(paths_helper, attr)
        if callable(resolved):
            resolved = resolved()
        if not isinstance(resolved, Path):
            raise SpecDslError(
                SpecDslErrorCode.INVALID_SPEC,
                ctx={"error": "Paths attribute did not resolve to Path", "attribute": attr},
            )
        return resolved

    candidate = Path(path_part)
    if candidate.is_absolute() or paths_helper is None:
        return candidate
    return paths_helper.data_root / candidate


def entrypoint() -> None:  # pragma: no cover - console entry
    try:
        raise SystemExit(main())
    except SpecDslError as exc:  # pragma: no cover - console behavior
        raise SystemExit(f"error: {exc}")
