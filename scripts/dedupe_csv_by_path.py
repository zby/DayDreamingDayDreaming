#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable


def normalize_key(v: str, *, lower: bool, strip: bool) -> str:
    if v is None:
        return ""
    s = v
    if strip:
        s = s.strip()
    if lower:
        s = s.lower()
    return s


def dedupe_csv(
    input_csv: Path,
    output_csv: Path,
    key_column: str = "response_file",
    lower: bool = False,
    strip: bool = True,
) -> dict:
    """Remove duplicate rows by a key column, keeping the first occurrence.

    Returns stats: {input_rows, output_rows, dropped}
    """
    if not input_csv.exists():
        raise FileNotFoundError(f"Input not found: {input_csv}")

    with input_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        header = reader.fieldnames or []

    if key_column not in header:
        raise ValueError(f"Key column '{key_column}' not present in {input_csv}")

    seen: set[str] = set()
    out_rows: list[dict] = []

    for r in rows:
        k = normalize_key(r.get(key_column, ""), lower=lower, strip=strip)
        if k and k not in seen:
            seen.add(k)
            out_rows.append(r)
        elif not k:
            # If key missing/empty, keep it (cannot dedupe reliably)
            out_rows.append(r)
        else:
            # duplicate; drop
            pass

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(out_rows)

    return {
        "input_rows": len(rows),
        "output_rows": len(out_rows),
        "dropped": len(rows) - len(out_rows),
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Dedupe a CSV by a path column (keep first occurrence)")
    p.add_argument("--input", type=Path, required=True, help="Input CSV path (e.g., draft_generation_results.csv)")
    p.add_argument("--output", type=Path, required=True, help="Output CSV path for deduped rows")
    p.add_argument("--key", type=str, default="response_file", help="Column name to dedupe on (default: response_file)")
    p.add_argument("--lower", action="store_true", help="Lowercase the key before comparing")
    p.add_argument("--no-strip", action="store_true", help="Do not strip whitespace around the key")
    args = p.parse_args(argv)

    stats = dedupe_csv(
        input_csv=args.input,
        output_csv=args.output,
        key_column=args.key,
        lower=bool(args.lower),
        strip=not bool(args.no_strip),
    )
    print(
        f"deduped '{args.input}' -> '{args.output}' "
        f"(input_rows={stats['input_rows']} output_rows={stats['output_rows']} dropped={stats['dropped']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

