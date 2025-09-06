#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


PREFIX = "[duplicate_same_content]"


@dataclass
class DupEntry:
    line_no: int
    hash_prefix: str | None
    src: str | None
    row: int | None
    task: str | None
    file: Path
    first_src: str | None
    first_row: int | None
    first_task: str | None
    first_file: Path


def parse_line(line: str, line_no: int) -> DupEntry | None:
    if not line.startswith(PREFIX):
        return None
    # Simple key=value tokenizer separated by spaces
    parts = line.strip().split()
    kv = {}
    for p in parts[1:]:  # skip prefix
        if "=" in p:
            k, v = p.split("=", 1)
            kv[k] = v
    try:
        return DupEntry(
            line_no=line_no,
            hash_prefix=kv.get("hash"),
            src=kv.get("src"),
            row=int(kv["row"]) if kv.get("row") else None,
            task=kv.get("task"),
            file=Path(kv.get("file", "")),
            first_src=kv.get("first_src"),
            first_row=int(kv["first_row"]) if kv.get("first_row") else None,
            first_task=kv.get("first_task"),
            first_file=Path(kv.get("first_file", "")),
        )
    except Exception:
        return None


def sha256_path(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def load_entries(log_path: Path) -> list[DupEntry]:
    entries: list[DupEntry] = []
    with log_path.open("r", encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f, start=1):
            e = parse_line(line, i)
            if e is not None:
                entries.append(e)
    return entries


def unique_pairs(entries: Iterable[DupEntry]) -> list[DupEntry]:
    seen: set[tuple[str, str]] = set()
    uniq: list[DupEntry] = []
    for e in entries:
        # Normalize order: always (first_file, file)
        a = str(e.first_file)
        b = str(e.file)
        key = (a, b)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(e)
    return uniq


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Verify true duplicates from backfill stderr log")
    p.add_argument("log", type=Path, help="Path to stderr log produced by backfill")
    p.add_argument("--root", type=Path, default=Path("."), help="Resolve relative paths against this root (default: CWD)")
    p.add_argument("--all", action="store_true", help="Check all occurrences (default deduplicates pairs)")
    p.add_argument("--list-mismatches", action="store_true", help="Print details of mismatches and missing files")
    args = p.parse_args(argv)

    entries = load_entries(args.log)
    total_lines = len(entries)
    if not args.all:
        entries = unique_pairs(entries)

    checked = 0
    identical = 0
    mismatched = 0
    missing = 0
    mismatch_details: list[str] = []
    missing_details: list[str] = []

    for e in entries:
        a = (args.root / e.first_file).resolve() if not e.first_file.is_absolute() else e.first_file
        b = (args.root / e.file).resolve() if not e.file.is_absolute() else e.file
        # Existence
        if not a.exists() or not b.exists():
            missing += 1
            missing_details.append(f"missing: line {e.line_no} first_file={a} exists={a.exists()} file={b} exists={b.exists()}")
            continue
        # Hash comparison
        try:
            ha = sha256_path(a)
            hb = sha256_path(b)
        except Exception as ex:
            missing += 1
            missing_details.append(f"error reading: line {e.line_no} first_file={a} file={b} err={ex}")
            continue
        checked += 1
        if ha == hb:
            identical += 1
        else:
            mismatched += 1
            mismatch_details.append(f"mismatch: line {e.line_no} first_file={a} file={b}")

    print(f"parsed_lines={total_lines}")
    print(f"pairs_checked={checked}")
    print(f"identical={identical}")
    print(f"mismatched={mismatched}")
    print(f"missing_or_errors={missing}")

    if args.list_mismatches and mismatch_details:
        print("-- mismatches --")
        for s in mismatch_details:
            print(s)
    if args.list_mismatches and missing_details:
        print("-- missing --")
        for s in missing_details:
            print(s)

    return 0 if (mismatched == 0 and missing == 0) else 2


if __name__ == "__main__":
    raise SystemExit(main())

