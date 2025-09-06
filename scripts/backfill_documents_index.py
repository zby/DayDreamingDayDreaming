#!/usr/bin/env python3
"""
Backfill script: index legacy draft/essay/evaluation artifacts into SQLite and
organize per-document directories under data/docs/<stage>/<doc_id>/.

This migrates existing files from:
- data/3_generation/draft_responses_raw/   (<draft_task_id>_vN.txt)
- data/3_generation/draft_responses/       (<draft_task_id>.txt)
- data/3_generation/essay_responses_raw/   (<essay_task_id>_vN.txt)
- data/3_generation/essay_responses/       (<essay_task_id>.txt)
- data/4_evaluation/evaluation_responses_raw/   (<evaluation_task_id>_vN.txt)
- data/4_evaluation/evaluation_responses/       (<evaluation_task_id>.txt)

Each backfilled attempt gets a new doc_id and a document directory containing:
- raw.txt (if available)
- parsed.txt (if available)
- metadata.json (larger metadata and provenance)

The SQLite index is stored at data/db/documents.sqlite by default.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


# ---------- Helpers ----------


def _safe_relpath(p: Path, root: Path) -> str:
    try:
        return str(p.relative_to(root))
    except Exception:
        return str(p)


def _to_base36(num: int) -> str:
    alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
    if num == 0:
        return "0"
    neg = num < 0
    num = abs(num)
    out = []
    while num:
        num, rem = divmod(num, 36)
        out.append(alphabet[rem])
    s = "".join(reversed(out))
    return ("-" + s) if neg else s


def _hash_to_base36(s: str, length: int = 16) -> str:
    h = hashlib.sha256(s.encode("utf-8")).digest()
    n = int.from_bytes(h, "big")
    return _to_base36(n)[:length]


def compute_logical_key_id(stage: str, task_id: str) -> str:
    return _hash_to_base36(f"{stage}|{task_id}", length=12)


def new_doc_id(logical_key_id: str, run_id: str, uniqueness: str) -> str:
    return _hash_to_base36(f"{logical_key_id}|{run_id}|{uniqueness}", length=16)


def ensure_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS documents (
              doc_id TEXT PRIMARY KEY,
              logical_key_id TEXT,
              stage TEXT,
              task_id TEXT,
              parent_doc_id TEXT,
              template_id TEXT,
              model_id TEXT,
              run_id TEXT,
              prompt_path TEXT,
              parser TEXT,
              status TEXT,
              usage_prompt_tokens INTEGER,
              usage_completion_tokens INTEGER,
              usage_max_tokens INTEGER,
              created_at TEXT,
              doc_dir TEXT,
              raw_chars INTEGER,
              parsed_chars INTEGER,
              meta_small TEXT,
              lineage_prev_doc_id TEXT
            );
            """
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_documents_logical ON documents(logical_key_id, created_at)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_documents_stage ON documents(stage, created_at)"
        )


@dataclass
class StageConfig:
    stage: str
    raw_dir: Path
    parsed_dir: Path
    pattern_raw: re.Pattern
    pattern_parsed: re.Pattern
    extra_parsed_dirs: List[Path]
    prompt_dir: Path
    pattern_prompt: re.Pattern


def stage_configs(data_root: Path) -> Dict[str, StageConfig]:
    return {
        "draft": StageConfig(
            stage="draft",
            raw_dir=data_root / "3_generation" / "draft_responses_raw",
            parsed_dir=data_root / "3_generation" / "draft_responses",
            pattern_raw=re.compile(r"^(?P<task_id>.+)_v(?P<ver>\d+)\.txt$"),
            pattern_parsed=re.compile(r"^(?P<task_id>.+)\.txt$"),
            extra_parsed_dirs=[
                # Legacy one-phase naming for draft-like outputs
                data_root / "3_generation" / "links_responses",
            ],
            prompt_dir=data_root / "3_generation" / "draft_prompts",
            pattern_prompt=re.compile(r"^(?P<task_id>.+)\.txt$"),
        ),
        "essay": StageConfig(
            stage="essay",
            raw_dir=data_root / "3_generation" / "essay_responses_raw",
            parsed_dir=data_root / "3_generation" / "essay_responses",
            pattern_raw=re.compile(r"^(?P<task_id>.+)_v(?P<ver>\d+)\.txt$"),
            pattern_parsed=re.compile(r"^(?P<task_id>.+)\.txt$"),
            extra_parsed_dirs=[],
            prompt_dir=data_root / "3_generation" / "essay_prompts",
            pattern_prompt=re.compile(r"^(?P<task_id>.+)\.txt$"),
        ),
        "evaluation": StageConfig(
            stage="evaluation",
            raw_dir=data_root / "4_evaluation" / "evaluation_responses_raw",
            parsed_dir=data_root / "4_evaluation" / "evaluation_responses",
            pattern_raw=re.compile(r"^(?P<task_id>.+)_v(?P<ver>\d+)\.txt$"),
            pattern_parsed=re.compile(r"^(?P<task_id>.+)\.txt$"),
            extra_parsed_dirs=[],
            prompt_dir=data_root / "4_evaluation" / "evaluation_prompts",
            pattern_prompt=re.compile(r"^(?P<task_id>.+)\.txt$"),
        ),
    }


def scan_legacy(cfg: StageConfig) -> Dict[str, dict]:
    """Return mapping task_id -> info dict with raw_versions and parsed_path.

    raw_versions: list of dicts {ver:int, path:Path, mtime:float}
    parsed_path: Optional[Path]
    """
    result: Dict[str, dict] = {}
    if cfg.raw_dir.exists():
        for name in os.listdir(cfg.raw_dir):
            if not name.endswith(".txt"):
                continue
            m = cfg.pattern_raw.match(name)
            if not m:
                continue
            task_id = m.group("task_id")
            ver = int(m.group("ver"))
            p = cfg.raw_dir / name
            info = result.setdefault(task_id, {"raw_versions": [], "parsed_path": None})
            try:
                mtime = p.stat().st_mtime
            except Exception:
                mtime = 0.0
            info["raw_versions"].append({"ver": ver, "path": p, "mtime": mtime})
    # Gather parsed files from canonical and any extra legacy directories
    parsed_dirs = [cfg.parsed_dir] + list(cfg.extra_parsed_dirs)
    for d in parsed_dirs:
        if not d.exists():
            continue
        for name in os.listdir(d):
            if not name.endswith(".txt"):
                continue
            m = cfg.pattern_parsed.match(name)
            if not m:
                continue
            task_id = m.group("task_id")
            p = d / name
            info = result.setdefault(task_id, {"raw_versions": [], "parsed_path": None})
            # Prefer canonical parsed_dir if both exist; only set if empty
            if info.get("parsed_path") is None or d == cfg.parsed_dir:
                info["parsed_path"] = p
    # Attach prompt paths
    if cfg.prompt_dir.exists():
        for name in os.listdir(cfg.prompt_dir):
            if not name.endswith(".txt"):
                continue
            m = cfg.pattern_prompt.match(name)
            if not m:
                continue
            task_id = m.group("task_id")
            p = cfg.prompt_dir / name
            info = result.setdefault(task_id, {"raw_versions": [], "parsed_path": None})
            info["prompt_path"] = p

    # sort raw versions by ver
    for task_id, info in result.items():
        info["raw_versions"].sort(key=lambda d: d.get("ver", 0))
    return result


def write_file(src: Optional[Path], dst: Path, link: bool = False) -> Optional[int]:
    if src is None:
        return None
    dst.parent.mkdir(parents=True, exist_ok=True)
    if link:
        try:
            if dst.exists():
                dst.unlink()
            os.link(src, dst)
        except Exception:
            shutil.copy2(src, dst)
    else:
        shutil.copy2(src, dst)
    try:
        return dst.stat().st_size
    except Exception:
        return None


def insert_row(con: sqlite3.Connection, row: dict) -> None:
    cols = ",".join(row.keys())
    placeholders = ",".join([":" + k for k in row.keys()])
    con.execute(f"INSERT OR REPLACE INTO documents ({cols}) VALUES ({placeholders})", row)


def backfill_stage(
    cfg: StageConfig,
    data_root: Path,
    db_path: Path,
    docs_root: Path,
    run_id: str,
    link: bool = False,
    dry_run: bool = False,
) -> Tuple[int, int]:
    """Backfill one stage. Returns (attempts_indexed, dirs_created)."""
    info_map = scan_legacy(cfg)
    if not info_map:
        return (0, 0)

    ensure_db(db_path)
    attempts = 0
    dirs_created = 0
    with sqlite3.connect(db_path) as con:
        con.execute("PRAGMA journal_mode=WAL")
        for task_id, info in info_map.items():
            raw_versions = info.get("raw_versions", [])
            parsed_path: Optional[Path] = info.get("parsed_path")
            latest_ver = max([rv["ver"] for rv in raw_versions], default=None)

            # If there are raw versions, create an attempt per version.
            if raw_versions:
                for rv in raw_versions:
                    ver = rv.get("ver")
                    raw_p: Path = rv.get("path")
                    mtime = rv.get("mtime") or 0.0
                    logical_key_id = compute_logical_key_id(cfg.stage, task_id)
                    uniqueness = f"{task_id}|v{ver}|{int(mtime)}"
                    doc_id = new_doc_id(logical_key_id, run_id, uniqueness)
                    doc_dir = docs_root / cfg.stage / doc_id
                    if doc_dir.exists():
                        # Skip if already present (idempotent-ish)
                        continue
                    if not dry_run:
                        doc_dir.mkdir(parents=True, exist_ok=True)
                        dirs_created += 1
                        raw_size = write_file(raw_p, doc_dir / "raw.txt", link=link)
                        parsed_size = None
                        # Attach parsed only to the latest raw version to avoid duplication
                        if parsed_path is not None and ver == latest_ver:
                            parsed_size = write_file(parsed_path, doc_dir / "parsed.txt", link=link)
                        meta = {
                            "backfill": True,
                            "stage": cfg.stage,
                            "task_id": task_id,
                            "raw_version": ver,
                            "source_paths": {
                                "raw": _safe_relpath(raw_p, data_root),
                                **({"parsed": _safe_relpath(parsed_path, data_root)} if parsed_path else {}),
                            },
                        }
                        row = {
                            "doc_id": doc_id,
                            "logical_key_id": logical_key_id,
                            "stage": cfg.stage,
                            "task_id": task_id,
                            "parent_doc_id": None,
                            "template_id": None,
                            "model_id": None,
                            "run_id": run_id,
                            "prompt_path": (
                                _safe_relpath(info.get("prompt_path"), data_root)
                                if info.get("prompt_path") is not None
                                else None
                            ),
                            "parser": None,
                            "status": "ok",
                            "usage_prompt_tokens": None,
                            "usage_completion_tokens": None,
                            "usage_max_tokens": None,
                            "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                            "doc_dir": _safe_relpath(doc_dir, data_root),
                            "raw_chars": int(raw_size or 0),
                            "parsed_chars": int(parsed_size or 0),
                            "meta_small": json.dumps(meta, ensure_ascii=False),
                            "lineage_prev_doc_id": None,
                        }
                        insert_row(con, row)
                    attempts += 1

            else:
                # No raw versions, but a parsed file exists → create a single attempt.
                if parsed_path is None:
                    continue
                logical_key_id = compute_logical_key_id(cfg.stage, task_id)
                try:
                    mtime = parsed_path.stat().st_mtime
                except Exception:
                    mtime = 0.0
                uniqueness = f"{task_id}|parsed|{int(mtime)}"
                doc_id = new_doc_id(logical_key_id, run_id, uniqueness)
                doc_dir = docs_root / cfg.stage / doc_id
                if doc_dir.exists():
                    continue
                if not dry_run:
                    doc_dir.mkdir(parents=True, exist_ok=True)
                    dirs_created += 1
                    raw_size = None
                    parsed_size = write_file(parsed_path, doc_dir / "parsed.txt", link=link)
                    meta = {
                        "backfill": True,
                        "stage": cfg.stage,
                        "task_id": task_id,
                        "source_paths": {"parsed": _safe_relpath(parsed_path, data_root)},
                    }
                    row = {
                        "doc_id": doc_id,
                        "logical_key_id": logical_key_id,
                        "stage": cfg.stage,
                        "task_id": task_id,
                        "parent_doc_id": None,
                        "template_id": None,
                        "model_id": None,
                        "run_id": run_id,
                        "prompt_path": (
                            _safe_relpath(info.get("prompt_path"), data_root)
                            if info.get("prompt_path") is not None
                            else None
                        ),
                        "parser": None,
                        "status": "ok",
                        "usage_prompt_tokens": None,
                        "usage_completion_tokens": None,
                        "usage_max_tokens": None,
                        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                        "doc_dir": _safe_relpath(doc_dir, data_root),
                        "raw_chars": int(raw_size or 0),
                        "parsed_chars": int(parsed_size or 0),
                        "meta_small": json.dumps(meta, ensure_ascii=False),
                        "lineage_prev_doc_id": None,
                    }
                    insert_row(con, row)
                attempts += 1

        con.commit()
    return (attempts, dirs_created)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-root", type=Path, default=Path("data"))
    ap.add_argument("--db", type=Path, default=None, help="Path to SQLite DB (default: <data-root>/db/documents.sqlite)")
    ap.add_argument("--stage", choices=["all", "draft", "essay", "evaluation"], default="all")
    ap.add_argument("--run-id", default=None, help="Run identifier to stamp on backfilled rows (default: backfill-YYYYMMDD)")
    ap.add_argument("--link", action="store_true", help="Hard-link files instead of copying (fallback to copy if linking fails)")
    ap.add_argument("--dry-run", action="store_true", help="Scan and report without writing files or DB rows")
    args = ap.parse_args()

    data_root: Path = args.data_root
    db_path: Path = args.db if args.db is not None else (data_root / "db" / "documents.sqlite")
    docs_root: Path = data_root / "docs"
    run_id = args.run_id or time.strftime("backfill-%Y%m%d")

    cfgs = stage_configs(data_root)
    stages = ["draft", "essay", "evaluation"] if args.stage == "all" else [args.stage]

    total_attempts = 0
    total_dirs = 0
    for s in stages:
        a, d = backfill_stage(cfgs[s], data_root, db_path, docs_root, run_id=run_id, link=args.link, dry_run=args.dry_run)
        print(f"Stage {s}: indexed {a} attempts; created {d} directories")
        total_attempts += a
        total_dirs += d

    print(f"Done. Indexed attempts: {total_attempts}; directories created: {total_dirs}; DB: {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
