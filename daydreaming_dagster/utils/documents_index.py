from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


DDL = """
CREATE TABLE IF NOT EXISTS documents (
  doc_id TEXT PRIMARY KEY,
  logical_key_id TEXT NOT NULL,
  stage TEXT NOT NULL CHECK (stage IN ('draft','essay','evaluation')),
  task_id TEXT NOT NULL,
  parent_doc_id TEXT,
  template_id TEXT,
  model_id TEXT,
  run_id TEXT,
  parser TEXT,
  status TEXT NOT NULL CHECK (status IN ('ok','truncated','parse_error','gen_error','skipped')),
  usage_prompt_tokens INTEGER,
  usage_completion_tokens INTEGER,
  usage_max_tokens INTEGER,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  doc_dir TEXT NOT NULL,
  raw_chars INTEGER,
  parsed_chars INTEGER,
  content_hash TEXT,
  meta_small TEXT,
  lineage_prev_doc_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_documents_logical ON documents(logical_key_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_documents_stage ON documents(stage, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_documents_task ON documents(stage, task_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_documents_parent ON documents(parent_doc_id, created_at DESC);
"""


def _row_factory(cursor, row):
    cols = [c[0] for c in cursor.description]
    return {k: row[i] for i, k in enumerate(cols)}


@dataclass
class DocumentRow:
    doc_id: str
    logical_key_id: str
    stage: str
    task_id: str
    doc_dir: str
    # Optional / defaulted fields must come after all required fields
    status: str = "ok"
    parent_doc_id: Optional[str] = None
    template_id: Optional[str] = None
    model_id: Optional[str] = None
    run_id: Optional[str] = None
    parser: Optional[str] = None
    usage_prompt_tokens: Optional[int] = None
    usage_completion_tokens: Optional[int] = None
    usage_max_tokens: Optional[int] = None
    raw_chars: Optional[int] = None
    parsed_chars: Optional[int] = None
    content_hash: Optional[str] = None
    meta_small: Optional[dict] = None
    lineage_prev_doc_id: Optional[str] = None


class SQLiteDocumentsIndex:
    """
    Minimal SQLite-backed index for LLM documents.

    Standalone (no Dagster dependency) so it can be unit-tested in isolation.
    """

    def __init__(self, db_path: Path, docs_root: Path):
        self.db_path = Path(db_path)
        self.docs_root = Path(docs_root)
        self._con: Optional[sqlite3.Connection] = None

    def connect(self) -> sqlite3.Connection:
        if self._con is None:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            con = sqlite3.connect(str(self.db_path), isolation_level=None, check_same_thread=False)
            con.row_factory = _row_factory
            # Pragmas for concurrency robustness
            con.execute("PRAGMA journal_mode=WAL;")
            con.execute("PRAGMA busy_timeout=5000;")
            self._con = con
        return self._con

    def close(self) -> None:
        if self._con is not None:
            try:
                self._con.close()
            finally:
                self._con = None

    # Context manager support
    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def init_maybe_create_tables(self) -> None:
        con = self.connect()
        with con:  # transaction
            for stmt in DDL.strip().split(";\n"):
                s = stmt.strip()
                if s:
                    con.execute(s)

    def insert_document(self, row: DocumentRow) -> None:
        con = self.connect()
        payload = row.__dict__.copy()
        # Serialize meta_small to JSON if dict provided
        meta_small = payload.get("meta_small")
        if isinstance(meta_small, dict):
            payload["meta_small"] = json.dumps(meta_small, ensure_ascii=False)
        with con:
            con.execute(
                """
                INSERT INTO documents (
                  doc_id, logical_key_id, stage, task_id, parent_doc_id,
                  template_id, model_id, run_id, parser,
                  status, usage_prompt_tokens, usage_completion_tokens, usage_max_tokens,
                  doc_dir, raw_chars, parsed_chars, content_hash, meta_small, lineage_prev_doc_id
                ) VALUES (
                  :doc_id, :logical_key_id, :stage, :task_id, :parent_doc_id,
                  :template_id, :model_id, :run_id, :parser,
                  :status, :usage_prompt_tokens, :usage_completion_tokens, :usage_max_tokens,
                  :doc_dir, :raw_chars, :parsed_chars, :content_hash, :meta_small, :lineage_prev_doc_id
                )
                """,
                payload,
            )

    def get_latest_by_task(self, stage: str, task_id: str, statuses: Iterable[str] = ("ok",)) -> Optional[dict]:
        con = self.connect()
        q = (
            "SELECT * FROM documents WHERE stage=? AND task_id=? "
            "AND status IN (" + ",".join(["?"] * len(tuple(statuses))) + ") "
            "ORDER BY created_at DESC, rowid DESC LIMIT 1"
        )
        args = [stage, task_id, *list(statuses)]
        cur = con.execute(q, args)
        return cur.fetchone()

    # Note: get_latest_by_logical removed; prefer task-based lookups or dir pointers

    def get_by_doc_id(self, doc_id: str) -> Optional[dict]:
        """Return the exact row for a given doc_id, or None."""
        con = self.connect()
        cur = con.execute("SELECT * FROM documents WHERE doc_id=? LIMIT 1", (str(doc_id),))
        return cur.fetchone()

    def get_by_doc_id_and_stage(self, doc_id: str, stage: str) -> Optional[dict]:
        """Return the row for a given doc_id and stage, or None."""
        con = self.connect()
        cur = con.execute(
            "SELECT * FROM documents WHERE doc_id=? AND stage=? LIMIT 1",
            (str(doc_id), str(stage)),
        )
        return cur.fetchone()

    # Filesystem helpers
    def resolve_doc_dir(self, row: dict) -> Path:
        # Prefer explicit doc_dir if provided; otherwise compute
        dd = row.get("doc_dir") if isinstance(row, dict) else None
        if isinstance(dd, str) and dd:
            p = Path(dd)
            return p if p.is_absolute() else (self.docs_root / p)
        # Fallback: compute from fields
        stage = row["stage"]
        # Flat layout: buckets removed; place docs directly under stage
        doc_id = row["doc_id"]
        return self.docs_root / stage / doc_id

    def read_text(self, row: dict, name: str) -> str:
        base = self.resolve_doc_dir(row)
        fp = base / name
        return fp.read_text(encoding="utf-8")

    def read_raw(self, row: dict) -> str:
        return self.read_text(row, "raw.txt")

    def read_parsed(self, row: dict) -> str:
        return self.read_text(row, "parsed.txt")

    def read_prompt(self, row: dict) -> str:
        return self.read_text(row, "prompt.txt")
