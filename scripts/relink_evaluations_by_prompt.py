#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from daydreaming_dagster.utils.documents_index import SQLiteDocumentsIndex


KNOWN_MODEL_IDS = {
    'deepseek_r1_f', 'deepseek_r1_zero_f', 'deepseek_r1_distill_70b_f', 'deepseek_r1_distill_32b_f',
    'deepseek_chat_v3_f', 'gemini_25_pro_f', 'gemini_20_flash_thinking_f', 'gemini_20_flash_f',
    'gemma_3_27b_f', 'llama_4_maverick_f', 'llama_4_scout_f', 'llama_33_70b_f', 'llama_33_8b_f',
    'nemotron_253b_f', 'qwq_32b_f', 'devstral_small_f', 'mistral_small_31_f', 'mistral_7b_f',
    'gemini_2.5_flash', 'sonnet-4', 'deepseek_r1_p', 'gemini_25_pro', 'llama_31_405b', 'qwen3-235b-a22b'
}


def read_text(p: Path) -> Optional[str]:
    try:
        return p.read_text(encoding='utf-8')
    except Exception:
        return None


def strip_version(stem: str) -> str:
    m = re.match(r"^(.*?)(?:[_-]v\d+)$", stem)
    return m.group(1) if m else stem


def split_eval_from_right(stem: str) -> Tuple[str, Optional[str], Optional[str]]:
    tokens = re.split(r"_+", stem)
    if not tokens:
        return stem, None, None
    eval_model = None
    model_start = None
    for j in range(len(tokens), 0, -1):
        cand = "_".join(tokens[j - 1:])
        if cand in KNOWN_MODEL_IDS:
            eval_model = cand
            model_start = j - 1
            break
    if eval_model is None or model_start is None:
        return stem, None, None
    if model_start - 1 < 0:
        return stem, None, eval_model
    eval_tmpl = tokens[model_start - 1]
    left = "_".join(tokens[: model_start - 1])
    return left, eval_tmpl, eval_model


def extract_candidate_lines(prompt: str) -> Optional[List[str]]:
    lines = prompt.splitlines()
    # Case-insensitive search for markers
    for i, line in enumerate(lines):
        low = line.lower()
        if ('candidate text:' in low) or ('response to evaluate:' in low):
            out: List[str] = []
            j = i + 1
            while j < len(lines) and len(out) < 2:
                s = lines[j].strip()
                if s:
                    out.append(s)
                j += 1
            if len(out) == 2:
                return out
            break
    # Fallback: use the first heading and next non-empty line
    for i, line in enumerate(lines):
        if line.strip().startswith('## '):
            out: List[str] = [line.strip()]
            # grab next non-empty line
            for j in range(i + 1, len(lines)):
                s = lines[j].strip()
                if s:
                    out.append(s)
                    break
            if len(out) == 2:
                return out
            break
    return None


@dataclass
class RelinkResult:
    evaluation_doc_id: str
    evaluation_task_id: str
    old_parent: Optional[str]
    new_parent: Optional[str]
    reason: str


def relink(
    db_path: Path,
    docs_root: Path,
    *,
    execute: bool = False,
    limit: Optional[int] = None,
) -> Dict:
    idx = SQLiteDocumentsIndex(db_path, docs_root)
    con = idx.connect()
    con.row_factory = lambda c, r: {c.description[i][0]: r[i] for i in range(len(r))}

    evaluations = con.execute("SELECT * FROM documents WHERE stage='evaluation' AND status='ok' ORDER BY created_at").fetchall()
    processed = 0
    checked = 0
    updated = 0
    no_candidate = 0
    no_match = 0
    multi_match = 0
    results: List[RelinkResult] = []

    for ev in evaluations:
        if limit and processed >= limit:
            break
        processed += 1
        ev_dir = idx.resolve_doc_dir(ev)
        prompt = read_text(ev_dir / 'prompt.txt')
        if not prompt:
            continue
        candidates = extract_candidate_lines(prompt)
        if not candidates:
            no_candidate += 1
            continue
        # Determine essay group key from evaluation task id
        base_no_ver = strip_version(ev['task_id'])
        left, eval_tmpl, eval_model = split_eval_from_right(base_no_ver)
        # Fetch essay candidates by left prefix in task_id (ignore essay model; it differs from eval model)
        if not left:
            continue
        like_prefix = left + '%'
        essays = con.execute(
            "SELECT * FROM documents WHERE stage='essay' AND task_id LIKE ? ORDER BY created_at DESC",
            (like_prefix,),
        ).fetchall()
        matches = []
        for es in essays:
            es_dir = idx.resolve_doc_dir(es)
            es_parsed = read_text(es_dir / 'parsed.txt') or ''
            if all(line in es_parsed for line in candidates):
                matches.append(es)
        checked += 1
        if not matches:
            no_match += 1
            continue
        if len(matches) > 1:
            # choose the most recent
            matches.sort(key=lambda r: r.get('created_at') or '', reverse=True)
            multi_match += 1
        chosen = matches[0]
        old_parent = ev.get('parent_doc_id')
        new_parent = chosen['doc_id']
        if old_parent == new_parent:
            continue
        results.append(RelikResult := RelinkResult(ev['doc_id'], ev['task_id'], old_parent, new_parent, 'matched_by_prompt_candidate_text'))
        if execute:
            with con:
                meta = ev.get('meta_small')
                try:
                    meta_obj = json.loads(meta) if isinstance(meta, str) and meta else {}
                except Exception:
                    meta_obj = {}
                meta_obj['relinked_reason'] = RelikResult.reason
                con.execute(
                    "UPDATE documents SET parent_doc_id=?, meta_small=? WHERE doc_id=?",
                    (new_parent, json.dumps(meta_obj, ensure_ascii=False), ev['doc_id']),
                )
            updated += 1

    return {
        'processed': processed,
        'checked': checked,
        'updated': updated,
        'no_candidate': no_candidate,
        'no_match': no_match,
        'multi_match': multi_match,
        'examples': results[:10],
    }


def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(description='Relink evaluations to essays by matching CANDIDATE TEXT lines in prompt')
    p.add_argument('--db', type=Path, default=Path('data/db/documents.sqlite'))
    p.add_argument('--docs-root', type=Path, default=Path('data/docs'))
    p.add_argument('--execute', action='store_true', help='Apply updates to parent_doc_id when a unique match is found')
    p.add_argument('--limit', type=int, help='Limit number of evaluations processed')
    args = p.parse_args(argv)

    res = relink(args.db, args.docs_root, execute=args.execute, limit=args.limit)
    print(f"processed={res['processed']} checked={res['checked']} updated={res['updated']} no_candidate={res['no_candidate']} no_match={res['no_match']} multi_match={res['multi_match']}")
    for ex in res['examples']:
        print(f"UPDATED eval={ex.evaluation_doc_id} task={ex.evaluation_task_id} old_parent={ex.old_parent} new_parent={ex.new_parent} reason={ex.reason}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
