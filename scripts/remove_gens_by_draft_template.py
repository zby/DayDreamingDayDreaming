#!/usr/bin/env python3
"""
Remove all gens for drafts produced by a specific draft template, plus
their downstream essays and evaluations.

Behavior:
- Finds draft gens where draft.metadata.template_id (or draft_template) == --template-id
- Finds essays with parent_gen_id in that draft id set
- Finds evaluations with parent_gen_id in that essay id set
- Prints a summary and, with --apply, deletes their gens directories

Usage:
  uv run python scripts/remove_gens_by_draft_template.py --template-id creative-synthesis-v8 --data-root data --apply
  uv run python scripts/remove_gens_by_draft_template.py --template-id creative-synthesis-v8 --data-root data          # dry run
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import sys
from typing import Dict, List, Tuple, Set


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--template-id", required=True, help="Draft template_id to target (e.g., creative-synthesis-v8)")
    p.add_argument("--data-root", type=Path, default=Path("data"), help="Project data root (default: data)")
    p.add_argument("--apply", action="store_true", help="Apply deletions (otherwise dry-run and fail if any found)")
    return p.parse_args()


def _read_json(path: Path) -> Dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def find_gens_by_draft_template(data_root: Path, template_id: str) -> Tuple[Set[str], Set[str], Set[str]]:
    gens_root = data_root / "gens"
    drafts: Set[str] = set()
    essays: Set[str] = set()
    evals: Set[str] = set()

    # Identify drafts to remove by template
    draft_root = gens_root / "draft"
    if draft_root.exists():
        for d in draft_root.iterdir():
            if not d.is_dir():
                continue
            mid = d.name
            dm = _read_json(d / "metadata.json")
            tid = str(dm.get("template_id") or dm.get("draft_template") or "").strip()
            if tid == template_id:
                drafts.add(mid)

    # Identify essays whose parent is in drafts
    essay_root = gens_root / "essay"
    if essay_root.exists():
        for e in essay_root.iterdir():
            if not e.is_dir():
                continue
            eid = e.name
            em = _read_json(e / "metadata.json")
            parent = str(em.get("parent_gen_id") or "").strip()
            if parent and parent in drafts:
                essays.add(eid)

    # Identify evaluations whose parent is in essays
    eval_root = gens_root / "evaluation"
    if eval_root.exists():
        for ev in eval_root.iterdir():
            if not ev.is_dir():
                continue
            evid = ev.name
            em = _read_json(ev / "metadata.json")
            parent = str(em.get("parent_gen_id") or "").strip()
            if parent and parent in essays:
                evals.add(evid)

    return drafts, essays, evals


def delete_dirs(gens_root: Path, stage: str, ids: Set[str]) -> int:
    cnt = 0
    for gid in ids:
        p = gens_root / stage / gid
        if p.exists():
            shutil.rmtree(p)
            cnt += 1
    return cnt


def main() -> int:
    args = parse_args()
    data_root = args.data_root
    gens_root = data_root / "gens"
    if not gens_root.exists():
        print(f"No gens store found at {gens_root} — nothing to do")
        return 0

    drafts, essays, evals = find_gens_by_draft_template(data_root, args.template_id)
    total = len(drafts) + len(essays) + len(evals)

    if total == 0:
        print(f"No gens found for draft template_id='{args.template_id}'. ✅")
        return 0

    print(f"Will remove gens for draft template_id='{args.template_id}':")
    print(f"- draft: {len(drafts)}")
    print(f"- essay: {len(essays)}")
    print(f"- evaluation: {len(evals)}")
    # Show samples
    def sample(items: Set[str], n: int = 20) -> List[str]:
        out = sorted(list(items))
        return out[:n]
    if drafts:
        print("  draft ids (sample):", ", ".join(sample(drafts)))
    if essays:
        print("  essay ids (sample):", ", ".join(sample(essays)))
    if evals:
        print("  evaluation ids (sample):", ", ".join(sample(evals)))

    if not args.apply:
        print("\nDry-run. Failing to require explicit --apply for deletion.")
        return 2

    # Apply deletions in dependency order: evaluations -> essays -> drafts
    n_eval = delete_dirs(gens_root, "evaluation", evals)
    n_essay = delete_dirs(gens_root, "essay", essays)
    n_draft = delete_dirs(gens_root, "draft", drafts)
    print(f"Deleted gens: evaluation={n_eval}, essay={n_essay}, draft={n_draft}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

