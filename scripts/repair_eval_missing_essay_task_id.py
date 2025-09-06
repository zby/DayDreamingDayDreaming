#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set


def iso_to_dt(s: str) -> Optional[datetime]:
    s = (s or "").strip().replace("Z", "")
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


@dataclass
class EssayRow:
    task_id: str
    combo_id: str
    essay_template: str
    model_id: str
    ts: datetime


def load_essays(path: Path) -> Dict[Tuple[str, str, str], List[EssayRow]]:
    """Index essays by (combo_id, essay_template, model_id) with rows sorted by timestamp asc.

    Returns a dict keyed by (combo, essay_template, model_id). A secondary index
    keyed by (combo, essay_template, base_model) is built by the caller if needed.
    """
    index: Dict[Tuple[str, str, str], List[EssayRow]] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            task_id = (row.get("essay_task_id") or "").strip()
            combo_id = (row.get("combo_id") or "").strip()
            essay_template = (row.get("essay_template_id") or row.get("essay_template") or "").strip() or (row.get("essay_template_id") or "").strip()
            model_id = (row.get("generation_model") or "").strip()
            ts = iso_to_dt(row.get("generation_timestamp") or "")
            if not (task_id and combo_id and essay_template and model_id and ts):
                continue
            key = (combo_id, essay_template, model_id)
            index.setdefault(key, []).append(EssayRow(task_id, combo_id, essay_template, model_id, ts))
    for lst in index.values():
        lst.sort(key=lambda e: e.ts)
    return index


def base_essay_template_from_eval_template(eval_tmpl: str) -> str:
    """Heuristic: evaluation_template often looks like '<essay_template>--<eval_variant>'.
    Return the leading essay_template before '--' when present, else the whole string.
    """
    s = (eval_tmpl or "").strip()
    if "--" in s:
        return s.split("--", 1)[0].strip()
    return s


def normalize_generation_model(model: str, essay_templates: List[str]) -> str:
    """Strip embedded essay template tokens from generation_model when present.

    Many eval rows wrongly include an essay template fragment appended to the
    generation_model (e.g., 'qwen3-235b-a22b_synthesis-theme-narrative-v19_').
    Remove any known essay_template occurrences with common separators.
    """
    s = (model or "").strip()
    if not s:
        return s
    # Remove any _<essay_template> or -<essay_template> or <essay_template>_ fragments
    for tmpl in sorted(essay_templates, key=len, reverse=True):
        if not tmpl:
            continue
        for pat in (
            f"_{re.escape(tmpl)}_",
            f"_{re.escape(tmpl)}",
            f"-{re.escape(tmpl)}-",
            f"-{re.escape(tmpl)}",
            re.escape(tmpl),
        ):
            s = re.sub(pat, "", s)
    # Unify separators, common numeric fixups
    s = s.replace("__", "_")
    s = re.sub(r"[_\-]{2,}", "_", s)
    s = s.strip("_-")
    # Heuristic: normalize common model tokens
    s = s.replace("gemini_25_pro", "gemini-2.5-pro").replace("gemini_2.5_pro", "gemini-2.5-pro")
    s = s.replace("gemini_25_flash", "gemini-2.5-flash").replace("gemini_2.5_flash", "gemini-2.5-flash")
    s = s.replace("gemma_3_27b", "gemma-3-27b")
    s = s.replace("deepseek_r1_p", "deepseek-r1").replace("deepseek_r1", "deepseek-r1")
    s = s.replace("sonnet_4", "claude-sonnet-4").replace("sonnet-4", "claude-sonnet-4")
    s = s.replace("qwen3_235b_a22b", "qwen3-235b-a22b")
    return s


def providerize_model(norm_model: str, essay_models: Set[str]) -> List[str]:
    """Generate possible provider-qualified model ids for a normalized base form.

    Tries a small mapping and also checks essay_models for unique matches.
    Returns a list of candidate model strings (including the input).
    """
    cands = [norm_model]
    base = norm_model.split("/")[-1]
    mapping = {
        "gemini-2.5-pro": "google/gemini-2.5-pro",
        "gemini-2.5-flash": "google/gemini-2.5-flash",
        "gemma-3-27b": "google/gemma-3-27b",
        "claude-sonnet-4": "anthropic/claude-sonnet-4",
        "deepseek-r1": "deepseek/deepseek-r1",
        "qwen3-235b-a22b": "qwen/qwen3-235b-a22b",
    }
    if base in mapping:
        cands.append(mapping[base])
    # If essays contain a unique provider-qualified model that endswith the base, add it
    matches = [m for m in essay_models if m.split("/")[-1] == base]
    if len(matches) == 1:
        cands.append(matches[0])
    # Deduplicate preserving order
    seen = set()
    out = []
    for m in cands:
        if m and m not in seen:
            seen.add(m)
            out.append(m)
    return out


def repair_eval_csv(
    eval_csv: Path,
    essays_csv: Path,
    output_csv: Path,
) -> dict:
    essays_idx = load_essays(essays_csv)
    essay_templates = sorted({key[1] for key in essays_idx.keys()}, key=len, reverse=True)
    essay_models: Set[str] = {key[2] for key in essays_idx.keys()}
    # Secondary index by base model name (strip provider prefix)
    essays_idx_base: Dict[Tuple[str, str, str], List[EssayRow]] = {}
    for (combo, tmpl, model), lst in essays_idx.items():
        base = model.split('/')[-1]
        essays_idx_base.setdefault((combo, tmpl, base), []).extend(lst)

    with eval_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        header = reader.fieldnames or []

    # Ensure essay_task_id column exists in header
    if "essay_task_id" not in header:
        header.insert(1, "essay_task_id")  # after evaluation_task_id

    fixed = 0
    total_candidates = 0
    ambiguous = 0
    not_found = 0

    for row in rows:
        current = (row.get("essay_task_id") or "").strip()
        if current:
            continue
        combo_id = (row.get("combo_id") or "").strip()
        eval_tmpl = (row.get("evaluation_template") or "").strip()
        # Use the generation model (essay model), not evaluation model
        eval_model = (row.get("generation_model") or "").strip()
        ts = iso_to_dt(row.get("evaluation_timestamp") or "")
        if not (combo_id and eval_tmpl and eval_model and ts):
            continue
        base_tmpl = base_essay_template_from_eval_template(eval_tmpl)
        norm_model = normalize_generation_model(eval_model, essay_templates)
        candidates: List[EssayRow] = []
        for m in providerize_model(norm_model, essay_models):
            key = (combo_id, base_tmpl, m)
            candidates = essays_idx.get(key, []) or essays_idx_base.get((combo_id, base_tmpl, m.split('/')[-1]), [])
            if candidates:
                break
        # If no candidates via base eval template, try inferring essay template from generation_model
        if not candidates:
            gm = (row.get("generation_model") or "").strip()
            hit_tmpl: Optional[str] = None
            for tmpl in essay_templates:
                if tmpl and tmpl in gm:
                    hit_tmpl = tmpl
                    break
            if hit_tmpl:
                for m in providerize_model(norm_model, essay_models):
                    key2 = (combo_id, hit_tmpl, m)
                    candidates = essays_idx.get(key2, []) or essays_idx_base.get((combo_id, hit_tmpl, m.split('/')[-1]), [])
                    if candidates:
                        break
        if not candidates:
            not_found += 1
            continue
        total_candidates += 1
        # choose latest <= ts
        chosen: Optional[EssayRow] = None
        for er in candidates:
            if er.ts <= ts:
                chosen = er
            else:
                break
        if chosen is None:
            not_found += 1
            continue
        # If multiple essays share same ts and template/model, we still take the last in sort order
        row["essay_task_id"] = chosen.task_id
        fixed += 1

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in header})

    return {
        "rows": len(rows),
        "fixed": fixed,
        "candidates": total_candidates,
        "ambiguous": ambiguous,
        "not_found": not_found,
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Fill missing essay_task_id in evaluation_results.csv by matching essays")
    p.add_argument("--eval-csv", type=Path, default=Path("data/7_cross_experiment/evaluation_results.csv"))
    p.add_argument("--essays-csv", type=Path, default=Path("data/7_cross_experiment/essay_generation_results.csv"))
    p.add_argument("--out", type=Path, required=True, help="Output CSV path for repaired evaluations")
    args = p.parse_args(argv)

    stats = repair_eval_csv(args.eval_csv, args.essays_csv, args.out)
    print(
        f"repaired '{args.eval_csv}' -> '{args.out}' rows={stats['rows']} fixed={stats['fixed']} "
        f"candidates={stats['candidates']} not_found={stats['not_found']} ambiguous={stats['ambiguous']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
