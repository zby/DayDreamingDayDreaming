#!/usr/bin/env python3
"""
Detect and optionally restore overwritten generations in data/gens from legacy stores.

Direction: gens -> legacy
- Scans data/gens/{draft,essay,evaluation}/*
- For each gen, reads metadata to reconstruct the legacy filename stem.
- Searches legacy dirs under data/3_generation and data/4_evaluation for candidates:
  - draft: prefer 3_generation/links_responses over draft_responses (older first)
  - essay: prefer 3_generation/generation_responses over essay_responses (older first)
  - evaluation: 4_evaluation/evaluation_responses only
- Handles versioned files (*_vN.txt): picks the lowest N (oldest)
- Compares legacy file content with gens/<stage>/<gen_id>/parsed.txt (fallback raw.txt). If different, reports as overwritten.
- With --apply, copies legacy content into gens raw.txt and parsed.txt for those overwritten only.

Usage:
  uv run python scripts/restore_overwritten_from_legacy.py \
    --data-root data \
    --stages draft essay evaluation \
    [--apply] [--limit 200] [--normalize] [--report report.csv]
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import re
from typing import Dict, List, Optional, Sequence, Tuple

def _load_known_templates(base_data: Path):
    draft: set[str] = set(); essay: set[str] = set()
    try:
        import pandas as pd
        d_csv = base_data / "1_raw" / "draft_templates.csv"
        if d_csv.exists():
            df = pd.read_csv(d_csv)
            if "template_id" in df.columns:
                draft.update(df["template_id"].astype(str).tolist())
        e_csv = base_data / "1_raw" / "essay_templates.csv"
        if e_csv.exists():
            df = pd.read_csv(e_csv)
            if "template_id" in df.columns:
                essay.update(df["template_id"].astype(str).tolist())
    except Exception:
        pass
    for p in (base_data / "1_raw" / "generation_templates" / "draft").glob("*.txt"):
        draft.add(p.stem)
    for p in (base_data / "1_raw" / "generation_templates" / "essay").glob("*.txt"):
        essay.add(p.stem)
    return draft, essay

def _parse_draft_doc_id(doc_id: str, draft_templates: set[str]):
    parts = doc_id.split("_")
    idx = None
    for i, token in enumerate(parts):
        if token in draft_templates:
            idx = i; break
    if idx is None:
        raise ValueError(f"draft template not found in {doc_id}")
    combo = "_".join(parts[:idx])
    d = parts[idx]
    m = "_".join(parts[idx+1:]) if idx+1 < len(parts) else ""
    if not combo or not d or not m:
        raise ValueError(f"invalid draft doc id {doc_id}")
    return combo, d, m

def _parse_essay_doc_id(doc_id: str, draft_templates: set[str], essay_templates: set[str]):
    parts = doc_id.split("_")
    e_idx = None
    for i in range(len(parts)-1, -1, -1):
        if parts[i] in essay_templates:
            e_idx = i; break
    if e_idx is None:
        raise ValueError(f"essay template not found in {doc_id}")
    essay_tpl = parts[e_idx]
    d_idx = None
    for i in range(0, e_idx):
        if parts[i] in draft_templates:
            d_idx = i; break
    if d_idx is None:
        combo = "_".join(parts[:e_idx]); draft_tpl = essay_tpl; model = "_".join(parts[e_idx+1:])
    else:
        combo = "_".join(parts[:d_idx]); draft_tpl = parts[d_idx]; model = "_".join(parts[d_idx+1:e_idx])
    if not combo or not draft_tpl or not model:
        raise ValueError(f"invalid essay doc id {doc_id}")
    return combo, draft_tpl, model, essay_tpl

def _load_eval_axes(base: Path):
    eval_tpls: set[str] = set(); model_ids: set[str] = set()
    try:
        import pandas as pd
        et = base / "1_raw" / "evaluation_templates.csv"
        if et.exists():
            df = pd.read_csv(et)
            if "template_id" in df.columns:
                eval_tpls.update(df["template_id"].astype(str).tolist())
        lm = base / "1_raw" / "llm_models.csv"
        if lm.exists():
            df = pd.read_csv(lm)
            if "id" in df.columns:
                model_ids.update(df["id"].astype(str).tolist())
    except Exception:
        pass
    return eval_tpls, model_ids


def _norm_text(s: str, normalize: bool) -> str:
    if not normalize:
        return s
    return "\n".join([line.rstrip() for line in s.replace("\r\n", "\n").splitlines()]).strip()


_V_RE = re.compile(r"_v(\d+)\.txt$")


def _pick_oldest(paths: List[Path]) -> Optional[Path]:
    if not paths:
        return None
    # Prefer explicit version with smallest vN; if none have version, pick the earliest mtime
    def key(p: Path):
        m = _V_RE.search(p.name)
        if m:
            return (0, int(m.group(1)))
        return (1, int(p.stat().st_mtime))
    return sorted(paths, key=key)[0]


def _candidate_legacy_paths(data_root: Path, stage: str, stems: Sequence[str]) -> List[Path]:
    out: List[Path] = []
    def add(glob_patterns: List[str]):
        for pat in glob_patterns:
            for s in stems:
                out.extend(sorted(Path(data_root).glob(pat.format(stem=s))))
    if stage == "draft":
        # older first
        add([
            "3_generation/links_responses/{stem}*.txt",
            "3_generation/draft_responses/{stem}*.txt",
        ])
    elif stage == "essay":
        add([
            "3_generation/generation_responses/{stem}*.txt",
            "3_generation/essay_responses/{stem}*.txt",
        ])
    elif stage == "evaluation":
        add(["4_evaluation/evaluation_responses/{stem}*.txt"])
    return out


def analyze_overwrites(
    *, data_root: Path, stages: Sequence[str], limit: Optional[int], normalize: bool, apply: bool, report_csv: Optional[Path]
) -> None:
    draft_tpls, essay_tpls = _load_known_templates(data_root)
    eval_tpls, eval_model_ids = _load_eval_axes(data_root)
    rows: List[Dict[str, str]] = []
    gens_root = data_root / "gens"
    count = 0
    for stage in stages:
        stage_dir = gens_root / stage
        if not stage_dir.exists():
            continue
        for gen_dir in sorted([p for p in stage_dir.iterdir() if p.is_dir()]):
            gen_id = gen_dir.name
            md_path = gen_dir / "metadata.json"
            parsed_path = gen_dir / "parsed.txt"
            raw_path = gen_dir / "raw.txt"
            if not md_path.exists():
                continue
            try:
                md = json.loads(md_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            try:
                # Build stems
                stems: List[str] = []
                if stage == "draft":
                    combo = str(md.get("combo_id") or "").strip()
                    d_tpl = str(md.get("template_id") or md.get("draft_template") or "").strip()
                    model = str(md.get("model_id") or "").strip()
                    if not (combo and d_tpl and model):
                        raise ValueError("missing combo/template/model in metadata")
                    stems = [f"{combo}_{d_tpl}_{model}"]
                elif stage == "essay":
                    essay_tpl = str(md.get("template_id") or md.get("essay_template") or "").strip()
                    parent = str(md.get("parent_gen_id") or "").strip()
                    if not (essay_tpl and parent):
                        raise ValueError("missing essay template or parent_gen_id")
                    # load parent draft metadata
                    try:
                        dmeta = json.loads((gens_root / "draft" / parent / "metadata.json").read_text(encoding="utf-8"))
                    except Exception:
                        dmeta = {}
                    combo = str(dmeta.get("combo_id") or "").strip()
                    d_tpl = str(dmeta.get("template_id") or dmeta.get("draft_template") or "").strip()
                    model = str(dmeta.get("model_id") or "").strip()
                    if combo and d_tpl and model:
                        stems = [f"{combo}_{d_tpl}_{model}_{essay_tpl}", f"{combo}_{essay_tpl}_{model}"]  # two-phase and one-phase fallback
                    else:
                        stems = [f"*_{essay_tpl}_*"]
                elif stage == "evaluation":
                    eval_tpl = str(md.get("template_id") or "").strip()
                    eval_model = str(md.get("model_id") or "").strip()
                    parent = str(md.get("parent_gen_id") or "").strip()
                    if not (eval_tpl and eval_model and parent):
                        raise ValueError("missing eval template/model/parent")
                    # parent essay stem
                    try:
                        emeta = json.loads((gens_root / "essay" / parent / "metadata.json").read_text(encoding="utf-8"))
                    except Exception:
                        emeta = {}
                    essay_tpl = str(emeta.get("template_id") or emeta.get("essay_template") or "").strip()
                    try:
                        dmeta = json.loads((gens_root / "draft" / str(emeta.get("parent_gen_id") or "") / "metadata.json").read_text(encoding="utf-8"))
                    except Exception:
                        dmeta = {}
                    combo = str(dmeta.get("combo_id") or "").strip()
                    d_tpl = str(dmeta.get("template_id") or dmeta.get("draft_template") or "").strip()
                    model = str(dmeta.get("model_id") or "").strip()
                    if combo and d_tpl and model and essay_tpl:
                        essay_stem = f"{combo}_{d_tpl}_{model}_{essay_tpl}"
                        stems = [f"{essay_stem}_{eval_tpl}_{eval_model}"]
                    else:
                        stems = []
                # Find candidates
                candidates = _candidate_legacy_paths(data_root, stage, stems)
                if not candidates:
                    continue
                best = _pick_oldest(candidates)
                if not best:
                    continue
                legacy_text = best.read_text(encoding="utf-8", errors="ignore")
                gens_text = None
                if parsed_path.exists():
                    gens_text = parsed_path.read_text(encoding="utf-8", errors="ignore")
                elif raw_path.exists():
                    gens_text = raw_path.read_text(encoding="utf-8", errors="ignore")
                else:
                    gens_text = ""
                lnorm = _norm_text(legacy_text, normalize)
                gnorm = _norm_text(gens_text, normalize)
                if lnorm != gnorm:
                    row = {
                        "stage": stage,
                        "gen_id": gen_id,
                        "legacy_path": str(best),
                        "gens_parsed": str(parsed_path) if parsed_path.exists() else "",
                        "gens_raw": str(raw_path) if raw_path.exists() else "",
                    }
                    rows.append(row)
                    print(f"OVERWRITTEN? stage={stage} gen_id={gen_id} <- {best}")
                    if apply:
                        base = gen_dir
                        # overwrite parsed/raw with legacy text
                        (base / "raw.txt").write_text(legacy_text, encoding="utf-8")
                        (base / "parsed.txt").write_text(legacy_text, encoding="utf-8")
                count += 1
                if limit and count >= limit:
                    break
            except Exception:
                continue
        if limit and count >= limit:
            break
    print(f"Analyzed gens: {count}; overwritten (differs): {len(rows)}")
    if report_csv:
        report_csv.parent.mkdir(parents=True, exist_ok=True)
        with report_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["stage","gen_id","legacy_path","gens_parsed","gens_raw"])
            w.writeheader()
            for r in rows:
                w.writerow(r)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-root", type=Path, default=Path("data"))
    ap.add_argument("--stages", nargs="+", choices=["draft","essay","evaluation"], default=["draft","essay","evaluation"])
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--normalize", action="store_true", help="Normalize newlines and trim trailing spaces before compare")
    ap.add_argument("--apply", action="store_true", help="Overwrite gens raw.txt/parsed.txt with legacy content for differing docs")
    ap.add_argument("--report", type=Path, default=None, help="Write CSV report of differing docs")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    analyze_overwrites(
        data_root=args.data_root,
        stages=args.stages,
        limit=args.limit,
        normalize=args.normalize,
        apply=args.apply,
        report_csv=args.report,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
