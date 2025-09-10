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
    [--limit 200] [--normalize] [--new-cohort-id restored-YYYYMMDD]

Outputs:
- Always writes one CSV per processed stage under data/reports:
  - data/reports/overwritten_draft.csv
  - data/reports/overwritten_essay.csv
  - data/reports/overwritten_evaluation.csv
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import re
from typing import Dict, List, Optional, Sequence, Tuple
import sys

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


def _resolve_src_path(data_root: Path, src: str) -> Path:
    """Resolve a metadata source_file path to an absolute Path.

    If src is absolute, return as-is. If it starts with 'data/', treat it as
    project-root relative. Otherwise, treat it as relative to data_root.
    """
    p = Path(src)
    if p.is_absolute():
        return p
    s = str(p)
    if s.startswith("data/"):
        return Path.cwd() / p
    return data_root / p


def _candidate_legacy_paths(data_root: Path, stage: str, stems: Sequence[str]) -> List[Path]:
    out: List[Path] = []
    def add(glob_patterns: List[str]):
        for pat in glob_patterns:
            for s in stems:
                out.extend(sorted(Path(data_root).glob(pat.format(stem=s))))
    if stage == "draft":
        # older first; include raw variants
        add([
            "3_generation/links_responses/{stem}*.txt",
            "3_generation/draft_responses/{stem}*.txt",
            "3_generation/draft_responses_raw/{stem}*.txt",
        ])
    elif stage == "essay":
        add([
            "3_generation/generation_responses/{stem}*.txt",
            "3_generation/essay_responses/{stem}*.txt",
            "3_generation/essay_responses_raw/{stem}*.txt",
        ])
    elif stage == "evaluation":
        add(["4_evaluation/evaluation_responses/{stem}*.txt"])
    return out


def analyze_overwrites(
    *, data_root: Path, stages: Sequence[str], limit: Optional[int], normalize: bool, new_cohort_id: Optional[str] = None
) -> None:
    draft_tpls, essay_tpls = _load_known_templates(data_root)
    eval_tpls, eval_model_ids = _load_eval_axes(data_root)
    rows: List[Dict[str, str]] = []
    diffs: List[Dict] = []  # keep full context for optional apply
    matches_by_stage: Dict[str, List[Tuple[str, str]]] = {s: [] for s in stages}
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
                raise FileNotFoundError(f"missing metadata.json: {md_path}")
            md = json.loads(md_path.read_text(encoding="utf-8"))
            # Build stems using task_id directly from metadata
            stems: List[str] = []
            task_id = str(md.get("task_id") or "").strip()
            if stage in ("draft", "essay"):
                src = str(md.get("source_file") or "").strip()
                if task_id:
                    # Legacy filenames use single '_' separators
                    stems = [task_id.replace("__", "_")]
                    candidates = _candidate_legacy_paths(data_root, stage, stems)
                elif src:
                    candidates = [_resolve_src_path(data_root, src)]
                else:
                    # Fail fast when both task_id and source_file are missing
                    raise ValueError(f"missing task_id and source_file in metadata: {md_path}")
            elif stage == "evaluation":
                # Prefer direct source_file from metadata (no guessing)
                src = str(md.get("source_file") or "").strip()
                if src:
                    candidates = [_resolve_src_path(data_root, src)]
                else:
                    eval_tpl = str(md.get("template_id") or "").strip()
                    eval_model = str(md.get("model_id") or "").strip()
                    parent = str(md.get("parent_gen_id") or "").strip()
                    if not (eval_tpl and eval_model and parent):
                        raise ValueError("missing eval template/model/parent")
                    # Read parent essay's task_id and convert to legacy stem
                    emeta = json.loads((gens_root / "essay" / parent / "metadata.json").read_text(encoding="utf-8"))
                    essay_task_id = str(emeta.get("task_id") or "").strip()
                    if not essay_task_id:
                        raise ValueError("missing parent essay task_id for evaluation")
                    essay_stem = essay_task_id.replace("__", "_")
                    stems = [f"{essay_stem}_{eval_tpl}_{eval_model}"]
                    candidates = _candidate_legacy_paths(data_root, stage, stems)
            # Compare oldest candidate only for overwrite detection; if none exist → NEW
            # Stage-specific compare: evaluations compare RAW (not parsed)
            gens_text = ""
            if stage == "evaluation":
                if raw_path.exists():
                    gens_text = raw_path.read_text(encoding="utf-8", errors="ignore")
                elif parsed_path.exists():
                    gens_text = parsed_path.read_text(encoding="utf-8", errors="ignore")
            else:
                if parsed_path.exists():
                    gens_text = parsed_path.read_text(encoding="utf-8", errors="ignore")
                elif raw_path.exists():
                    gens_text = raw_path.read_text(encoding="utf-8", errors="ignore")
            gnorm = _norm_text(gens_text, normalize)
            if not candidates:
                # No legacy candidates ⇒ new generation (not overwritten)
                count += 1
                if limit and count >= limit:
                    break
                continue
            # choose the oldest candidate for comparison
            best = _pick_oldest(candidates)
            # Compare all candidates; if any matches, record that candidate; otherwise treat as overwritten
            match_path: Optional[Path] = None
            for cand in candidates:
                try:
                    c_text = cand.read_text(encoding="utf-8", errors="ignore")
                except Exception:
                    continue
                if _norm_text(c_text, normalize) == gnorm:
                    match_path = cand
                    break
            if match_path is not None:
                # record a match with the concrete file paths used
                if stage == "evaluation":
                    new_path = str(raw_path) if raw_path.exists() else (str(parsed_path) if parsed_path.exists() else "")
                else:
                    new_path = str(parsed_path) if parsed_path.exists() else (str(raw_path) if raw_path.exists() else "")
                matches_by_stage.setdefault(stage, []).append((new_path, str(match_path)))
            else:
                best = _pick_oldest(candidates)
                row = {
                    "stage": stage,
                    "gen_id": gen_id,
                    "legacy_path": str(best) if best else "",
                    "gens_parsed": str(parsed_path) if parsed_path.exists() else "",
                    "gens_raw": str(raw_path) if raw_path.exists() else "",
                }
                rows.append(row)
                diffs.append({
                    "stage": stage,
                    "gen_id": gen_id,
                    "gen_dir": gen_dir,
                    "metadata": md,
                    "best": best,
                })
            count += 1
            if limit and count >= limit:
                break
        if limit and count >= limit:
            break
    print(f"Analyzed gens: {count}; overwritten (differs): {len(rows)}")
    # Write per-stage reports into data/reports
    reports_dir = data_root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    by_stage: Dict[str, List[Dict[str, str]]] = {s: [] for s in stages}
    for r in rows:
        st = r.get("stage")
        if st in by_stage:
            by_stage[st].append(r)
    for st in stages:
        out = reports_dir / f"overwritten_{st}.csv"
        with out.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["stage","gen_id","legacy_path","gens_parsed","gens_raw"])
            w.writeheader()
            for r in by_stage.get(st, []):
                w.writerow(r)
        print(f"Report written: {out} (rows={len(by_stage.get(st, []))})")
        # Write matches report with just two columns: new store path, old store path
        match_rows = matches_by_stage.get(st, [])
        mout = reports_dir / f"matches_{st}.csv"
        with mout.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["new_path", "old_path"])
            w.writerows(match_rows)
        print(f"Report written: {mout} (rows={len(match_rows)})")
    if new_cohort_id:
        _apply_restore_new_cohort(data_root, diffs, new_cohort_id)


def _apply_restore_new_cohort(data_root: Path, diffs: List[Dict], cohort_id: str) -> None:
    """Create a new cohort of generations for the differing docs by copying legacy content.

    - Drafts: create new draft under cohort_id using current task_id; copy best legacy content when available.
    - Essays: ensure parent draft exists in new cohort; create essay under cohort_id; copy best legacy content when available.
    - Evaluations: ensure parent essay (and its draft) exist in new cohort; create evaluation under cohort_id; copy best legacy content when available.
    """
    from daydreaming_dagster.utils.ids import reserve_gen_id
    from daydreaming_dagster.utils.metadata import build_generation_metadata
    from daydreaming_dagster.utils.document import Generation

    gens_root = data_root / "gens"

    # caches to avoid duplicate creation
    created: Dict[Tuple[str, str], str] = {}  # (stage, task_id) -> new_gen_id

    def read_text(path: Optional[Path], fallback: Optional[Path] = None) -> str:
        if isinstance(path, Path) and path.exists():
            return path.read_text(encoding="utf-8", errors="ignore")
        if isinstance(fallback, Path) and fallback.exists():
            return fallback.read_text(encoding="utf-8", errors="ignore")
        return ""

    def ensure_draft(md: Dict) -> str:
        task_id = str(md.get("task_id") or "").strip()
        if not task_id:
            combo = str(md.get("combo_id") or "").strip()
            d_tpl = str(md.get("template_id") or md.get("draft_template") or "").strip()
            model = str(md.get("model_id") or "").strip()
            if not (combo and d_tpl and model):
                raise RuntimeError("missing draft task_id and insufficient fields to compose it")
            task_id = f"{combo}__{d_tpl}__{model}"
        key = ("draft", task_id)
        if key in created:
            return created[key]
        new_id = reserve_gen_id("draft", task_id, run_id=cohort_id)
        # choose legacy text if we can find it
        stem = task_id.replace("__", "_")
        cands = _candidate_legacy_paths(data_root, "draft", [stem])
        best = _pick_oldest(cands)
        # fallback to current gens/raw
        cur = Generation.load(gens_root, "draft", str(md.get("gen_id") or ""))
        text = read_text(best, Path(cur.target_dir(gens_root) / "parsed.txt")) or cur.raw_text
        meta = build_generation_metadata(
            stage="draft",
            gen_id=new_id,
            parent_gen_id=None,
            template_id=str(md.get("template_id") or md.get("draft_template") or ""),
            model_id=str(md.get("model_id") or ""),
            task_id=task_id,
            function="restore_overwritten_from_legacy",
            cohort_id=cohort_id,
            extra={"combo_id": str(md.get("combo_id") or "")},
        )
        Generation(
            stage="draft",
            gen_id=new_id,
            parent_gen_id=None,
            raw_text=text,
            parsed_text=text,
            prompt_text=None,
            metadata=meta,
        ).write_files(gens_root)
        created[key] = new_id
        return new_id

    def ensure_essay(md: Dict) -> str:
        task_id = str(md.get("task_id") or "").strip()
        parent_id = str(md.get("parent_gen_id") or "").strip()
        if not task_id:
            # compose from parent draft + essay template
            dmeta_path = gens_root / "draft" / parent_id / "metadata.json"
            dmeta = json.loads(dmeta_path.read_text(encoding="utf-8")) if dmeta_path.exists() else {}
            combo = str(dmeta.get("combo_id") or "").strip()
            d_tpl = str(dmeta.get("template_id") or dmeta.get("draft_template") or "").strip()
            model = str(dmeta.get("model_id") or "").strip()
            essay_tpl = str(md.get("template_id") or md.get("essay_template") or "").strip()
            if not (combo and d_tpl and model and essay_tpl):
                raise RuntimeError("missing essay task_id and insufficient fields to compose it")
            task_id = f"{combo}__{d_tpl}__{model}__{essay_tpl}"
        key = ("essay", task_id)
        if key in created:
            return created[key]
        # ensure parent draft
        parent_id = str(md.get("parent_gen_id") or "").strip()
        dmeta_path = gens_root / "draft" / parent_id / "metadata.json"
        dmeta = json.loads(dmeta_path.read_text(encoding="utf-8")) if dmeta_path.exists() else {}
        new_draft_id = ensure_draft(dmeta)
        new_id = reserve_gen_id("essay", task_id, run_id=cohort_id)
        # legacy text
        stem = task_id.replace("__", "_")
        cands = _candidate_legacy_paths(data_root, "essay", [stem])
        best = _pick_oldest(cands)
        cur = Generation.load(gens_root, "essay", str(md.get("gen_id") or ""))
        text = read_text(best, Path(cur.target_dir(gens_root) / "parsed.txt")) or cur.raw_text
        meta = build_generation_metadata(
            stage="essay",
            gen_id=new_id,
            parent_gen_id=new_draft_id,
            template_id=str(md.get("template_id") or md.get("essay_template") or ""),
            model_id=str(md.get("model_id") or ""),
            task_id=task_id,
            function="restore_overwritten_from_legacy",
            cohort_id=cohort_id,
            extra={"essay_template": str(md.get("essay_template") or md.get("template_id") or "")},
        )
        Generation(
            stage="essay",
            gen_id=new_id,
            parent_gen_id=new_draft_id,
            raw_text=text,
            parsed_text=text,
            prompt_text=None,
            metadata=meta,
        ).write_files(gens_root)
        created[key] = new_id
        return new_id

    def ensure_evaluation(md: Dict) -> str:
        task_id = str(md.get("task_id") or "").strip()
        parent_id = str(md.get("parent_gen_id") or "").strip()
        if not task_id:
            # compose from parent essay gen_id + eval template/model
            eval_tpl = str(md.get("template_id") or "").strip()
            eval_model = str(md.get("model_id") or "").strip()
            if not (parent_id and eval_tpl and eval_model):
                raise RuntimeError("missing evaluation task_id and insufficient fields to compose it")
            task_id = f"{parent_id}__{eval_tpl}__{eval_model}"
        key = ("evaluation", task_id)
        if key in created:
            return created[key]
        # ensure parent essay
        parent_id = str(md.get("parent_gen_id") or "").strip()
        emeta_path = gens_root / "essay" / parent_id / "metadata.json"
        emeta = json.loads(emeta_path.read_text(encoding="utf-8")) if emeta_path.exists() else {}
        new_essay_id = ensure_essay(emeta)
        new_id = reserve_gen_id("evaluation", task_id, run_id=cohort_id)
        # Prefer direct metadata source_file for evaluation
        src = str(md.get("source_file") or "").strip()
        best = Path(src) if src else None
        if best and not best.is_absolute():
            best = (data_root / best)
        if not best or not best.exists():
            stem = task_id.replace("__", "_")
            cands = _candidate_legacy_paths(data_root, "evaluation", [stem])
            best = _pick_oldest(cands)
        cur = Generation.load(gens_root, "evaluation", str(md.get("gen_id") or ""))
        text = read_text(best, Path(cur.target_dir(gens_root) / "parsed.txt")) or cur.raw_text
        meta = build_generation_metadata(
            stage="evaluation",
            gen_id=new_id,
            parent_gen_id=new_essay_id,
            template_id=str(md.get("template_id") or ""),
            model_id=str(md.get("model_id") or ""),
            task_id=task_id,
            function="restore_overwritten_from_legacy",
            cohort_id=cohort_id,
            extra={"evaluation_template": str(md.get("template_id") or "")},
        )
        Generation(
            stage="evaluation",
            gen_id=new_id,
            parent_gen_id=new_essay_id,
            raw_text=text,
            parsed_text=text,
            prompt_text=None,
            metadata=meta,
        ).write_files(gens_root)
        created[key] = new_id
        return new_id

    for d in diffs:
        st = d["stage"]
        md = d["metadata"]
        try:
            if st == "draft":
                ensure_draft(md)
            elif st == "essay":
                ensure_essay(md)
            elif st == "evaluation":
                ensure_evaluation(md)
        except Exception as e:
            print(f"Apply failed for stage={st} gen_id={d.get('gen_id')}: {e}")
    print(f"Restored {len(created)} generations into cohort '{cohort_id}'.")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-root", type=Path, default=Path("data"))
    ap.add_argument("--stages", nargs="+", choices=["draft","essay","evaluation"], default=["draft","essay","evaluation"])
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--normalize", action="store_true", help="Normalize newlines and trim trailing spaces before compare")
    ap.add_argument("--new-cohort-id", type=str, default=None, help="If provided, create a restored cohort with these differing docs")
    # reports are always written per stage under data/reports
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    analyze_overwrites(
        data_root=args.data_root,
        stages=args.stages,
        limit=args.limit,
        normalize=args.normalize,
        new_cohort_id=args.new_cohort_id,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
