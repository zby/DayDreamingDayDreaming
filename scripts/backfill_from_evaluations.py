#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, UTC
from pathlib import Path
from typing import Dict, Optional, Tuple, List

from daydreaming_dagster.utils.documents_index import (
    DocumentRow,
    SQLiteDocumentsIndex,
)
from daydreaming_dagster.utils.ids import (
    compute_logical_key_id_draft,
    compute_logical_key_id_essay,
    compute_logical_key_id_evaluation,
    doc_dir as make_doc_dir,
    new_doc_id,
)

# Known model IDs copied from data/1_raw/llm_models.csv (id column)
KNOWN_MODEL_IDS = {
    'deepseek_r1_f', 'deepseek_r1_zero_f', 'deepseek_r1_distill_70b_f', 'deepseek_r1_distill_32b_f',
    'deepseek_chat_v3_f', 'gemini_25_pro_f', 'gemini_20_flash_thinking_f', 'gemini_20_flash_f',
    'gemma_3_27b_f', 'llama_4_maverick_f', 'llama_4_scout_f', 'llama_33_70b_f', 'llama_33_8b_f',
    'nemotron_253b_f', 'qwq_32b_f', 'devstral_small_f', 'mistral_small_31_f', 'mistral_7b_f',
    'gemini_2.5_flash', 'sonnet-4', 'deepseek_r1_p', 'gemini_25_pro', 'llama_31_405b', 'qwen3-235b-a22b'
}

# Known draft templates that may include underscores in their names
KNOWN_DRAFT_TEMPLATES = {
    'gwern_original',
    'recursive_construction',
}


def ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def read_text(p: Path) -> Optional[str]:
    try:
        return p.read_text(encoding="utf-8")
    except Exception:
        return None


def write_text(p: Path, text: str) -> None:
    ensure_parent(p)
    p.write_text(text, encoding="utf-8")


def stem_of(p: Path) -> str:
    s = p.name
    if s.endswith(".txt"):
        return s[:-4]
    return p.stem


def split_version(stem: str) -> Tuple[str, Optional[int]]:
    """Return (base_without_version, version_int or None) for stems ending with _vN or -vN."""
    import re
    m = re.match(r"^(.*?)(?:[_-]v(\d+))$", stem)
    if not m:
        return stem, None
    base, v = m.groups()
    try:
        return base, int(v)
    except Exception:
        return base, None


def split_eval_stem(stem: str) -> Tuple[str, Optional[str], Optional[str]]:
    """Parse evaluation filename from the right.

    Expected overall: <left_part>[_|__]<eval_template>_<eval_model>
    But separators may be single or double underscores; parse from right using
    KNOWN_MODEL_IDS to identify the model suffix. The token immediately before
    the model is the evaluation template (no underscores). The remaining left
    side is returned as a single string joined by underscores.
    """
    import re
    # Treat one or more underscores as separators for tokenization
    tokens = re.split(r"_+", stem)
    if not tokens:
        return stem, None, None
    # Find the longest matching model id at the end
    eval_model = None
    model_start = None
    for j in range(len(tokens), 0, -1):
        candidate = "_".join(tokens[j - 1 :])
        if candidate in KNOWN_MODEL_IDS:
            eval_model = candidate
            model_start = j - 1
            break
    if eval_model is None or model_start is None:
        # No recognizable model suffix; treat whole stem as left
        return stem, None, None
    # Evaluation template is the token immediately before model
    if model_start - 1 < 0:
        return stem, None, eval_model
    eval_tmpl = tokens[model_start - 1]
    # Left base is everything before eval_tmpl
    left_tokens = tokens[: model_start - 1]
    left_base = "_".join(left_tokens) if left_tokens else ""
    return left_base, eval_tmpl, eval_model


def parse_eval_left_part(left: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Parse the left part (before '__') of an evaluation filename.

    Expected: <combo>_<draft_template>_<model>[_<essay_template>]
    Where <model> is one of KNOWN_MODEL_IDS (may contain underscores or hyphens),
    and draft/essay templates contain no underscores.

    Returns (combo_id, draft_template, model_id, essay_template)
    Any element may be None if not parsed.
    """
    combo_id = parse_combo_id(left)
    if not combo_id:
        return None, None, None, None
    prefix = combo_id + "_"
    if not left.startswith(prefix):
        return combo_id, None, None, None
    rest = left[len(prefix):]
    parts = rest.split("_") if rest else []
    if not parts:
        return combo_id, None, None, None
    # Allow multi-token draft templates that are known to include underscores
    draft_tmpl = None
    start_idx = 0
    # Try known multi-token templates first
    for dt in sorted(KNOWN_DRAFT_TEMPLATES, key=lambda s: -len(s)):
        if rest == dt or rest.startswith(dt + "_"):
            draft_tmpl = dt
            start_idx = len(dt.split("_"))
            break
    if draft_tmpl is None:
        draft_tmpl = parts[0]
        start_idx = 1
    # Find the longest model match starting at parts[1]
    model_id = None
    model_end = None
    for j in range(len(parts), start_idx, -1):
        candidate = "_".join(parts[start_idx:j])
        if candidate in KNOWN_MODEL_IDS:
            model_id = candidate
            model_end = j
            break
    essay_tmpl = parts[model_end] if (model_end is not None and model_end < len(parts)) else None
    return combo_id, draft_tmpl, model_id, essay_tmpl


def parse_combo_id(stem: str) -> Optional[str]:
    """Extract combo id from a filename stem.

    Supports:
    - combo_v1_<12 hex>
    - combo_<3 digits>
    Returns the combo id token (e.g., 'combo_v1_1f0b4806fc4a' or 'combo_007').
    """
    import re
    m = re.match(r"^(combo_v1_[0-9a-f]{12})", stem)
    if m:
        return m.group(1)
    m = re.match(r"^(combo_\d{3})", stem)
    if m:
        return m.group(1)
    return None


@dataclass
class CreatedRefs:
    draft_doc_id: Optional[str]
    essay_doc_id: str


def backfill_from_evals(
    *,
    db_path: Path,
    docs_root: Path,
    gen_root: Path,
    eval_root: Path,
    run_id: str,
    dry_run: bool = False,
) -> None:
    idx = SQLiteDocumentsIndex(db_path, docs_root)
    idx.init_maybe_create_tables()

    eval_dir = eval_root / "evaluation_responses"
    essay_dir = gen_root / "essay_responses"
    gen_dir = gen_root / "generation_responses"
    draft_dir = gen_root / "draft_responses"
    links_dir = gen_root / "links_responses"

    # Pre-index available response files by stem
    essay_files: Dict[str, Path] = {stem_of(p): p for p in essay_dir.glob("*.txt")}
    gen_files: Dict[str, Path] = {stem_of(p): p for p in gen_dir.glob("*.txt")}

    # Caches of created docs to avoid duplicates
    created_essays: Dict[str, CreatedRefs] = {}  # base_essay_stem -> CreatedRefs
    created_drafts_by_combo: Dict[str, str] = {}  # combo_id -> draft_doc_id

    # Stats
    total_eval_files = 0
    selected_eval_files = 0
    skipped_bar_in_name = 0
    skipped_parse_fail = 0
    skipped_missing_source = 0
    created_evals = 0
    created_essays_count = 0
    created_drafts_count = 0
    parse_fail_examples: list[str] = []
    missing_source_examples: list[str] = []

    def ensure_draft_for_parts(combo_id: Optional[str], draft_tmpl: Optional[str], model_id: Optional[str]) -> Optional[str]:
        nonlocal created_drafts_count
        # Strict: require exact parts
        if not (combo_id and draft_tmpl and model_id):
            return None
        key = f"{combo_id}_{draft_tmpl}_{model_id}"
        if key in created_drafts_by_combo:
            return created_drafts_by_combo[key]
        # Find an exact draft or link response for these parts
        draft_path: Optional[Path] = None
        cand = draft_dir / f"{key}.txt"
        if cand.exists():
            draft_path = cand
        else:
            cand2 = links_dir / f"{key}.txt"
            if cand2.exists():
                draft_path = cand2
        if draft_path is None:
            return None
        content = read_text(draft_path)
        if content is None:
            return None
        # Minimal identifiers
        template_id = "unknown"
        model_id = "unknown"
        logical = compute_logical_key_id_draft(combo_id, template_id, model_id)
        attempt_key = f"draft:{draft_path.name}"
        doc_id = new_doc_id(logical, run_id, attempt_key)
        doc_base = make_doc_dir(docs_root, "draft", logical, doc_id)
        if not dry_run:
            write_text(doc_base / "raw.txt", content)
            write_text(doc_base / "parsed.txt", content)
            meta = {
                "source_file": str(draft_path),
            }
            write_text(doc_base / "metadata.json", __import__("json").dumps(meta, ensure_ascii=False, indent=2))
            idx.insert_document(
                DocumentRow(
                    doc_id=doc_id,
                    logical_key_id=logical,
                    stage="draft",
                    task_id=f"{combo_id}_auto_draft",
                    doc_dir=str(Path("draft") / logical / doc_id),
                    parent_doc_id=None,
                    template_id=template_id,
                    model_id=model_id,
                    run_id=run_id,
                    status="ok",
                    raw_chars=len(content),
                    parsed_chars=len(content),
                    meta_small=meta,
                )
            )
        created_drafts_by_combo[key] = doc_id
        created_drafts_count += 1
        return doc_id

    def ensure_essay_for_parts(combo_id: str, draft_tmpl: str, model_id: str, essay_tmpl: Optional[str]) -> CreatedRefs:
        nonlocal created_essays_count
        # Construct strict stems to look up
        key_base = f"{combo_id}_{draft_tmpl}_{model_id}"
        key_essay = f"{key_base}_{essay_tmpl}" if essay_tmpl else key_base
        cache_key = key_essay
        if cache_key in created_essays:
            return created_essays[cache_key]
        # Exact essay match first (with or without essay_tmpl)
        src_path: Optional[Path] = essay_files.get(key_essay) or essay_files.get(key_base)
        created_from = "essay_responses"
        template_id = "unknown"
        # Strict fallback: legacy generation exact key_base only
        if src_path is None:
            gen_cand = gen_files.get(key_base)
            if gen_cand is not None:
                src_path = gen_cand
                created_from = "generation_responses"
                template_id = "parsed-from-links-v1"
        # Additional fallback: reuse exact draft/links response content as essay
        if src_path is None:
            draft_cand = draft_dir / f"{key_base}.txt"
            link_cand = links_dir / f"{key_base}.txt"
            if draft_cand.exists():
                src_path = draft_cand
                created_from = "draft_responses"
                template_id = "parsed-from-links-v1"
            elif link_cand.exists():
                src_path = link_cand
                created_from = "links_responses"
                template_id = "parsed-from-links-v1"
        if src_path is None:
            raise FileNotFoundError(f"No essay or generation response for key={key_essay}")
        content = read_text(src_path) or ""
        # Create draft parent if available
        draft_doc_id = ensure_draft_for_parts(combo_id, draft_tmpl, model_id)
        logical = compute_logical_key_id_essay(draft_doc_id or combo_id, template_id, "unknown")
        attempt_key = f"essay:{src_path.name}"
        doc_id = new_doc_id(logical, run_id, attempt_key)
        doc_base = make_doc_dir(docs_root, "essay", logical, doc_id)
        if not dry_run:
            write_text(doc_base / "raw.txt", content)
            write_text(doc_base / "parsed.txt", content)
            meta = {
                "source_file": str(src_path),
                "created_from": created_from,
                "parent_task_id": f"{combo_id}_auto_draft" if draft_doc_id else None,
            }
            write_text(doc_base / "metadata.json", __import__("json").dumps(meta, ensure_ascii=False, indent=2))
            idx.insert_document(
                DocumentRow(
                    doc_id=doc_id,
                    logical_key_id=logical,
                    stage="essay",
                    task_id=key_essay,
                    doc_dir=str(Path("essay") / logical / doc_id),
                    parent_doc_id=draft_doc_id,
                    template_id=template_id,
                    model_id=model_id,
                    run_id=run_id,
                    status="ok",
                    raw_chars=len(content),
                    parsed_chars=len(content),
                    meta_small=meta,
                )
            )
        ref = CreatedRefs(draft_doc_id=draft_doc_id, essay_doc_id=doc_id)
        created_essays[cache_key] = ref
        created_essays_count += 1
        return ref

    # Walk evaluations (dedupe by version: keep the highest _vN/-vN per base)
    selected: Dict[str, Path] = {}
    selected_ver: Dict[str, int] = {}
    for ev_path in eval_dir.glob("*.txt"):
        # Skip filenames containing '|' per policy
        total_eval_files += 1
        if '|' in ev_path.name:
            skipped_bar_in_name += 1
            continue
        stem = stem_of(ev_path)
        base_no_ver, ver = split_version(stem)
        cur_ver = selected_ver.get(base_no_ver)
        if cur_ver is None or (ver or 0) > cur_ver:
            selected[base_no_ver] = ev_path
            selected_ver[base_no_ver] = (ver or 0)

    selected_eval_files = len(selected)
    for base_no_ver, ev_path in sorted(selected.items(), key=lambda kv: kv[1].name):
        stem = stem_of(ev_path)
        # Parse on the versionless stem for template/model parts
        # Extract eval_left_part without assuming '__' exists
        left_base, eval_tmpl_like, eval_model_like = split_eval_stem(base_no_ver)
        # Parse strictly: combo + draft_template + model (+ optional essay_template)
        combo_id, draft_tmpl, model_id, essay_tmpl = parse_eval_left_part(left_base)
        if not (combo_id and draft_tmpl and model_id):
            # Strict matching requires these parts
            skipped_parse_fail += 1
            if len(parse_fail_examples) < 10:
                parse_fail_examples.append(base_no_ver)
            continue
        try:
            ref = ensure_essay_for_parts(combo_id, draft_tmpl, model_id, essay_tmpl)
        except FileNotFoundError:
            # Skip if no corresponding essay/gen content
            skipped_missing_source += 1
            if len(missing_source_examples) < 10:
                missing_source_examples.append(f"{combo_id}_{draft_tmpl}_{model_id} (essay_tmpl={essay_tmpl or '-'})")
            continue
        ev_content = read_text(ev_path) or ""
        logical = compute_logical_key_id_evaluation(ref.essay_doc_id, eval_tmpl_like or "unknown", eval_model_like or "unknown")
        attempt_key = f"evaluation:{ev_path.name}"
        doc_id = new_doc_id(logical, run_id, attempt_key)
        doc_base = make_doc_dir(docs_root, "evaluation", logical, doc_id)
        if not dry_run:
            write_text(doc_base / "raw.txt", ev_content)
            write_text(doc_base / "parsed.txt", ev_content)
            meta = {
                "source_file": str(ev_path),
                "parent_doc_id": ref.essay_doc_id,
            }
            write_text(doc_base / "metadata.json", __import__("json").dumps(meta, ensure_ascii=False, indent=2))
            idx.insert_document(
                DocumentRow(
                    doc_id=doc_id,
                    logical_key_id=logical,
                    stage="evaluation",
                    # Use versionless stem as evaluation task_id
                    task_id=base_no_ver,
                    doc_dir=str(Path("evaluation") / logical / doc_id),
                    parent_doc_id=ref.essay_doc_id,
                    template_id=eval_tmpl_like or "unknown",
                    model_id=eval_model_like or "unknown",
                    run_id=run_id,
                    status="ok",
                    raw_chars=len(ev_content),
                    parsed_chars=len(ev_content),
                    meta_small=meta,
                )
            )
        created_evals += 1

    # Summary
    print(f"eval_files_total={total_eval_files}")
    print(f"eval_files_selected_latest_version={selected_eval_files}")
    print(f"eval_created={created_evals}")
    print(f"essays_created={created_essays_count}")
    print(f"drafts_created={created_drafts_count}")
    print(f"skipped_bar_in_name={skipped_bar_in_name}")
    print(f"skipped_parse_fail={skipped_parse_fail}")
    print(f"skipped_missing_source={skipped_missing_source}")
    if parse_fail_examples:
        print("parse_fail_examples:")
        for ex in parse_fail_examples:
            print(f"  - {ex}")
    if missing_source_examples:
        print("missing_source_examples:")
        for ex in missing_source_examples:
            print(f"  - {ex}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Backfill DB/docs starting from evaluation files")
    p.add_argument("--db", type=Path, default=Path("data/db/documents.sqlite"))
    p.add_argument("--docs-root", type=Path, default=Path("data/docs"))
    p.add_argument("--gen-root", type=Path, default=Path("data/3_generation"))
    p.add_argument("--eval-root", type=Path, default=Path("data/4_evaluation"))
    p.add_argument("--run-id", type=str, default=datetime.now(UTC).strftime("evalbf-%Y%m%d-%H%M%S%z"))
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    backfill_from_evals(
        db_path=args.db,
        docs_root=args.docs_root,
        gen_root=args.gen_root,
        eval_root=args.eval_root,
        run_id=args.run_id,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
