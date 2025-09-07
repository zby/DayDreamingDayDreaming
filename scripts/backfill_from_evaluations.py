#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, UTC
from pathlib import Path
from typing import Dict, Optional, Tuple, List
import csv

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


def strip_version_suffix(stem: str) -> str:
    import re
    m = re.match(r"^(.*?)(?:[_-]v\d+)$", stem)
    return m.group(1) if m else stem


def extract_candidate_lines_from_prompt(prompt: str) -> Optional[List[str]]:
    lines = prompt.splitlines()
    # Prefer explicit marker (case-insensitive)
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
    # Fallback: use first markdown heading line and next non-empty line
    for i, line in enumerate(lines):
        if line.strip().startswith('## '):
            out: List[str] = [line.strip()]
            for j in range(i + 1, len(lines)):
                s = lines[j].strip()
                if s:
                    out.append(s)
                    break
            if len(out) == 2:
                return out
            break
    return None


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
    essay_raw_dir = gen_root / "essay_responses_raw"
    gen_dir = gen_root / "generation_responses"
    draft_dir = gen_root / "draft_responses"
    draft_raw_dir = gen_root / "draft_responses_raw"
    links_dir = gen_root / "links_responses"
    essay_prompts_dir = gen_root / "essay_prompts"
    gen_prompts_dir = gen_root / "generation_prompts"
    draft_prompts_dir = gen_root / "draft_prompts"
    links_prompts_dir = gen_root / "links_prompts"
    eval_prompts_dir = eval_root / "evaluation_prompts"

    # Load draft template parsers (template_id -> parser name or None)
    draft_template_parsers: dict[str, str|None] = {}
    dt_csv = Path("data/1_raw/draft_templates.csv")
    if dt_csv.exists():
        import csv as _csv1
        with dt_csv.open("r", encoding="utf-8", newline="") as f:
            r = _csv1.DictReader(f)
            for row in r:
                tid = (row.get("template_id") or row.get("id") or "").strip()
                parser = (row.get("parser") or "").strip() or None
                if tid:
                    draft_template_parsers[tid] = parser

    # Load essay template generators (template_id -> generator class)
    essay_template_generators: dict[str, str] = {}
    et_csv = Path("data/1_raw/essay_templates.csv")
    if et_csv.exists():
        import csv as _csv2
        with et_csv.open("r", encoding="utf-8", newline="") as f:
            r = _csv2.DictReader(f)
            for row in r:
                tid = (row.get("template_id") or row.get("id") or "").strip()
                gen = (row.get("generator") or "").strip().lower()
                if tid:
                    essay_template_generators[tid] = gen
    # Known override: v9 is llm
    essay_template_generators.setdefault("creative-synthesis-v9", "llm")

    # Draft parsers registry
    from daydreaming_dagster.utils.draft_parsers import get_draft_parser


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
        input_text = read_text(draft_path)
        if input_text is None:
            return None
        # Optional raw content if available in draft_responses_raw
        raw_content = None
        raw_cand = draft_raw_dir / f"{key}.txt"
        if raw_cand.exists():
            raw_content = read_text(raw_cand)
        # If no dedicated raw exists, persist input as raw to preserve source
        if raw_content is None:
            raw_content = input_text
        # Compute parsed via parser if configured; else copy raw
        parser_name = draft_template_parsers.get(draft_tmpl)
        if parser_name:
            parser_fn = get_draft_parser(parser_name)
            if parser_fn is None:
                raise RuntimeError(f"unknown_draft_parser name={parser_name} for template={draft_tmpl}")
            parsed_content = parser_fn(raw_content or "")
        else:
            parsed_content = raw_content or ""
        # Use parsed identifiers for stable logical grouping
        template_id = draft_tmpl
        gen_model_id = model_id
        logical = compute_logical_key_id_draft(combo_id, template_id, gen_model_id)
        attempt_key = f"draft:{draft_path.name}"
        doc_id = new_doc_id(logical, run_id, attempt_key)
        doc_base = make_doc_dir(docs_root, "draft", logical, doc_id)
        if not dry_run:
            if raw_content is not None:
                write_text(doc_base / "raw.txt", raw_content)
            write_text(doc_base / "parsed.txt", parsed_content)
            # Write prompt.txt when available from corresponding prompts dir
            prompt_stem = key
            pr = draft_prompts_dir / f"{prompt_stem}.txt"
            if not pr.exists():
                pr = links_prompts_dir / f"{prompt_stem}.txt"
            if pr.exists():
                try:
                    ptxt = read_text(pr)
                    if ptxt is not None:
                        write_text(doc_base / "prompt.txt", ptxt)
                except Exception:
                    pass
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
                    model_id=gen_model_id,
                    run_id=run_id,
                    status="ok",
                    raw_chars=len(raw_content) if raw_content is not None else None,
                    parsed_chars=len(parsed_content),
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
        matched_key: Optional[str] = None
        src_path: Optional[Path] = None
        if key_essay in essay_files:
            src_path = essay_files[key_essay]
            matched_key = key_essay
        elif key_base in essay_files:
            src_path = essay_files[key_base]
            matched_key = key_base
        created_from = "essay_responses" if src_path is not None else ""
        template_id: Optional[str] = None
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
            gen_cand_path = gen_dir / f"{key_base}.txt"
            present = [("draft", draft_cand.exists()), ("links", link_cand.exists()), ("gen", gen_cand_path.exists())]
            if sum(1 for _, ok in present if ok) > 1:
                raise RuntimeError(f"ambiguous_sources for {key_base}: " + ",".join([k for k, ok in present if ok]))
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
        parsed_content = read_text(src_path) or ""
        # Determine essay template_id strictly (never 'unknown')
        if created_from == "essay_responses":
            if matched_key == key_essay and essay_tmpl:
                template_id = essay_tmpl
            elif matched_key == key_base:
                # Older naming omitted essay template; default to draft template name
                template_id = draft_tmpl
        elif created_from in ("generation_responses", "draft_responses", "links_responses"):
            template_id = "parsed-from-links-v1"
        if not template_id:
            raise RuntimeError(f"No essay template_id derived for {key_essay}")
        # Optional raw content when we sourced from essay_responses or draft_responses
        raw_content = None
        if created_from == "essay_responses":
            # Try raw for key_essay, then key_base
            for rc in (essay_raw_dir / f"{key_essay}.txt", essay_raw_dir / f"{key_base}.txt"):
                if rc.exists():
                    raw_content = read_text(rc)
                    break
        elif created_from == "draft_responses":
            rc = draft_raw_dir / f"{key_base}.txt"
            if rc.exists():
                raw_content = read_text(rc)
        # Create draft parent if available
        draft_doc_id = ensure_draft_for_parts(combo_id, draft_tmpl, model_id)
        # If we sourced from generation_responses and no draft exists, synthesize a draft from this file
        if created_from == "generation_responses" and draft_doc_id is None:
            # Mirror ensure_draft_for_parts but using the generation file as parsed content
            nonlocal created_drafts_count
            key = key_base
            # Optional prompt from generation_prompts
            gen_prompt = gen_prompts_dir / f"{key}.txt"
            draft_parsed = parsed_content
            draft_raw = None  # no raw counterpart for generation_responses
            template_id_draft = draft_tmpl
            gen_model_id = model_id
            logical_draft = compute_logical_key_id_draft(combo_id, template_id_draft, gen_model_id)
            attempt_key_draft = f"draft_from_gen:{src_path.name}"
            draft_doc_id = new_doc_id(logical_draft, run_id, attempt_key_draft)
            draft_base = make_doc_dir(docs_root, "draft", logical_draft, draft_doc_id)
            if not dry_run:
                if draft_raw is not None:
                    write_text(draft_base / "raw.txt", draft_raw)
                write_text(draft_base / "parsed.txt", draft_parsed)
                if gen_prompt.exists():
                    ptxt = read_text(gen_prompt)
                    if ptxt is not None:
                        write_text(draft_base / "prompt.txt", ptxt)
                meta_d = {"source_file": str(src_path), "synthesized_from": "generation_responses"}
                write_text(draft_base / "metadata.json", __import__("json").dumps(meta_d, ensure_ascii=False, indent=2))
                idx.insert_document(
                    DocumentRow(
                        doc_id=draft_doc_id,
                        logical_key_id=logical_draft,
                        stage="draft",
                        task_id=f"{combo_id}_auto_draft",
                        doc_dir=str(Path("draft") / logical_draft / draft_doc_id),
                        parent_doc_id=None,
                        template_id=template_id_draft,
                        model_id=gen_model_id,
                        run_id=run_id,
                        status="ok",
                        raw_chars=None,
                        parsed_chars=len(draft_parsed),
                        meta_small=meta_d,
                    )
                )
            created_drafts_by_combo[key] = draft_doc_id
            created_drafts_count += 1
        logical = compute_logical_key_id_essay(draft_doc_id or combo_id, template_id, model_id)
        attempt_key = f"essay:{src_path.name}"
        doc_id = new_doc_id(logical, run_id, attempt_key)
        doc_base = make_doc_dir(docs_root, "essay", logical, doc_id)
        if not dry_run:
            if raw_content is not None:
                write_text(doc_base / "raw.txt", raw_content)
            write_text(doc_base / "parsed.txt", parsed_content)
            # Write prompt.txt if available for essays
            # Prefer essay_prompts for essay_responses; else generation or draft/link prompts based on created_from
            prompt_candidates = []
            if created_from == "essay_responses":
                prompt_candidates.append(essay_prompts_dir / f"{key_essay}.txt")
                if key_essay != key_base:
                    prompt_candidates.append(essay_prompts_dir / f"{key_base}.txt")
            if created_from == "generation_responses":
                prompt_candidates.append(gen_prompts_dir / f"{key_base}.txt")
            if created_from in ("draft_responses", "links_responses"):
                prompt_candidates.append(draft_prompts_dir / f"{key_base}.txt")
                prompt_candidates.append(links_prompts_dir / f"{key_base}.txt")
            for pr in prompt_candidates:
                if pr.exists():
                    ptxt = read_text(pr)
                    if ptxt is not None:
                        write_text(doc_base / "prompt.txt", ptxt)
                        break
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
                    raw_chars=len(raw_content) if raw_content is not None else None,
                    parsed_chars=len(parsed_content),
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

    # Special-case skip list for problematic evaluation tasks (by versionless task_id prefix)
    SKIP_EVAL_TASK_PREFIXES = {
        # User-requested skip; note: allow both 'flash' and possible truncated 'flas'
        "combo_v1_13c725cf02fa_creative-synthesis-v7_gemini_2.5_flash",
        "combo_v1_13c725cf02fa_creative-synthesis-v7_gemini_2.5_flas",
    }

    selected_eval_files = len(selected)
    for base_no_ver, ev_path in sorted(selected.items(), key=lambda kv: kv[1].name):
        # Skip explicitly flagged tasks
        if any(base_no_ver.startswith(pref) for pref in SKIP_EVAL_TASK_PREFIXES):
            continue
        stem = stem_of(ev_path)
        # Parse on the versionless stem for template/model parts
        # Extract eval_left_part without assuming '__' exists
        left_base, eval_tmpl_like, eval_model_like = split_eval_stem(base_no_ver)
        # Parse strictly: combo + draft_template + model (+ optional essay_template)
        combo_id, draft_tmpl, model_id, essay_tmpl = parse_eval_left_part(left_base)
        if not (combo_id and draft_tmpl and model_id and eval_tmpl_like and eval_model_like):
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
        logical = compute_logical_key_id_evaluation(ref.essay_doc_id, eval_tmpl_like, eval_model_like)
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
            # Write evaluation prompt if present (try exact then versionless)
            ev_prompt = eval_prompts_dir / f"{ev_path.stem}.txt"
            if not ev_prompt.exists():
                ev_prompt = eval_prompts_dir / f"{base_no_ver}.txt"
            if ev_prompt.exists():
                ptxt = read_text(ev_prompt)
                if ptxt is not None:
                    write_text(doc_base / "prompt.txt", ptxt)
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
    # Post-backfill validation: essay generation semantics
    try:
        import sys as _sys
        from pathlib import Path as _Path
        # Ensure we can import the validator from the scripts directory
        _this_dir = _Path(__file__).parent
        if str(_this_dir) not in _sys.path:
            _sys.path.append(str(_this_dir))
        from validate_essay_generation_semantics import validate as _validate

        res = _validate(args.db, args.docs_root, _Path("data/1_raw/essay_templates.csv"))
        print(f"essay_semantics_checked={res['checked']}")
        print(f"essay_semantics_copy_ok={res['copy_ok']}")
        print(f"essay_semantics_llm_ok={res['llm_ok']}")
        print(f"essay_semantics_skipped_unknown={res['skipped_unknown']}")
        print(f"essay_semantics_violations={len(res['violations'])}")
        if res.get('violations'):
            for v in res['violations'][:10]:
                print(f"VIOLATION: essay={v.essay_doc_id} tmpl={v.template_id} gen={v.generator} parent={v.parent_doc_id} problem={v.problem} detail={v.detail}")
        # Fail if any unknown/skipped or violations are present
        if res.get('skipped_unknown', 0) > 0 or len(res.get('violations', [])) > 0:
            return 2
    except Exception as _ex:
        print(f"warning: validation step failed to run: {_ex}")
        return 1

    # Relink evaluations by prompt candidate text when needed (version-independent)
    try:
        # Lightweight inline relinker to avoid import path issues
        idx2 = SQLiteDocumentsIndex(args.db, args.docs_root)
        con2 = idx2.connect()
        con2.row_factory = lambda c, r: {c.description[i][0]: r[i] for i in range(len(r))}
        processed = 0
        updated = 0
        for ev in con2.execute("SELECT * FROM documents WHERE stage='evaluation' AND status='ok' ORDER BY created_at").fetchall():
            processed += 1
            ev_dir = idx2.resolve_doc_dir(ev)
            prompt = read_text(ev_dir / 'prompt.txt')
            if not prompt:
                continue
            parent_id = ev.get('parent_doc_id')
            essay_row = con2.execute("SELECT * FROM documents WHERE doc_id=?", (parent_id,)).fetchone() if parent_id else None
            es_ok = False
            if essay_row:
                es_dir = idx2.resolve_doc_dir(essay_row)
                es_parsed = read_text(es_dir / 'parsed.txt')
                if es_parsed and (es_parsed in prompt):
                    es_ok = True
            if es_ok:
                continue
            # Need relink: extract candidates and match essays by left prefix
            cand_lines = extract_candidate_lines_from_prompt(prompt)
            if not cand_lines:
                continue
            base_no_ver = strip_version_suffix(ev['task_id'])
            left, _et, _em = split_eval_stem(base_no_ver)
            if not left:
                continue
            like_prefix = left + '%'
            essays = con2.execute("SELECT * FROM documents WHERE stage='essay' AND status='ok' AND task_id LIKE ? ORDER BY created_at DESC", (like_prefix,)).fetchall()
            matches = []
            for es in essays:
                es_dir2 = idx2.resolve_doc_dir(es)
                es_parsed2 = read_text(es_dir2 / 'parsed.txt') or ''
                if all(line in es_parsed2 for line in cand_lines):
                    matches.append(es)
            if not matches:
                continue
            # Choose the most recent
            new_parent = matches[0]['doc_id']
            if new_parent == parent_id:
                continue
            # Update parent_doc_id and annotate meta
            meta = ev.get('meta_small')
            try:
                meta_obj = json.loads(meta) if isinstance(meta, str) and meta else {}
            except Exception:
                meta_obj = {}
            meta_obj['relinked_reason'] = 'matched_by_prompt_candidate_text'
            with con2:
                con2.execute("UPDATE documents SET parent_doc_id=?, meta_small=? WHERE doc_id=?", (new_parent, json.dumps(meta_obj, ensure_ascii=False), ev['doc_id']))
            updated += 1
        print(f"relink_by_prompt_processed={processed} relink_by_prompt_updated={updated}")
    except Exception as _ex:
        print(f"warning: relink by prompt failed: {_ex}")

    # Post-backfill validation: evaluation prompt contains essay parsed
    try:
        import sys as _sys2
        from pathlib import Path as _Path2
        _this_dir2 = _Path2(__file__).parent
        if str(_this_dir2) not in _sys2.path:
            _sys2.path.append(str(_this_dir2))
        from validate_evaluation_prompt_contains_essay import validate as _validate_eval

        res2 = _validate_eval(args.db, args.docs_root)
        print(f"eval_prompt_checked={res2['checked']}")
        print(f"eval_prompt_ok={res2['ok']}")
        print(f"eval_prompt_violations={len(res2['violations'])}")
        if res2.get('violations'):
            for v in res2['violations'][:10]:
                print(f"EVAL_VIOLATION: eval={v['evaluation_doc_id']} task={v['evaluation_task_id']} problem={v['problem']} detail={v['detail']}")
        if len(res2.get('violations', [])) > 0:
            return 2
    except Exception as _ex:
        print(f"warning: evaluation prompt validation failed to run: {_ex}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
