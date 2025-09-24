from __future__ import annotations

import csv
import json
import re
import secrets
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple


_ID_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyz"
_ID_LENGTH = 16


@dataclass
class FilenameParts:
    combo_id: str
    template_id: str
    llm_model_id: str
    parent_template_id: str
    parent_task_id: str
    replicate: Optional[int]
    stem: str


@dataclass
class EssayFilenameParts(FilenameParts):
    pass


@dataclass
class EvaluationFilenameParts(FilenameParts):
    essay_template_id: str


@dataclass
class StageStats:
    evaluated: int = 0
    converted: int = 0
    skipped: int = 0
    errors: int = 0

    def record_skip(self) -> None:
        self.skipped += 1

    def record_error(self) -> None:
        self.errors += 1

    def record_eval(self) -> None:
        self.evaluated += 1

    def record_convert(self) -> None:
        self.converted += 1


@dataclass
class EssayMigrationResult:
    stats: StageStats
    parent_map: Dict[str, List[str]] = field(default_factory=dict)


class MigrationConfigError(ValueError):
    pass


def _read_csv_column(path: Path, column: str) -> Set[str]:
    if not path.exists():
        raise MigrationConfigError(f"Required CSV not found: {path}")
    values: Set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if column not in reader.fieldnames:  # type: ignore[arg-type]
            raise MigrationConfigError(f"Column '{column}' missing in {path}")
        for row in reader:
            value = (row.get(column) or "").strip()
            if value:
                values.add(value)
    return values



def _match_prefix(stem: str, options: Iterable[str]) -> Optional[str]:
    for candidate in sorted(options, key=len, reverse=True):
        if stem == candidate or stem.startswith(candidate + "_"):
            return candidate
    return None


def _strip_replicate(stem: str) -> Tuple[str, Optional[int]]:
    match = re.search(r"_v(\d+)$", stem)
    if not match:
        return stem, None
    replicate = int(match.group(1))
    base = stem[: match.start()]
    return base, replicate


def _match_suffix(source: str, candidates: Sequence[str]) -> Optional[Tuple[str, str]]:
    for candidate in sorted(candidates, key=len, reverse=True):
        if source == candidate:
            return "", candidate
        suffix = "_" + candidate
        if source.endswith(suffix):
            return source[: -len(suffix)], candidate
    return None


def parse_essay_filename(
    stem: str,
    *,
    combo_ids: Set[str],
    template_ids: Set[str],
    llm_ids: Set[str],
) -> EssayFilenameParts:
    combo_id = _match_prefix(stem, combo_ids)
    if not combo_id:
        raise ValueError(f"Combo id not recognized in essay filename '{stem}'")
    suffix = stem[len(combo_id) :].lstrip("_")
    suffix, replicate = _strip_replicate(suffix)

    template_match = _match_suffix(suffix, sorted(template_ids))
    if not template_match:
        raise ValueError(f"Essay template not recognized in '{stem}'")
    prefix, template_id = template_match
    llm_match = _match_suffix(prefix, sorted(llm_ids))
    if not llm_match:
        raise ValueError(f"Essay LLM not recognized in '{stem}'")
    parent_template_raw, llm_model_id = llm_match
    parent_template_id = parent_template_raw.rstrip("_") or parent_template_raw
    parent_template_id = parent_template_id.rstrip("_")
    if not parent_template_id:
        raise ValueError(f"Essay parent template missing in '{stem}'")

    parent_task_id = f"{combo_id}_{parent_template_id}_{llm_model_id}"
    return EssayFilenameParts(
        combo_id=combo_id,
        template_id=template_id,
        llm_model_id=llm_model_id,
        parent_template_id=parent_template_id,
        parent_task_id=parent_task_id,
        replicate=replicate,
        stem=stem,
    )


def parse_evaluation_filename(
    stem: str,
    *,
    combo_ids: Set[str],
    evaluation_template_ids: Set[str],
    essay_template_ids: Set[str],
    llm_ids: Set[str],
) -> EvaluationFilenameParts:
    combo_id = _match_prefix(stem, combo_ids)
    if not combo_id:
        raise ValueError(f"Combo id not recognized in evaluation filename '{stem}'")
    suffix = stem[len(combo_id) :].lstrip("_")
    suffix, replicate = _strip_replicate(suffix)

    # Evaluation LLM at tail
    llm_match = _match_suffix(suffix, sorted(llm_ids))
    if not llm_match:
        raise ValueError(f"Evaluation LLM not recognized in '{stem}'")
    prefix_after_llm, eval_llm_id = llm_match

    # Evaluation template next
    template_match = _match_suffix(prefix_after_llm, sorted(evaluation_template_ids))
    if not template_match:
        raise ValueError(f"Evaluation template not recognized in '{stem}'")
    prefix_after_template, eval_template_id = template_match

    # Essay template
    essay_match = _match_suffix(prefix_after_template, sorted(essay_template_ids))
    if not essay_match:
        raise ValueError(f"Essay template not recognized in '{stem}'")
    prefix_after_essay, essay_template_id = essay_match

    # Parent (draft) llm
    draft_llm_match = _match_suffix(prefix_after_essay, sorted(llm_ids))
    if not draft_llm_match:
        raise ValueError(f"Parent draft LLM not recognized in '{stem}'")
    parent_template_raw, draft_llm_id = draft_llm_match
    parent_template_id = parent_template_raw.rstrip("_") or parent_template_raw
    parent_template_id = parent_template_id.rstrip("_")
    if not parent_template_id:
        raise ValueError(f"Parent draft template missing in '{stem}'")

    parent_task_id = (
        f"{combo_id}_{parent_template_id}_{draft_llm_id}_{essay_template_id}"
    )

    return EvaluationFilenameParts(
        combo_id=combo_id,
        template_id=eval_template_id,
        llm_model_id=eval_llm_id,
        parent_template_id=parent_template_id,
        parent_task_id=parent_task_id,
        replicate=replicate,
        essay_template_id=essay_template_id,
        stem=stem,
    )


def _generate_gen_id() -> str:
    return "".join(secrets.choice(_ID_ALPHABET) for _ in range(_ID_LENGTH))


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _extract_score(text: str) -> str:
    match = re.search(r"score\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", text, flags=re.IGNORECASE)
    if not match:
        raise ValueError("Evaluation score not found in response text")
    return match.group(1).strip()


def _write_json(path: Path, payload: Dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _relative_source(original: Path, root: Path) -> str:
    try:
        return str(original.relative_to(root))
    except ValueError:
        return str(original)


def _index_existing_by_origin(stage_dir: Path) -> Dict[str, Path]:
    index: Dict[str, Path] = {}
    if not stage_dir.exists():
        return index
    for gen_dir in stage_dir.iterdir():
        if not gen_dir.is_dir():
            continue
        meta_path = gen_dir / "metadata.json"
        if not meta_path.exists():
            continue
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8")) or {}
        except Exception:
            continue
        legacy_fields = meta.get("legacy_fields")
        if isinstance(legacy_fields, dict):
            origin = legacy_fields.get("origin_filename")
            if isinstance(origin, str) and origin:
                index[origin] = gen_dir
    return index


def migrate_legacy_gens(
    *,
    legacy_root: Path,
    target_root: Path,
    dry_run: bool = True,
) -> Dict[str, StageStats]:
    legacy_root = Path(legacy_root)
    target_root = Path(target_root)

    legacy_combo_csv = legacy_root / "combo_mappings.csv"
    if legacy_combo_csv.exists():
        combo_ids = _read_csv_column(legacy_combo_csv, "combo_id")
    else:
        combo_ids = set()

    if not combo_ids:
        raise MigrationConfigError("No combo ids available in legacy combo_mappings.csv")

    essay_templates_csv = target_root / "1_raw" / "essay_templates.csv"
    evaluation_templates_csv = target_root / "1_raw" / "evaluation_templates.csv"
    llm_models_csv = target_root / "1_raw" / "llm_models.csv"

    essay_template_ids = _read_csv_column(essay_templates_csv, "template_id")
    evaluation_template_ids = _read_csv_column(evaluation_templates_csv, "template_id")
    llm_ids = _read_csv_column(llm_models_csv, "id")

    stats: Dict[str, StageStats] = {
        "essay": StageStats(),
        "evaluation": StageStats(),
    }

    essay_result = _migrate_essays(
        legacy_root=legacy_root,
        target_root=target_root,
        combos=combo_ids,
        essay_template_ids=essay_template_ids,
        llm_ids=llm_ids,
        dry_run=dry_run,
    )
    stats["essay"] = essay_result.stats

    _migrate_evaluations(
        legacy_root=legacy_root,
        target_root=target_root,
        combos=combo_ids,
        evaluation_template_ids=evaluation_template_ids,
        essay_template_ids=essay_template_ids,
        llm_ids=llm_ids,
        essay_parent_map=essay_result.parent_map,
        dry_run=dry_run,
        stats=stats["evaluation"],
    )

    return stats


def _migrate_essays(
    *,
    legacy_root: Path,
    target_root: Path,
    combos: Set[str],
    essay_template_ids: Set[str],
    llm_ids: Set[str],
    dry_run: bool,
) -> EssayMigrationResult:
    stats = StageStats()
    parent_map: Dict[str, List[str]] = {}

    prompts_dir = legacy_root / "3_generation" / "essay_prompts"
    responses_dir = legacy_root / "3_generation" / "essay_responses"
    if not prompts_dir.exists() or not responses_dir.exists():
        return EssayMigrationResult(stats=stats, parent_map={})

    target_stage_dir = target_root / "gens" / "essay"
    if not dry_run:
        _ensure_dir(target_stage_dir)
        existing_by_origin = _index_existing_by_origin(target_stage_dir)
    else:
        existing_by_origin = {}

    for prompt_path in sorted(prompts_dir.glob("*.txt")):
        stem = prompt_path.stem
        stats.record_eval()
        try:
            parts = parse_essay_filename(
                stem,
                combo_ids=combos,
                template_ids=essay_template_ids,
                llm_ids=llm_ids,
            )
        except ValueError:
            stats.record_skip()
            continue

        if parts.combo_id not in combos:
            stats.record_skip()
            continue

        response_path = responses_dir / f"{stem}.txt"
        if not response_path.exists():
            stats.record_skip()
            continue

        if dry_run:
            stats.record_convert()
            continue

        prompt_text = prompt_path.read_text(encoding="utf-8")
        response_text = response_path.read_text(encoding="utf-8")

        origin_name = f"{stem}.txt"
        existing_dir = existing_by_origin.get(origin_name)
        if existing_dir is not None:
            dest_dir = existing_dir
            gen_id = existing_dir.name
        else:
            gen_id = _generate_gen_id()
            dest_dir = target_stage_dir / gen_id
            _ensure_dir(dest_dir)
            existing_by_origin[origin_name] = dest_dir

        (dest_dir / "prompt.txt").write_text(prompt_text, encoding="utf-8")
        raw_path = dest_dir / "raw.txt"
        raw_path.write_text(response_text, encoding="utf-8")
        parsed_path = dest_dir / "parsed.txt"
        parsed_path.write_text(response_text, encoding="utf-8")

        source_rel = _relative_source(response_path, legacy_root)

        metadata = {
            "stage": "essay",
            "gen_id": gen_id,
            "mode": "copy",
            "template_id": parts.template_id,
            "llm_model_id": parts.llm_model_id,
            "parent_gen_id": parts.parent_task_id,
            "combo_id": parts.combo_id,
            "parent_task_id": parts.parent_task_id,
            "draft_template": parts.parent_template_id,
            "source_file": source_rel,
            "created_from": "essay_responses",
        }
        if parts.replicate is not None:
            metadata["replicate"] = parts.replicate
        metadata["legacy_fields"] = {"origin_filename": f"{stem}.txt"}

        raw_metadata = {
            "stage": "essay",
            "gen_id": gen_id,
            "mode": "copy",
            "llm_model_id": parts.llm_model_id,
            "combo_id": parts.combo_id,
            "function": "essay_raw",
            "input_mode": "copy",
            "copied_from": source_rel,
            "raw_path": str(raw_path),
        }
        if parts.replicate is not None:
            raw_metadata["replicate"] = parts.replicate

        parsed_metadata = {
            "stage": "essay",
            "gen_id": gen_id,
            "parser_name": "legacy",
            "success": True,
            "error": None,
            "combo_id": parts.combo_id,
            "input_mode": "copy",
            "copied_from": source_rel,
            "parsed_path": str(parsed_path),
            "function": "essay_parsed",
        }
        if parts.replicate is not None:
            parsed_metadata["replicate"] = parts.replicate

        _write_json(dest_dir / "metadata.json", metadata)
        _write_json(dest_dir / "raw_metadata.json", raw_metadata)
        _write_json(dest_dir / "parsed_metadata.json", parsed_metadata)

        base_key = parts.parent_task_id
        full_key = f"{base_key}_{parts.template_id}"
        for key in {base_key, full_key}:
            ids = parent_map.setdefault(key, [])
            if gen_id not in ids:
                ids.append(gen_id)
        stats.record_convert()

    return EssayMigrationResult(stats=stats, parent_map=parent_map)


def _migrate_evaluations(
    *,
    legacy_root: Path,
    target_root: Path,
    combos: Set[str],
    evaluation_template_ids: Set[str],
    essay_template_ids: Set[str],
    llm_ids: Set[str],
    essay_parent_map: Dict[str, List[str]],
    dry_run: bool,
    stats: StageStats,
) -> None:
    prompts_dir = legacy_root / "4_evaluation" / "evaluation_prompts"
    responses_dir = legacy_root / "4_evaluation" / "evaluation_responses"
    if not prompts_dir.exists() or not responses_dir.exists():
        return

    target_stage_dir = target_root / "gens" / "evaluation"
    if not dry_run:
        _ensure_dir(target_stage_dir)
        existing_by_origin = _index_existing_by_origin(target_stage_dir)
    else:
        existing_by_origin = {}

    for prompt_path in sorted(prompts_dir.glob("*.txt")):
        stem = prompt_path.stem
        stats.record_eval()
        try:
            parts = parse_evaluation_filename(
                stem,
                combo_ids=combos,
                evaluation_template_ids=evaluation_template_ids,
                essay_template_ids=essay_template_ids,
                llm_ids=llm_ids,
            )
        except ValueError:
            stats.record_skip()
            continue

        if parts.combo_id not in combos:
            stats.record_skip()
            continue

        response_path = responses_dir / f"{stem}.txt"
        if not response_path.exists():
            stats.record_skip()
            continue

        parent_candidates = essay_parent_map.get(parts.parent_task_id)
        if not parent_candidates:
            suffix = f"_{parts.essay_template_id}"
            if parts.parent_task_id.endswith(suffix):
                base_key = parts.parent_task_id[: -len(suffix)]
                parent_candidates = essay_parent_map.get(base_key)
        parent_gen_id = parent_candidates[0] if parent_candidates else None

        if dry_run:
            stats.record_convert()
            continue

        prompt_text = prompt_path.read_text(encoding="utf-8")
        response_text = response_path.read_text(encoding="utf-8")
        try:
            parsed_score = _extract_score(response_text)
        except ValueError:
            stats.record_error()
            continue

        origin_name = f"{stem}.txt"
        existing_dir = existing_by_origin.get(origin_name)
        if existing_dir is not None:
            dest_dir = existing_dir
            gen_id = existing_dir.name
        else:
            gen_id = _generate_gen_id()
            dest_dir = target_stage_dir / gen_id
            _ensure_dir(dest_dir)
            existing_by_origin[origin_name] = dest_dir

        (dest_dir / "prompt.txt").write_text(prompt_text, encoding="utf-8")
        raw_path = dest_dir / "raw.txt"
        raw_path.write_text(response_text, encoding="utf-8")
        parsed_path = dest_dir / "parsed.txt"
        parsed_path.write_text(parsed_score, encoding="utf-8")

        source_rel = _relative_source(response_path, legacy_root)

        metadata = {
            "stage": "evaluation",
            "gen_id": gen_id,
            "mode": "copy",
            "template_id": parts.template_id,
            "llm_model_id": parts.llm_model_id,
            "combo_id": parts.combo_id,
            "parent_task_id": parts.parent_task_id,
            "essay_template_id": parts.essay_template_id,
            "draft_template": parts.parent_template_id,
            "source_file": source_rel,
            "created_from": "evaluation_responses",
        }
        if parent_gen_id:
            metadata["parent_gen_id"] = parent_gen_id
        if parts.replicate is not None:
            metadata["replicate"] = parts.replicate
        metadata["legacy_fields"] = {"origin_filename": f"{stem}.txt"}

        raw_metadata = {
            "stage": "evaluation",
            "gen_id": gen_id,
            "mode": "copy",
            "llm_model_id": parts.llm_model_id,
            "combo_id": parts.combo_id,
            "function": "evaluation_raw",
            "input_mode": "copy",
            "copied_from": source_rel,
            "raw_path": str(raw_path),
        }
        if parts.replicate is not None:
            raw_metadata["replicate"] = parts.replicate
        if parent_gen_id:
            raw_metadata["parent_gen_id"] = parent_gen_id

        parsed_metadata = {
            "stage": "evaluation",
            "gen_id": gen_id,
            "parser_name": "legacy",
            "success": True,
            "error": None,
            "combo_id": parts.combo_id,
            "input_mode": "copy",
            "copied_from": source_rel,
            "parsed_path": str(parsed_path),
            "function": "evaluation_parsed",
            "score": parsed_score,
        }
        if parts.replicate is not None:
            parsed_metadata["replicate"] = parts.replicate
        if parent_gen_id:
            parsed_metadata["parent_gen_id"] = parent_gen_id

        _write_json(dest_dir / "metadata.json", metadata)
        _write_json(dest_dir / "raw_metadata.json", raw_metadata)
        _write_json(dest_dir / "parsed_metadata.json", parsed_metadata)

        stats.record_convert()


__all__ = [
    "EssayFilenameParts",
    "EvaluationFilenameParts",
    "StageStats",
    "MigrationConfigError",
    "parse_essay_filename",
    "parse_evaluation_filename",
    "migrate_legacy_gens",
]
