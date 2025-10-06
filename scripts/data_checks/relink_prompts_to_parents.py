"""Verify prompts reference their parent generations and repair metadata.

Supports essay and evaluation stages:

- Essay prompts should embed the parent draft's parsed text.
- Evaluation prompts should embed the parent essay's parsed text.

When the prompt contains exactly one parent parsed text, the script can update
``parent_gen_id`` (dry-run by default, use ``--apply`` to persist).
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
import shutil


PARENT_STAGE = {
    "essay": "draft",
    "evaluation": "essay",
}

DEFAULT_STAGES = tuple(PARENT_STAGE.keys())


@dataclass
class Result:
    stage: str
    gen_id: str
    status: str
    message: str
    matched_parent: str | None = None
    match_mode: str | None = None


def _load_text(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return None


def _load_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _parent_texts(data_root: Path, stage: str) -> Dict[str, str]:
    texts: Dict[str, str] = {}
    stage_dir = data_root / "gens" / stage
    if not stage_dir.exists():
        return texts
    for parsed_path in sorted(stage_dir.glob("*/parsed.txt")):
        text = _load_text(parsed_path)
        if text:
            texts[parsed_path.parent.name] = text
    return texts


def _iter_generations(data_root: Path, stage: str) -> Iterable[Tuple[str, Path, dict | None, str | None]]:
    stage_dir = data_root / "gens" / stage
    if not stage_dir.exists():
        return []
    for meta_path in sorted(stage_dir.glob("*/metadata.json")):
        gen_id = meta_path.parent.name
        meta = _load_json(meta_path)
        prompt = _load_text(meta_path.parent / "prompt.txt")
        yield gen_id, meta_path, meta, prompt


def _find_matches(
    *,
    mode: str,
    prompt: str,
    parsed: str | None,
    parent_texts: Dict[str, str],
) -> List[str]:
    matches: List[str] = []
    normalized_parsed = (parsed or "").strip()
    for parent_id, text in parent_texts.items():
        if not text:
            continue
        stripped = text.strip()
        if mode == "copy":
            if normalized_parsed and (text == parsed or stripped == normalized_parsed):
                matches.append(parent_id)
        else:
            if text in prompt or (stripped and stripped in prompt):
                matches.append(parent_id)
                continue
            if normalized_parsed and stripped == normalized_parsed:
                matches.append(parent_id)
    return matches


def _clear_generated_files(gen_dir: Path) -> list[str]:
    removed: list[str] = []
    for name in ("prompt.txt", "raw.txt", "parsed.txt", "raw_metadata.json", "parsed_metadata.json"):
        path = gen_dir / name
        if path.exists():
            try:
                path.unlink()
                removed.append(name)
            except OSError:
                pass
    return removed


def relink_prompts_to_parents(
    *,
    data_root: Path,
    stages: Iterable[str],
    apply: bool,
    limit: int | None,
) -> Tuple[List[Result], dict[str, int]]:
    results: List[Result] = []
    stats = {
        "parent_updates": 0,
        "cleared": 0,
        "pruned": 0,
    }

    for stage in stages:
        parent_stage = PARENT_STAGE.get(stage)
        if not parent_stage:
            results.append(
                Result(
                    stage, "*", "skip", f"no parent stage configured for {stage}",
                )
            )
            continue

        parent_texts = _parent_texts(data_root, parent_stage)

        for gen_id, meta_path, meta, prompt in _iter_generations(data_root, stage):
            gen_dir = meta_path.parent
            if meta is None:
                results.append(Result(stage, gen_id, "error", "metadata missing/unreadable"))
                continue

            if not prompt:
                results.append(Result(stage, gen_id, "skip", "prompt.txt missing"))
                continue

            current_parent = str(meta.get("parent_gen_id") or "").strip() or None
            current_text = parent_texts.get(current_parent) if current_parent else None
            mode = str(meta.get("mode") or "").strip().lower()
            parsed = _load_text(gen_dir / "parsed.txt")

            if mode == "copy":
                if current_text and parsed and (
                    current_text == parsed or current_text.strip() == parsed.strip()
                ):
                    results.append(
                        Result(stage, gen_id, "ok", "parsed matches current parent", current_parent)
                    )
                    continue
            else:
                if current_text and (
                    (current_text in prompt)
                    or (current_text.strip() and current_text.strip() in prompt)
                    or (parsed and current_text.strip() == parsed.strip())
                ):
                    results.append(
                        Result(stage, gen_id, "ok", "prompt contains current parent", current_parent)
                    )
                    continue

            matches = _find_matches(
                mode=mode,
                prompt=prompt,
                parsed=parsed,
                parent_texts=parent_texts,
            )

            if not matches:
                detail = "parsed does not match any parent" if mode == "copy" else "no parent text found in prompt"
                if apply:
                    removed_files = _clear_generated_files(gen_dir)
                    origin = str(meta.get("origin_cohort_id") or "").strip() or None
                    cohort_exists = bool(origin) and (data_root / "cohorts" / origin).exists()
                    if not cohort_exists:
                        try:
                            shutil.rmtree(gen_dir, ignore_errors=True)
                        finally:
                            stats["pruned"] += 1
                        results.append(
                            Result(stage, gen_id, "pruned", "removed empty generation", matched_parent=None, match_mode=None)
                        )
                        if limit is not None and stats["pruned"] >= limit:
                            return results, stats
                    else:
                        stats["cleared"] += 1
                        results.append(
                            Result(stage, gen_id, "cleared", f"removed files {removed_files}", matched_parent=None, match_mode=None)
                        )
                else:
                    origin = str(meta.get("origin_cohort_id") or "").strip() or None
                    cohort_exists = bool(origin) and (data_root / "cohorts" / origin).exists()
                    label = "would_prune" if not cohort_exists else "would_clear"
                    results.append(
                        Result(stage, gen_id, label, detail + ("; apply to remove" if not cohort_exists else "; apply to clear files"), current_parent)
                    )
                continue

            if len(matches) > 1:
                results.append(
                    Result(
                        stage,
                        gen_id,
                        "ambiguous",
                        f"multiple parent matches: {matches[:5]}" + ("..." if len(matches) > 5 else ""),
                        current_parent,
                    )
                )
                continue

            match = matches[0]

            mode_label = "parsed_match" if mode == "copy" else "prompt_search"

            if current_parent:
                change_msg = f"parent {'changed' if apply else 'would change'}: {current_parent} -> {match}"
                results.append(
                    Result(
                        stage,
                        gen_id,
                        "fixed" if apply else "would_fix",
                        change_msg,
                        matched_parent=match,
                        match_mode=mode_label,
                    )
                )
            else:
                add_msg = "parent updated" if apply else "would add"
                results.append(
                    Result(
                        stage,
                        gen_id,
                        "fixed" if apply else "would_fix",
                        add_msg,
                        matched_parent=match,
                        match_mode=mode_label,
                    )
                )

            if apply:
                meta["parent_gen_id"] = match
                meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
                stats["parent_updates"] += 1
                if limit is not None and stats["parent_updates"] >= limit:
                    return results, stats

    return results, stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="Path to data root (default: data)",
    )
    parser.add_argument(
        "--stages",
        nargs="*",
        default=list(DEFAULT_STAGES),
        help="Stages to inspect (default: evaluation essay)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Update parent_gen_id when a unique match is found",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on number of metadata updates",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    stages = args.stages or list(DEFAULT_STAGES)
    stages = [stage for stage in stages if stage in PARENT_STAGE]

    results, stats = relink_prompts_to_parents(
        data_root=args.data_root,
        stages=stages,
        apply=args.apply,
        limit=args.limit,
    )

    change_count = {stage: 0 for stage in PARENT_STAGE}
    add_count = {stage: 0 for stage in PARENT_STAGE}
    counts = {}

    for res in results:
        counts[res.status] = counts.get(res.status, 0) + 1

        if res.status != "ok":
            line = f"{res.status.upper():9s} {res.stage}/{res.gen_id}"
            if res.matched_parent:
                line += f" -> {res.matched_parent}"
            if res.match_mode:
                line += f" ({res.match_mode})"
            line += f": {res.message}"
            print(line)

        if res.status == "would_fix" and res.stage in change_count:
            if "would change" in res.message:
                change_count[res.stage] += 1
            elif "would add" in res.message:
                add_count[res.stage] += 1

    print("\nSummary:")
    for status, count in sorted(counts.items()):
        print(f"  {status:9s}: {count}")

    if args.apply:
        print(f"  parent updates : {stats['parent_updates']}")
        print(f"  cleared files  : {stats['cleared']}")
        print(f"  pruned dirs    : {stats['pruned']}")
    else:
        print("  applied  : 0 (dry run)")
        for stage in stages:
            if stage not in change_count:
                continue
            c = change_count[stage]
            a = add_count[stage]
            if c or a:
                print(f"    {stage}: would change={c}, would add={a}")


if __name__ == "__main__":
    main()
