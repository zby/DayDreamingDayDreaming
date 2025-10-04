"""Report (and optionally fix) unexpected copy-mode generations across stages.

Rules enforced here:
- Drafts must never be copy mode.
- Evaluations must never be copy mode.
- Essays may be copy mode only when the essay template ID begins with
  ``parsed-from-links``.

Pass `--fix` to rewrite offending metadata files in-place, setting ``mode`` to
``llm`` and clearing legacy ``source_file`` fields. Draft and evaluation fixes
also drop any stray ``parent_gen_id`` left over from copy mode.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

STAGES = ("draft", "essay", "evaluation")


def _load_metadata(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except Exception as exc:  # pragma: no cover - defensive logging
        print(f"⚠️  failed to read {path}: {exc}")
        return None


def _write_metadata(path: Path, meta: dict) -> None:
    path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def _iter_stage_metadata(stage: str) -> Iterable[tuple[Path, dict]]:
    root = Path("data/gens") / stage
    if not root.exists():
        return
    for meta_path in root.glob("*/metadata.json"):
        meta = _load_metadata(meta_path)
        if meta is None:
            continue
        yield meta_path, meta


def _is_expected_essay_copy(meta: dict) -> bool:
    template = str(meta.get("template_id") or "").strip().lower()
    return template.startswith("parsed-from-links-") or template == "parsed-from-links-v1"


def check_stage(stage: str, *, fix: bool) -> int:
    violations = 0
    for meta_path, meta in _iter_stage_metadata(stage):
        mode = str(meta.get("mode") or "").strip().lower()
        gen_id = meta_path.parent.name

        if stage == "essay":
            if mode == "copy" and not _is_expected_essay_copy(meta):
                violations += 1
                print(
                    f"UNEXPECTED COPY\tstage={stage}\tgen_id={gen_id}\t"
                    f"template={meta.get('template_id')}\tparent={meta.get('parent_gen_id')}"
                )
                if fix:
                    meta["mode"] = "llm"
                    meta.pop("source_file", None)
                    _write_metadata(meta_path, meta)
        else:  # draft or evaluation
            if mode == "copy":
                violations += 1
                print(
                    f"INVALID COPY\tstage={stage}\tgen_id={gen_id}\t"
                    f"template={meta.get('template_id')}\tparent={meta.get('parent_gen_id')}"
                )
                if fix:
                    meta["mode"] = "llm"
                    meta.pop("source_file", None)
                    meta.pop("parent_gen_id", None)
                    _write_metadata(meta_path, meta)
    return violations


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fix", action="store_true", help="Rewrite offending metadata in-place")
    args = parser.parse_args()

    total_violations = 0
    for stage in STAGES:
        total_violations += check_stage(stage, fix=args.fix)

    if total_violations:
        print(f"\nFound {total_violations} copy-mode violations across gens")
        if args.fix:
            print("Updated metadata for offending partitions; rerun the report to confirm.")
    else:
        print("No copy-mode violations detected")


if __name__ == "__main__":
    main()
