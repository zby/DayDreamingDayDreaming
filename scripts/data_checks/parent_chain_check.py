#!/usr/bin/env python3
"""Validate parent-child chains for essay and evaluation generations.

Checks that metadata exists, parent directories exist, and (for evaluations)
that the parent essay's draft still exists. Supersedes the legacy
``find_missing_parents.py`` helper.
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable, List, Optional


@dataclass
class ParentIssue:
    stage: str
    gen_id: str
    issue: str
    detail: str
    metadata_path: str


def _load_metadata(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8")) or {}
    except FileNotFoundError:
        return None
    except json.JSONDecodeError:
        return {}


def _check_essay_parents(data_root: Path) -> List[ParentIssue]:
    issues: List[ParentIssue] = []
    essays_dir = data_root / "gens" / "essay"
    drafts_dir = data_root / "gens" / "draft"
    if not essays_dir.exists():
        return issues

    for essay_dir in essays_dir.iterdir():
        if not essay_dir.is_dir():
            continue
        gen_id = essay_dir.name
        meta_path = essay_dir / "metadata.json"
        metadata = _load_metadata(meta_path)
        if metadata is None:
            issues.append(
                ParentIssue(
                    stage="essay",
                    gen_id=gen_id,
                    issue="metadata_missing",
                    detail="metadata.json not found",
                    metadata_path=str(meta_path),
                )
            )
            continue
        if metadata == {}:
            issues.append(
                ParentIssue(
                    stage="essay",
                    gen_id=gen_id,
                    issue="metadata_invalid",
                    detail="metadata.json is invalid JSON",
                    metadata_path=str(meta_path),
                )
            )
            continue

        parent_id = str(metadata.get("parent_gen_id") or "").strip()
        if not parent_id:
            issues.append(
                ParentIssue(
                    stage="essay",
                    gen_id=gen_id,
                    issue="missing_parent_id",
                    detail="parent_gen_id missing or empty",
                    metadata_path=str(meta_path),
                )
            )
            continue

        draft_dir = drafts_dir / parent_id
        if not draft_dir.exists():
            issues.append(
                ParentIssue(
                    stage="essay",
                    gen_id=gen_id,
                    issue="parent_missing",
                    detail=f"draft {parent_id} not found",
                    metadata_path=str(meta_path),
                )
            )
            continue

        draft_meta = _load_metadata(draft_dir / "metadata.json")
        if draft_meta is None:
            issues.append(
                ParentIssue(
                    stage="essay",
                    gen_id=gen_id,
                    issue="parent_metadata_missing",
                    detail=f"draft {parent_id} metadata.json missing",
                    metadata_path=str(meta_path),
                )
            )
        elif draft_meta == {}:
            issues.append(
                ParentIssue(
                    stage="essay",
                    gen_id=gen_id,
                    issue="parent_metadata_invalid",
                    detail=f"draft {parent_id} metadata.json invalid",
                    metadata_path=str(meta_path),
                )
            )

    return issues


def _check_evaluation_parents(data_root: Path) -> List[ParentIssue]:
    issues: List[ParentIssue] = []
    evaluations_dir = data_root / "gens" / "evaluation"
    essays_dir = data_root / "gens" / "essay"
    drafts_dir = data_root / "gens" / "draft"
    if not evaluations_dir.exists():
        return issues

    for evaluation_dir in evaluations_dir.iterdir():
        if not evaluation_dir.is_dir():
            continue
        gen_id = evaluation_dir.name
        meta_path = evaluation_dir / "metadata.json"
        metadata = _load_metadata(meta_path)
        if metadata is None:
            issues.append(
                ParentIssue(
                    stage="evaluation",
                    gen_id=gen_id,
                    issue="metadata_missing",
                    detail="metadata.json not found",
                    metadata_path=str(meta_path),
                )
            )
            continue
        if metadata == {}:
            issues.append(
                ParentIssue(
                    stage="evaluation",
                    gen_id=gen_id,
                    issue="metadata_invalid",
                    detail="metadata.json is invalid JSON",
                    metadata_path=str(meta_path),
                )
            )
            continue

        parent_id = str(metadata.get("parent_gen_id") or "").strip()
        if not parent_id:
            issues.append(
                ParentIssue(
                    stage="evaluation",
                    gen_id=gen_id,
                    issue="missing_parent_id",
                    detail="parent_gen_id missing or empty",
                    metadata_path=str(meta_path),
                )
            )
            continue

        essay_dir = essays_dir / parent_id
        if not essay_dir.exists():
            issues.append(
                ParentIssue(
                    stage="evaluation",
                    gen_id=gen_id,
                    issue="parent_missing",
                    detail=f"essay {parent_id} not found",
                    metadata_path=str(meta_path),
                )
            )
            continue

        essay_meta = _load_metadata(essay_dir / "metadata.json")
        if essay_meta is None:
            issues.append(
                ParentIssue(
                    stage="evaluation",
                    gen_id=gen_id,
                    issue="parent_metadata_missing",
                    detail=f"essay {parent_id} metadata.json missing",
                    metadata_path=str(meta_path),
                )
            )
        elif essay_meta == {}:
            issues.append(
                ParentIssue(
                    stage="evaluation",
                    gen_id=gen_id,
                    issue="parent_metadata_invalid",
                    detail=f"essay {parent_id} metadata.json invalid",
                    metadata_path=str(meta_path),
                )
            )
        else:
            draft_parent = str(essay_meta.get("parent_gen_id") or "").strip()
            if draft_parent:
                draft_dir = drafts_dir / draft_parent
                if not draft_dir.exists():
                    issues.append(
                        ParentIssue(
                            stage="evaluation",
                            gen_id=gen_id,
                            issue="grandparent_missing",
                            detail=f"draft {draft_parent} (from essay {parent_id}) not found",
                            metadata_path=str(meta_path),
                        )
                    )

    return issues


def _write_csv(path: Path, issues: Iterable[ParentIssue]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=["stage", "gen_id", "issue", "detail", "metadata_path"])
        writer.writeheader()
        for issue in issues:
            writer.writerow(asdict(issue))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="Path to the project data root (default: data)",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=None,
        help="Optional CSV to write issues",
    )
    parser.add_argument(
        "--max-print",
        type=int,
        default=20,
        help="Maximum number of issues to print per stage (default: 20)",
    )
    args = parser.parse_args()

    data_root = args.data_root.resolve()
    essay_issues = _check_essay_parents(data_root)
    evaluation_issues = _check_evaluation_parents(data_root)
    all_issues = essay_issues + evaluation_issues

    def _print(stage: str, issues: List[ParentIssue]) -> None:
        print(f"Stage: {stage}")
        print(f"  Issues: {len(issues)}")
        for issue in issues[: args.max_print]:
            print(f"    - {issue.gen_id}: {issue.issue} ({issue.detail})")
        if len(issues) > args.max_print:
            print(f"    ... {len(issues) - args.max_print} more")
        print()

    _print("essay", essay_issues)
    _print("evaluation", evaluation_issues)

    if args.output_csv:
        _write_csv(args.output_csv, all_issues)

    if all_issues:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
