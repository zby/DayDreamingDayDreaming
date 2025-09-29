from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

from daydreaming_dagster.maintenance.deterministic_id_migration import STAGE_ORDER
from daydreaming_dagster.utils.ids import signature_from_metadata


@dataclass
class ReplicateUpdate:
    stage: str
    gen_id: str
    base_signature: Tuple
    old_replicate: int | None
    new_replicate: int
    metadata_changed: bool
    raw_changed: bool
    parsed_changed: bool


@dataclass
class ReplicateMigrationResult:
    stages: List[str]
    total_generations: int
    updates: List[ReplicateUpdate]
    issues: List[str]
    dry_run: bool


@dataclass
class _GenerationRecord:
    stage: str
    gen_id: str
    directory: Path
    metadata_path: Path
    metadata: dict
    raw_metadata_path: Path | None
    raw_metadata: dict | None
    parsed_metadata_path: Path | None
    parsed_metadata: dict | None
    base_signature: Tuple
    metadata_replicate: int | None
    raw_replicate: int | None
    parsed_replicate: int | None

    @property
    def existing_replicate(self) -> int | None:
        for value in (self.metadata_replicate, self.raw_replicate, self.parsed_replicate):
            if value is not None:
                return value
        return None


def migrate_replicates(
    data_root: Path,
    stages: Sequence[str] | None = None,
    *,
    execute: bool = False,
) -> ReplicateMigrationResult:
    root = Path(data_root)
    stage_order = [stage for stage in (stages or STAGE_ORDER) if stage in STAGE_ORDER]
    records: List[_GenerationRecord] = []
    issues: List[str] = []

    for stage in stage_order:
        stage_dir = root / "gens" / stage
        if not stage_dir.exists():
            continue
        for child in sorted(p for p in stage_dir.iterdir() if p.is_dir()):
            record, error = _load_generation_record(stage, child)
            if error:
                issues.append(error)
                continue
            if record is not None:
                records.append(record)

    updates = _plan_updates(records)
    record_index = {(rec.stage, rec.gen_id): rec for rec in records}

    if execute:
        for update in updates:
            record = record_index.get((update.stage, update.gen_id))
            if not record:
                continue
            _apply_update(record, update)

    return ReplicateMigrationResult(
        stages=stage_order,
        total_generations=len(records),
        updates=updates,
        issues=issues,
        dry_run=not execute,
    )


def _load_generation_record(stage: str, directory: Path) -> tuple[_GenerationRecord | None, str | None]:
    metadata_path = directory / "metadata.json"
    if not metadata_path.exists():
        return None, f"missing-metadata stage={stage} path={metadata_path}"

    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return None, f"invalid-metadata stage={stage} path={metadata_path} error={exc}"

    raw_path = directory / "raw_metadata.json"
    raw_metadata = None
    if raw_path.exists():
        try:
            raw_metadata = json.loads(raw_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            return None, f"invalid-raw-metadata stage={stage} path={raw_path} error={exc}"

    parsed_path = directory / "parsed_metadata.json"
    parsed_metadata = None
    if parsed_path.exists():
        try:
            parsed_metadata = json.loads(parsed_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            return None, f"invalid-parsed-metadata stage={stage} path={parsed_path} error={exc}"

    try:
        signature = signature_from_metadata(stage, metadata)
    except ValueError as exc:
        gen_id = metadata.get("gen_id") or directory.name
        return None, f"invalid-signature stage={stage} gen={gen_id} error={exc}"

    metadata_rep = _normalize_replicate(metadata.get("replicate"))
    raw_rep = _normalize_replicate(raw_metadata.get("replicate")) if raw_metadata else None
    parsed_rep = _normalize_replicate(parsed_metadata.get("replicate")) if parsed_metadata else None

    record = _GenerationRecord(
        stage=stage,
        gen_id=str(metadata.get("gen_id") or directory.name),
        directory=directory,
        metadata_path=metadata_path,
        metadata=metadata,
        raw_metadata_path=raw_path if raw_path.exists() else None,
        raw_metadata=raw_metadata,
        parsed_metadata_path=parsed_path if parsed_path.exists() else None,
        parsed_metadata=parsed_metadata,
        base_signature=signature[:-1],
        metadata_replicate=metadata_rep,
        raw_replicate=raw_rep,
        parsed_replicate=parsed_rep,
    )

    return record, None


def _plan_updates(records: List[_GenerationRecord]) -> List[ReplicateUpdate]:
    updates: List[ReplicateUpdate] = []
    groups: Dict[Tuple, List[_GenerationRecord]] = {}
    for record in records:
        groups.setdefault(record.base_signature, []).append(record)

    for base_signature, group in groups.items():
        used: set[int] = set()
        # Sort records to make assignment deterministic: prefer explicit replicate values, then gen_id.
        group_sorted = sorted(
            group,
            key=lambda rec: (
                rec.existing_replicate if rec.existing_replicate is not None else float("inf"),
                rec.gen_id,
            ),
        )
        for record in group_sorted:
            desired = record.existing_replicate
            if desired is not None and desired > 0 and desired not in used:
                assigned = desired
            else:
                assigned = 1
                while assigned in used:
                    assigned += 1
            used.add(assigned)

            metadata_changed = record.metadata_replicate != assigned
            raw_changed = (
                record.raw_metadata is not None
                and record.raw_replicate != assigned
            )
            parsed_changed = (
                record.parsed_metadata is not None
                and record.parsed_replicate != assigned
            )

            if metadata_changed or raw_changed or parsed_changed:
                updates.append(
                    ReplicateUpdate(
                        stage=record.stage,
                        gen_id=record.gen_id,
                        base_signature=base_signature,
                        old_replicate=record.existing_replicate,
                        new_replicate=assigned,
                        metadata_changed=metadata_changed,
                        raw_changed=raw_changed,
                        parsed_changed=parsed_changed,
                    )
                )

    return updates


def _apply_update(record: _GenerationRecord, update: ReplicateUpdate) -> None:
    if update.metadata_changed:
        updated_metadata = dict(record.metadata)
        updated_metadata["replicate"] = update.new_replicate
        record.metadata_path.write_text(
            json.dumps(updated_metadata, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        record.metadata = updated_metadata
        record.metadata_replicate = update.new_replicate

    if update.raw_changed and record.raw_metadata_path:
        updated_raw = dict(record.raw_metadata or {})
        updated_raw["replicate"] = update.new_replicate
        record.raw_metadata_path.write_text(
            json.dumps(updated_raw, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        record.raw_metadata = updated_raw
        record.raw_replicate = update.new_replicate

    if update.parsed_changed and record.parsed_metadata_path:
        updated_parsed = dict(record.parsed_metadata or {})
        updated_parsed["replicate"] = update.new_replicate
        record.parsed_metadata_path.write_text(
            json.dumps(updated_parsed, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        record.parsed_metadata = updated_parsed
        record.parsed_replicate = update.new_replicate


def _normalize_replicate(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        rep = int(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text.isdigit():
            return None
        rep = int(text)
    else:
        return None
    return rep if rep > 0 else None


def _format_summary(result: ReplicateMigrationResult) -> str:
    return (
        "stages={stages} total={total} updates={updates}"
        .format(
            stages=",".join(result.stages) or "<none>",
            total=result.total_generations,
            updates=len(result.updates),
        )
    )


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replicate index migration tool")
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path.cwd(),
        help="Data root containing gens/",
    )
    parser.add_argument(
        "--stages",
        nargs="*",
        choices=STAGE_ORDER,
        default=STAGE_ORDER,
        help="Stages to process",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Persist replicate index updates",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    result = migrate_replicates(args.data_root, stages=args.stages, execute=args.execute)

    print(_format_summary(result))
    if result.issues:
        for issue in result.issues:
            print(f"ERROR: {issue}")
        return 1

    if result.dry_run:
        print("mode=dry-run")
    else:
        print("mode=execute")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
