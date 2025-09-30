#!/usr/bin/env python3
"""Generate a bash script to backfill `evaluation_parsed` partitions.

This helper inspects aggregated_scores.csv (default `data/5_parsing/aggregated_scores.csv`),
finds generations whose `error` column equals "missing parsed.txt", confirms that `raw.txt`
exists while `parsed.txt` does not, and writes a shell script containing Dagster commands.
The generated script performs two steps:

1. Re-register cohort partitions referenced by the missing rows.
2. Materialize `evaluation_parsed` for each target partition.

You can preview the commands with `--dry-run`, or write the script (default path
`backfill_eval_parsed.sh`) and execute it manually.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import List, Dict

import pandas as pd


def _ensure_src_on_path() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    src_dir = repo_root / "src"
    if src_dir.exists():
        sys.path.insert(0, str(src_dir))


_ensure_src_on_path()

from daydreaming_dagster.utils.errors import DDError, Err


def load_missing_gen_infos(
    aggregated_scores: Path,
    data_root: Path,
    template: str | None,
    limit: int | None,
) -> List[Dict[str, str]]:
    if not aggregated_scores.exists():
        raise DDError(
            Err.DATA_MISSING,
            ctx={"reason": "aggregated_scores_missing", "path": str(aggregated_scores)},
        )

    df = pd.read_csv(aggregated_scores)
    if "error" not in df.columns:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={"reason": "aggregated_scores_missing_error_column"},
        )

    candidates = df[df["error"] == "missing parsed.txt"].copy()
    if template:
        if "evaluation_template" not in candidates.columns:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={"reason": "aggregated_scores_missing_template_column"},
            )
        candidates = candidates[candidates["evaluation_template"].astype(str) == template]

    if candidates.empty:
        return []

    missing: List[Dict[str, str]] = []
    for _, row in candidates.iterrows():
        gen_id = str(row["gen_id"]).strip()
        if not gen_id:
            continue
        stage = str(row.get("stage", "evaluation")).strip() or "evaluation"
        cohort_id = str(row.get("origin_cohort_id") or "").strip()
        parsed_path = data_root / "gens" / stage / gen_id / "parsed.txt"
        raw_path = data_root / "gens" / stage / gen_id / "raw.txt"
        if not raw_path.exists():
            continue
        if parsed_path.exists():
            continue
        missing.append({
            "gen_id": gen_id,
            "stage": stage,
            "cohort_id": cohort_id,
        })
        if limit is not None and len(missing) >= limit:
            break
    return missing


def _command_to_str(cmd: List[str]) -> str:
    return " ".join(cmd)


def _build_commands(
    gen_infos: List[Dict[str, str]],
    *,
    definitions: Path,
    asset: str,
    dagster_cmd: List[str],
) -> List[List[str]]:
    commands: List[List[str]] = []
    for info in gen_infos:
        commands.append(
            dagster_cmd
            + [
                "asset",
                "materialize",
                "--select",
                asset,
                "--partition",
                info["gen_id"],
                "-f",
                str(definitions),
            ]
        )

    return commands


def _preview_commands(
    gen_infos: List[Dict[str, str]],
    *,
    definitions: Path,
    asset: str,
    dagster_cmd: List[str],
) -> None:
    commands = _build_commands(
        gen_infos,
        definitions=definitions,
        asset=asset,
        dagster_cmd=dagster_cmd,
    )
    print("\nCommands to run:")
    for cmd in commands:
        print(_command_to_str(cmd))


def write_script(
    gen_infos: List[Dict[str, str]],
    *,
    script_path: Path,
    definitions: Path,
    asset: str,
    dagster_cmd: List[str],
    overwrite: bool,
) -> None:
    if script_path.exists() and not overwrite:
        raise DDError(
            Err.INVALID_CONFIG,
            ctx={
                "reason": "script_already_exists",
                "path": str(script_path),
            },
        )

    commands = _build_commands(
        gen_infos,
        definitions=definitions,
        asset=asset,
        dagster_cmd=dagster_cmd,
    )

    script_path.parent.mkdir(parents=True, exist_ok=True)
    with script_path.open("w", encoding="utf-8") as handle:
        handle.write("#!/usr/bin/env bash\n")
        handle.write("set -euo pipefail\n\n")
        cohort_ids = sorted({info.get("cohort_id") for info in gen_infos if info.get("cohort_id")})
        if cohort_ids:
            handle.write("# Ensure cohort partitions are registered before running these commands:\n")
            for cohort in cohort_ids:
                handle.write(
                    f"#   uv run dagster asset materialize --select cohort_membership --partition {cohort} -f {definitions}\n"
                )
                handle.write(
                    f"#   uv run dagster asset materialize --select register_cohort_partitions -f {definitions}\n"
                )
            handle.write("\n")
        for cmd in commands:
            handle.write(_command_to_str(cmd) + "\n")

    script_path.chmod(script_path.stat().st_mode | 0o111)
    print(f"Wrote {len(commands)} commands to {script_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="Base data directory (default: data)",
    )
    parser.add_argument(
        "--aggregated-scores",
        type=Path,
        default=Path("data/5_parsing/aggregated_scores.csv"),
        help="Path to aggregated_scores.csv (default: data/5_parsing/aggregated_scores.csv)",
    )
    parser.add_argument(
        "--definitions",
        type=Path,
        default=Path("src/daydreaming_dagster/definitions.py"),
        help="Dagster definitions file (default: src/daydreaming_dagster/definitions.py)",
    )
    parser.add_argument(
        "--template",
        default=None,
        help="Optional evaluation_template filter (e.g., novelty-v2)",
    )
    parser.add_argument(
        "--asset",
        default="evaluation_parsed",
        help="Asset selector to materialize (default: evaluation_parsed)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of partitions to process (default: all)",
    )
    parser.add_argument(
        "--dagster-cmd",
        nargs="*",
        default=["uv", "run", "dagster"],
        help="Command prefix to invoke Dagster (default: uv run dagster)",
    )
    parser.add_argument(
        "--script",
        type=Path,
        default=Path("backfill_eval_parsed.sh"),
        help="Output path for generated bash script (default: backfill_eval_parsed.sh)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow overwriting an existing script file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview commands instead of writing the script",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    gen_infos = load_missing_gen_infos(
        aggregated_scores=args.aggregated_scores,
        data_root=args.data_root,
        template=args.template,
        limit=args.limit,
    )
    if not gen_infos:
        print("No partitions require backfill.")
        return 0

    print(f"Identified {len(gen_infos)} partitions lacking parsed.txt (template={args.template or 'any'}).")
    if args.dry_run:
        _preview_commands(
            gen_infos,
            definitions=args.definitions,
            asset=args.asset,
            dagster_cmd=args.dagster_cmd,
        )
        return 0

    write_script(
        gen_infos,
        script_path=args.script,
        definitions=args.definitions,
        asset=args.asset,
        dagster_cmd=args.dagster_cmd,
        overwrite=args.overwrite,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except DDError as err:
        print(f"ERROR [{err.code.name}]: {err.ctx}", file=sys.stderr)
        raise SystemExit(2)
