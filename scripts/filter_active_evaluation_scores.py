"""Utilities to filter evaluation scores to active evaluation templates only."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parent.parent
EVAL_TEMPLATES_PATH = ROOT / "data" / "1_raw" / "evaluation_templates.csv"
SOURCE_PATH = ROOT / "data" / "7_cross_experiment" / "evaluation_scores_by_template_model.csv"
TARGET_PATH = ROOT / "data" / "7_cross_experiment" / "evaluation_scores_by_template_model_limited.csv"
SUM_COLUMN = "active_template_score_sum"


def load_active_templates() -> set[str]:
    active: set[str] = set()
    with EVAL_TEMPLATES_PATH.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("active", "").strip().lower() == "true":
                template_id = row.get("template_id", "").strip()
                if template_id:
                    active.add(template_id)
    return active


def select_columns(fieldnames: Sequence[str], active_templates: set[str]) -> tuple[list[str], list[str]]:
    fixed: list[str] = []
    metrics: list[str] = []
    for name in fieldnames:
        if "__" not in name:
            fixed.append(name)
            continue
        template_id = name.split("__", 1)[0]
        if template_id in active_templates:
            metrics.append(name)
    return fixed, metrics


def to_float(value: str) -> float:
    value = value.strip()
    if not value:
        return 0.0
    try:
        return float(value)
    except ValueError:
        return 0.0


def main() -> None:
    active_templates = load_active_templates()
    if not active_templates:
        raise SystemExit("No active evaluation templates found.")

    with SOURCE_PATH.open(newline="") as src_handle:
        reader = csv.DictReader(src_handle)
        if reader.fieldnames is None:
            raise SystemExit("Source CSV has no header row.")
        fixed_columns, metric_columns = select_columns(reader.fieldnames, active_templates)
        output_columns = [*fixed_columns, *metric_columns, SUM_COLUMN]

        with TARGET_PATH.open("w", newline="") as dst_handle:
            writer = csv.DictWriter(dst_handle, fieldnames=output_columns)
            writer.writeheader()

            for row in reader:
                total = sum(to_float(row.get(col, "")) for col in metric_columns)
                filtered_row = {col: row.get(col, "") for col in output_columns if col != SUM_COLUMN}
                filtered_row[SUM_COLUMN] = f"{total:.1f}" if metric_columns else "0"
                writer.writerow(filtered_row)


if __name__ == "__main__":
    main()
