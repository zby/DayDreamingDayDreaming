#!/usr/bin/env python3
"""Validate that an evaluation generation references a known template.

Exit codes:
0  -> template found and active (prints template_id)
5  -> template found but inactive (prints template_id)
1  -> metadata.json missing
2  -> metadata.json unreadable
3  -> template_id missing in metadata
4  -> evaluation_templates.csv missing
6  -> template_id not present in evaluation_templates.csv
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

DATA_ROOT = Path("data")
TEMPLATES_CSV = DATA_ROOT / "1_raw" / "evaluation_templates.csv"
GENS_ROOT = DATA_ROOT / "gens" / "evaluation"


def main(gen_id: str) -> int:
    meta_path = GENS_ROOT / gen_id / "metadata.json"
    if not meta_path.exists():
        return 1

    try:
        metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return 2

    template_id = str(metadata.get("template_id") or "").strip()
    if not template_id:
        return 3

    if not TEMPLATES_CSV.exists():
        return 4

    with TEMPLATES_CSV.open(encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if str(row.get("template_id") or "").strip() == template_id:
                active_field = row.get("active")
                is_active = True if active_field is None else str(active_field).strip().lower() == "true"
                print(template_id)
                return 0 if is_active else 5

    return 6


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(99)
    sys.exit(main(sys.argv[1]))
