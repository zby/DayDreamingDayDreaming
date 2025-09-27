#!/usr/bin/env bash
# Re-run evaluation assets for the legacy evaluations whose scores previously exceeded 9.
#
# Prerequisites:
#   * Run `python scripts/migrate_evaluation_scores_max9.py` (without --dry-run) first
#     to switch metadata to LLM mode and clear stale artifacts.
#   * Ensure `DAGSTER_HOME` points at the desired instance root before invoking this script.
#   * Requires `uv` (or adjust calls to use `.venv/bin/dagster`).

set -u -o pipefail

PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "${PROJECT_ROOT}"

register_partition() {
  local gen_id="$1"
  .venv/bin/python - "$gen_id" <<'PY'
from dagster._core.instance import DagsterInstance
from daydreaming_dagster.assets.partitions import evaluation_gens_partitions
import sys

gen_id = sys.argv[1]
instance = DagsterInstance.get()
existing = set(instance.get_dynamic_partitions(evaluation_gens_partitions.name))
if gen_id not in existing:
    instance.add_dynamic_partitions(evaluation_gens_partitions.name, [gen_id])
    print(f"Registered evaluation partition {gen_id}")
else:
    print(f"Partition {gen_id} already registered")
PY
}

run_with_retry() {
  local gen_id="$1"
  local parsed_path="data/gens/evaluation/${gen_id}/parsed.txt"

  echo "=== Rebuilding evaluation for ${gen_id} ==="
  if [ -f "${parsed_path}" ]; then
    echo "parsed.txt already present for ${gen_id}; skipping."
    return
  fi

  register_partition "${gen_id}"

  if uv run dagster asset materialize --select "evaluation_prompt,evaluation_raw,evaluation_parsed" --partition "${gen_id}" -f src/daydreaming_dagster/definitions.py; then
    echo "Materialization succeeded for ${gen_id}."
    return
  fi

  echo "First attempt failed for ${gen_id}; retrying once..."
  register_partition "${gen_id}"
  if uv run dagster asset materialize --select "evaluation_prompt,evaluation_raw,evaluation_parsed" --partition "${gen_id}" -f src/daydreaming_dagster/definitions.py; then
    echo "Retry succeeded for ${gen_id}."
  else
    echo "Retry failed for ${gen_id}. Check logs before continuing." >&2
  fi
}

run_with_retry "0fjwmpqqxlm31ism"
run_with_retry "10m6tgun7otuo4j5"
run_with_retry "30h5n4h0cttaj5ix"
run_with_retry "4e2qqbav9p8cdy9c"
run_with_retry "52tsf3t4p526aria"
run_with_retry "9lluq5xb1zj60b7z"
run_with_retry "9w2tyd8bxdav6jzq"
run_with_retry "k5zy76rxp250no16"
run_with_retry "ku3po1bv4xiovn0v"
run_with_retry "lw0c8q00308juynv"
run_with_retry "odk59j67qgp167ow"
run_with_retry "uvn2tphr3374wd87"
run_with_retry "zgrg591djc9s7r25"
run_with_retry "zjr94fjsqpaklhv8"

echo "=== Refreshing aggregated scores and pivots ==="
uv run dagster asset materialize --select aggregated_scores -f src/daydreaming_dagster/definitions.py
.venv/bin/python scripts/build_pivot_tables.py --limit-to-active-templates

echo "Done."
