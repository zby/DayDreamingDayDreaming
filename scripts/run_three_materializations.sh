#!/usr/bin/env bash
set -euo pipefail

# Runs three representative materializations: draft → essay → evaluation
#
# Usage:
#   scripts/run_three_materializations.sh [<draft_task_id> [<essay_task_id> [<evaluation_task_id>]]]
#
# Notes:
# - Uses in-process executor to avoid OS semaphore issues: DD_IN_PROCESS=1
# - Enables docs index dual-write/reads: DD_DOCS_INDEX_ENABLED=1
# - Copies prompt snapshots into doc_dir: DD_DOCS_PROMPT_COPY_ENABLED=1
# - Requires OPENROUTER_API_KEY set for live LLM calls

PROJECT_ROOT=$(cd "$(dirname "$0")/.." && pwd)
DATA_ROOT=${DATA_ROOT:-"$PROJECT_ROOT/data"}
DAGSTER_HOME=${DAGSTER_HOME:-"$PROJECT_ROOT/dagster_home"}
DAGSTER_CMD=${DAGSTER_CMD:-"$PROJECT_ROOT/.venv/bin/dagster"}
DEFS_FILE="daydreaming_dagster/definitions.py"

export DAGSTER_HOME
export DD_IN_PROCESS=1
export DD_DOCS_INDEX_ENABLED=1
export DD_DOCS_PROMPT_COPY_ENABLED=1

# Load .env if present to pick up OPENROUTER_API_KEY and other vars
if [[ -f "$PROJECT_ROOT/.env" ]]; then
  # Export all variables defined in .env for this process
  set -a
  # shellcheck disable=SC1090
  . "$PROJECT_ROOT/.env"
  set +a
fi

if [[ -z "${OPENROUTER_API_KEY:-}" ]]; then
  echo "[warn] OPENROUTER_API_KEY is not set in the environment (.env can be used) — materializations may fail." >&2
fi

draft_pk=${1:-}
essay_pk=${2:-}
eval_pk=${3:-}

py_get_first_draft='import csv,sys;from pathlib import Path; p=Path(sys.argv[1]);
import pandas as pd
df=pd.read_csv(p)
print(df["draft_task_id"].iloc[0])
'

py_get_essay_for_draft='import sys,pandas as pd
from pathlib import Path
draft_id,essay_csv,templates_csv=sys.argv[1],sys.argv[2],sys.argv[3]
edf=pd.read_csv(essay_csv)
# 1) Prefer rows that explicitly link to the chosen draft
m=edf[edf.get("draft_task_id")==draft_id]
if len(m)>0:
    print(m.iloc[0]["essay_task_id"])
    raise SystemExit(0)
# 2) Otherwise prefer an LLM-mode essay template if available for the same combo/model
tpl_df=pd.read_csv(templates_csv)
llm_templates=set(tpl_df[tpl_df.get("generator").fillna("").str.lower().ne("parser")]["template_id"].tolist())
if "essay_template" in edf.columns and len(llm_templates)>0:
    m2=edf[edf["essay_template"].isin(llm_templates)]
    if len(m2)>0:
        print(m2.iloc[0]["essay_task_id"])
        raise SystemExit(0)
# 3) Fallback: first row
print(edf.iloc[0]["essay_task_id"])'

py_get_eval_for_doc='import sys,pandas as pd
doc,ev=sys.argv[1],sys.argv[2]
df=pd.read_csv(ev)
m=df[df.get("document_id")==doc]
print((m.iloc[0]["evaluation_task_id"] if len(m)>0 else df.iloc[0]["evaluation_task_id"]))
'

draft_csv="$DATA_ROOT/2_tasks/draft_generation_tasks.csv"
essay_csv="$DATA_ROOT/2_tasks/essay_generation_tasks.csv"
essay_templates_csv="$DATA_ROOT/1_raw/essay_templates.csv"
eval_csv="$DATA_ROOT/2_tasks/evaluation_tasks.csv"

if [[ -z "$draft_pk" ]]; then
  if [[ -f "$draft_csv" ]]; then
    draft_pk=$(python -c "$py_get_first_draft" "$draft_csv")
  else
    echo "[err] missing $draft_csv — seed tasks first." >&2
    exit 1
  fi
fi

if [[ -z "$essay_pk" ]]; then
  if [[ -f "$essay_csv" ]]; then
    if [[ ! -f "$essay_templates_csv" ]]; then
      echo "[warn] missing $essay_templates_csv — will not prefer LLM mode" >&2
      essay_templates_csv="/dev/null"
    fi
    essay_pk=$(python -c "$py_get_essay_for_draft" "$draft_pk" "$essay_csv" "$essay_templates_csv")
  else
    echo "[err] missing $essay_csv — seed tasks first." >&2
    exit 1
  fi
fi

if [[ -z "$eval_pk" ]]; then
  if [[ -f "$eval_csv" ]]; then
    eval_pk=$(python -c "$py_get_eval_for_doc" "$essay_pk" "$eval_csv")
  else
    echo "[err] missing $eval_csv — seed tasks first." >&2
    exit 1
  fi
fi

echo "[info] Using partitions:" >&2
echo "  draft:      $draft_pk" >&2
echo "  essay:      $essay_pk" >&2
echo "  evaluation: $eval_pk" >&2

run_step() {
  local sel="$1"; shift
  local pk="$1"; shift
  echo "[run] $sel :: $pk" >&2
  # 3-minute timeout per step
  if ! timeout 180s "$DAGSTER_CMD" asset materialize --select "$sel" --partition "$pk" -f "$DEFS_FILE"; then
    echo "[err] materialization failed for $sel :: $pk" >&2
    exit 2
  fi
}

# Draft (prompt + response to ensure prompt exists for IO manager)
run_step "draft_prompt,draft_response" "$draft_pk"

# Essay
run_step "essay_prompt,essay_response" "$essay_pk"

# Evaluation
run_step "evaluation_prompt,evaluation_response" "$eval_pk"

echo "[ok] All three materializations completed." >&2
