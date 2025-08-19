# Tier-Aware LLM Concurrency and Rate Limits

This guide explains how we split free vs paid LLM calls for OpenRouter and how to operate runs to observe serialized free calls and parallel paid calls.

## Goals
- Prevent conservative free-tier limits from throttling paid calls
- Enforce global concurrency across processes for each tier
- Avoid manual partition selection errors

## Implementation

### 1) Tier detection
- A model is considered free-tier if its identifier contains `:free` (e.g., `deepseek/deepseek-r1:free`).

### 2) Client-level tier limits
- `daydreaming_dagster/resources/llm_client.py` enforces separate rate limits and mandatory delays:
  - `free_rate_limit_calls`, `free_rate_limit_period`, `free_mandatory_delay`
  - `paid_rate_limit_calls`, `paid_rate_limit_period`, `paid_mandatory_delay`
- The client maintains separate last-call timestamps per tier to avoid cross-throttling.

### 3) Global concurrency (Dagster run-queue limits)
- Configured in `dagster_home/dagster.yaml` using tag-based concurrency:

```yaml
run_queue:
  max_concurrent_runs: 20
  tag_concurrency_limits:
    - key: "dagster/concurrency_key"
      value: "llm_api"
      limit: 10
    - key: "dagster/concurrency_key"
      value: "llm_api_free"
      limit: 1
    - key: "dagster/concurrency_key"
      value: "llm_api_paid"
      limit: 5
```

- Assets carry tags that map to these keys:
  - `generation_prompt`, `generation_response`, `evaluation_prompt`, `evaluation_response` → `llm_api`
  - `generation_response_free` → `llm_api_free`
  - `generation_response_paid` → `llm_api_paid`
  - `evaluation_response_free` → `llm_api_free`
  - `evaluation_response_paid` → `llm_api_paid`

### 4) Auto-splitting by tier (no manual partition selection)
- Split assets self-filter by model name and skip mismatched partitions:
  - `generation_response_free` returns immediately for paid models
  - `generation_response_paid` returns immediately for free models
  - `evaluation_response_free/paid` mirror the same logic
- You can safely select all partitions in the web UI; each asset will process only its tier.

## How to run (CLI or Dagit)

1. Prepare data and tasks:
```bash
uv run dagster asset materialize -f daydreaming_dagster/definitions.py --select "group:raw_data,group:task_definitions"
```

2. Free generation (serialized globally):
- Dagit: open `generation_response_free`, select all partitions, add tag `experiment_id=exp_free_vs_paid`, launch.
- CLI:
```bash
uv run dagster asset materialize -f daydreaming_dagster/definitions.py \
  --select "generation_response_free" \
  --partition-range START..END \
  --tag experiment_id=exp_free_vs_paid
```

3. Paid generation (parallel):
- Dagit: open `generation_response_paid`, select all partitions, add the same run tag, launch.
- CLI analogous to step 2 with `generation_response_paid`.

4. Evaluation (optional): use `evaluation_response_paid` or `evaluation_response_free` similarly.

## Tips
- Ensure `DAGSTER_HOME` points to `./dagster_home` and `OPENROUTER_API_KEY` is set.
- Adjust limits in `dagster_home/dagster.yaml` to match your account’s policy and desired throughput.
- To include/exclude models, edit `data/1_raw/llm_models.csv` (for_generation / for_evaluation flags).

## Troubleshooting
- "Asset has partitions, but no '--partition'": launch via response assets (they bring prompts upstream) or specify `--partition`/`--partition-range`.
- "Invalid model id": verify model names in `llm_models.csv` match OpenRouter IDs (free models include `:free`).
