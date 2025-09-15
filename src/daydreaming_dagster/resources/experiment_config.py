from dagster import ConfigurableResource


class ExperimentConfig(ConfigurableResource):
    """Experiment configuration for two‑phase generation and evaluation."""
    k_max: int = 6
    description_level: str = "paragraph"
    evaluation_temperature: float = 0.1
    # Draft/essay specific max tokens (canonical)
    draft_generation_max_tokens: int = 20480
    essay_generation_max_tokens: int = 20480
    # Validation rule: minimum number of non-empty lines required in drafts
    min_draft_lines: int = 3
    evaluation_max_tokens: int = 20480

    # Replication controls (per-stage); defaults keep legacy behavior (no replicates)
    # Set evaluation_replicates > 1 to generate multiple evaluation gen_ids per (essay, tpl, model)
    draft_replicates: int = 1
    essay_replicates: int = 1
    evaluation_replicates: int = 1

    # COMMENTED OUT: Variance tracking configuration (future feature)
    # num_evaluation_runs: int = 3  # Number of evaluation runs per generation response for variance tracking
