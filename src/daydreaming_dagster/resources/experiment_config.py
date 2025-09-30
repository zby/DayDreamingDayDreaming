from typing import Dict

from dagster import Config, ConfigurableResource
from pydantic import Field


class StageSettings(Config):
    """Per-stage configuration values.

    Attributes:
        generation_max_tokens: Maximum tokens for LLM generation (None = no limit)
        min_lines: Minimum non-empty lines required for parsed output (None = no validation)
        force: If True, regenerate artifacts even if they already exist (default: False).
               By default (force=False), stages reuse existing raw.txt/parsed.txt to avoid
               redundant LLM calls. Set force=True to explicitly regenerate.
    """

    generation_max_tokens: int | None = None
    min_lines: int | None = None
    force: bool = False


class ExperimentConfig(ConfigurableResource):
    """Experiment configuration for two-phase generation and evaluation."""

    k_max: int = 6
    description_level: str = "paragraph"
    evaluation_temperature: float = 0.1
    stage_config: Dict[str, StageSettings] = Field(
        default_factory=lambda: {
            "draft": StageSettings(generation_max_tokens=20480, min_lines=3),
            "essay": StageSettings(generation_max_tokens=20480, min_lines=None),
            "evaluation": StageSettings(generation_max_tokens=20480, min_lines=None),
        }
    )

    # Replication counts are configured via CSV only (data/1_raw/replication_config.csv).
    # No code defaults here — assets will fail if the CSV is missing or malformed.

    # COMMENTED OUT: Variance tracking configuration (future feature)
    # num_evaluation_runs: int = 3  # Number of evaluation runs per generation response for variance tracking
