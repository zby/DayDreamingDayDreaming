from dagster import ConfigurableResource


class ExperimentConfig(ConfigurableResource):
    """
    Experiment configuration for two‑phase generation and evaluation.
    """
    k_max: int = 6
    description_level: str = "paragraph"
    generation_temperature: float = 0.7
    evaluation_temperature: float = 0.1
    generation_max_tokens: int = 8192
    # Draft/essay specific max tokens (canonical)
    draft_generation_max_tokens: int = 20480
    essay_generation_max_tokens: int = 4096
    # Validation rule: minimum number of non-empty lines required in drafts
    min_draft_lines: int = 3
    evaluation_max_tokens: int = 2048

    # COMMENTED OUT: Variance tracking configuration (future feature)
    # num_evaluation_runs: int = 3  # Number of evaluation runs per generation response for variance tracking

    def to_dict(self) -> dict:
        """Convert to dictionary for compatibility with existing node functions."""
        return {
            "k_max": self.k_max,
            "description_level": self.description_level,
            "temperature": {
                "generation": self.generation_temperature,
                "evaluation": self.evaluation_temperature,
            },
            "max_tokens": {
                "generation": self.generation_max_tokens,
                "draft_generation": self.draft_generation_max_tokens,
                "essay_generation": self.essay_generation_max_tokens,
                "evaluation": self.evaluation_max_tokens,
            },
            "rules": {
                "min_draft_lines": self.min_draft_lines,
            },
            # "num_evaluation_runs": self.num_evaluation_runs,  # COMMENTED OUT
        }
