from dagster import ConfigurableResource


class ExperimentConfig(ConfigurableResource):
    """
    Experiment configuration matching parameters.yml from Kedro.
    """
    k_max: int = 4
    description_level: str = "paragraph"
    current_gen_template: str = "00_systematic_analytical"
    current_eval_template: str = "creativity_metrics"
    rate_limit_delay: float = 0.1
    default_generation_template: str = "00_systematic_analytical"
    default_evaluation_template: str = "creativity_metrics"
    generation_temperature: float = 0.7
    evaluation_temperature: float = 0.1
    generation_max_tokens: int = 8192
    evaluation_max_tokens: int = 2048
    concept_ids_filter: list[str] = None
    template_names_filter: list[str] = None
    def to_dict(self) -> dict:
        """Convert to dictionary for compatibility with existing node functions."""
        return {
            "k_max": self.k_max,
            "description_level": self.description_level,
            "current_gen_template": self.current_gen_template,
            "current_eval_template": self.current_eval_template,
            "rate_limit_delay": self.rate_limit_delay,
            "default_generation_template": self.default_generation_template,
            "default_evaluation_template": self.default_evaluation_template,
            "temperature": {
                "generation": self.generation_temperature,
                "evaluation": self.evaluation_temperature
            },
            "max_tokens": {
                "generation": self.generation_max_tokens,
                "evaluation": self.evaluation_max_tokens
            },
            "concept_ids_filter": self.concept_ids_filter,
            "template_names_filter": self.template_names_filter
        }
