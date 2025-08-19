from dagster import Definitions, multiprocess_executor
from daydreaming_dagster.assets.llm_generation import (
    generation_prompt,
    generation_response
)
from daydreaming_dagster.assets.parsed_generation import (
    parsed_generation_responses
)
from daydreaming_dagster.assets.two_phase_generation import (
    links_prompt,
    links_response,
    essay_prompt,
    essay_response
)
from daydreaming_dagster.assets.llm_evaluation import (
    evaluation_prompt,
    evaluation_response
)
from daydreaming_dagster.assets.results_processing import (
    parsed_scores
)
from daydreaming_dagster.assets.results_analysis import (
    evaluator_agreement_analysis,
    comprehensive_variance_analysis
)
from daydreaming_dagster.assets.results_summary import (
    final_results,
    perfect_score_paths,
    generation_scores_pivot,
    evaluation_model_template_pivot
)
from daydreaming_dagster.assets.raw_data import (
    concepts,
    llm_models,
    generation_templates,
    evaluation_templates
)
from daydreaming_dagster.assets.core import (
    content_combinations,
    content_combinations_csv,
    generation_tasks,
    evaluation_tasks
)
from daydreaming_dagster.assets.cross_experiment import (
    filtered_evaluation_results,
    template_version_comparison_pivot,
    generation_results_append,
    evaluation_results_append
)
from daydreaming_dagster.resources.llm_client import LLMClientResource
from daydreaming_dagster.resources.experiment_config import ExperimentConfig
from daydreaming_dagster.resources.io_managers import (
    PartitionedTextIOManager,
    CSVIOManager
)
from daydreaming_dagster.resources.cross_experiment_io_manager import CrossExperimentIOManager
from pathlib import Path


defs = Definitions(
    assets=[
        # Raw data assets (now load all data, no filtering)
        concepts,                       # Loads ALL concepts with descriptions and applies filtering
        llm_models,                     # Loads ALL models
        generation_templates,           # Loads ALL generation templates with content
        evaluation_templates,           # Loads ALL evaluation templates with content
        
        # Core processing assets
        content_combinations,
        content_combinations_csv,
        generation_tasks,
        evaluation_tasks,
        
        # LLM prompt and response assets
        generation_prompt,
        generation_response,
        parsed_generation_responses,
        
        # Two-phase generation assets
        links_prompt,
        links_response,
        essay_prompt,
        essay_response,
        
        evaluation_prompt,
        evaluation_response,
        
        # Results processing assets
        parsed_scores,
        evaluator_agreement_analysis,
        comprehensive_variance_analysis,
        generation_scores_pivot,
        evaluation_model_template_pivot,
        final_results,
        perfect_score_paths,
        
        # Cross-experiment analysis assets
        filtered_evaluation_results,
        template_version_comparison_pivot,
        
        # Auto-materializing results tracking assets
        generation_results_append,
        evaluation_results_append
    ],
    resources={
        "openrouter_client": LLMClientResource(),
        "experiment_config": ExperimentConfig(),
        
        # Infrastructure configuration
        "data_root": "data",
        
        # Simplified I/O managers - no complex source mappings or filtering
        "csv_io_manager": CSVIOManager(base_path=Path("data") / "2_tasks"),
        # Prompts can safely overwrite to reflect latest template code
        "generation_prompt_io_manager": PartitionedTextIOManager(base_path=Path("data") / "3_generation" / "generation_prompts", overwrite=True),
        # Responses must never overwrite by default to preserve prior generations
        "generation_response_io_manager": PartitionedTextIOManager(base_path=Path("data") / "3_generation" / "generation_responses", overwrite=False),
        # Parsed generation responses go into generation directory as structured data
        "parsed_generation_io_manager": PartitionedTextIOManager(base_path=Path("data") / "3_generation" / "parsed_generation_responses", overwrite=True),
        
        # Two-phase generation I/O managers
        "links_prompt_io_manager": PartitionedTextIOManager(base_path=Path("data") / "3_generation" / "links_prompts", overwrite=True),
        "links_response_io_manager": PartitionedTextIOManager(base_path=Path("data") / "3_generation" / "links_responses", overwrite=False),
        "essay_prompt_io_manager": PartitionedTextIOManager(base_path=Path("data") / "3_generation" / "essay_prompts", overwrite=True),
        "essay_response_io_manager": PartitionedTextIOManager(base_path=Path("data") / "3_generation" / "essay_responses", overwrite=False),
        "evaluation_prompt_io_manager": PartitionedTextIOManager(base_path=Path("data") / "4_evaluation" / "evaluation_prompts", overwrite=True),
        # Evaluations are outputs; keep overwrite disabled for safety
        "evaluation_response_io_manager": PartitionedTextIOManager(base_path=Path("data") / "4_evaluation" / "evaluation_responses", overwrite=False),
        "error_log_io_manager": CSVIOManager(base_path=Path("data") / "7_reporting"),
        "parsing_results_io_manager": CSVIOManager(base_path=Path("data") / "5_parsing"),
        "parsed_generation_io_manager": PartitionedTextIOManager(base_path=Path("data") / "3_generation" / "parsed_generation_responses", overwrite=True),
        "summary_results_io_manager": CSVIOManager(base_path=Path("data") / "6_summary"),
        "cross_experiment_io_manager": CrossExperimentIOManager()
    },
    executor=multiprocess_executor.configured({"max_concurrent": 10})
    # Note: Pool concurrency is configured via dagster_home/dagster.yaml
)