from dagster import Definitions, multiprocess_executor
import subprocess
import os
from daydreaming_dagster.assets.llm_generation import (
    generation_prompt,
    generation_response
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
    perfect_score_paths
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
from daydreaming_dagster.resources.llm_client import LLMClientResource
from daydreaming_dagster.resources.experiment_config import ExperimentConfig
from daydreaming_dagster.resources.io_managers import (
    PartitionedTextIOManager,
    CSVIOManager
)
from pathlib import Path

# Programmatically configure pool concurrency based on llm_concurrency_limit resource
def ensure_llm_pool_concurrency(llm_limit):
    """Ensure the llm_api pool concurrency is set based on llm_concurrency_limit resource."""
    try:
        # Check if DAGSTER_HOME is set
        if "DAGSTER_HOME" not in os.environ:
            print(f"⚠️ DAGSTER_HOME not set, skipping pool concurrency configuration")
            print(f"   Pool concurrency should be manually set: dagster instance concurrency set llm_api {llm_limit}")
            return
            
        # Use Dagster CLI to set the pool concurrency
        result = subprocess.run([
            "dagster", "instance", "concurrency", "set", "llm_api", str(llm_limit)
        ], capture_output=True, text=True, check=False)
        
        if result.returncode == 0:
            print(f"✅ Set llm_api pool concurrency to {llm_limit}")
        else:
            print(f"⚠️ Failed to set llm_api pool concurrency: {result.stderr}")
            print(f"   Manual setup: dagster instance concurrency set llm_api {llm_limit}")
            
    except Exception as e:
        print(f"⚠️ Error configuring llm_api pool concurrency: {e}")
        print(f"   Manual setup: dagster instance concurrency set llm_api {llm_limit}")

# Define the concurrency limit
LLM_CONCURRENCY_LIMIT = 1  # NO CONCURRENCY: Only 1 LLM call at a time globally

# Configure pool concurrency based on our setting
ensure_llm_pool_concurrency(LLM_CONCURRENCY_LIMIT)

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
        evaluation_prompt,
        evaluation_response,
        
        # Results processing assets
        parsed_scores,
        evaluator_agreement_analysis,
        comprehensive_variance_analysis,
        final_results,
        perfect_score_paths
    ],
    resources={
        "openrouter_client": LLMClientResource(),
        "experiment_config": ExperimentConfig(),
        
        # Infrastructure configuration
        "data_root": "data",
        "llm_concurrency_limit": LLM_CONCURRENCY_LIMIT,  # Pool concurrency limit
        
        # Simplified I/O managers - no complex source mappings or filtering
        "csv_io_manager": CSVIOManager(base_path=Path("data") / "2_tasks"),
        "generation_prompt_io_manager": PartitionedTextIOManager(base_path=Path("data") / "3_generation" / "generation_prompts"),
        "generation_response_io_manager": PartitionedTextIOManager(base_path=Path("data") / "3_generation" / "generation_responses"),
        "evaluation_prompt_io_manager": PartitionedTextIOManager(base_path=Path("data") / "4_evaluation" / "evaluation_prompts"),
        "evaluation_response_io_manager": PartitionedTextIOManager(base_path=Path("data") / "4_evaluation" / "evaluation_responses"),
        "error_log_io_manager": CSVIOManager(base_path=Path("data") / "7_reporting"),
        "parsing_results_io_manager": CSVIOManager(base_path=Path("data") / "5_parsing"),
        "summary_results_io_manager": CSVIOManager(base_path=Path("data") / "6_summary")
    },
    executor=multiprocess_executor.configured({"max_concurrent": 10})
    # Note: Pool concurrency is now automatically configured above using llm_concurrency_limit
)