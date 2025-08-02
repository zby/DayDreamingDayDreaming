from dagster import Definitions, load_assets_from_modules, fs_io_manager  
from .assets import core, partitions, llm_prompts_responses, raw_data
from .resources.llm_client import LLMClientResource
from .resources.experiment_config import ExperimentConfig
from .resources.io_managers import (
    generation_prompt_io_manager,
    generation_response_io_manager,
    evaluation_prompt_io_manager,
    evaluation_response_io_manager,
    csv_io_manager,
    partitioned_text_io_manager,
    error_log_io_manager,
    parsing_results_io_manager,
    summary_results_io_manager
)

# Load all assets
raw_assets = load_assets_from_modules([raw_data])
core_assets = load_assets_from_modules([core])
partition_assets = load_assets_from_modules([partitions])
llm_assets = load_assets_from_modules([llm_prompts_responses])

defs = Definitions(  
    assets=[*raw_assets, *core_assets, *partition_assets, *llm_assets],
    resources={  
        # API and Configuration Resources
        "openrouter_client": LLMClientResource.configure_at_launch(),
        "experiment_config": ExperimentConfig(),
        
        # Default I/O Manager
        "io_manager": fs_io_manager,
        
        # I/O Managers for Different Data Types
        "csv_io_manager": csv_io_manager(),
        "partitioned_text_io_manager": partitioned_text_io_manager(),
        "error_log_io_manager": error_log_io_manager(),
        "parsing_results_io_manager": parsing_results_io_manager(),
        "summary_results_io_manager": summary_results_io_manager(),
        
        # Partitioned LLM I/O Managers
        "generation_prompt_io_manager": generation_prompt_io_manager(),
        "generation_response_io_manager": generation_response_io_manager(),
        "evaluation_prompt_io_manager": evaluation_prompt_io_manager(),
        "evaluation_response_io_manager": evaluation_response_io_manager(),
    }
)