"""
Core execution logic for the daydreaming experiment.
"""
import os
from openai import OpenAI
from typing import List, Dict
from .config import PRE_JUNE_2025_MODELS, SEED, OPENROUTER_API_KEY
from .concept_dag import ConceptDAG
from .storage import DataStorage, ExperimentResult


def create_openai_client() -> OpenAI:
    """Create an OpenAI client configured for OpenRouter."""
    return OpenAI(
        api_key=OPENROUTER_API_KEY,
        base_url="https://openrouter.ai/api/v1"
    )


class ExperimentExecutor:
    """Executes the daydreaming experiment across models and prompt nodes."""
    
    def __init__(self, dag: ConceptDAG, storage: DataStorage, client: OpenAI):
        """
        Initialize the experiment executor.
        
        Args:
            dag: ConceptDAG containing the experiment concepts
            storage: DataStorage for saving results
            client: OpenAI client for API calls
        """
        self.dag = dag
        self.storage = storage
        self.client = client
        
    def run_experiment(self, models: List[str] = None, nodes: List[str] = None) -> None:
        """
        Run the experiment across specified models and nodes.
        
        Args:
            models: List of model identifiers to test (defaults to PRE_JUNE_2025_MODELS)
            nodes: List of node IDs to test (defaults to all leaf nodes)
        """
        if models is None:
            models = PRE_JUNE_2025_MODELS
            
        if nodes is None:
            nodes = list(self.dag.get_leaf_nodes())
            
        results = []
        
        for model in models:
            for node_id in nodes:
                # Construct full concept
                prompt = self.dag.get_full_concept(node_id)
                
                # Get response from model
                response = self._get_model_response(model, prompt)
                
                # Store result
                result = ExperimentResult(
                    model=model,
                    node_id=node_id,
                    prompt=prompt,
                    response=response,
                    seed=SEED
                )
                
                results.append(result)
                self.storage.save_result(result)
                
        # Save batch results
        self.storage.save_results_batch(results)
        
    def _get_model_response(self, model: str, prompt: str) -> str:
        """
        Get response from a specific model for a given prompt.
        
        Args:
            model: Model identifier
            prompt: Prompt text
            
        Returns:
            Model response
        """
        try:
            response = self.client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                seed=SEED
            )
            return response.choices[0].message.content
        except Exception as e:
            print(f"Error getting response from {model}: {e}")
            return f"ERROR: {str(e)}"