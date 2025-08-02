"""Pytest fixtures for Dagster integration testing."""

import pytest
from unittest.mock import Mock
from dagster import DagsterInstance
from daydreaming_dagster.definitions import defs
from daydreaming_dagster.resources.llm_client import LLMClientResource
from dagster import ConfigurableResource
import tempfile
from pathlib import Path


class MockLLMClient:
    """Stateful mock LLM client for testing."""
    
    def __init__(self, responses=None, should_fail=False):
        self.responses = responses or {}
        self.should_fail = should_fail
        self.call_count = 0
        self.calls_made = []
    
    def generate(self, prompt, model, temperature=0.7):
        if self.should_fail:
            raise Exception("Mock LLM API failure")
        
        self.call_count += 1
        call_info = {
            'prompt': prompt[:100] + "..." if len(prompt) > 100 else prompt,
            'model': model,
            'temperature': temperature
        }
        self.calls_made.append(call_info)
        
        if model in self.responses:
            return self.responses[model]
        return f"Mock response {self.call_count} for {model}"


class MockLLMResource(ConfigurableResource):
    """Reusable mock LLM resource for testing - matches LLMClientResource interface."""
    
    responses: dict = {}
    should_fail: bool = False
    
    def model_post_init(self, __context):
        """Called after pydantic initialization - safe place to create client."""
        super().model_post_init(__context)
        # Create a persistent client that can be accessed for call tracking
        self._client = MockLLMClient(responses=self.responses, should_fail=self.should_fail)
    
    def generate(self, prompt: str, model: str, temperature: float = 0.7, max_tokens: int = None) -> str:
        """Generate method that matches LLMClientResource interface."""
        # Ensure client exists (in case of timing issues)
        if not hasattr(self, '_client'):
            self._client = MockLLMClient(responses=self.responses, should_fail=self.should_fail)
        return self._client.generate(prompt, model, temperature)
    
    @property
    def call_count(self):
        """Proxy to client's call count."""
        if hasattr(self, '_client'):
            return self._client.call_count
        return 0
    
    @property 
    def calls_made(self):
        """Proxy to client's calls made."""
        if hasattr(self, '_client'):
            return self._client.calls_made
        return []


@pytest.fixture
def ephemeral_instance():
    """Provide a fresh Dagster instance for each test."""
    return DagsterInstance.ephemeral()


@pytest.fixture
def mock_creative_llm():
    """Mock LLM that gives creative responses."""
    return MockLLMResource(
        responses={
            "deepseek/deepseek-r1:free": "This represents a breakthrough in daydreaming methodology, combining innovative approaches with systematic analysis.",
            "google/gemma-3-27b-it:free": "A creative synthesis emerges from these concepts, suggesting novel pathways for discovery."
        }
    )


@pytest.fixture
def mock_evaluator_llm():
    """Mock LLM that gives evaluation scores."""
    return MockLLMResource(
        responses={
            "deepseek/deepseek-r1:free": "**SCORE**: 8.5\n\nThis response demonstrates exceptional creativity and insight into the daydreaming process.",
            "google/gemma-3-27b-it:free": "**SCORE**: 7.2\n\nGood analysis with some creative elements, but could explore deeper connections."
        }
    )


@pytest.fixture
def mock_failing_llm():
    """Mock LLM that simulates API failures."""
    return MockLLMResource(should_fail=True)


@pytest.fixture
def test_resources_with_mock_llm(mock_creative_llm):
    """Complete resource set with mock LLM for testing."""
    return {
        **defs.resources,
        "openrouter_client": mock_creative_llm
    }


@pytest.fixture
def temp_directory():
    """Provide a temporary directory for test data."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def small_test_data():
    """Provide minimal test data for faster tests."""
    import pandas as pd
    
    return {
        "concepts_metadata": pd.DataFrame([
            {"concept_id": "test-concept-1", "name": "Test Concept One"},
            {"concept_id": "test-concept-2", "name": "Test Concept Two"}
        ]),
        "generation_models": pd.DataFrame([
            {"model_name": "test-model", "active": True}
        ]),
        "evaluation_models": pd.DataFrame([
            {"model_name": "test-eval-model", "active": True}
        ])
    }