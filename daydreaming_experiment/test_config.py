"""
Unit tests for the config module.
"""
import pytest
import os
from daydreaming_experiment.config import OPENROUTER_API_KEY, PRE_JUNE_2025_MODELS, SEED, SCORING_RUBRIC


class TestConfig:
    """Test cases for config module."""
    
    def test_api_key_exists(self):
        """Test that API key is defined (may be None if not set in environment)."""
        # API key should be defined but may be None if not set in environment
        assert OPENROUTER_API_KEY is None or isinstance(OPENROUTER_API_KEY, str)
        
    def test_models_list_exists(self):
        """Test that models list is defined."""
        assert isinstance(PRE_JUNE_2025_MODELS, list)
        
    def test_seed_value(self):
        """Test that seed is set to expected value."""
        assert SEED == 42
        
    def test_scoring_rubric_exists(self):
        """Test that scoring rubric is defined."""
        assert isinstance(SCORING_RUBRIC, dict)
        assert len(SCORING_RUBRIC) > 0