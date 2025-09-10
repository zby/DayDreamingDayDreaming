"""Tests for evaluation processing utilities."""

import pandas as pd
import pytest
from pathlib import Path
from unittest.mock import Mock

from daydreaming_dagster.utils.evaluation_processing import calculate_evaluation_metadata


"""Legacy parsing helpers were removed. Remaining helpers operate on DataFrames.

This test module validates the enrichment utilities that are still used by
gens-store based assets.
"""


def dummy():
    pass


def test_calculate_evaluation_metadata():
    """Test evaluation metadata calculation."""
    df = pd.DataFrame({
        'score': [8.0, 9.0, 7.0],
        'error': [None, None, None]
    })
    
    metadata = calculate_evaluation_metadata(df)
    
    assert metadata['total_responses'].value == 3
    assert metadata['successful_parses'].value == 3
    assert metadata['success_rate'].value == 100.0
    assert metadata['avg_score'].value == 8.0


    # Parsing functions removed; file path helper removed.
