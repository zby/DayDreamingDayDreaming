"""Tests for evaluation processing utilities."""

import pandas as pd
import pytest

from daydreaming_dagster.utils.errors import DDError, Err
from daydreaming_dagster.utils.evaluation_processing import (
    calculate_evaluation_metadata,
    filter_valid_scores,
)


"""Legacy parsing helpers were removed. Remaining helpers operate on DataFrames.

This test module validates the enrichment utilities that are still used by
gens-store based assets.
"""


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


def test_calculate_evaluation_metadata_requires_error_column():
    df = pd.DataFrame({'score': [1, 2, 3]})
    with pytest.raises(DDError) as err:
        calculate_evaluation_metadata(df)
    assert err.value.code is Err.INVALID_CONFIG


def test_filter_valid_scores_missing_columns():
    df = pd.DataFrame({'score': [1, 2, 3]})
    with pytest.raises(DDError) as err:
        filter_valid_scores(df)
    assert err.value.code is Err.INVALID_CONFIG


def test_filter_valid_scores_none():
    with pytest.raises(DDError) as err:
        filter_valid_scores(None)
    assert err.value.code is Err.DATA_MISSING
