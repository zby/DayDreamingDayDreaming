"""Tests for evaluation task ID validation."""

from daydreaming_dagster.utils.dataframe_helpers import is_valid_evaluation_task_id


def test_is_valid_evaluation_task_id_happy_path():
    # Properly formatted: three parts separated by double underscores
    assert is_valid_evaluation_task_id(
        "combo123_tplX_modelY__daydreaming-verification-v2__deepseek_r1_f"
    )


def test_is_valid_evaluation_task_id_missing_separators():
    # Only one separator
    assert not is_valid_evaluation_task_id("doc__template_only")
    # Too many separators
    assert not is_valid_evaluation_task_id("a__b__c__d")


def test_is_valid_evaluation_task_id_whitespace_and_empty():
    assert not is_valid_evaluation_task_id(" ")
    assert not is_valid_evaluation_task_id("doc __tpl__model")
    assert not is_valid_evaluation_task_id("doc__tpl__")
    assert not is_valid_evaluation_task_id("__tpl__model")
    assert not is_valid_evaluation_task_id(123)  # type: ignore[arg-type]

