"""Schema tests for document_index and evaluation_tasks outputs.

These tests validate that the unified evaluation plan’s normalized columns
and identifiers remain stable. They do not execute Dagster assets; instead
they construct minimal representative rows to assert schema and ID rules.
"""

import pandas as pd
from daydreaming_dagster.utils.dataframe_helpers import is_valid_evaluation_task_id


def test_document_index_three_sources_schema():
    # Simulate three document sources
    rows = [
        {
            # Draft-as-one-phase
            "document_id": "comboA_links-v4_deepseek_r1_f",
            "stage": "essay1p",
            "origin": "draft",
            "file_path": "data/3_generation/draft_responses/comboA_links-v4_deepseek_r1_f.txt",
            "combo_id": "comboA",
            "draft_template": "links-v4",
            "essay_template": "links-v4",
            "generation_model_id": "deepseek_r1_f",
            "generation_model_name": "deepseek/deepseek-r1:free",
            "draft_task_id": "comboA_links-v4_deepseek_r1_f",
            "essay_task_id": None,
            "source_asset": "draft_response",
            "source_dir": "draft_responses",
        },
        {
            # Two-phase essay
            "document_id": "comboB_links-v4_deepseek_r1_f_creative-synthesis-v10",
            "stage": "essay2p",
            "origin": "two_phase",
            "file_path": "data/3_generation/essay_responses/comboB_links-v4_deepseek_r1_f_creative-synthesis-v10.txt",
            "combo_id": "comboB",
            "draft_template": "links-v4",
            "essay_template": "creative-synthesis-v10",
            "generation_model_id": "deepseek_r1_f",
            "generation_model_name": "deepseek/deepseek-r1:free",
            "draft_task_id": "comboB_links-v4_deepseek_r1_f",
            "essay_task_id": "comboB_links-v4_deepseek_r1_f_creative-synthesis-v10",
            "source_asset": "essay_response",
            "source_dir": "essay_responses",
        },
        {
            # Legacy one-phase
            "document_id": "comboC_creative-synthesis-v2_qwq_32b_f",
            "stage": "essay1p",
            "origin": "legacy",
            "file_path": "data/3_generation/generation_responses/comboC_creative-synthesis-v2_qwq_32b_f.txt",
            "combo_id": "comboC",
            "draft_template": None,
            "essay_template": "creative-synthesis-v2",
            "generation_model_id": "qwq_32b_f",
            "generation_model_name": "qwen/qwq-32b:free",
            "draft_task_id": None,
            "essay_task_id": None,
            "source_asset": "legacy_generation_response",
            "source_dir": "generation_responses",
        },
    ]
    df = pd.DataFrame(rows)

    required_columns = {
        "document_id",
        "stage",
        "origin",
        "file_path",
        "combo_id",
        "draft_template",
        "essay_template",
        "generation_model_id",
        "generation_model_name",
        "draft_task_id",
        "essay_task_id",
        "source_asset",
        "source_dir",
    }
    assert required_columns.issubset(set(df.columns))

    # Stage/origin should align with plan
    assert set(df[df.origin == "draft"]["stage"]) == {"essay1p"}
    assert set(df[df.origin == "two_phase"]["stage"]) == {"essay2p"}
    assert set(df[df.origin == "legacy"]["stage"]) == {"essay1p"}

    # Source directory mapping sanity checks
    assert set(df[df.origin == "draft"]["source_dir"]) == {"draft_responses"}
    assert set(df[df.origin == "two_phase"]["source_dir"]) == {"essay_responses"}
    assert set(df[df.origin == "legacy"]["source_dir"]) == {"generation_responses"}


def test_evaluation_tasks_id_and_columns():
    # Simulate a small document_index
    docs = pd.DataFrame([
        {
            "document_id": "comboA_links-v4_deepseek_r1_f",
            "stage": "essay1p",
            "origin": "draft",
            "file_path": "data/3_generation/links_responses/comboA_links-v4_deepseek_r1_f.txt",
            "combo_id": "comboA",
            "draft_template": "links-v4",
            "essay_template": "links-v4",
            "generation_model_id": "deepseek_r1_f",
            "generation_model_name": "deepseek/deepseek-r1:free",
            "draft_task_id": "comboA_links-v4_deepseek_r1_f",
            "essay_task_id": None,
            "source_asset": "links_response",
            "source_dir": "links_responses",
        },
        {
            "document_id": "comboB_links-v4_deepseek_r1_f_creative-synthesis-v10",
            "stage": "essay2p",
            "origin": "two_phase",
            "file_path": "data/3_generation/essay_responses/comboB_links-v4_deepseek_r1_f_creative-synthesis-v10.txt",
            "combo_id": "comboB",
            "draft_template": "links-v4",
            "essay_template": "creative-synthesis-v10",
            "generation_model_id": "deepseek_r1_f",
            "generation_model_name": "deepseek/deepseek-r1:free",
            "draft_task_id": "comboB_links-v4_deepseek_r1_f",
            "essay_task_id": "comboB_links-v4_deepseek_r1_f_creative-synthesis-v10",
            "source_asset": "essay_response",
            "source_dir": "essay_responses",
        },
    ])

    # Single evaluation template/model for cross product
    eval_templates = ["daydreaming-verification-v2"]
    eval_models = [("qwq_32b_f", "qwen/qwq-32b:free")]

    rows = []
    for _, doc in docs.iterrows():
        for tpl in eval_templates:
            for eval_model_id, eval_model_name in eval_models:
                    rows.append(
                    {
                        "evaluation_task_id": f"{doc['document_id']}__{tpl}__{eval_model_id}",
                        "document_id": doc["document_id"],
                        "parent_doc_id": doc["document_id"],
                        "stage": doc["stage"],
                        "origin": doc["origin"],
                        "file_path": doc["file_path"],
                        "combo_id": doc["combo_id"],
                        "draft_template": doc["draft_template"],
                        "essay_template": doc["essay_template"],
                        "generation_model": doc["generation_model_id"],
                        "generation_model_name": doc["generation_model_name"],
                        "draft_task_id": doc["draft_task_id"],
                        "essay_task_id": doc["essay_task_id"],
                        "source_asset": doc["source_asset"],
                        "source_dir": doc["source_dir"],
                        "evaluation_template": tpl,
                        "evaluation_model": eval_model_id,
                        "evaluation_model_name": eval_model_name,
                    }
                )

    evaluation_tasks_df = pd.DataFrame(rows)

    # Required columns for downstream processing
    required_cols = {
        "evaluation_task_id",
        "document_id",
        "parent_doc_id",
        "stage",
        "origin",
        "file_path",
        "combo_id",
        "draft_template",
        "essay_template",
        "generation_model",
        "generation_model_name",
        "draft_task_id",
        "essay_task_id",
        "source_asset",
        "source_dir",
        "evaluation_template",
        "evaluation_model",
        "evaluation_model_name",
    }
    assert required_cols.issubset(set(evaluation_tasks_df.columns))

    # Validate ID format
    for tid in evaluation_tasks_df["evaluation_task_id"].unique():
        assert is_valid_evaluation_task_id(tid)
