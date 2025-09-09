"""Unit test: select top-N essays by prior-art scores and build evaluation tasks.

This test demonstrates the external-selection workflow without touching Dagster assets:
- Filter parsed_scores for prior-art templates
- Aggregate best score per document across the two templates
- Select top-N documents
- Create evaluation_task_id entries for a target evaluation template/model
"""

import pandas as pd

from daydreaming_dagster.utils.dataframe_helpers import is_valid_evaluation_task_id


def select_top_n_documents(parsed_scores: pd.DataFrame, templates: list[str], top_n: int) -> list[str]:
    """Select top-N document_ids by max score across the given templates (union).

    - Filters to rows where evaluation_template ∈ templates and error is null
    - Groups by document_id and takes the maximum score across those templates
    - Returns the top-N document_ids by descending score (stable order by score then doc id)
    """
    if parsed_scores is None or parsed_scores.empty:
        return []

    filt = parsed_scores[
        parsed_scores["evaluation_template"].isin(templates)
        & parsed_scores["error"].isna()
        & parsed_scores["score"].notna()
    ].copy()
    if filt.empty:
        return []

    best = (
        filt.groupby("document_id")["score"].max().reset_index().sort_values(
            by=["score", "document_id"], ascending=[False, True]
        )
    )
    return best.head(top_n)["document_id"].tolist()


def test_select_top_n_and_build_eval_tasks():
    # Simulated parsed_scores across two prior-art templates
    parsed_scores = pd.DataFrame(
        [
            {"document_id": "docA", "evaluation_template": "gemini-prior-art-eval", "score": 8.9, "error": None},
            {"document_id": "docA", "evaluation_template": "gemini-prior-art-eval-v2", "score": 9.1, "error": None},
            {"document_id": "docB", "evaluation_template": "gemini-prior-art-eval", "score": 9.3, "error": None},
            {"document_id": "docC", "evaluation_template": "gemini-prior-art-eval-v2", "score": 7.8, "error": None},
            {"document_id": "docD", "evaluation_template": "gemini-prior-art-eval", "score": 9.3, "error": None},
            {"document_id": "docE", "evaluation_template": "gemini-prior-art-eval-v2", "score": 6.5, "error": None},
        ]
    )

    prior_art_templates = ["gemini-prior-art-eval", "gemini-prior-art-eval-v2"]
    top_n = 2
    selected_docs = select_top_n_documents(parsed_scores, prior_art_templates, top_n)

    # Expect docB and docD tie at 9.3; stable tiebreaker by doc id picks docB, then docD
    assert selected_docs == ["docB", "docD"]

    # Build evaluation tasks for a target evaluation (e.g., novelty) and single model
    eval_template = "novelty"
    eval_models = [("qwq_32b_f", "qwen/qwq-32b:free")]

    rows = []
    for doc_id in selected_docs:
        for model_id, model_name in eval_models:
            rows.append(
                {
                    "evaluation_task_id": f"{doc_id}__{eval_template}__{model_id}",
                    "parent_gen_id": doc_id,
                    "evaluation_template": eval_template,
                    "evaluation_model": model_id,
                    "evaluation_model_name": model_name,
                }
            )

    evaluation_tasks_df = pd.DataFrame(rows)

    # Validate counts and ID format
    assert len(evaluation_tasks_df) == top_n * len(eval_models)
    for tid in evaluation_tasks_df["evaluation_task_id"].tolist():
        assert is_valid_evaluation_task_id(tid), f"Bad evaluation_task_id: {tid}"

    # Ensure only selected docs are present
    assert set(evaluation_tasks_df["parent_gen_id"]) == set(selected_docs)
