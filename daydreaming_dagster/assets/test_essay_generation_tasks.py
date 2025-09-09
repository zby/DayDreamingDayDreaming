import pandas as pd
from dagster import build_asset_context
from daydreaming_dagster.utils.ids import reserve_doc_id


def test_essay_generation_tasks_crosses_drafts_with_active_templates(monkeypatch):
    # Arrange: two draft tasks
    draft_df = pd.DataFrame(
        [
            {
                "draft_task_id": "comboA_links-v1_modelX",
                "combo_id": "comboA",
                "draft_template": "links-v1",
                "generation_model": "modelX",
                "generation_model_name": "provider/modelX",
            },
            {
                "draft_task_id": "comboB_links-v1_modelY",
                "combo_id": "comboB",
                "draft_template": "links-v1",
                "generation_model": "modelY",
                "generation_model_name": "provider/modelY",
            },
        ]
    )
    # Add required doc_id column (deterministic from task id)
    draft_df["doc_id"] = draft_df["draft_task_id"].apply(lambda t: reserve_doc_id("draft", t))

    # Active essay templates (2)
    essay_templates_df = pd.DataFrame(
        [
            {"template_id": "essay-tpl-A", "active": True},
            {"template_id": "essay-tpl-B", "active": True},
        ]
    )

    # Monkeypatch the reader used by the asset function
    import daydreaming_dagster.assets.group_task_definitions as m

    monkeypatch.setattr(m, "read_essay_templates", lambda _root: essay_templates_df)

    # Act: call asset with a real Dagster context (unit-friendly)
    ctx = build_asset_context(resources={"data_root": "data"})
    result = m.essay_generation_tasks(ctx, content_combinations=[], draft_generation_tasks=draft_df)

    # Assert: 2 drafts × 2 templates = 4 essay tasks
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 4

    # Task ids follow draft-based scheme and metadata is propagated
    expected_ids = {
        "comboA_links-v1_modelX__essay-tpl-A",
        "comboA_links-v1_modelX__essay-tpl-B",
        "comboB_links-v1_modelY__essay-tpl-A",
        "comboB_links-v1_modelY__essay-tpl-B",
    }
    assert set(result["essay_task_id"]) == expected_ids
    # Model/name propagated from draft tasks
    assert set(result[result["combo_id"] == "comboA"]["generation_model"]) == {"modelX"}
    assert set(result[result["combo_id"] == "comboB"]["generation_model_name"]) == {"provider/modelY"}
    # doc_id reserved for each essay row and parent_doc_id propagated from draft
    assert result["doc_id"].notna().all()
    assert result["doc_id"].astype(str).str.len().ge(1).all()
    assert result["parent_doc_id"].notna().all()
    # parent_doc_id must map to the draft doc ids
    expected_parent_ids = set(draft_df.set_index("draft_task_id")["doc_id"].tolist())
    assert set(result["parent_doc_id"]) <= expected_parent_ids
