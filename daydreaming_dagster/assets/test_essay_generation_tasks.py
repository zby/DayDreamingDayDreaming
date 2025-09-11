import pandas as pd
from dagster import build_asset_context
from daydreaming_dagster.utils.ids import reserve_gen_id


def test_essay_generation_tasks_projects_from_membership(tmp_path, monkeypatch):
    import daydreaming_dagster.assets.group_task_definitions as m

    cohort = "COH-T1"
    (tmp_path / "cohorts" / cohort).mkdir(parents=True, exist_ok=True)
    # Build membership with two essay rows
    def make(draft_combo, draft_tpl, model, essay_tpl):
        draft_task = f"{draft_combo}__{draft_tpl}__{model}"
        draft_gid = reserve_gen_id("draft", draft_task, run_id=cohort)
        essay_task = f"{draft_task}__{essay_tpl}"
        essay_gid = reserve_gen_id("essay", essay_task, run_id=cohort)
        return draft_gid, essay_gid, draft_task, essay_task

    r1 = make("comboA", "links-v1", "modelX", "essay-tpl-A")
    r2 = make("comboB", "links-v1", "modelY", "essay-tpl-B")
    # Normalized membership rows: include parent drafts and essays
    dfm = pd.DataFrame([
        {"stage": "draft", "gen_id": r1[0], "cohort_id": cohort, "parent_gen_id": "", "combo_id": "comboA", "template_id": "links-v1", "llm_model_id": "modelX"},
        {"stage": "draft", "gen_id": r2[0], "cohort_id": cohort, "parent_gen_id": "", "combo_id": "comboB", "template_id": "links-v1", "llm_model_id": "modelY"},
        {"stage": "essay", "gen_id": r1[1], "cohort_id": cohort, "parent_gen_id": r1[0], "combo_id": "comboA", "template_id": "essay-tpl-A", "llm_model_id": "modelX"},
        {"stage": "essay", "gen_id": r2[1], "cohort_id": cohort, "parent_gen_id": r2[0], "combo_id": "comboB", "template_id": "essay-tpl-B", "llm_model_id": "modelY"},
    ])
    (tmp_path / "cohorts" / cohort / "membership.csv").write_text(dfm.to_csv(index=False), encoding="utf-8")
    monkeypatch.setenv("DD_COHORT", cohort)

    # Run asset projection
    ctx = build_asset_context(resources={"data_root": str(tmp_path)})
    result = m.essay_generation_tasks(ctx, content_combinations=[], draft_generation_tasks=pd.DataFrame(columns=["draft_task_id"]))

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert set(result["essay_template"]) == {"essay-tpl-A", "essay-tpl-B"}
    # Computed ids
    assert set(result["essay_task_id"]) == {f"comboA__links-v1__modelX__essay-tpl-A".replace("__", "__").replace("__", "__"), f"comboB__links-v1__modelY__essay-tpl-B".replace("__", "__").replace("__", "__")}
    # Parent linkage
    assert set(result["parent_gen_id"]) == {r1[0], r2[0]}
