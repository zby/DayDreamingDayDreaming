import pandas as pd
from daydreaming_dagster.utils.ids import reserve_gen_id


def test_membership_contains_essay_rows_with_parent_links(tmp_path):
    cohort = "COH-T1"
    (tmp_path / "cohorts" / cohort).mkdir(parents=True, exist_ok=True)
    def make(draft_combo, draft_tpl, model, essay_tpl):
        draft_task = f"{draft_combo}__{draft_tpl}__{model}"
        draft_gid = reserve_gen_id("draft", draft_task, run_id=cohort)
        essay_task = f"{draft_task}__{essay_tpl}"
        essay_gid = reserve_gen_id("essay", essay_task, run_id=cohort)
        return draft_gid, essay_gid

    r1 = make("comboA", "links-v1", "modelX", "essay-tpl-A")
    r2 = make("comboB", "links-v1", "modelY", "essay-tpl-B")
    dfm = pd.DataFrame([
        {"stage": "draft", "gen_id": r1[0], "cohort_id": cohort, "parent_gen_id": "", "combo_id": "comboA", "template_id": "links-v1", "llm_model_id": "modelX"},
        {"stage": "draft", "gen_id": r2[0], "cohort_id": cohort, "parent_gen_id": "", "combo_id": "comboB", "template_id": "links-v1", "llm_model_id": "modelY"},
        {"stage": "essay", "gen_id": r1[1], "cohort_id": cohort, "parent_gen_id": r1[0], "combo_id": "comboA", "template_id": "essay-tpl-A", "llm_model_id": "modelX"},
        {"stage": "essay", "gen_id": r2[1], "cohort_id": cohort, "parent_gen_id": r2[0], "combo_id": "comboB", "template_id": "essay-tpl-B", "llm_model_id": "modelY"},
    ])
    (tmp_path / "cohorts" / cohort / "membership.csv").write_text(dfm.to_csv(index=False), encoding="utf-8")

    mdf = pd.read_csv(tmp_path / "cohorts" / cohort / "membership.csv")
    essays = mdf[mdf["stage"] == "essay"]
    assert len(essays) == 2
    assert set(essays["parent_gen_id"]) == {r1[0], r2[0]}
