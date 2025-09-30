from daydreaming_dagster.assets.group_cohorts import MembershipRow


def test_membership_row_to_dict_defaults():
    row = MembershipRow(stage="draft", gen_id="gid", origin_cohort_id="cohort")
    assert row.to_dict() == {
        "stage": "draft",
        "gen_id": "gid",
        "origin_cohort_id": "cohort",
        "parent_gen_id": "",
        "combo_id": "",
        "template_id": "",
        "llm_model_id": "",
        "replicate": 1,
    }


def test_membership_row_to_dict_overrides():
    row = MembershipRow(
        stage="essay",
        gen_id="essay-1",
        origin_cohort_id="cohort-9",
        parent_gen_id="draft-1",
        combo_id="combo-2",
        template_id="tpl-3",
        llm_model_id="model-4",
        replicate="5",
    )
    assert row.to_dict() == {
        "stage": "essay",
        "gen_id": "essay-1",
        "origin_cohort_id": "cohort-9",
        "parent_gen_id": "draft-1",
        "combo_id": "combo-2",
        "template_id": "tpl-3",
        "llm_model_id": "model-4",
        "replicate": 5,
    }
