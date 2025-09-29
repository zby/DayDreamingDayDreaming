from daydreaming_dagster.utils.ids import (
    reserve_gen_id,
    draft_signature,
    essay_signature,
    evaluation_signature,
    compute_deterministic_gen_id,
    signature_from_metadata,
)


def test_draft_signature_normalization():
    sig = draft_signature("ComboA", "Draft-Tpl", "Gen-Model", 2)
    assert sig == ("comboa", "draft-tpl", "gen-model", 2)


def test_deterministic_gen_id_stable():
    sig = draft_signature("combo", "tpl", "model", 1)
    gid1 = compute_deterministic_gen_id("draft", sig)
    gid2 = compute_deterministic_gen_id("draft", sig)
    assert gid1 == gid2 and gid1.startswith("d_")


def test_signature_from_metadata_draft():
    meta = {
        "combo_id": "Combo-1",
        "template_id": "Draft-Alpha",
        "llm_model_id": "Model-X",
        "replicate": "3",
    }
    sig = signature_from_metadata("draft", meta)
    assert sig == ("combo-1", "draft-alpha", "model-x", 3)


def test_signature_from_metadata_eval():
    meta = {
        "parent_gen_id": "essay123",
        "template_id": "Eval-Temp",
        "llm_model_id": "Eval-Model",
        "replicate": 1,
    }
    sig = signature_from_metadata("evaluation", meta)
    assert sig == ("essay123", "eval-temp", "eval-model", 1)


class TestIDs:
    def test_reserve_gen_id_basic(self):
        a = reserve_gen_id("draft", "comboA__tmplX__model1")
        b = reserve_gen_id("draft", "comboA__tmplX__model1")
        c = reserve_gen_id("draft", "comboA__tmplX__model1", run_id="r1")
        d = reserve_gen_id("draft", "comboA__tmplX__model1", salt="s1")
        # Stable without run_id/salt
        assert a == b and len(a) == 16
        # run_id or salt perturbs id
        assert a != c and a != d
