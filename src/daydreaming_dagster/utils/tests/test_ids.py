import pytest

from daydreaming_dagster.utils.errors import DDError, Err
import json

from daydreaming_dagster.utils.ids import (
    reserve_gen_id,
    draft_signature,
    essay_signature,
    evaluation_signature,
    compute_deterministic_gen_id,
    compute_collision_resolved_gen_id,
    signature_from_metadata,
)


def test_draft_signature_normalization():
    sig = draft_signature("ComboA", "Draft-Tpl", "Gen-Model", 2)
    assert sig == ("comboa", "draft-tpl", "gen-model", 2)


def test_draft_signature_missing_component_raises():
    with pytest.raises(DDError) as err:
        draft_signature(None, "tpl", "model", 1)
    assert err.value.code is Err.INVALID_CONFIG


def test_deterministic_gen_id_stable():
    sig = draft_signature("combo", "tpl", "model", 1)
    gid1 = compute_deterministic_gen_id("draft", sig)
    gid2 = compute_deterministic_gen_id("draft", sig)
    assert gid1 == gid2 and gid1.startswith("d_")


def test_deterministic_gen_id_collision_suffix():
    sig = draft_signature("combo", "tpl", "model", 1)
    base = compute_deterministic_gen_id("draft", sig)
    suffixed = compute_deterministic_gen_id("draft", sig, collision_index=1)
    assert suffixed.endswith("-1")
    assert suffixed != base


def test_compute_deterministic_gen_id_rejects_non_stage():
    sig = draft_signature("combo", "tpl", "model", 1)
    with pytest.raises(DDError) as err:
        compute_deterministic_gen_id("DRAFT", sig)  # type: ignore[arg-type]
    assert err.value.code is Err.INVALID_CONFIG


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


def test_signature_from_metadata_invalid_stage():
    with pytest.raises(DDError) as err:
        signature_from_metadata("unknown", {})  # type: ignore[arg-type]
    assert err.value.code is Err.INVALID_CONFIG


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


def test_compute_collision_resolved_gen_id_reuses_existing(tmp_path, monkeypatch):
    from daydreaming_dagster.utils import ids

    def fake_compute(stage, signature, collision_index=0):
        base = f"{stage[0]}_fixed"
        return base if collision_index == 0 else f"{base}-{collision_index}"

    monkeypatch.setattr(ids, "compute_deterministic_gen_id", fake_compute)

    stage_root = tmp_path / "draft" / "d_fixed"
    stage_root.mkdir(parents=True)
    metadata = {
        "combo_id": "combo-a",
        "template_id": "tpl-a",
        "llm_model_id": "model-a",
        "replicate": 1,
    }
    (stage_root / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")

    sig = ids.draft_signature("combo-a", "tpl-a", "model-a", 1)
    result = compute_collision_resolved_gen_id("draft", sig, tmp_path)
    assert result == "d_fixed"


def test_compute_collision_resolved_gen_id_generates_suffix(tmp_path, monkeypatch):
    from daydreaming_dagster.utils import ids

    def fake_compute(stage, signature, collision_index=0):
        base = f"{stage[0]}_fixed"
        return base if collision_index == 0 else f"{base}-{collision_index}"

    monkeypatch.setattr(ids, "compute_deterministic_gen_id", fake_compute)

    occupied = tmp_path / "draft" / "d_fixed"
    occupied.mkdir(parents=True)
    metadata = {
        "combo_id": "combo-existing",
        "template_id": "tpl-existing",
        "llm_model_id": "model-existing",
        "replicate": 1,
    }
    (occupied / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")

    sig = ids.draft_signature("combo-new", "tpl-new", "model-new", 1)
    result = compute_collision_resolved_gen_id("draft", sig, tmp_path)
    assert result == "d_fixed-1"
