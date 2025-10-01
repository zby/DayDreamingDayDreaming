from __future__ import annotations

import json

from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer
from daydreaming_dagster.utils.ids import compute_deterministic_gen_id, evaluation_signature


def test_reserve_draft_id_deterministic_stable(tmp_path) -> None:
    layer = GensDataLayer(tmp_path)

    gen_id_first = layer.reserve_draft_id(
        combo_id="combo-1",
        template_id="draft-tpl",
        llm_model_id="model-a",
        cohort_id="cohort-x",
        replicate=1,
    )
    gen_id_second = layer.reserve_draft_id(
        combo_id="combo-1",
        template_id="draft-tpl",
        llm_model_id="model-a",
        cohort_id="cohort-x",
        replicate=1,
    )

    assert gen_id_first == gen_id_second
    assert gen_id_first.startswith("d_")


def test_reserve_evaluation_id_collision_yields_suffix(tmp_path) -> None:
    layer = GensDataLayer(tmp_path)

    signature = evaluation_signature("essay-1", "eval-tpl", "model-b", 1)
    candidate = compute_deterministic_gen_id("evaluation", signature)

    metadata_path = layer.paths.metadata_path("evaluation", candidate)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(
        json.dumps(
            {
                "stage": "evaluation",
                "gen_id": candidate,
                "parent_gen_id": "essay-other",
                "template_id": "eval-tpl",
                "llm_model_id": "model-b",
                "replicate": 1,
            }
        ),
        encoding="utf-8",
    )

    resolved = layer.reserve_evaluation_id(
        essay_gen_id="essay-1",
        template_id="eval-tpl",
        llm_model_id="model-b",
        cohort_id="cohort-x",
        replicate=1,
    )

    assert resolved != candidate
    assert resolved.startswith("v_")
    assert resolved.endswith("-1")


def test_reserve_draft_id_hash_fallback_uses_salt(tmp_path, monkeypatch) -> None:
    try:
        import daydreaming_dagster.data_layer.gens_data_layer as layer_module
    except ImportError:  # pragma: no cover - defensive
        raise AssertionError("gens_data_layer module missing")

    monkeypatch.setattr(layer_module, "DETERMINISTIC_GEN_IDS_ENABLED", False)
    monkeypatch.setattr("daydreaming_dagster.utils.ids.DETERMINISTIC_GEN_IDS_ENABLED", False)

    layer = GensDataLayer(tmp_path)

    base_params = {
        "combo_id": "combo-1",
        "template_id": "draft-tpl",
        "llm_model_id": "model-a",
        "cohort_id": "cohort-x",
    }

    gen_id_rep1 = layer.reserve_draft_id(replicate=1, **base_params)
    gen_id_rep2 = layer.reserve_draft_id(replicate=2, **base_params)

    assert not gen_id_rep1.startswith("d_")
    assert gen_id_rep1 != gen_id_rep2
    assert len(gen_id_rep1) == len(gen_id_rep2) == 16
