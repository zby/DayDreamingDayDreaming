from __future__ import annotations

import types

import daydreaming_dagster.unified.stage_prompts as sp
import daydreaming_dagster.assets._helpers as helpers


def _make_context(tmp_path, *, partition_key: str = "G1", membership_service=None):
    class _Ctx:
        def __init__(self, pk: str, resources_dict):
            self.partition_key = pk
            self.resources = types.SimpleNamespace(**resources_dict)
            self.captured_metadata = None

        def add_output_metadata(self, metadata):
            self.captured_metadata = metadata

    resources = {"data_root": str(tmp_path)}
    if membership_service is not None:
        resources["membership_service"] = membership_service
    return _Ctx(partition_key, resources)


def test_prompt_asset_copy_mode(monkeypatch, tmp_path):
    # Arrange stubs
    parent_stage = "essay"
    parent_gen = "P1"
    parent_text = "Essay body"

    monkeypatch.setattr(
        helpers,
        "load_parent_parsed_text",
        lambda context, stage, gen_id, **kwargs: (parent_gen, parent_text),
    )

    monkeypatch.setattr(
        sp,
        "get_stage_spec",
        lambda stage: types.SimpleNamespace(
            prompt_fields=["template_id", "parent_gen_id"],
            parent_stage=parent_stage,
        ),
    )

    monkeypatch.setattr(
        sp,
        "read_membership_fields",
        lambda row: types.SimpleNamespace(
            template_id=row.get("template_id"),
            parent_gen_id=row.get("parent_gen_id"),
            combo_id=row.get("combo_id"),
            llm_model_id=row.get("llm_model_id"),
        ),
    )

    monkeypatch.setattr(
        sp,
        "resolve_generator_mode",
        lambda *, kind, data_root, template_id, override_from_prompt=None, filter_active=None: "copy",
    )

    captured = {}
    original_build_prompt_metadata = helpers.build_prompt_metadata

    def _capture_metadata(context, **kwargs):
        captured["kwargs"] = kwargs
        return original_build_prompt_metadata(context, **kwargs)

    monkeypatch.setattr(helpers, "build_prompt_metadata", _capture_metadata)

    class StubMembership:
        def require_row(self, data_root, stage, gen_id, require_columns=None):
            return ({"template_id": "essay-tpl", "parent_gen_id": parent_gen}, "C1")

    ctx = _make_context(tmp_path, partition_key="E1", membership_service=StubMembership())

    # Act
    result = sp.prompt_asset(ctx, "essay")

    # Assert
    assert result == parent_text
    extras = captured["kwargs"]["extras"]
    assert extras["input_mode"] == "copy"
    assert extras["copied_from"].endswith("essay/P1/parsed.txt")


def test_prompt_asset_prompt_mode(monkeypatch, tmp_path):
    combo = types.SimpleNamespace(combo_id="combo-1", contents=["idea"])

    monkeypatch.setattr(
        sp,
        "get_stage_spec",
        lambda stage: types.SimpleNamespace(
            prompt_fields=["template_id", "combo_id"],
            parent_stage=None,
            build_prompt_values=lambda context, gen_id, mf, combinations: (
                {"concepts": combinations[0].contents}, {"combo": mf.combo_id}, None
            ),
        ),
    )

    monkeypatch.setattr(
        sp,
        "read_membership_fields",
        lambda row: types.SimpleNamespace(
            template_id=row.get("template_id"),
            parent_gen_id=row.get("parent_gen_id"),
            combo_id=row.get("combo_id"),
            llm_model_id=row.get("llm_model_id"),
        ),
    )

    monkeypatch.setattr(
        sp,
        "resolve_generator_mode",
        lambda *, kind, data_root, template_id, override_from_prompt=None, filter_active=None: "llm",
    )

    monkeypatch.setattr(
        sp,
        "render_template",
        lambda stage, template_id, values: f"PROMPT-{stage}-{template_id}-{values['concepts'][0]}",
    )

    captured = {}
    original_build_prompt_metadata = helpers.build_prompt_metadata

    def _capture_metadata(context, **kwargs):
        captured["kwargs"] = kwargs
        return original_build_prompt_metadata(context, **kwargs)

    monkeypatch.setattr(helpers, "build_prompt_metadata", _capture_metadata)

    class StubMembership:
        def require_row(self, data_root, stage, gen_id, require_columns=None):
            return ({"template_id": "draft-tpl", "combo_id": combo.combo_id}, "C1")

    ctx = _make_context(tmp_path, partition_key="D1", membership_service=StubMembership())

    # Act
    result = sp.prompt_asset(ctx, "draft", content_combinations=[combo])

    # Assert
    assert result == "PROMPT-draft-draft-tpl-idea"
    extras = captured["kwargs"]["extras"]
    assert extras["input_mode"] == "prompt"
