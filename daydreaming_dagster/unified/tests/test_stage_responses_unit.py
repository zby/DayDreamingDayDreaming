from __future__ import annotations

import types

import pytest


def _dummy_context(partition_key: str = "G1"):
    class _Log:
        def info(self, *_args, **_kwargs):
            return None

    class _Ctx:
        def __init__(self, pk: str):
            self.partition_key = pk
            self.log = _Log()
            # Minimal resources used by tokens_and_min_lines and (stubbed) llm
            self.resources = types.SimpleNamespace(
                experiment_config=types.SimpleNamespace(
                    draft_generation_max_tokens=128,
                    evaluation_max_tokens=64,
                    min_draft_lines=2,
                ),
                openrouter_client=object(),
            )
            self.run = types.SimpleNamespace(run_id="RUN123")

        def add_output_metadata(self, _md):
            # Accept and ignore; used by emit_standard_output_metadata
            return None

    return _Ctx(partition_key)


@pytest.mark.parametrize("mode", ["llm", "copy"], ids=["llm-mode", "copy-mode"])
def test_response_asset_includes_cohort_id_in_metadata(monkeypatch, tmp_path, mode: str):
    # Import the module under test
    from daydreaming_dagster.unified import stage_responses as sr

    ctx = _dummy_context("E1")

    # Force data root to tmp_path to keep paths deterministic (even though we stub IO paths)
    import daydreaming_dagster.assets._helpers as helpers

    monkeypatch.setattr(helpers, "get_data_root", lambda _c: tmp_path)

    # Membership row + cohort
    row = {"template_id": "tpl1", "parent_gen_id": "D1", "llm_model_id": "m1"}
    monkeypatch.setattr(
        helpers,
        "require_membership_row",
        lambda _c, _stage, _gid, require_columns=None: (row, "C1"),
    )

    # Resolve generator mode
    monkeypatch.setattr(
        helpers,
        "resolve_generator_mode",
        lambda *, kind, data_root, template_id, override_from_prompt=None, filter_active=None: mode,
    )

    captured: dict = {}

    if mode == "llm":
        def _fake_execute_llm(**kwargs):
            captured["meta"] = dict(kwargs.get("metadata_extra") or {})
            # Minimal result object with the attributes used by response_asset and emit_standard_output_metadata
            return types.SimpleNamespace(
                prompt_text=str(kwargs.get("prompt_text")),
                raw_text="RAW_TEXT",
                parsed_text=None,
                info={},
                metadata={},
            )

        monkeypatch.setattr(sr, "execute_llm", _fake_execute_llm)
    else:
        def _fake_execute_copy(**kwargs):
            captured["meta"] = dict(kwargs.get("metadata_extra") or {})
            return types.SimpleNamespace(
                prompt_text=None,
                raw_text=None,
                parsed_text="PARSED_TEXT",
                info=None,
                metadata={},
            )

        monkeypatch.setattr(sr, "execute_copy", _fake_execute_copy)

    # Exercise
    out = sr.response_asset(ctx, prompt_text="PROMPT", stage="essay")

    # Verify: cohort_id is propagated into metadata_extra
    assert captured.get("meta") is not None, "metadata_extra not captured"
    assert captured["meta"].get("cohort_id") == "C1"
    # sanity: standard extras are present too
    assert captured["meta"].get("function") == "essay_response"
    assert captured["meta"].get("run_id") == "RUN123"

    # Verify return value shape based on path taken
    if mode == "llm":
        assert out == "RAW_TEXT"
    else:
        assert out == "PARSED_TEXT"

