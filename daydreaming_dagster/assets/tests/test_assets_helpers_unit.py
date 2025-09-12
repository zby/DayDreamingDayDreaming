from __future__ import annotations

from pathlib import Path
import json

import pytest

from dagster import Failure

from daydreaming_dagster.assets._helpers import (
    require_membership_row,
    load_generation_parsed_text,
    resolve_essay_generator_mode,
    emit_standard_output_metadata,
)


class _Resources:
    def __init__(self, data_root: Path):
        self.data_root = data_root


class _Ctx:
    def __init__(self, data_root: Path):
        self.resources = _Resources(data_root)
        self._last_metadata = None

    def add_output_metadata(self, md):
        self._last_metadata = md


def _write_membership(tmp_path: Path, rows: list[str]):
    root = tmp_path / "cohorts" / "C1"
    root.mkdir(parents=True, exist_ok=True)
    (root / "membership.csv").write_text("\n".join(rows), encoding="utf-8")


def test_require_membership_row_success_and_missing(tmp_path: Path):
    _write_membership(
        tmp_path,
        [
            "stage,gen_id,template_id,llm_model_id,parent_gen_id",
            "draft,D1,t,m,",
        ],
    )
    ctx = _Ctx(tmp_path)
    row, cohort = require_membership_row(ctx, "draft", "D1")
    assert row.get("template_id") == "t"
    assert cohort == "C1"
    with pytest.raises(Failure):
        require_membership_row(ctx, "essay", "E404")


def test_require_membership_row_missing_required_columns(tmp_path: Path):
    _write_membership(
        tmp_path,
        [
            "stage,gen_id,template_id,llm_model_id,parent_gen_id",
            "essay,E1,t,,D1",
        ],
    )
    ctx = _Ctx(tmp_path)
    with pytest.raises(Failure) as ei:
        require_membership_row(ctx, "essay", "E1", require_columns=["llm_model_id"])  # empty
    assert "missing_columns" in str(ei.value)


def test_load_generation_parsed_text_success_and_failure(tmp_path: Path):
    # Success
    base = tmp_path / "gens" / "draft" / "D1"
    base.mkdir(parents=True, exist_ok=True)
    (base / "parsed.txt").write_text("X\n", encoding="utf-8")
    ctx = _Ctx(tmp_path)
    text = load_generation_parsed_text(ctx, "draft", "D1", failure_fn_name="fn")
    assert text == "X\n"
    # Failure when missing
    with pytest.raises(Failure):
        load_generation_parsed_text(ctx, "draft", "D404", failure_fn_name="fn")


def test_resolve_essay_generator_mode_csv_and_override(tmp_path: Path):
    base = tmp_path / "1_raw"
    base.mkdir(parents=True, exist_ok=True)
    (base / "essay_templates.csv").write_text(
        "template_id,template_name,description,generator,active\n"
        "t1,T,Desc,llm,True\n"
        "t2,T,Desc,copy,True\n",
        encoding="utf-8",
    )
    assert resolve_essay_generator_mode(tmp_path, "t1") == "llm"
    assert resolve_essay_generator_mode(tmp_path, "t2") == "copy"
    # invalid row -> Failure
    with pytest.raises(Failure):
        resolve_essay_generator_mode(tmp_path, "missing")
    # override
    assert resolve_essay_generator_mode(tmp_path, "t1", override_from_prompt="COPY_MODE: force") == "copy"


def test_emit_standard_output_metadata(tmp_path: Path):
    ctx = _Ctx(tmp_path)
    # Mimic an ExecutionResult-like dict
    result = {
        "prompt": "A\nB",
        "raw": "X\nY\nZ",
        "parsed": "9.0\n",
        "info": {"finish_reason": "stop", "truncated": False},
    }
    emit_standard_output_metadata(ctx, function="fn", gen_id="G1", result=result, extras={"note": "ok"})
    md = ctx._last_metadata
    # Minimal checks for presence and types
    assert md["function"].value == "fn"
    assert md["gen_id"].value == "G1"
    assert md["finish_reason"].value == "stop"
    assert md["truncated"].value is False
    assert md["raw_lines"].value == 3
    assert md["parsed_chars"].value == 4
    assert md["note"].value == "ok"

