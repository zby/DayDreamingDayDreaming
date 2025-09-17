from __future__ import annotations

from pathlib import Path
import json

import pytest

from dagster import Failure

from daydreaming_dagster.assets._helpers import emit_standard_output_metadata
from daydreaming_dagster.resources.membership_service import MembershipServiceResource
from daydreaming_dagster.unified.stage_core import resolve_generator_mode


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
    svc = MembershipServiceResource()
    row, cohort = svc.require_row(tmp_path, "draft", "D1")
    assert row.get("template_id") == "t"
    assert cohort == "C1"
    with pytest.raises(Failure):
        svc.require_row(tmp_path, "essay", "E404")


def test_require_membership_row_missing_required_columns(tmp_path: Path):
    _write_membership(
        tmp_path,
        [
            "stage,gen_id,template_id,llm_model_id,parent_gen_id",
            "essay,E1,t,,D1",
        ],
    )
    svc = MembershipServiceResource()
    with pytest.raises(Failure) as ei:
        svc.require_row(tmp_path, "essay", "E1", require_columns=["llm_model_id"])  # empty
    assert "missing_columns" in str(ei.value)


def test_resolve_essay_generator_mode_csv_and_override(tmp_path: Path):
    base = tmp_path / "1_raw"
    base.mkdir(parents=True, exist_ok=True)
    (base / "essay_templates.csv").write_text(
        "template_id,template_name,description,generator,active\n"
        "t1,T,Desc,llm,True\n"
        "t2,T,Desc,copy,True\n",
        encoding="utf-8",
    )
    assert resolve_generator_mode(kind="essay", data_root=tmp_path, template_id="t1") == "llm"
    assert resolve_generator_mode(kind="essay", data_root=tmp_path, template_id="t2") == "copy"
    # invalid row -> ValueError (policy layer)
    with pytest.raises(ValueError):
        resolve_generator_mode(kind="essay", data_root=tmp_path, template_id="missing")
    # override
    assert (
        resolve_generator_mode(
            kind="essay", data_root=tmp_path, template_id="t1", override_from_prompt="COPY_MODE: force"
        )
        == "copy"
    )


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
    # Minimal checks for presence and types (no re-derived counts in UI metadata)
    assert md["function"].value == "fn"
    assert md["gen_id"].value == "G1"
    assert md["finish_reason"].value == "stop"
    assert md["truncated"].value is False
    assert md["note"].value == "ok"
