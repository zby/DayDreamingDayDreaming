from __future__ import annotations

from pathlib import Path

import pytest

from daydreaming_dagster.resources.membership_service import MembershipServiceResource
from daydreaming_dagster.unified.stage_core import resolve_generator_mode
from daydreaming_dagster.utils.errors import DDError, Err


class _Resources:
    def __init__(self, data_root: Path):
        self.data_root = data_root


def _write_membership(tmp_path: Path, rows: list[str]):
    root = tmp_path / "cohorts" / "C1"
    root.mkdir(parents=True, exist_ok=True)
    (root / "membership.csv").write_text("\n".join(rows), encoding="utf-8")


def test_stage_gen_ids_basic(tmp_path: Path):
    _write_membership(
        tmp_path,
        [
            "stage,gen_id",
            "draft,D1",
            "essay,E1",
            "evaluation,V1",
        ],
    )
    svc = MembershipServiceResource()
    assert svc.stage_gen_ids(tmp_path, "draft") == ["D1"]
    assert set(svc.stage_gen_ids(tmp_path, "evaluation")) == {"V1"}


def test_stage_gen_ids_filters_by_cohort(tmp_path: Path):
    root1 = tmp_path / "cohorts" / "C1"
    root1.mkdir(parents=True, exist_ok=True)
    (root1 / "membership.csv").write_text("stage,gen_id\ndraft,D1\n", encoding="utf-8")
    root2 = tmp_path / "cohorts" / "C2"
    root2.mkdir(parents=True, exist_ok=True)
    (root2 / "membership.csv").write_text("stage,gen_id\ndraft,D2\n", encoding="utf-8")

    svc = MembershipServiceResource()
    assert svc.stage_gen_ids(tmp_path, "draft") == ["D1", "D2"]
    assert svc.stage_gen_ids(tmp_path, "draft", cohort_id="C1") == ["D1"]


def test_resolve_essay_generator_mode_csv_and_override(tmp_path: Path):
    base = tmp_path / "1_raw"
    base.mkdir(parents=True, exist_ok=True)
    (base / "essay_templates.csv").write_text(
        "template_id,template_name,description,generator\n"
        "t1,T,Desc,llm\n"
        "t2,T,Desc,copy\n",
        encoding="utf-8",
    )
    assert resolve_generator_mode(kind="essay", data_root=tmp_path, template_id="t1") == "llm"
    assert resolve_generator_mode(kind="essay", data_root=tmp_path, template_id="t2") == "copy"
    # invalid row -> structured error
    with pytest.raises(DDError) as err:
        resolve_generator_mode(kind="essay", data_root=tmp_path, template_id="missing")
    assert err.value.code is Err.DATA_MISSING
    # override
    assert (
        resolve_generator_mode(
            kind="essay", data_root=tmp_path, template_id="t1", override_from_prompt="COPY_MODE: force"
        )
        == "copy"
    )
