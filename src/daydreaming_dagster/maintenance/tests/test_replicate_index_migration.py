import json
from pathlib import Path

import pytest

from daydreaming_dagster.maintenance.replicate_index_migration import migrate_replicates


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


@pytest.fixture()
def data_root(tmp_path: Path) -> Path:
    root = tmp_path / "data"
    (root / "gens").mkdir(parents=True)
    return root


def test_assigns_incremental_replicates_for_duplicate_drafts(data_root: Path) -> None:
    stage_dir = data_root / "gens" / "draft"
    stage_dir.mkdir()

    def make_gen(gen_id: str) -> None:
        gen_dir = stage_dir / gen_id
        gen_dir.mkdir()
        metadata = {
            "stage": "draft",
            "gen_id": gen_id,
            "combo_id": "combo-1",
            "template_id": "template-a",
            "llm_model_id": "model-x",
        }
        raw_metadata = {
            "stage": "draft",
            "gen_id": gen_id,
            "combo_id": "combo-1",
            "template_id": "template-a",
            "llm_model_id": "model-x",
        }
        parsed_metadata = {
            "stage": "draft",
            "gen_id": gen_id,
        }
        _write_json(gen_dir / "metadata.json", metadata)
        _write_json(gen_dir / "raw_metadata.json", raw_metadata)
        _write_json(gen_dir / "parsed_metadata.json", parsed_metadata)

    make_gen("aaa")
    make_gen("bbb")

    result = migrate_replicates(data_root, stages=["draft"], execute=True)

    assert {(u.gen_id, u.new_replicate) for u in result.updates} == {("aaa", 1), ("bbb", 2)}

    for gen_id, expected in {"aaa": 1, "bbb": 2}.items():
        for filename in ("metadata.json", "raw_metadata.json", "parsed_metadata.json"):
            payload = json.loads((stage_dir / gen_id / filename).read_text(encoding="utf-8"))
            assert payload["replicate"] == expected


def test_preserves_existing_replicates_when_unique(data_root: Path) -> None:
    stage_dir = data_root / "gens" / "evaluation"
    stage_dir.mkdir()

    def make_eval(gen_id: str, replicate: int) -> None:
        gen_dir = stage_dir / gen_id
        gen_dir.mkdir()
        metadata = {
            "stage": "evaluation",
            "gen_id": gen_id,
            "template_id": "style-coherence",
            "llm_model_id": "sonnet-4",
            "parent_gen_id": "draft-123",
            "replicate": replicate,
        }
        raw_metadata = dict(metadata)
        parsed_metadata = dict(metadata)
        _write_json(gen_dir / "metadata.json", metadata)
        _write_json(gen_dir / "raw_metadata.json", raw_metadata)
        _write_json(gen_dir / "parsed_metadata.json", parsed_metadata)

    make_eval("eval-1", replicate=2)
    make_eval("eval-2", replicate=3)

    result = migrate_replicates(data_root, stages=["evaluation"], execute=True)

    assert not result.updates

    for gen_id, expected in {"eval-1": 2, "eval-2": 3}.items():
        payload = json.loads((stage_dir / gen_id / "metadata.json").read_text(encoding="utf-8"))
        assert payload["replicate"] == expected
