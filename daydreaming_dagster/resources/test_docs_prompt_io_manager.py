from __future__ import annotations

from pathlib import Path
import pandas as pd

from .gens_prompt_io_manager import GensPromptIOManager


class _OutCtx:
    def __init__(self, partition_key: str):
        self.partition_key = partition_key


class _Upstream:
    def __init__(self, partition_key: str):
        self.partition_key = partition_key


class _InCtx:
    def __init__(self, upstream):
        self.upstream_output = upstream


def test_gens_prompt_io_manager_write_and_read(tmp_path: Path):
    data_root = tmp_path / "data"
    gens_root = data_root / "gens"
    tasks_root = data_root / "2_tasks"
    tasks_root.mkdir(parents=True, exist_ok=True)

    # Prepare a draft tasks CSV with preallocated gen_id
    draft_task_id = "combo_x_links-v1_modelA"
    gen_id = "abcd1234efgh5678"
    df = pd.DataFrame([
        {"draft_task_id": draft_task_id, "gen_id": gen_id}
    ])
    (tasks_root / "draft_generation_tasks.csv").write_text(df.to_csv(index=False), encoding="utf-8")

    io = GensPromptIOManager(
        gens_root=gens_root,
        tasks_root=tasks_root,
        stage="draft",
        tasks_csv_name="draft_generation_tasks.csv",
        id_col="draft_task_id",
    )

    # Write prompt
    io.handle_output(_OutCtx(draft_task_id), "hello world")
    fp = gens_root / "draft" / gen_id / "prompt.txt"
    assert fp.exists()
    assert fp.read_text(encoding="utf-8") == "hello world"

    # Read prompt via input context
    text = io.load_input(_InCtx(_Upstream(draft_task_id)))
    assert text == "hello world"
