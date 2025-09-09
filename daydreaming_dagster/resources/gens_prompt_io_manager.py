from __future__ import annotations

from dagster import IOManager, OutputContext, InputContext
from pathlib import Path
import pandas as pd


class GensPromptIOManager(IOManager):
    """
    IO manager that persists prompts to the gens store (data/gens/<stage>/<gen_id>/prompt.txt)
    and reads them back for downstream assets. Gen IDs are read from a configured
    tasks CSV under data/2_tasks/ by matching the partition key column.

    Config:
    - gens_root: base generations directory (data/gens)
    - tasks_root: tasks directory (data/2_tasks)
    - stage: one of {draft, essay, evaluation}
    - tasks_csv_name: name of the tasks CSV (e.g., draft_generation_tasks.csv)
    - id_col: partition key column (must be gen_id when using doc-id partitions)
    """

    def __init__(self, gens_root: Path, tasks_root: Path, *, stage: str, tasks_csv_name: str | None, id_col: str):
        self.gens_root = Path(gens_root)
        self.tasks_root = Path(tasks_root)
        self.stage = str(stage)
        self.tasks_csv_name = str(tasks_csv_name) if tasks_csv_name is not None else None
        self.id_col = str(id_col)

    def _resolve_gen_id(self, partition_key: str) -> str:
        # If id_col is already gen_id, the partition key is the gen id.
        if self.id_col == "gen_id" or self.tasks_csv_name is None:
            return str(partition_key)
        csv = self.tasks_root / str(self.tasks_csv_name)
        if not csv.exists():
            raise FileNotFoundError(f"Tasks CSV not found: {csv}")
        df = pd.read_csv(csv)
        if "gen_id" not in df.columns:
            raise ValueError(f"Tasks CSV missing required 'gen_id' column: {csv}")
        m = df[df[self.id_col] == partition_key]
        if m.empty:
            raise KeyError(f"No row in {csv} for {self.id_col}={partition_key}")
        gen_id = str(m.iloc[0]["gen_id"])  # type: ignore[index]
        if not gen_id or gen_id.lower() == "nan":
            raise ValueError(f"Empty gen_id in {csv} for {self.id_col}={partition_key}")
        return gen_id

    def handle_output(self, context: OutputContext, obj: str):
        pk = context.partition_key
        if not isinstance(obj, str):
            raise ValueError(f"Expected prompt text (str), got {type(obj)}")
        gen_id = self._resolve_gen_id(pk)
        base = self.gens_root / self.stage / gen_id
        base.mkdir(parents=True, exist_ok=True)
        (base / "prompt.txt").write_text(obj, encoding="utf-8")

    def load_input(self, context: InputContext) -> str:
        upstream = context.upstream_output
        pk = upstream.partition_key
        gen_id = self._resolve_gen_id(pk)
        base = self.gens_root / self.stage / gen_id
        fp = base / "prompt.txt"
        if not fp.exists():
            raise FileNotFoundError(f"Prompt not found: {fp}")
        return fp.read_text(encoding="utf-8")

