from __future__ import annotations

from dagster import IOManager, OutputContext, InputContext
from pathlib import Path
import pandas as pd


class DocsPromptIOManager(IOManager):
    """
    IO manager that persists prompts to the docs store (data/docs/<stage>/<doc_id>/prompt.txt)
    and reads them back for downstream assets. Doc IDs are read from a configured
    tasks CSV under data/2_tasks/ by matching the partition key column.

    Config:
    - docs_root: base docs directory (data/docs)
    - tasks_root: tasks directory (data/2_tasks)
    - stage: one of {draft, essay, evaluation}
    - tasks_csv_name: name of the tasks CSV (e.g., draft_generation_tasks.csv)
    - id_col: partition key column (e.g., draft_task_id)
    """

    def __init__(self, docs_root: Path, tasks_root: Path, *, stage: str, tasks_csv_name: str | None, id_col: str):
        self.docs_root = Path(docs_root)
        self.tasks_root = Path(tasks_root)
        self.stage = str(stage)
        self.tasks_csv_name = str(tasks_csv_name) if tasks_csv_name is not None else None
        self.id_col = str(id_col)

    def _resolve_doc_id(self, partition_key: str) -> str:
        # If id_col is already doc_id, the partition key is the doc id.
        if self.id_col == "doc_id" or self.tasks_csv_name is None:
            return str(partition_key)
        csv = self.tasks_root / str(self.tasks_csv_name)
        if not csv.exists():
            raise FileNotFoundError(f"Tasks CSV not found: {csv}")
        df = pd.read_csv(csv)
        if "doc_id" not in df.columns:
            raise ValueError(f"Tasks CSV missing required 'doc_id' column: {csv}")
        m = df[df[self.id_col] == partition_key]
        if m.empty:
            raise KeyError(f"No row in {csv} for {self.id_col}={partition_key}")
        doc_id = str(m.iloc[0]["doc_id"])  # type: ignore[index]
        if not doc_id or doc_id.lower() == "nan":
            raise ValueError(f"Empty doc_id in {csv} for {self.id_col}={partition_key}")
        return doc_id

    def handle_output(self, context: OutputContext, obj: str):
        pk = context.partition_key
        if not isinstance(obj, str):
            raise ValueError(f"Expected prompt text (str), got {type(obj)}")
        doc_id = self._resolve_doc_id(pk)
        base = self.docs_root / self.stage / doc_id
        base.mkdir(parents=True, exist_ok=True)
        (base / "prompt.txt").write_text(obj, encoding="utf-8")

    def load_input(self, context: InputContext) -> str:
        upstream = context.upstream_output
        pk = upstream.partition_key
        doc_id = self._resolve_doc_id(pk)
        base = self.docs_root / self.stage / doc_id
        fp = base / "prompt.txt"
        if not fp.exists():
            raise FileNotFoundError(f"Prompt not found: {fp}")
        return fp.read_text(encoding="utf-8")
