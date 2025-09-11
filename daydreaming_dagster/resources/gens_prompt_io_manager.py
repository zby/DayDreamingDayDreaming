from __future__ import annotations

from dagster import IOManager, OutputContext, InputContext
from pathlib import Path
import pandas as pd
from ..constants import FILE_PROMPT, STAGES


class GensPromptIOManager(IOManager):
    """
    IO manager that persists prompts to the gens store (data/gens/<stage>/<gen_id>/prompt.txt)
    and reads them back for downstream assets. Partition keys are gen_ids.

    Config:
    - gens_root: base generations directory (data/gens)
    - stage: one of {draft, essay, evaluation}
    """

    def __init__(self, gens_root: Path, *, stage: str):
        self.gens_root = Path(gens_root)
        stage_str = str(stage)
        if stage_str not in STAGES:
            raise ValueError(f"Invalid stage '{stage_str}'. Expected one of {STAGES}.")
        self.stage = stage_str

    def _resolve_gen_id(self, partition_key: str) -> str:
        # Partition key is the gen_id in membership-first mode
        return str(partition_key)

    def handle_output(self, context: OutputContext, obj: str):
        pk = context.partition_key
        if not isinstance(obj, str):
            raise ValueError(f"Expected prompt text (str), got {type(obj)}")
        gen_id = self._resolve_gen_id(pk)
        base = self.gens_root / self.stage / gen_id
        base.mkdir(parents=True, exist_ok=True)
        (base / FILE_PROMPT).write_text(obj, encoding="utf-8")

    def load_input(self, context: InputContext) -> str:
        # Avoid deprecated upstream_output.partition_key; use InputContext.partition_key
        pk = context.partition_key
        gen_id = self._resolve_gen_id(pk)
        base = self.gens_root / self.stage / gen_id
        fp = base / FILE_PROMPT
        if not fp.exists():
            raise FileNotFoundError(f"Prompt not found: {fp}")
        return fp.read_text(encoding="utf-8")
