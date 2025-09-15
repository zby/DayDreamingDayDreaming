from __future__ import annotations

from dagster import IOManager, OutputContext, InputContext
from pathlib import Path
from ..constants import STAGES
from ..utils.generation import write_gen_prompt, load_generation
from ..config.paths import Paths


class GensPromptIOManager(IOManager):
    """
    IO manager that persists prompts to the gens store (data/gens/<stage>/<gen_id>/prompt.txt)
    and reads them back for downstream assets. Partition keys are gen_ids.

    Config:
    - gens_root: base generations directory (data/gens)
    - stage: one of {draft, essay, evaluation}

    Note: Responses (raw/parsed/metadata) are not handled via an IO manager.
    Response writes in stage_core.execute_llm intentionally happen before validation
    so failures still leave raw.txt and metadata.json for postmortem debugging. An IO
    manager writes only after a successful return, and thus cannot provide the same
    early-write behavior that tests and operators rely on.
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
        write_gen_prompt(self.gens_root, self.stage, gen_id, obj)

    def load_input(self, context: InputContext) -> str:
        # Avoid deprecated upstream_output.partition_key; use InputContext.partition_key
        pk = context.partition_key
        gen_id = self._resolve_gen_id(pk)
        doc = load_generation(self.gens_root, self.stage, gen_id)
        prompt = doc.get("prompt_text")
        if not isinstance(prompt, str) or not prompt:
            p = Paths.from_str(str(self.gens_root.parent)).prompt_path(self.stage, gen_id)
            raise FileNotFoundError(f"Prompt not found: {p}")
        return prompt
