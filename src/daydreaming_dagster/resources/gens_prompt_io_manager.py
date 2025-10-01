from __future__ import annotations

from dagster import IOManager, OutputContext, InputContext
from pathlib import Path

from ..types import STAGES
from ..data_layer.gens_data_layer import GensDataLayer
from ..utils.errors import DDError, Err


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
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={"stage": stage_str, "reason": "invalid_stage_for_prompt_io"},
            )
        self.stage = stage_str
        data_root = self.gens_root.parent
        self._data_layer = GensDataLayer.from_root(data_root)

    def _resolve_gen_id(self, partition_key: str) -> str:
        # Partition key is the gen_id in membership-first mode
        return str(partition_key)

    def handle_output(self, context: OutputContext, obj: str):
        pk = context.partition_key
        if not isinstance(obj, str):
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={"reason": "prompt_io_requires_str", "type": str(type(obj))},
            )
        gen_id = self._resolve_gen_id(pk)
        self._data_layer.write_input(self.stage, gen_id, obj)

    def load_input(self, context: InputContext) -> str:
        # Avoid deprecated upstream_output.partition_key; use InputContext.partition_key
        pk = context.partition_key
        gen_id = self._resolve_gen_id(pk)
        return self._data_layer.read_input(self.stage, gen_id)
