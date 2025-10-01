from __future__ import annotations

"""BACKCOMPAT: legacy generation helpers forwarding to :class:`GensDataLayer`."""

from pathlib import Path
from typing import Any, Callable, Dict, Optional

from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer
from daydreaming_dagster.utils.errors import DDError, Err

__all__ = [
    "write_gen_raw",
    "write_gen_parsed",
    "write_gen_prompt",
    "write_gen_metadata",
    "load_generation",
    "GensDataLayer",
]


def _infer_data_root(gens_root: Path | str) -> Path:
    root = Path(gens_root)
    if root.name == "gens":
        parent = root.parent
        return parent if str(parent) != "" else root
    return root


def _layer_for(gens_root: Path | str) -> GensDataLayer:
    return GensDataLayer.from_root(_infer_data_root(gens_root))


def _generation_dir(layer: GensDataLayer, stage: str, gen_id: str) -> Path:
    path = layer.paths.generation_dir(stage, gen_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_gen_raw(gens_root: Path, stage: str, gen_id: str, text: str) -> Path:
    """Write raw.txt for a generation, returning the generation directory."""

    layer = _layer_for(gens_root)
    layer.write_raw(stage, gen_id, text)
    return _generation_dir(layer, stage, gen_id)


def write_gen_parsed(gens_root: Path, stage: str, gen_id: str, text: str) -> Path:
    """Write parsed.txt for a generation, returning the generation directory."""

    layer = _layer_for(gens_root)
    layer.write_parsed(stage, gen_id, text)
    return _generation_dir(layer, stage, gen_id)


def write_gen_prompt(gens_root: Path, stage: str, gen_id: str, text: str) -> Path:
    """Write prompt.txt for a generation, returning the generation directory."""

    layer = _layer_for(gens_root)
    layer.write_input(stage, gen_id, text)
    return _generation_dir(layer, stage, gen_id)


def write_gen_metadata(gens_root: Path, stage: str, gen_id: str, metadata: Dict[str, Any]) -> Path:
    """Write metadata.json for a generation, returning the generation directory."""

    layer = _layer_for(gens_root)
    layer.write_main_metadata(stage, gen_id, metadata)
    return _generation_dir(layer, stage, gen_id)


def _read_optional(callable_: Callable[[str, str], Any], stage: str, gen_id: str) -> Optional[Any]:
    try:
        return callable_(stage, gen_id)
    except DDError as err:
        if err.code is Err.DATA_MISSING:
            return None
        raise


def load_generation(gens_root: Path, stage: str, gen_id: str) -> Dict[str, Any]:
    """Best-effort read of an existing generation into a dict of artifacts."""

    layer = _layer_for(gens_root)

    metadata: Optional[Dict[str, Any]]
    try:
        metadata = layer.read_main_metadata(stage, gen_id)
    except DDError as err:
        if err.code is Err.DATA_MISSING:
            metadata = None
        else:
            raise

    raw_text = _read_optional(layer.read_raw, stage, gen_id) or ""
    parsed_text = _read_optional(layer.read_parsed, stage, gen_id)
    prompt_text = _read_optional(layer.read_input, stage, gen_id)

    return {
        "stage": str(stage),
        "gen_id": str(gen_id),
        "parent_gen_id": (metadata or {}).get("parent_gen_id"),
        "raw_text": raw_text,
        "parsed_text": parsed_text,
        "prompt_text": prompt_text,
        "metadata": metadata,
    }
