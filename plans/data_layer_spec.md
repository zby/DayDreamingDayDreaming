# DataLayer Module Spec (daydreaming_dagster/data_layer)

## Overview

Introduce a dedicated `daydreaming_dagster.data_layer` package that centralises all gens store filesystem operations. This package will contain:

1. A relocated `Paths` class (`data_layer/paths.py`) identical in functionality to the current `config.paths.Paths`, keeping the canonical directory helpers for gens/assets.
2. A new `GensDataLayer` class (`data_layer/gens_data_layer.py`) that wraps a data root and provides higher-level APIs (gen-id reservation, read/write of prompt/raw/parsed plus metadata).

Moving `Paths` into this package keeps the data access layer self-contained; other modules import `Paths` and/or `GensDataLayer` from `daydreaming_dagster.data_layer` going forward.

## Paths module migration
- Create `daydreaming_dagster/data_layer/paths.py` with the current `Paths` implementation (including filename constants and helper methods).
- Update existing imports (`from ..config.paths import Paths`) across the codebase to reference `from daydreaming_dagster.data_layer.paths import Paths`.
- Ensure `__all__` and filename constants remain available to avoid breaking scripts/tests.
- Remove or deprecate the old `config.paths` module after migration (or leave a shim that re-exports the new class during the transition).

## GensDataLayer class requirements (`gens_data_layer.py`)

### Construction
- Field: `data_root: Path` (absolute or relative). This should be the only stored attribute.
- Internally instantiate `Paths` when needed via `Paths.from_str(data_root)`.
- Constructor: `@classmethod from_root(cls, data_root: Path | str) -> GensDataLayer` (keep it as the sole convenience helper until callers need more).

### Directory utilities
- `generation_dir(stage, gen_id)`
- `prompt_path(stage, gen_id)`
- `raw_path(stage, gen_id)`
- `parsed_path(stage, gen_id)`
- `main_metadata_path(stage, gen_id)`
- `raw_metadata_path(stage, gen_id)`
- `parsed_metadata_path(stage, gen_id)`

### Generation reservation
- Method `reserve_generation(stage, gen_id, *, create=True)` that creates the directory and returns the path. Should reuse `Paths.generation_dir` and `mkdir(parents=True, exist_ok=True)` when `create` is True.
- Optional helper `exists(stage, gen_id)`.

### Write helpers
- `write_input(stage, gen_id, text)`
- `write_raw(stage, gen_id, text)`
- `write_parsed(stage, gen_id, text)`
- `write_main_metadata(stage, gen_id, metadata_dict)`
- `write_raw_metadata(stage, gen_id, metadata_dict)`
- `write_parsed_metadata(stage, gen_id, metadata_dict)`

Each method should ensure parent directories exist (call `reserve_generation`). Metadata writers should pretty-print JSON with UTF-8 encoding.

### Read helpers
- `read_prompt`, `read_raw`, `read_parsed` (optional convenience) returning strings.
- `read_main_metadata`, `read_raw_metadata`, `read_parsed_metadata` returning dicts (raise `FileNotFoundError` or `ValueError` appropriately).
- Skip compatibility helpers (e.g., `load_generation`) until there is a concrete need.

### Additional utilities
- Consider methods to list existing gen IDs for a stage (`list_generations(stage)`), to support scripts replacing `Paths.stage_gen_ids`.
- Provide context manager or methods to remove generations (`delete_generation(stage, gen_id)`) if needed later.
- Implementation note: for the initial cut, ship the simplest version that satisfies current needs—skip new convenience helpers until there is a concrete caller.

### Generation metadata helpers
- Colocate a typed wrapper around the JSON metadata so downstream code can avoid dealing with raw dicts.
- `resolve_generation_metadata(layer, stage, gen_id)` should load `main_metadata.json`, enforce required relationships, and return `GenerationMetadata`.

```python
from dataclasses import dataclass
from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer
from daydreaming_dagster.unified.stage_policy import parent_stage_of


def _effective_parent_stage(stage: Stage) -> Stage | None:
    parent = parent_stage_of(stage)
    if parent is None and stage == "draft":
        return "draft"
    return parent

@dataclass
class GenerationMetadata:
    stage: str
    template_id: str
    parent_gen_id: str | None
    mode: str  # "llm" or "copy"
    combo_id: str | None
    cohort_id: str | None


def resolve_generation_metadata(layer: GensDataLayer, stage: str, gen_id: str) -> GenerationMetadata:
    meta = layer.read_main_metadata(stage, gen_id)
    template_id = str(meta.get("template_id") or "")
    mode = str(meta.get("mode") or "llm").lower() or "llm"
    parent_gen_id = str(meta.get("parent_gen_id") or "").strip() or None

    parent_required = stage in ("essay", "evaluation") or mode == "copy"
    if parent_required and not parent_gen_id:
        raise ValueError(f"parent_gen_id required for {stage}/{gen_id} (mode={mode})")

    combo_val = meta.get("combo_id")
    cohort_val = meta.get("cohort_id")
    return GenerationMetadata(
        stage=stage,
        template_id=template_id,
        parent_gen_id=parent_gen_id,
        mode=mode,
        combo_id=str(combo_val).strip() if combo_val is not None else None,
        cohort_id=str(cohort_val).strip() if cohort_val is not None else None,
    )
```

- Place these definitions alongside `GensDataLayer` (e.g., in `gens_data_layer.py`) for easy imports across assets and scripts.

### Error handling
- Raises `FileNotFoundError` when trying to read missing files.
- Raises `ValueError` when metadata JSON is malformed.
- Keeps API pure (no Dagster-specific dependencies) to ease unit testing.

### Testing
- Add unit tests under `daydreaming_dagster/data_layer/tests`, covering:
  - Reservation + write/read cycle for prompt/raw/parsed/metadata.
  - Behaviour when reading missing files.
  - Interaction with legacy generation loader.

## Migration plan
1. Copy `Paths` from `config.paths` into `data_layer/paths.py`.
2. Update imports across the project to use the new module.
3. Create `gens_data_layer.py` implementing the spec above.
4. Add unit tests for the new data layer.
5. Incrementally refactor assets/scripts to use `GensDataLayer` (replace direct `Paths` usage, raw metadata writes, etc.).
6. Remove legacy `config.paths` (leave shim if necessary).

## Stage Input Asset Reimplementation (using GensDataLayer)

Assuming main metadata (template, parent references, etc.) is already written to the generation directory, refactor `unified/stage_prompts.py` so the asset becomes `stage_input_asset` and always materialises the text handed to the next phase:

```python
from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer

def stage_input_asset(context, stage: Stage, *, content_combinations=None) -> str:
    layer = GensDataLayer.from_root(context.resources.data_root)
    gen_id = context.partition_key

    metadata = resolve_generation_metadata(layer, stage, gen_id)
    layer.reserve_generation(stage, gen_id, create=True)

    parent_stage = _effective_parent_stage(stage)

    if metadata.mode == "copy":
        input_text = layer.read_parsed(parent_stage, metadata.parent_gen_id)
        layer.write_input(stage, gen_id, input_text)
        return input_text

    if stage == "draft":
        if content_combinations is None:
            raise ValueError("draft inputs require preloaded content combinations")
        combo_id = (metadata.combo_id or "").strip()
        match = next((c for c in content_combinations if getattr(c, "combo_id", None) == combo_id), None)
        if match is None:
            raise ValueError(f"content combination '{combo_id}' missing for draft/{gen_id}")
        template_vars = {"concepts": match.contents}
    elif stage in ("essay", "evaluation"):
        parent_text = layer.read_parsed(parent_stage, metadata.parent_gen_id)
        if stage == "essay":
            template_vars = {
                "draft_block": parent_text,
                "links_block": parent_text,
            }
        else:
            template_vars = {"response": parent_text}
    else:
        raise ValueError(f"Unsupported stage: {stage}")

    input_text = render_template(stage, metadata.template_id, template_vars)
    layer.write_input(stage, gen_id, input_text)
    return input_text
```

Key points:
- `resolve_generation_metadata(...)` reads the current generation’s `metadata.json` (via the data layer) and surfaces core fields (`template_id`, `mode`, `parent_gen_id`, etc.); callers reuse `parent_stage_of` when they need the upstream stage, eliminating the direct dependency on the membership service inside the asset (though cohort lookups can still be validated elsewhere).
- All filesystem interactions (directory creation, reading parent parsed text, writing prompts/inputs) go through `GensDataLayer`.
- Downstream stages can call `resolve_generation_metadata` again as needed, keeping this asset focused on producing the stage input text.
