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
- Provide alternate constructors:
  - `@classmethod from_root(cls, data_root: Path | str) -> GensDataLayer`
  - `@classmethod from_context(cls, context) -> GensDataLayer` (grabs `context.resources.data_root`).

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
- `write_prompt(stage, gen_id, text)`
- `write_raw(stage, gen_id, text)`
- `write_parsed(stage, gen_id, text)`
- `write_main_metadata(stage, gen_id, metadata_dict)`
- `write_raw_metadata(stage, gen_id, metadata_dict)`
- `write_parsed_metadata(stage, gen_id, metadata_dict)`

Each method should ensure parent directories exist (call `reserve_generation`). Metadata writers should pretty-print JSON with UTF-8 encoding.

### Read helpers
- `read_prompt`, `read_raw`, `read_parsed` (optional convenience) returning strings.
- `read_main_metadata`, `read_raw_metadata`, `read_parsed_metadata` returning dicts (raise `FileNotFoundError` or `ValueError` appropriately).
- `load_generation(stage, gen_id)` delegating to `utils.generation.load_generation` for compatibility.

### Additional utilities
- Consider methods to list existing gen IDs for a stage (`list_generations(stage)`), to support scripts replacing `Paths.stage_gen_ids`.
- Provide context manager or methods to remove generations (`delete_generation(stage, gen_id)`) if needed later.

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

