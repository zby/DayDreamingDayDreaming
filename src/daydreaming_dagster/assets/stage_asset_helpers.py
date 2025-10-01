"""Helper builders for stage prompt/raw/parsed Dagster assets.

These helpers encapsulate the shared wiring logic between Dagster assets and the unified stage primitives so individual stage modules can remain declarative.
"""

from __future__ import annotations

from typing import Optional, Sequence, Set

from dagster import AssetIn, AssetKey, MetadataValue

from ._decorators import asset_with_boundary
from ._error_boundary import resume_notice
from ._helpers import build_stage_artifact_metadata, get_run_id
from ..data_layer.gens_data_layer import GensDataLayer, resolve_generation_metadata
from ..unified.stage_core import Stage, resolve_parser_name
from ..unified.stage_inputs import _stage_input_asset
from ..unified.stage_raw import _stage_raw_asset
from ..unified.stage_parsed import _stage_parsed_asset
from ..utils.errors import DDError, Err


def _ensure_deps(deps: Optional[Sequence[AssetKey]]) -> dict:
    return {"deps": set(deps)} if deps else {}


def _resolve_stage_settings(context, stage: Stage):
    experiment_config = getattr(context.resources, "experiment_config", None)
    if experiment_config is None:
        return None
    return experiment_config.stage_config.get(stage)


def build_prompt_asset(
    *,
    stage: Stage,
    name: str,
    docstring: str,
    partitions_def,
    group_name: str,
    io_manager_key: str,
    required_resource_keys: Set[str],
    deps: Optional[Sequence[AssetKey]] = None,
    needs_content_combinations: bool = False,
    skip_if_parsed_exists: bool = False,
):
    """Create a prompt asset that delegates to the unified stage input helper."""

    asset_kwargs = {
        "name": name,
        "partitions_def": partitions_def,
        "group_name": group_name,
        "io_manager_key": io_manager_key,
        "required_resource_keys": required_resource_keys,
    }
    asset_kwargs.update(_ensure_deps(deps))

    if needs_content_combinations:

        @asset_with_boundary(stage=str(stage), **asset_kwargs)
        def prompt_asset(context, content_combinations):
            data_layer = GensDataLayer.from_root(context.resources.data_root)
            gen_id = str(context.partition_key)

            if skip_if_parsed_exists and data_layer.parsed_exists(stage, gen_id):
                input_text = data_layer.read_input(stage, gen_id)
                context.add_output_metadata(
                    {
                        "resume": MetadataValue.json(
                            resume_notice(
                                stage=str(stage),
                                gen_id=gen_id,
                                artifact="prompt",
                                reason="parsed_exists",
                            )
                        )
                    }
                )
                return input_text

            input_text, info = _stage_input_asset(
                data_layer=data_layer,
                stage=stage,
                gen_id=gen_id,
                content_combinations=content_combinations,
            )

            context.add_output_metadata(
                build_stage_artifact_metadata(
                    function=f"{stage}_stage_input",
                    artifact_label="input",
                    metadata=info,
                    text=input_text,
                )
            )
            return input_text

    else:

        @asset_with_boundary(stage=str(stage), **asset_kwargs)
        def prompt_asset(context):
            data_layer = GensDataLayer.from_root(context.resources.data_root)
            gen_id = str(context.partition_key)

            if skip_if_parsed_exists and data_layer.parsed_exists(stage, gen_id):
                input_text = data_layer.read_input(stage, gen_id)
                context.add_output_metadata(
                    {
                        "resume": MetadataValue.json(
                            resume_notice(
                                stage=str(stage),
                                gen_id=gen_id,
                                artifact="prompt",
                                reason="parsed_exists",
                            )
                        )
                    }
                )
                return input_text

            input_text, info = _stage_input_asset(
                data_layer=data_layer,
                stage=stage,
                gen_id=gen_id,
            )

            context.add_output_metadata(
                build_stage_artifact_metadata(
                    function=f"{stage}_stage_input",
                    artifact_label="input",
                    metadata=info,
                    text=input_text,
                )
            )
            return input_text

    prompt_asset.__doc__ = docstring
    return prompt_asset


def build_raw_asset(
    *,
    stage: Stage,
    name: str,
    docstring: str,
    partitions_def,
    group_name: str,
    io_manager_key: str,
    required_resource_keys: Set[str],
    prompt_input_asset_key: str,
    prompt_input_param: str | None = None,
    deps: Optional[Sequence[AssetKey]] = None,
    skip_if_parsed_exists: bool = False,
):
    """Create a raw asset that delegates to the unified raw helper."""

    input_param = prompt_input_param or prompt_input_asset_key
    asset_kwargs = {
        "name": name,
        "partitions_def": partitions_def,
        "group_name": group_name,
        "io_manager_key": io_manager_key,
        "required_resource_keys": required_resource_keys,
        "ins": {
            input_param: AssetIn(key=AssetKey(prompt_input_asset_key))
        },
    }
    asset_kwargs.update(_ensure_deps(deps))

    alias_keys = {input_param, "prompt_text"}

    def _raw_asset_impl(context, prompt_value, asset_inputs):
        data_layer = GensDataLayer.from_root(context.resources.data_root)
        gen_id = str(context.partition_key)

        if skip_if_parsed_exists and data_layer.parsed_exists(stage, gen_id):
            raw_text = data_layer.read_raw(stage, gen_id)
            context.add_output_metadata(
                {
                    "resume": MetadataValue.json(
                        resume_notice(
                            stage=str(stage),
                            gen_id=gen_id,
                            artifact="raw",
                            reason="parsed_exists",
                        )
                    )
                }
            )
            return raw_text

        prompt_text = prompt_value
        if prompt_text is None:
            for alias in (input_param, "prompt_text"):
                if alias in asset_inputs:
                    prompt_text = asset_inputs[alias]
                    break
        else:
            if alias_keys & asset_inputs.keys():
                raise DDError(
                    Err.INVALID_CONFIG,
                    ctx={"stage": stage, "gen_id": gen_id, "reason": "duplicate_prompt_input"},
                )

        if prompt_text is None:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={"stage": stage, "gen_id": gen_id, "reason": "missing_prompt_input"},
            )

        unexpected = set(asset_inputs) - alias_keys
        if unexpected:
            raise DDError(
                Err.INVALID_CONFIG,
                ctx={
                    "stage": stage,
                    "gen_id": gen_id,
                    "reason": "unexpected_prompt_kwargs",
                    "keys": sorted(unexpected),
                },
            )

        stage_settings = _resolve_stage_settings(context, stage)
        llm_client = getattr(context.resources, "openrouter_client", None)
        run_id = get_run_id(context)

        raw_text, raw_metadata = _stage_raw_asset(
            data_layer=data_layer,
            stage=stage,
            gen_id=gen_id,
            prompt_text=prompt_text,
            llm_client=llm_client,
            stage_settings=stage_settings,
            run_id=run_id,
        )

        context.add_output_metadata(
            build_stage_artifact_metadata(
                function=f"{stage}_raw",
                artifact_label="raw",
                metadata=raw_metadata,
                text=raw_text,
            )
        )
        return raw_text

    namespace: dict[str, object] = {"_raw_asset_impl": _raw_asset_impl}
    exec(
        "def _generated_raw_asset(context, {param}=None, **asset_inputs):\n"
        "    return _raw_asset_impl(context, {param}, asset_inputs)\n".format(param=input_param),
        namespace,
    )
    raw_fn = namespace["_generated_raw_asset"]

    raw_asset = asset_with_boundary(stage=f"{stage}_raw", **asset_kwargs)(raw_fn)
    raw_asset.__doc__ = docstring
    return raw_asset


def build_parsed_asset(
    *,
    stage: Stage,
    name: str,
    docstring: str,
    partitions_def,
    group_name: str,
    io_manager_key: str,
    required_resource_keys: Set[str],
    deps: Sequence[AssetKey],
    skip_if_parsed_exists: bool = False,
    fail_on_truncation: bool = True,
):
    """Create a parsed asset that delegates to the unified parsed helper."""

    asset_kwargs = {
        "name": name,
        "partitions_def": partitions_def,
        "group_name": group_name,
        "io_manager_key": io_manager_key,
        "required_resource_keys": required_resource_keys,
        "deps": set(deps),
    }

    @asset_with_boundary(stage=f"{stage}_parsed", **asset_kwargs)
    def parsed_asset(context):
        data_layer = GensDataLayer.from_root(context.resources.data_root)
        gen_id = str(context.partition_key)

        if skip_if_parsed_exists and data_layer.parsed_exists(stage, gen_id):
            parsed_text = data_layer.read_parsed(stage, gen_id)
            context.add_output_metadata(
                {
                    "resume": MetadataValue.json(
                        resume_notice(
                            stage=str(stage),
                            gen_id=gen_id,
                            artifact="parsed",
                            reason="parsed_exists",
                        )
                    )
                }
            )
            return parsed_text

        raw_text = data_layer.read_raw(stage, gen_id)

        metadata = resolve_generation_metadata(data_layer, stage, gen_id)
        try:
            raw_metadata = data_layer.read_raw_metadata(stage, gen_id)
        except DDError as err:
            if err.code is Err.DATA_MISSING:
                raw_metadata = {}
            else:
                raise

        parser_name = resolve_parser_name(
            data_layer.data_root,
            stage,
            metadata.template_id,
            None,
        )

        stage_settings = _resolve_stage_settings(context, stage)
        min_lines_override = stage_settings.min_lines if stage_settings else None

        parsed_text, parsed_metadata = _stage_parsed_asset(
            data_layer=data_layer,
            stage=stage,
            gen_id=gen_id,
            raw_text=raw_text,
            parser_name=parser_name,
            raw_metadata=raw_metadata,
            stage_settings=stage_settings,
            min_lines_override=min_lines_override,
            fail_on_truncation=fail_on_truncation,
        )

        run_id = get_run_id(context)
        if run_id:
            parsed_metadata["run_id"] = run_id
            data_layer.write_parsed_metadata(stage, gen_id, parsed_metadata)

        context.add_output_metadata(
            build_stage_artifact_metadata(
                function=f"{stage}_parsed",
                artifact_label="parsed",
                metadata=parsed_metadata,
                text=parsed_text,
            )
        )
        return parsed_text

    parsed_asset.__doc__ = docstring
    return parsed_asset


__all__ = [
    "build_prompt_asset",
    "build_raw_asset",
    "build_parsed_asset",
]
