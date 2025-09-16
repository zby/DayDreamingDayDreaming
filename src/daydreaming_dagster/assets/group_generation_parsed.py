"""
Experimental parsed generation assets for draft, essay, and evaluation stages.

These assets transform raw generation outputs into parsed text and metadata.
They currently coexist with legacy response assets and are not yet wired into
Definitions.
"""

from __future__ import annotations

from dagster import MetadataValue

from ._decorators import asset_with_boundary
from .partitions import draft_gens_partitions, essay_gens_partitions, evaluation_gens_partitions
from ..config.paths import Paths
from ..unified.raw_generation import RawGenerationResult
from ..unified.parsed_generation import ParsedGenerationResult, perform_parsed_generation
from ..unified.stage_policy import (
    get_stage_spec,
    read_membership_fields,
)
from ..unified.stage_core import resolve_parser_name
from ..assets._helpers import get_run_id


def _membership_service(context):
    svc = getattr(getattr(context, "resources", object()), "membership_service", None)
    if not (svc and hasattr(svc, "require_row")):
        from daydreaming_dagster.resources.membership_service import MembershipServiceResource
        svc = MembershipServiceResource()
    return svc


def _parsed_metadata_extras(raw_metadata):
    extras = {}
    for key in ("input_mode", "copied_from", "cohort_id", "replicate", "combo_id"):
        if key in raw_metadata:
            extras[key] = raw_metadata[key]
    return extras


@asset_with_boundary(
    stage="draft_parsed",
    partitions_def=draft_gens_partitions,
    group_name="generation_draft",
    io_manager_key="draft_parsed_io_manager",
    required_resource_keys={"data_root"},
)
def draft_parsed(context, draft_raw: RawGenerationResult) -> ParsedGenerationResult:
    paths = Paths.from_context(context)
    data_root = paths.data_root
    svc = _membership_service(context)
    spec = get_stage_spec("draft")
    row, _cohort = svc.require_row(data_root, "draft", str(context.partition_key), require_columns=spec.response_fields)
    mf = read_membership_fields(row)

    parser = resolve_parser_name(data_root, "draft", mf.template_id, None)
    _, min_lines = spec.tokens_and_min_lines(context)

    result = perform_parsed_generation(
        stage="draft",
        gen_id=str(context.partition_key),
        data_root=data_root,
        raw_text=draft_raw.raw_text,
        raw_metadata=draft_raw.raw_metadata,
        parser_name=parser,
        min_lines=min_lines,
        fail_on_truncation=True,
        metadata_extras={"function": "draft_parsed", "run_id": get_run_id(context), **_parsed_metadata_extras(draft_raw.raw_metadata)},
    )

    parsed_path = paths.parsed_path("draft", str(context.partition_key))
    context.add_output_metadata(
        {
            "function": MetadataValue.text("draft_parsed"),
            "gen_id": MetadataValue.text(str(context.partition_key)),
            "parsed_path": MetadataValue.path(str(parsed_path)),
            "parsed_metadata": MetadataValue.json(result.parsed_metadata),
        }
    )
    return result


@asset_with_boundary(
    stage="essay_parsed",
    partitions_def=essay_gens_partitions,
    group_name="generation_essays",
    io_manager_key="essay_parsed_io_manager",
    required_resource_keys={"data_root"},
)
def essay_parsed(context, essay_raw: RawGenerationResult) -> ParsedGenerationResult:
    paths = Paths.from_context(context)
    data_root = paths.data_root
    svc = _membership_service(context)
    spec = get_stage_spec("essay")
    row, _cohort = svc.require_row(data_root, "essay", str(context.partition_key), require_columns=spec.response_fields)
    mf = read_membership_fields(row)

    parser = resolve_parser_name(data_root, "essay", mf.template_id, None)
    _, min_lines = spec.tokens_and_min_lines(context)

    result = perform_parsed_generation(
        stage="essay",
        gen_id=str(context.partition_key),
        data_root=data_root,
        raw_text=essay_raw.raw_text,
        raw_metadata=essay_raw.raw_metadata,
        parser_name=parser,
        min_lines=min_lines,
        fail_on_truncation=True,
        metadata_extras={"function": "essay_parsed", "run_id": get_run_id(context), **_parsed_metadata_extras(essay_raw.raw_metadata)},
    )

    parsed_path = paths.parsed_path("essay", str(context.partition_key))
    context.add_output_metadata(
        {
            "function": MetadataValue.text("essay_parsed"),
            "gen_id": MetadataValue.text(str(context.partition_key)),
            "parsed_path": MetadataValue.path(str(parsed_path)),
            "parsed_metadata": MetadataValue.json(result.parsed_metadata),
        }
    )
    return result


@asset_with_boundary(
    stage="evaluation_parsed",
    partitions_def=evaluation_gens_partitions,
    group_name="evaluation",
    io_manager_key="evaluation_parsed_io_manager",
    required_resource_keys={"data_root"},
)
def evaluation_parsed(context, evaluation_raw: RawGenerationResult) -> ParsedGenerationResult:
    paths = Paths.from_context(context)
    data_root = paths.data_root
    svc = _membership_service(context)
    spec = get_stage_spec("evaluation")
    row, _cohort = svc.require_row(data_root, "evaluation", str(context.partition_key), require_columns=spec.response_fields)
    mf = read_membership_fields(row)

    parser = resolve_parser_name(data_root, "evaluation", mf.template_id, None)
    _, min_lines = spec.tokens_and_min_lines(context)

    result = perform_parsed_generation(
        stage="evaluation",
        gen_id=str(context.partition_key),
        data_root=data_root,
        raw_text=evaluation_raw.raw_text,
        raw_metadata=evaluation_raw.raw_metadata,
        parser_name=parser,
        min_lines=min_lines,
        fail_on_truncation=True,
        metadata_extras={"function": "evaluation_parsed", "run_id": get_run_id(context), **_parsed_metadata_extras(evaluation_raw.raw_metadata)},
    )

    parsed_path = paths.parsed_path("evaluation", str(context.partition_key))
    context.add_output_metadata(
        {
            "function": MetadataValue.text("evaluation_parsed"),
            "gen_id": MetadataValue.text(str(context.partition_key)),
            "parsed_path": MetadataValue.path(str(parsed_path)),
            "parsed_metadata": MetadataValue.json(result.parsed_metadata),
        }
    )
    return result


__all__ = [
    "draft_parsed",
    "essay_parsed",
    "evaluation_parsed",
]
