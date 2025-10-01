from collections import OrderedDict

import pytest

from daydreaming_dagster.spec_dsl.compiler import compile_design
from daydreaming_dagster.spec_dsl.errors import SpecDslError, SpecDslErrorCode
from daydreaming_dagster.spec_dsl.models import AxisSpec, ExperimentSpec, ReplicateSpec


def make_spec(**kwargs):
    return ExperimentSpec(**kwargs)


def test_compile_design_cartesian_product() -> None:
    spec = make_spec(
        axes={
            "draft_template": AxisSpec("draft_template", ("draft-A", "draft-A"), {"catalog": "drafts"}),
            "essay_model": AxisSpec("essay_model", ("llm-1", "llm-2")),
        },
        rules=(),
        output={},
    )

    rows = compile_design(spec, catalogs={"drafts": {"draft-A"}})

    assert rows == [
        OrderedDict([("draft_template", "draft-A"), ("essay_model", "llm-1")]),
        OrderedDict([("draft_template", "draft-A"), ("essay_model", "llm-2")]),
    ]


def test_compile_design_supports_subset_rules() -> None:
    spec = make_spec(
        axes={"draft_template": AxisSpec("draft_template", ("draft-A",))},
        rules=({"subset": {"axis": "draft_template", "keep": ["draft-A"]}},),
        output={},
    )

    rows = compile_design(spec)

    assert rows == [OrderedDict([("draft_template", "draft-A")])]


@pytest.mark.parametrize(
    "subset_payload,error_key",
    [
        ({"axis": "draft_template", "keep": "not-list"}, "subset.keep must be list"),
        ({"axis": 1, "keep": ["draft-A"]}, "subset.axis must be string"),
        ({"axis": "missing", "keep": ["draft-A"]}, "subset axis missing"),
    ],
)
def test_compile_design_subset_validation(subset_payload, error_key) -> None:
    spec = make_spec(
        axes={"draft_template": AxisSpec("draft_template", ("draft-A", "draft-B"))},
        rules=( {"subset": subset_payload}, ),
        output={},
    )

    with pytest.raises(SpecDslError) as exc:
        compile_design(spec)

    assert exc.value.code is SpecDslErrorCode.INVALID_SPEC
    assert error_key in exc.value.ctx["error"]


def test_compile_design_subset_must_leave_values() -> None:
    spec = make_spec(
        axes={"draft_template": AxisSpec("draft_template", ("draft-A", "draft-B"))},
        rules=({"subset": {"axis": "draft_template", "keep": ["missing"]}},),
        output={},
    )

    with pytest.raises(SpecDslError) as exc:
        compile_design(spec)

    assert exc.value.code is SpecDslErrorCode.INVALID_SPEC
    assert exc.value.ctx["axis"] == "draft_template"


def test_compile_design_catalog_validation() -> None:
    spec = make_spec(
        axes={
            "draft_template": AxisSpec(
                "draft_template",
                ("draft-A", "draft-B"),
                {"catalog": "drafts"},
            )
        },
        rules=(),
        output={},
    )

    with pytest.raises(SpecDslError) as exc:
        compile_design(spec, catalogs={"drafts": {"draft-A"}})

    assert exc.value.code is SpecDslErrorCode.INVALID_SPEC
    assert exc.value.ctx["missing"] == ("draft-B",)


def test_compile_design_tie_merges_axes_and_intersects_levels() -> None:
    spec = make_spec(
        axes={
            "draft_template": AxisSpec("draft_template", ("draft-A", "draft-B", "draft-C")),
            "essay_template": AxisSpec("essay_template", ("draft-B", "draft-C", "draft-D")),
        },
        rules=({"tie": {"axes": ["draft_template", "essay_template"]}},),
        output={},
    )

    rows = compile_design(spec)

    assert rows == [
        OrderedDict([
            ("draft_template", "draft-B"),
            ("essay_template", "draft-B"),
        ]),
        OrderedDict([
            ("draft_template", "draft-C"),
            ("essay_template", "draft-C"),
        ]),
    ]


def test_compile_design_tie_with_to_keeps_new_name() -> None:
    spec = make_spec(
        axes={
            "draft_template": AxisSpec("draft_template", ("d1", "d2")),
            "essay_template": AxisSpec("essay_template", ("d1", "d3")),
        },
        rules=({"tie": {"axes": ["draft_template", "essay_template"], "to": "paired_templates"}},),
        output={},
    )

    rows = compile_design(spec)

    assert rows == [
        OrderedDict([
            ("paired_templates", "d1"),
            ("draft_template", "d1"),
            ("essay_template", "d1"),
        ])
    ]


def test_compile_design_tie_validation_errors() -> None:
    spec = make_spec(
        axes={"draft_template": AxisSpec("draft_template", ("d1",))},
        rules=({"tie": {"axes": ["draft_template", "missing"]}},),
        output={},
    )

    with pytest.raises(SpecDslError) as exc:
        compile_design(spec)

    assert exc.value.code is SpecDslErrorCode.INVALID_SPEC
    assert exc.value.ctx["axis"] == "missing"


def test_compile_design_tie_empty_intersection_errors() -> None:
    spec = make_spec(
        axes={
            "draft_template": AxisSpec("draft_template", ("d1", "d2")),
            "essay_template": AxisSpec("essay_template", ("d3", "d4")),
        },
        rules=({"tie": {"axes": ["draft_template", "essay_template"]}},),
        output={},
    )

    with pytest.raises(SpecDslError) as exc:
        compile_design(spec)

    assert exc.value.code is SpecDslErrorCode.INVALID_SPEC
    assert "empty intersection" in exc.value.ctx["error"]


def test_compile_design_pair_replaces_axes_with_pairs() -> None:
    spec = make_spec(
        axes={
            "draft_template": AxisSpec("draft_template", ("d1", "d2"), {"catalog": "drafts"}),
            "essay_template": AxisSpec("essay_template", ("e1", "e2")),
        },
        rules=(
            {
                "pair": {
                    "left": "draft_template",
                    "right": "essay_template",
                    "name": "draft_essay",
                    "allowed": [["d1", "e1"], ["d2", "e2"], ["d1", "e1"]],
                }
            },
        ),
        output={},
    )

    rows = compile_design(spec, catalogs={"drafts": {"d1", "d2"}})

    assert rows == [
        OrderedDict([("draft_template", "d1"), ("essay_template", "e1")]),
        OrderedDict([("draft_template", "d2"), ("essay_template", "e2")]),
    ]


def test_compile_design_pair_validates_balance_and_domains() -> None:
    spec = make_spec(
        axes={
            "draft_template": AxisSpec("draft_template", ("d1", "d2"), {"catalog": "drafts"}),
            "essay_template": AxisSpec("essay_template", ("e1", "e2")),
        },
        rules=(
            {
                "pair": {
                    "left": "draft_template",
                    "right": "essay_template",
                    "name": "draft_essay",
                    "allowed": [["d1", "e1"], ["d2", "e2"], ["d2", "e1"]],
                    "balance": "left",
                }
            },
        ),
        output={},
    )

    with pytest.raises(SpecDslError) as exc:
        compile_design(spec, catalogs={"drafts": {"d1", "d2"}})

    assert exc.value.code is SpecDslErrorCode.INVALID_SPEC
    assert exc.value.ctx and "imbalance" in exc.value.ctx["error"]


def test_compile_design_pair_can_keep_pair_axis() -> None:
    spec = make_spec(
        axes={
            "draft_template": AxisSpec("draft_template", ("d1",), {"catalog": "drafts"}),
            "essay_template": AxisSpec("essay_template", ("e1",)),
        },
        rules=({
            "pair": {
                "left": "draft_template",
                "right": "essay_template",
                "name": "draft_essay",
                "allowed": [["d1", "e1"]],
            }
        },),
        output={"keep_pair_axis": True},
    )

    rows = compile_design(spec, catalogs={"drafts": {"d1"}})

    assert rows == [
        OrderedDict([
            ("draft_essay", ("d1", "e1")),
            ("draft_template", "d1"),
            ("essay_template", "e1"),
        ])
    ]


def test_compile_design_pair_without_expand_pairs() -> None:
    spec = make_spec(
        axes={
            "draft_template": AxisSpec("draft_template", ("d1",), {"catalog": "drafts"}),
            "essay_template": AxisSpec("essay_template", ("e1",)),
        },
        rules=({
            "pair": {
                "left": "draft_template",
                "right": "essay_template",
                "name": "draft_essay",
                "allowed": [["d1", "e1"]],
            }
        },),
        output={"expand_pairs": False, "keep_pair_axis": True},
    )

    rows = compile_design(spec, catalogs={"drafts": {"d1"}})

    assert rows == [OrderedDict([("draft_essay", ("d1", "e1"))])]


def test_compile_design_pair_after_tie_resolves_axis_names() -> None:
    spec = make_spec(
        axes={
            "draft_template": AxisSpec("draft_template", ("d1", "d2"), {"catalog": "drafts"}),
            "essay_template": AxisSpec("essay_template", ("d1", "d2")),
            "eval_template": AxisSpec("eval_template", ("e1", "e2")),
        },
        rules=(
            {"tie": {"axes": ["draft_template", "essay_template"], "to": "paired"}},
            {
                "pair": {
                    "left": "draft_template",
                    "right": "eval_template",
                    "name": "draft_eval",
                    "allowed": [["d1", "e1"], ["d2", "e2"]],
                }
            },
        ),
        output={},
    )

    rows = compile_design(spec, catalogs={"drafts": {"d1", "d2"}})

    assert rows == [
        OrderedDict([("draft_template", "d1"), ("eval_template", "e1")]),
        OrderedDict([("draft_template", "d2"), ("eval_template", "e2")]),
    ]


def test_compile_design_pair_validation_errors() -> None:
    spec = make_spec(
        axes={
            "draft_template": AxisSpec("draft_template", ("d1",), {"catalog": "drafts"}),
            "essay_template": AxisSpec("essay_template", ("e1",)),
        },
        rules=(
            {
                "pair": {
                    "left": "draft_template",
                    "right": "essay_template",
                    "name": "draft_essay",
                    "allowed": [["d1", "missing"]],
                }
            },
        ),
        output={},
    )

    with pytest.raises(SpecDslError) as exc:
        compile_design(spec, catalogs={"drafts": {"d1"}})

    assert exc.value.code is SpecDslErrorCode.INVALID_SPEC
    assert exc.value.ctx and exc.value.ctx["pair"] == ("d1", "missing")


def test_compile_design_tuple_replaces_axes_and_expands_by_default() -> None:
    spec = make_spec(
        axes={
            "essay_template": AxisSpec("essay_template", ("e1", "e2")),
            "essay_llm": AxisSpec("essay_llm", ("mA", "mB")),
        },
        rules=({
            "tuple": {
                "name": "essay_bundle",
                "axes": ["essay_template", "essay_llm"],
                "items": [["e1", "mA"], ["e2", "mB"], ["e1", "mA"]],
            }
        },),
        output={},
    )

    rows = compile_design(spec)

    assert rows == [
        OrderedDict([
            ("essay_template", "e1"),
            ("essay_llm", "mA"),
        ]),
        OrderedDict([
            ("essay_template", "e2"),
            ("essay_llm", "mB"),
        ]),
    ]


def test_compile_design_tuple_can_keep_axis_without_expansion() -> None:
    spec = make_spec(
        axes={
            "essay_template": AxisSpec("essay_template", ("e1",)),
            "essay_llm": AxisSpec("essay_llm", ("mA",)),
        },
        rules=({
            "tuple": {
                "name": "essay_bundle",
                "axes": ["essay_template", "essay_llm"],
                "items": [["e1", "mA"]],
                "expand": False,
            }
        },),
        output={"keep_tuple_axis": True},
    )

    rows = compile_design(spec)

    assert rows == [OrderedDict([("essay_bundle", ("e1", "mA"))])]


def test_compile_design_tuple_validation_errors() -> None:
    spec = make_spec(
        axes={
            "essay_template": AxisSpec("essay_template", ("e1",)),
            "essay_llm": AxisSpec("essay_llm", ("mA",)),
        },
        rules=({
            "tuple": {
                "name": "essay_bundle",
                "axes": ["essay_template", "essay_llm"],
                "items": [["e1", "wrong"]],
            }
        },),
        output={},
    )

    with pytest.raises(SpecDslError) as exc:
        compile_design(spec)

    assert exc.value.code is SpecDslErrorCode.INVALID_SPEC
    assert exc.value.ctx and exc.value.ctx["value"] == "wrong"


def test_compile_design_respects_seed_for_shuffling() -> None:
    spec = make_spec(
        axes={
            "draft_template": AxisSpec("draft_template", ("d1", "d2", "d3")),
        },
        rules=(),
        output={},
    )

    rows_a = compile_design(spec, seed=123)
    rows_b = compile_design(spec, seed=123)
    rows_c = compile_design(spec, seed=456)

    assert rows_a == rows_b
    assert rows_a != rows_c


def test_compile_design_applies_replicates() -> None:
    replicates = {"draft_template": ReplicateSpec(axis="draft_template", count=2, column="draft_rep")}
    spec = make_spec(
        axes={"draft_template": AxisSpec("draft_template", ("d1", "d2"))},
        rules=(),
        output={},
        replicates=replicates,
    )

    rows = compile_design(spec)

    assert len(rows) == 4
    assert rows[0]["draft_rep"] == 1
    assert rows[1]["draft_rep"] == 2


def test_compile_design_replicate_axis_missing_errors() -> None:
    replicates = {"missing_axis": ReplicateSpec(axis="missing_axis", count=2, column="missing_rep")}
    spec = make_spec(
        axes={"draft_template": AxisSpec("draft_template", ("d1",))},
        rules=(),
        output={},
        replicates=replicates,
    )

    with pytest.raises(SpecDslError) as exc:
        compile_design(spec)

    assert exc.value.code is SpecDslErrorCode.INVALID_SPEC
    assert exc.value.ctx["axis"] == "missing_axis"
    spec = make_spec(
        axes={
            "essay_template": AxisSpec("essay_template", ("e1",)),
            "essay_llm": AxisSpec("essay_llm", ("mA",)),
        },
        rules=({
            "tuple": {
                "name": "essay_bundle",
                "axes": ["essay_template", "essay_llm"],
                "items": [["e1", "wrong"]],
            }
        },),
        output={},
    )

    with pytest.raises(SpecDslError) as exc:
        compile_design(spec)

    assert exc.value.code is SpecDslErrorCode.INVALID_SPEC
    assert exc.value.ctx and exc.value.ctx["value"] == "wrong"
