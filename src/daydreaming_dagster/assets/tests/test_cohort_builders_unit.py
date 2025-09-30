from __future__ import annotations

from daydreaming_dagster.assets.group_cohorts import (
    CuratedDraft,
    CuratedEssay,
    ExistingEvaluation,
    build_curated_entries,
    build_from_drafts,
    build_evaluations_for_essay,
    expand_cartesian_drafts,
    expand_cartesian_essays,
)


def test_build_curated_entries_adds_rows_and_caches(tmp_path):
    entries = [
        CuratedEssay(
            essay_gen_id="essay-1",
            draft_gen_id="draft-1",
            combo_id="combo-1",
            draft_template_id="draft-tpl",
            essay_template_id="essay-tpl",
            draft_llm_model_id="draft-llm",
            essay_llm_model_id="essay-llm",
            draft_replicate=1,
            essay_replicate=2,
        )
    ]

    calls: list[tuple[str, tuple]] = []

    def add_draft_row(*args):
        calls.append(("draft", args))

    def add_essay_row(*args):
        calls.append(("essay", args))

    essay_seed_combo: dict[str, str] = {}
    existing_eval_cache: dict[str, dict[tuple[str, str], list[ExistingEvaluation]]] = {}

    def fake_loader(data_root, essay_id):
        assert data_root == tmp_path
        assert essay_id == "essay-1"
        return {("eval-tpl", "eval-llm"): [ExistingEvaluation("eval-1", 1)]}

    build_curated_entries(
        entries,
        data_root=tmp_path,
        add_draft_row=add_draft_row,
        add_essay_row=add_essay_row,
        essay_seed_combo=essay_seed_combo,
        existing_eval_cache=existing_eval_cache,
        existing_eval_loader=fake_loader,
    )

    assert calls == [
        ("draft", ("draft-1", "combo-1", "draft-tpl", "draft-llm", 1)),
        ("essay", ("essay-1", "draft-1", "combo-1", "essay-tpl", "essay-llm", 2)),
    ]
    assert essay_seed_combo == {"essay-1": "combo-1"}
    assert existing_eval_cache == {
        "essay-1": {("eval-tpl", "eval-llm"): [ExistingEvaluation("eval-1", 1)]}
    }


class _SequentialAllocator:
    def __init__(self):
        self.calls: list[tuple[str, tuple, int]] = []

    def allocate(self, stage, base_signature, count):
        self.calls.append((stage, base_signature, count))
        return list(range(1, count + 1))


def test_build_from_drafts_allocates_and_records_seed():
    entries = [
        CuratedDraft(
            draft_gen_id="draft-1",
            combo_id="combo-1",
            draft_template_id="draft-tpl",
            draft_llm_model_id="draft-llm",
            draft_replicate=1,
        )
    ]

    draft_calls: list[tuple] = []
    essay_calls: list[tuple] = []

    def add_draft_row(*args):
        draft_calls.append(args)

    def add_essay_row(*args):
        essay_calls.append(args)

    def fake_essay_id_factory(**kwargs):
        return f"essay-{kwargs['replicate_index']}"

    seed: dict[str, str] = {}

    build_from_drafts(
        entries,
        essay_template_ids=["essay-tpl"],
        essay_rep_count=2,
        add_draft_row=add_draft_row,
        add_essay_row=add_essay_row,
        allocator=_SequentialAllocator(),
        cohort_id="cohort-1",
        essay_seed_combo=seed,
        essay_id_factory=fake_essay_id_factory,
    )

    assert draft_calls == [("draft-1", "combo-1", "draft-tpl", "draft-llm", 1)]
    assert essay_calls == [
        ("essay-1", "draft-1", "combo-1", "essay-tpl", "draft-llm", 1),
        ("essay-2", "draft-1", "combo-1", "essay-tpl", "draft-llm", 2),
    ]
    assert seed == {"essay-1": "combo-1", "essay-2": "combo-1"}


def test_expand_cartesian_drafts_generates_expected_rows():
    draft_calls: list[tuple] = []
    captured_kwargs: list[dict] = []

    def add_draft_row(gen_id, combo_id, template_id, llm_model_id, replicate):
        draft_calls.append((gen_id, combo_id, template_id, llm_model_id, replicate))

    def fake_draft_id_factory(**kwargs):
        captured_kwargs.append(kwargs)
        return f"draft-{kwargs['combo_id']}-{kwargs['replicate_index']}"

    expand_cartesian_drafts(
        cohort_id="cohort-1",
        combo_ids=["combo-1"],
        draft_template_ids=["draft-tpl"],
        generation_model_ids=["draft-llm"],
        draft_rep_count=2,
        add_draft_row=add_draft_row,
        draft_id_factory=fake_draft_id_factory,
    )

    assert draft_calls == [
        ("draft-combo-1-1", "combo-1", "draft-tpl", "draft-llm", 1),
        ("draft-combo-1-2", "combo-1", "draft-tpl", "draft-llm", 2),
    ]
    assert captured_kwargs[0]["salt"] is None
    assert captured_kwargs[1]["salt"] == "rep2"


def test_expand_cartesian_essays_uses_allocator_and_salting():
    draft_rows = [
        {
            "stage": "draft",
            "gen_id": "draft-1",
            "combo_id": "combo-1",
            "template_id": "draft-tpl",
            "llm_model_id": "draft-llm",
            "replicate": 1,
        }
    ]

    essay_calls: list[tuple] = []
    captured_kwargs: list[dict] = []
    seed: dict[str, str] = {}
    allocator = _SequentialAllocator()

    def add_essay_row(*args):
        essay_calls.append(args)

    def fake_essay_id_factory(**kwargs):
        captured_kwargs.append(kwargs)
        return f"essay-{kwargs['replicate_index']}"

    expand_cartesian_essays(
        cohort_id="cohort-1",
        draft_rows=draft_rows,
        essay_template_ids=["essay-tpl"],
        essay_rep_count=2,
        add_essay_row=add_essay_row,
        essay_seed_combo=seed,
        essay_id_factory=fake_essay_id_factory,
        allocator=allocator,
    )

    assert allocator.calls == [("essay", ("draft-1", "essay-tpl"), 2)]
    assert essay_calls == [
        ("essay-1", "draft-1", "combo-1", "essay-tpl", "draft-llm", 1),
        ("essay-2", "draft-1", "combo-1", "essay-tpl", "draft-llm", 2),
    ]
    assert seed == {"essay-1": "combo-1", "essay-2": "combo-1"}
    assert captured_kwargs[0]["salt"] is None
    assert captured_kwargs[1]["salt"] == "rep1-2"


def test_build_evaluations_for_essay_reuses_existing():
    existing_counts = {
        ("eval-tpl", "eval-llm"): [
            ExistingEvaluation("eval-1", 1),
            ExistingEvaluation("eval-2", 2),
        ]
    }

    allocator = _SequentialAllocator()

    rows, created = build_evaluations_for_essay(
        essay_gen_id="essay-1",
        combo_id="combo-1",
        evaluation_templates=["eval-tpl"],
        evaluation_models=["eval-llm"],
        existing_counts=existing_counts,
        evaluation_rep_count=2,
        cohort_id="cohort-1",
        allocator=allocator,
        evaluation_id_factory=lambda **kwargs: f"eval-{kwargs['replicate_index']}",
    )

    assert created == 0
    assert allocator.calls == []
    assert [row.to_dict() for row in rows] == [
        {
            "stage": "evaluation",
            "gen_id": "eval-1",
            "origin_cohort_id": "cohort-1",
            "parent_gen_id": "essay-1",
            "combo_id": "combo-1",
            "template_id": "eval-tpl",
            "llm_model_id": "eval-llm",
            "replicate": 1,
        },
        {
            "stage": "evaluation",
            "gen_id": "eval-2",
            "origin_cohort_id": "cohort-1",
            "parent_gen_id": "essay-1",
            "combo_id": "combo-1",
            "template_id": "eval-tpl",
            "llm_model_id": "eval-llm",
            "replicate": 2,
        },
    ]


def test_build_evaluations_for_essay_allocates_missing_replicates():
    allocator = _SequentialAllocator()

    rows, created = build_evaluations_for_essay(
        essay_gen_id="essay-1",
        combo_id="combo-1",
        evaluation_templates=["eval-tpl"],
        evaluation_models=["eval-llm"],
        existing_counts={},
        evaluation_rep_count=2,
        cohort_id="cohort-1",
        allocator=allocator,
        evaluation_id_factory=lambda **kwargs: f"eval-{kwargs['replicate_index']}",
    )

    assert created == 2
    assert allocator.calls == [("evaluation", ("essay-1", "eval-tpl", "eval-llm"), 2)]
    assert [row.to_dict() for row in rows] == [
        {
            "stage": "evaluation",
            "gen_id": "eval-1",
            "origin_cohort_id": "cohort-1",
            "parent_gen_id": "essay-1",
            "combo_id": "combo-1",
            "template_id": "eval-tpl",
            "llm_model_id": "eval-llm",
            "replicate": 1,
        },
        {
            "stage": "evaluation",
            "gen_id": "eval-2",
            "origin_cohort_id": "cohort-1",
            "parent_gen_id": "essay-1",
            "combo_id": "combo-1",
            "template_id": "eval-tpl",
            "llm_model_id": "eval-llm",
            "replicate": 2,
        },
    ]
