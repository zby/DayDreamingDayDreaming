from pathlib import Path

import pytest

from daydreaming_dagster.unified.stage_services import render_template, execute_evaluation_llm


pytestmark = pytest.mark.integration


class _Log:
    def info(self, *_args, **_kwargs):
        pass


class _Ctx:
    def __init__(self, partition_key: str, data_root: Path, llm):
        class _Res:
            pass

        self.partition_key = partition_key
        self.log = _Log()
        self.resources = _Res()
        self.resources.data_root = str(data_root)
        self.resources.openrouter_client = llm
        self.resources.experiment_config = ExperimentConfig()

    def add_output_metadata(self, _md: dict):
        pass


def _write_membership(data_root: Path, rows: list[dict]):
    import pandas as pd

    cohort = "T-INTEG"
    cdir = data_root / "cohorts" / cohort
    cdir.mkdir(parents=True, exist_ok=True)
    cols = [
        "stage",
        "gen_id",
        "cohort_id",
        "parent_gen_id",
        "combo_id",
        "template_id",
        "llm_model_id",
    ]
    norm = []
    for r in rows:
        d = {k: r.get(k, "") for k in cols}
        if not d["cohort_id"]:
            d["cohort_id"] = cohort
        norm.append(d)
    df = pd.DataFrame(norm, columns=cols)
    (cdir / "membership.csv").write_text(df.to_csv(index=False), encoding="utf-8")


@pytest.mark.llm_cfg(tail_score="8.5")
def test_evaluation_parser_in_last_line_emits_single_float_line(tiny_data_root: Path, mock_llm):
    # Create a minimal essay parent document
    essay_parent_id = "e-for-eval"
    e_dir = tiny_data_root / "gens" / "essay" / essay_parent_id
    e_dir.mkdir(parents=True, exist_ok=True)
    (e_dir / "parsed.txt").write_text("Lorem ipsum dolor...", encoding="utf-8")

    # Membership row for evaluation linking to essay parent and using test-eval template
    eval_id = "v-parse"
    _write_membership(
        tiny_data_root,
        [
            {
                "stage": "evaluation",
                "gen_id": eval_id,
                "parent_gen_id": essay_parent_id,
                "template_id": "test-eval",
                "llm_model_id": "m-eval",
            }
        ],
    )

    # Execute StageRunner directly to avoid Dagster run property requirements
    doc_text = (e_dir / "parsed.txt").read_text(encoding="utf-8")
    runner = StageRunner(templates_root=tiny_data_root / "1_raw" / "templates")
    spec = StageRunSpec(
        stage="evaluation",
        gen_id=eval_id,
        template_id="test-eval",
        prompt_text=render_template("evaluation", "test-eval", {"response": doc_text}),
        model="m-eval",
        parser_name="in_last_line",
        max_tokens=2048,
        parent_gen_id=essay_parent_id,
    )

    vdir = tiny_data_root / "gens" / "evaluation" / eval_id
    assert (vdir / "parsed.txt").exists()
    assert (vdir / "parsed.txt").read_text(encoding="utf-8") == "8.5\n"
