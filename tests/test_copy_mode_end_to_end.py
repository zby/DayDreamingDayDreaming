from pathlib import Path

from dagster import build_asset_context, materialize, DagsterInstance

from daydreaming_dagster.assets.group_generation_essays import essay_response, essay_prompt
from daydreaming_dagster.resources.experiment_config import ExperimentConfig
from daydreaming_dagster.resources.io_managers import InMemoryIOManager
from daydreaming_dagster.resources.gens_prompt_io_manager import GensPromptIOManager


class _StubLLM:
    def generate_with_info(self, prompt: str, model: str, max_tokens: int):
        # Not used in copy mode; return a plausible tuple
        return "UNUSED", {"finish_reason": "stop", "max_tokens": max_tokens}


def test_copy_mode_essay_is_verbatim_draft(tmp_path: Path):
    data_root = tmp_path

    # Seed cohort membership with one draft and one essay linked by parent_gen_id
    cohort = "C-COPY"
    draft_id = "D100"
    essay_id = "E100"
    (data_root / "cohorts" / cohort).mkdir(parents=True, exist_ok=True)
    (data_root / "cohorts" / cohort / "membership.csv").write_text(
        "stage,gen_id,cohort_id,parent_gen_id,combo_id,template_id,llm_model_id\n"
        f"draft,{draft_id},{cohort},,,links-vX,gen-1\n"
        f"essay,{essay_id},{cohort},{draft_id},combo-1,parsed-from-links-v1,gen-1\n",
        encoding="utf-8",
    )

    # Seed draft parsed text in gens store
    draft_text = "\n".join(["• a", "• b", "• c"])
    ddir = data_root / "gens" / "draft" / draft_id
    ddir.mkdir(parents=True, exist_ok=True)
    (ddir / "parsed.txt").write_text(draft_text, encoding="utf-8")

    # Provide essay_templates.csv with copy-mode template so asset can resolve mode
    raw_dir = data_root / "1_raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "essay_templates.csv").write_text(
        "template_id,template_name,description,active,generator\n"
        "parsed-from-links-v1,Parsed From Links,Copy parsed draft,true,copy\n",
        encoding="utf-8",
    )

    # Resources: copy-mode does not call LLM, but resource must be present
    resources = {
        "data_root": str(data_root),
        "experiment_config": ExperimentConfig(min_draft_lines=3),
        "openrouter_client": _StubLLM(),
        # IO managers used by essay assets
        "essay_prompt_io_manager": GensPromptIOManager(
            gens_root=data_root / "gens",
            stage="essay",
        ),
        "essay_response_io_manager": InMemoryIOManager(),
    }

    # Materialize essay assets end-to-end with partition key under an ephemeral instance
    with DagsterInstance.ephemeral() as instance:
        # Register the essay partition key in the ephemeral instance
        instance.add_dynamic_partitions("essay_gens", [essay_id])
        result = materialize(
            assets=[essay_prompt, essay_response],
            resources=resources,
            partition_key=essay_id,
            instance=instance,
        )
        assert result.success
    # The essay should be persisted under gens/essay/<essay_id>/parsed.txt with identical content
    edir = data_root / "gens" / "essay" / essay_id
    assert (edir / "parsed.txt").exists()
    assert (edir / "parsed.txt").read_text(encoding="utf-8") == draft_text
