from pathlib import Path

from dagster import materialize, DagsterInstance

from daydreaming_dagster.assets.group_essay import essay_prompt, essay_raw, essay_parsed
from daydreaming_dagster.data_layer.gens_data_layer import GensDataLayer
from daydreaming_dagster.resources.experiment_config import ExperimentConfig
from daydreaming_dagster.resources.gens_prompt_io_manager import GensPromptIOManager
from daydreaming_dagster.resources.io_managers import InMemoryIOManager
from tests.helpers.membership import write_membership_csv


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
    write_membership_csv(
        data_root,
        [
            {"stage": "draft", "gen_id": draft_id},
            {"stage": "essay", "gen_id": essay_id},
        ],
        cohort=cohort,
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
        "template_id,template_name,description,generator\n"
        "parsed-from-links-v1,Parsed From Links,Copy parsed draft,copy\n",
        encoding="utf-8",
    )

    # Seed main metadata for essay
    data_layer = GensDataLayer.from_root(data_root)
    data_layer.write_main_metadata(
        "essay",
        essay_id,
        {
            "template_id": "parsed-from-links-v1",
            "mode": "copy",
            "parent_gen_id": draft_id,
            "combo_id": "combo-1",
        },
    )

    # Resources: copy-mode does not call LLM, but resource must be present
    resources = {
        "data_root": str(data_root),
        "experiment_config": ExperimentConfig(),
        "openrouter_client": _StubLLM(),
        # IO managers used by essay assets
        "essay_prompt_io_manager": GensPromptIOManager(
            gens_root=data_root / "gens",
            stage="essay",
        ),
        "in_memory_io_manager": InMemoryIOManager(),
    }

    # Materialize essay assets end-to-end with partition key under an ephemeral instance
    with DagsterInstance.ephemeral() as instance:
        # Register the essay partition key in the ephemeral instance
        instance.add_dynamic_partitions("essay_gens", [essay_id])
        result = materialize(
            assets=[essay_prompt, essay_raw, essay_parsed],
            resources=resources,
            partition_key=essay_id,
            instance=instance,
        )
        assert result.success
    # The essay should be persisted under gens/essay/<essay_id>/parsed.txt with identical content
    edir = data_root / "gens" / "essay" / essay_id
    assert (edir / "parsed.txt").exists()
    assert (edir / "parsed.txt").read_text(encoding="utf-8") == draft_text
