from pathlib import Path

from daydreaming_dagster.unified.stage_runner import StageRunSpec, StageRunner


class _StubLLM:
    def generate_with_info(self, prompt: str, model: str, max_tokens: int | None):
        return f"RAW::{prompt}", {"finish_reason": "stop"}


def test_copy_mode_uses_pass_through(tmp_path: Path):
    # Create templates root
    templates_root = tmp_path / "1_raw" / "templates" / "essay"
    templates_root.mkdir(parents=True, exist_ok=True)
    (templates_root / "essay_copy.txt").write_text("COPY: {{ draft_block }}", encoding="utf-8")

    # Create pass-through source file
    source = tmp_path / "gens" / "draft" / "D1" / "parsed.txt"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("line A\nline B", encoding="utf-8")

    runner = StageRunner(templates_root=tmp_path / "1_raw" / "templates")
    out_dir = tmp_path / "gens"
    spec = StageRunSpec(
        stage="essay",
        gen_id="E1",
        template_id="essay_copy",
        values={"draft_block": source.read_text(encoding="utf-8")},
        out_dir=out_dir,
        mode="copy",
        pass_through_from=source,
    )
    res = runner.run(spec, llm_client=_StubLLM())
    assert (out_dir / "essay" / "E1" / "parsed.txt").exists()
    assert (out_dir / "essay" / "E1" / "parsed.txt").read_text(encoding="utf-8").strip() == "line A\nline B"

