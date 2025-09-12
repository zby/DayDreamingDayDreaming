from pathlib import Path
import json

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
    # Only parsed + metadata written in copy mode
    assert not (out_dir / "essay" / "E1" / "prompt.txt").exists()
    assert not (out_dir / "essay" / "E1" / "raw.txt").exists()
    assert (out_dir / "essay" / "E1" / "metadata.json").exists()


class _StubLLMTruncated:
    def __init__(self, text: str, truncated: bool = True):
        self._text = text
        self._truncated = truncated

    def generate_with_info(self, prompt: str, model: str, max_tokens: int | None):
        return self._text, {"finish_reason": "length" if self._truncated else "stop", "truncated": self._truncated, "usage": {"completion_tokens": max_tokens or 0, "max_tokens": max_tokens or 0}}


def test_draft_llm_parses_via_csv_and_writes_metadata(tmp_path: Path):
    # Set up CSV declaring parser for template
    csv_dir = tmp_path / "1_raw"
    csv_dir.mkdir(parents=True, exist_ok=True)
    (csv_dir / "draft_templates.csv").write_text(
        "template_id,template_name,description,parser,generator,active\n"
        "tpl1,Tpl,Desc,essay_block,llm,True\n",
        encoding="utf-8",
    )

    # Create runner with templates_root; we will pass prompt_text so template file isn't needed
    runner = StageRunner(templates_root=tmp_path / "1_raw" / "templates")
    out_dir = tmp_path / "gens"
    llm = _StubLLMTruncated("header\n<essay>Foo</essay>\n", truncated=False)

    spec = StageRunSpec(
        stage="draft",
        gen_id="D1",
        template_id="tpl1",
        values={},
        out_dir=out_dir,
        mode="llm",
        model="m1",
        max_tokens=128,
        prompt_text="ignored",  # skip template rendering
        min_lines=1,
    )
    res = runner.run(spec, llm_client=llm)
    base = out_dir / "draft" / "D1"
    assert (base / "prompt.txt").exists()
    assert (base / "raw.txt").read_text(encoding="utf-8").strip().endswith("<essay>Foo</essay>")
    # Parser from CSV should extract the essay block
    assert (base / "parsed.txt").read_text(encoding="utf-8").strip() == "Foo"
    md = json.loads((base / "metadata.json").read_text(encoding="utf-8"))
    assert md.get("parser_name") == "essay_block"
    assert md.get("llm_model_id") == "m1"
    # Return dict still provided for back-compat
    assert isinstance(res, dict) and "info" in res and "metadata" in res


def test_llm_truncation_enforced_after_raw_written(tmp_path: Path):
    runner = StageRunner(templates_root=tmp_path / "1_raw" / "templates")
    out_dir = tmp_path / "gens"
    (tmp_path / "1_raw" / "templates" / "draft").mkdir(parents=True, exist_ok=True)
    (tmp_path / "1_raw" / "templates" / "draft" / "tpl.txt").write_text("X", encoding="utf-8")

    spec = StageRunSpec(
        stage="draft",
        gen_id="D2",
        template_id="tpl",
        values={},
        out_dir=out_dir,
        mode="llm",
        model="m2",
        max_tokens=8,
        min_lines=1,
        fail_on_truncation=True,
    )
    llm = _StubLLMTruncated("short text", truncated=True)
    try:
        runner.run(spec, llm_client=llm)
        assert False, "Expected truncation to raise"
    except ValueError:
        base = out_dir / "draft" / "D2"
        # RAW should be present for debuggability; parsed should not
        assert (base / "raw.txt").exists()
        assert not (base / "parsed.txt").exists()


def test_evaluation_requires_parser_and_parent(tmp_path: Path):
    runner = StageRunner(templates_root=tmp_path / "1_raw" / "templates")
    out_dir = tmp_path / "gens"
    (tmp_path / "1_raw" / "templates" / "evaluation").mkdir(parents=True, exist_ok=True)
    (tmp_path / "1_raw" / "templates" / "evaluation" / "t.txt").write_text("{{ response }}", encoding="utf-8")

    # Missing parser_name should raise
    spec = StageRunSpec(
        stage="evaluation",
        gen_id="V1",
        template_id="t",
        values={"response": "ok"},
        out_dir=out_dir,
        mode="llm",
        model="m3",
        max_tokens=8,
        # parser_name missing
        parent_gen_id="E1",
    )
    try:
        runner.run(spec, llm_client=_StubLLM())
        assert False, "Expected missing parser_name to raise"
    except ValueError:
        base = out_dir / "evaluation" / "V1"
        # Directory may exist from mkdir, but no files should be written
        assert not (base / "raw.txt").exists()
        assert not (base / "parsed.txt").exists()
        assert not (base / "metadata.json").exists()
