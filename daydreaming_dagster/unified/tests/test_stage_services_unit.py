from __future__ import annotations

from pathlib import Path
import json
import types

import pytest

from daydreaming_dagster.unified.stage_services import (
    render_template,
    generate_llm,
    resolve_parser_name,
    parse_text,
    execute_copy,
    execute_llm,
)


class _StubLLM:
    def __init__(self, text: str, info: dict | None = None):
        self._text = text
        self._info = info or {"finish_reason": "stop", "truncated": False}

    def generate_with_info(self, prompt: str, *, model: str, max_tokens=None):
        # Echo prompt to inspect in some tests
        text = self._text.replace("${PROMPT}", prompt)
        return text, dict(self._info)


def test_render_template_happy(tmp_path: Path):
    root = tmp_path / "1_raw" / "templates" / "draft"
    root.mkdir(parents=True, exist_ok=True)
    (root / "foo.txt").write_text("Hello {{name}}", encoding="utf-8")
    out = render_template("draft", "foo", {"name": "X"}, templates_root=tmp_path / "1_raw" / "templates")
    assert out == "Hello X"


def test_render_template_missing_file(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        render_template("draft", "missing", {}, templates_root=tmp_path / "1_raw" / "templates")


def test_render_template_strict_undefined(tmp_path: Path):
    root = tmp_path / "1_raw" / "templates" / "draft"
    root.mkdir(parents=True, exist_ok=True)
    (root / "foo.txt").write_text("Hello {{name}}", encoding="utf-8")
    with pytest.raises(Exception):
        render_template("draft", "foo", {}, templates_root=tmp_path / "1_raw" / "templates")


def test_generate_llm_normalizes_crlf():
    llm = _StubLLM("a\r\nb")
    raw, info = generate_llm(llm, "p", model="m")
    assert raw == "a\nb"
    assert info.get("finish_reason") == "stop"


def test_resolve_parser_name_fallback(tmp_path: Path):
    csv_dir = tmp_path / "1_raw"
    csv_dir.mkdir(parents=True, exist_ok=True)
    (csv_dir / "draft_templates.csv").write_text(
        "template_id,template_name,description,parser,generator,active\n"
        "foo,Foo,Desc,essay_block,llm,True\n",
        encoding="utf-8",
    )
    name = resolve_parser_name(tmp_path, "draft", "foo", None)
    assert name == "essay_block"
    # Missing CSV: returns None
    name2 = resolve_parser_name(tmp_path / "nope", "draft", "foo", None)
    assert name2 is None


def test_parse_draft_uses_registry():
    text = "<essay>Body</essay>"
    parsed = parse_text("draft", text, "essay_block")
    assert parsed == "Body"
    assert parse_text("draft", text, "missing_parser") is None


def test_parse_evaluation_in_last_line():
    txt = "Line\nSCORE: 8.5\n"
    out = parse_text("evaluation", txt, "in_last_line")
    assert out == "8.5\n"
    assert parse_text("evaluation", "no score", "in_last_line") is None


def test_execute_copy_writes_only_parsed_and_metadata(tmp_path: Path):
    src = tmp_path / "gens" / "draft" / "D1" / "parsed.txt"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text("A\nB\n", encoding="utf-8")
    res = execute_copy(
        out_dir=tmp_path / "gens",
        stage="essay",
        gen_id="E1",
        template_id="t",
        parent_gen_id="D1",
        pass_through_from=src,
    )
    base = tmp_path / "gens" / "essay" / "E1"
    assert (base / "parsed.txt").exists()
    assert (base / "metadata.json").exists()
    assert not (base / "raw.txt").exists()
    assert not (base / "prompt.txt").exists()
    assert res.metadata.get("mode") == "copy"


def test_execute_draft_llm_happy_path(tmp_path: Path):
    # CSV declares parser for template
    csv_dir = tmp_path / "1_raw"
    csv_dir.mkdir(parents=True, exist_ok=True)
    (csv_dir / "draft_templates.csv").write_text(
        "template_id,template_name,description,parser,generator,active\n"
        "tpl1,Tpl,Desc,essay_block,llm,True\n",
        encoding="utf-8",
    )
    llm = _StubLLM("header\n<essay>Foo</essay>\n", info={"finish_reason": "stop", "truncated": False})
    res = execute_llm(
        stage="draft",
        llm=llm,
        root_dir=tmp_path,
        gen_id="D1",
        template_id="tpl1",
        prompt_text="ignored",
        model="m1",
        max_tokens=128,
        min_lines=1,
    )
    base = tmp_path / "gens" / "draft" / "D1"
    # execute_draft_llm no longer writes prompt.txt
    assert not (base / "prompt.txt").exists()
    assert (base / "raw.txt").exists()
    assert (base / "parsed.txt").read_text(encoding="utf-8").strip() == "Foo"
    md = json.loads((base / "metadata.json").read_text(encoding="utf-8"))
    assert md.get("parser_name") == "essay_block"
    assert md.get("llm_model_id") == "m1"


def test_execute_draft_llm_min_lines_failure(tmp_path: Path):
    llm = _StubLLM("one line only", info={"finish_reason": "stop", "truncated": False})
    with pytest.raises(ValueError):
        execute_llm(
            stage="draft",
            llm=llm,
            root_dir=tmp_path,
            gen_id="D2",
            template_id="tpl",
            prompt_text="p",
            model="m",
            max_tokens=16,
            min_lines=3,
        )
    base = tmp_path / "gens" / "draft" / "D2"
    assert (base / "raw.txt").exists()
    assert not (base / "parsed.txt").exists()


def test_execute_draft_llm_truncation_failure_after_raw(tmp_path: Path):
    llm = _StubLLM("short text", info={"finish_reason": "length", "truncated": True})
    with pytest.raises(ValueError):
        execute_llm(
            stage="draft",
            llm=llm,
            root_dir=tmp_path,
            gen_id="D3",
            template_id="tpl",
            prompt_text="p",
            model="m",
            max_tokens=8,
            min_lines=1,
            fail_on_truncation=True,
        )
    base = tmp_path / "gens" / "draft" / "D3"
    assert (base / "raw.txt").exists()
    assert not (base / "parsed.txt").exists()


def test_execute_essay_llm_identity_parse(tmp_path: Path):
    llm = _StubLLM("Line A\nLine B\n")
    res = execute_llm(
        stage="essay",
        llm=llm,
        root_dir=tmp_path,
        gen_id="E1",
        template_id="t",
        prompt_text="PROMPT",
        model="m",
        max_tokens=64,
        min_lines=None,
        parent_gen_id="D1",
    )
    base = tmp_path / "gens" / "essay" / "E1"
    assert (base / "parsed.txt").exists()
    assert res.parsed_text == res.raw_text


def test_execute_evaluation_llm_requires_parser(tmp_path: Path):
    llm = _StubLLM("Response\nSCORE: 9\n")
    with pytest.raises(ValueError):
        execute_llm(
            stage="evaluation",
            llm=llm,
            root_dir=tmp_path,
            gen_id="V1",
            template_id="t",
            prompt_text="p",
            model="m",
            max_tokens=8,
            min_lines=None,
            parent_gen_id="E1",
        )
    # with parser
    # Declare parser in evaluation_templates.csv for template 't'
    csv_dir = tmp_path / "1_raw"
    csv_dir.mkdir(parents=True, exist_ok=True)
    (csv_dir / "evaluation_templates.csv").write_text(
        "template_id,template_name,description,parser,active\n"
        "t,T,Desc,in_last_line,True\n",
        encoding="utf-8",
    )
    res = execute_llm(
        stage="evaluation",
        llm=llm,
        root_dir=tmp_path,
        gen_id="V2",
        template_id="t",
        prompt_text="p",
        model="m",
        max_tokens=8,
        min_lines=None,
        parent_gen_id="E2",
    )
    base = tmp_path / "gens" / "evaluation" / "V2"
    assert (base / "parsed.txt").exists()
    assert json.loads((base / "metadata.json").read_text(encoding="utf-8")).get("parser_name") == "in_last_line"


def test_metadata_extra_does_not_override(tmp_path: Path):
    llm = _StubLLM("ok")
    res = execute_llm(
        stage="essay",
        llm=llm,
        root_dir=tmp_path,
        gen_id="E10",
        template_id="t",
        prompt_text="p",
        model="m",
        max_tokens=8,
        min_lines=None,
        parent_gen_id="D",
        metadata_extra={"stage": "hack", "run_id": "X"},
    )
    assert res.metadata.get("stage") == "essay"  # not overridden
    assert res.metadata.get("run_id") == "X"
