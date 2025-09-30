from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "select_top_essays.py"


def _write_sample_scores(path: Path) -> None:
    rows = [
        "parent_gen_id,gen_id,evaluation_template,evaluation_llm_model,score,error",
        "essay_a,eval_a_s,gemini-prior-art-eval,sonnet-4,9.0,",
        "essay_a,eval_a_g,gemini-prior-art-eval,gemini_25_pro,10.0,",
        "essay_b,eval_b_s,gemini-prior-art-eval,sonnet-4,8.0,",
        "essay_b,eval_b_g,gemini-prior-art-eval,gemini_25_pro,11.0,",
        "essay_c,eval_c_s,gemini-prior-art-eval,sonnet-4,7.0,",
        "essay_c,eval_c_g,gemini-prior-art-eval,gemini_25_pro,6.0,",
    ]
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def _read_selected_essays(path: Path) -> list[str]:
    lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines()]
    return [line for line in lines if line and not line.startswith("#")]


def test_top_n_selects_requested_count(tmp_path: Path) -> None:
    parsed_scores = tmp_path / "data" / "7_cross_experiment"
    parsed_scores.mkdir(parents=True)
    parsed_csv = parsed_scores / "parsed_scores.csv"
    _write_sample_scores(parsed_csv)

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--aggregated-scores",
            str(parsed_csv),
            "--template",
            "gemini-prior-art-eval",
            "--average-models",
            "sonnet-4",
            "--score-span",
            "0",
            "--top-n",
            "1",
        ],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    )
    selected_path = tmp_path / "data" / "2_tasks" / "selected_essays.txt"
    content = _read_selected_essays(selected_path)
    assert content == ["essay_a"], f"stdout={result.stdout} stderr={result.stderr}"


def test_max_only_selects_all_maximum(tmp_path: Path) -> None:
    parsed_scores = tmp_path / "data" / "7_cross_experiment"
    parsed_scores.mkdir(parents=True)
    parsed_csv = parsed_scores / "parsed_scores.csv"
    _write_sample_scores(parsed_csv)

    subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--aggregated-scores",
            str(parsed_csv),
            "--template",
            "gemini-prior-art-eval",
            "--average-models",
            "sonnet-4",
            "gemini_25_pro",
            "--score-span",
            "0",
            "--max-only",
        ],
        cwd=tmp_path,
        check=True,
    )
    selected_path = tmp_path / "data" / "2_tasks" / "selected_essays.txt"
    content = _read_selected_essays(selected_path)
    assert content == ["essay_a", "essay_b"], content


def test_score_span_defaults_include_range(tmp_path: Path) -> None:
    parsed_scores = tmp_path / "data" / "7_cross_experiment"
    parsed_scores.mkdir(parents=True)
    parsed_csv = parsed_scores / "parsed_scores.csv"
    _write_sample_scores(parsed_csv)

    subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--aggregated-scores",
            str(parsed_csv),
            "--template",
            "gemini-prior-art-eval",
            "--average-models",
            "sonnet-4",
        ],
        cwd=tmp_path,
        check=True,
    )
    selected_path = tmp_path / "data" / "2_tasks" / "selected_essays.txt"
    content = _read_selected_essays(selected_path)
    assert content[:2] == ["essay_a", "essay_b"], content
