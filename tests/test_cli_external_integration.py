import os
import subprocess
import tempfile
from pathlib import Path


def run(cmd, cwd=None, env=None) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def test_cli_materialize_selected_then_content():
    repo_root = Path(__file__).resolve().parents[1]
    defs_path = repo_root / "daydreaming_dagster" / "definitions.py"
    assert defs_path.exists(), "definitions.py not found"

    with tempfile.TemporaryDirectory() as td:
        dagster_home = Path(td) / "dagster_home"
        dagster_home.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        env["DAGSTER_HOME"] = str(dagster_home)

        # Step 1: generate selection
        p1 = run(["uv", "run", "dagster", "asset", "materialize", "-f", str(defs_path), "--select", "selected_combo_mappings"], cwd=str(repo_root), env=env)
        assert p1.returncode == 0, "selected_combo_mappings failed: \n" + p1.stdout

        # Step 2: build content combinations from selected file
        p2 = run(["uv", "run", "dagster", "asset", "materialize", "-f", str(defs_path), "--select", "content_combinations"], cwd=str(repo_root), env=env)
        assert p2.returncode == 0, "content_combinations failed: \n" + p2.stdout

        # Verify selected CSV was written and is non-empty
        selected_csv = repo_root / "data" / "2_tasks" / "selected_combo_mappings.csv"
        assert selected_csv.exists() and selected_csv.stat().st_size > 0
