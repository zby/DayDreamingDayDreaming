import os
from pathlib import Path


def test_no_legacy_curated_combo_mappings_reference():
    """Ensure code no longer references the legacy curated_combo_mappings.csv name.

    We scan only Python source files under the package and scripts (not docs).
    """
    repo_root = Path(__file__).resolve().parents[1]
    targets = [repo_root / "daydreaming_dagster", repo_root / "scripts"]
    banned = "curated_combo_mappings"
    hits = []
    self_path = Path(__file__).resolve()
    for base in targets:
        for root, _dirs, files in os.walk(base):
            for name in files:
                if not name.endswith(".py"):
                    continue
                fp = Path(root) / name
                if fp.resolve() == self_path:
                    continue
                try:
                    text = fp.read_text(encoding="utf-8")
                except Exception:
                    continue
                if banned in text:
                    hits.append(str(fp))
    assert not hits, f"Found legacy curated reference in: {hits}"
