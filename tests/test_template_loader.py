import os
import pytest
from pathlib import Path
from unittest.mock import patch
from daydreaming_dagster.utils.template_loader import load_generation_template


pytestmark = [pytest.mark.unit]


class TestTemplateLoader:
    """Phase-aware template loading against a temporary templates root."""

    def test_load_draft_and_essay_from_temp_root(self, tmp_path: Path):
        root = tmp_path / "gen_templates"
        (root / "draft").mkdir(parents=True)
        (root / "essay").mkdir(parents=True)

        (root / "draft" / "my-template.txt").write_text("Draft: {% for concept in concepts %}{{ concept.name }}{% endfor %}")
        (root / "essay" / "my-template.txt").write_text("Essay: {{ links_block }}")

        with patch.dict(os.environ, {"GEN_TEMPLATES_ROOT": str(root)}):
            draft = load_generation_template("my-template", "draft")
            essay = load_generation_template("my-template", "essay")

        assert "Draft:" in draft
        assert "Essay:" in essay

    def test_missing_template_raises_file_not_found(self, tmp_path: Path):
        root = tmp_path / "gen_templates"
        (root / "draft").mkdir(parents=True)
        (root / "essay").mkdir(parents=True)

        with patch.dict(os.environ, {"GEN_TEMPLATES_ROOT": str(root)}):
            with pytest.raises(FileNotFoundError) as exc_info:
                load_generation_template("nope", "draft")
            assert "Template not found for phase='draft'" in str(exc_info.value)

            with pytest.raises(FileNotFoundError) as exc_info2:
                load_generation_template("nope", "essay")
            assert "Template not found for phase='essay'" in str(exc_info2.value)
