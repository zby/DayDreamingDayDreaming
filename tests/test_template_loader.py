import os
import pytest
from pathlib import Path
from unittest.mock import patch
from daydreaming_dagster.utils.template_loader import load_generation_template


pytestmark = [pytest.mark.unit]


class TestTemplateLoader:
    """Phase-aware template loading against a temporary templates root."""

    def test_load_links_and_essay_from_temp_root(self, tmp_path: Path):
        root = tmp_path / "gen_templates"
        (root / "links").mkdir(parents=True)
        (root / "essay").mkdir(parents=True)

        (root / "links" / "my-template.txt").write_text("Links: {% for concept in concepts %}{{ concept.name }}{% endfor %}")
        (root / "essay" / "my-template.txt").write_text("Essay: {{ links_block }}")

        with patch.dict(os.environ, {"GEN_TEMPLATES_ROOT": str(root)}):
            links = load_generation_template("my-template", "links")
            essay = load_generation_template("my-template", "essay")

        assert "Links:" in links
        assert "Essay:" in essay

    def test_missing_template_raises_file_not_found(self, tmp_path: Path):
        root = tmp_path / "gen_templates"
        (root / "links").mkdir(parents=True)
        (root / "essay").mkdir(parents=True)

        with patch.dict(os.environ, {"GEN_TEMPLATES_ROOT": str(root)}):
            with pytest.raises(FileNotFoundError) as exc_info:
                load_generation_template("nope", "links")
            assert "Template not found for phase='links'" in str(exc_info.value)

            with pytest.raises(FileNotFoundError) as exc_info2:
                load_generation_template("nope", "essay")
            assert "Template not found for phase='essay'" in str(exc_info2.value)
