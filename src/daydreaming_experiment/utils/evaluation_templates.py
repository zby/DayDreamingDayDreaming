from pathlib import Path
from typing import Dict, List
from jinja2 import Environment, FileSystemLoader, Template, TemplateNotFound


class EvaluationTemplateLoader:
    """Loads and manages evaluation templates using Jinja2."""

    def __init__(self, templates_dir: str = "data/evaluation_templates"):
        """Initialize template loader with templates directory."""
        self.templates_dir = Path(templates_dir)
        if not self.templates_dir.exists():
            raise FileNotFoundError(f"Templates directory not found: {templates_dir}")

        # Setup Jinja2 environment
        self.env = Environment(
            loader=FileSystemLoader(str(self.templates_dir)),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def list_templates(self) -> List[str]:
        """List available evaluation templates."""
        templates = []
        for template_file in self.templates_dir.glob("*.txt"):
            templates.append(template_file.stem)
        return sorted(templates)

    def load_template(self, template_name: str) -> Template:
        """Load a specific evaluation template."""
        template_file = f"{template_name}.txt"
        try:
            return self.env.get_template(template_file)
        except TemplateNotFound:
            available = self.list_templates()
            raise ValueError(
                f"Template '{template_name}' not found. Available templates: {available}"
            )

    def render_evaluation_prompt(self, template_name: str, response: str) -> str:
        """Render evaluation prompt using template and response."""
        template = self.load_template(template_name)
        return template.render(response=response)

    def get_default_template(self) -> str:
        """Get the default evaluation template name."""
        # Use iterative_loops as default if available, otherwise first available
        available = self.list_templates()
        if not available:
            raise ValueError("No evaluation templates found")

        return "iterative_loops" if "iterative_loops" in available else available[0]
