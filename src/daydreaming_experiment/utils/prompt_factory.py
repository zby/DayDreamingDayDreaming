from typing import List, Iterator, Tuple
from pathlib import Path
from jinja2 import Environment, Template, meta
from .concept import Concept


def load_templates_from_directory(
    templates_dir: str = "data/01_raw/generation_templates/",
) -> tuple[Template, ...]:
    """Load all template files from the specified directory as Jinja2 templates."""
    templates_path = Path(templates_dir)
    if not templates_path.exists():
        raise FileNotFoundError(f"Templates directory not found: {templates_dir}")

    # Find all .txt files and sort them by filename for consistent ordering
    template_files = sorted(templates_path.glob("*.txt"))
    if not template_files:
        raise FileNotFoundError(f"No template files found in: {templates_dir}")

    env = Environment()
    templates = []

    for template_file in template_files:
        try:
            with open(template_file, "r", encoding="utf-8") as f:
                template_content = f.read().strip()
                if not template_content:
                    raise ValueError(f"Template file is empty: {template_file}")

                # Parse template and check for required variables
                parsed = env.parse(template_content)
                variables = meta.find_undeclared_variables(parsed)
                if "concepts" not in variables:
                    raise ValueError(
                        f"Template missing 'concepts' variable: {template_file}"
                    )

                template = env.from_string(template_content)
                templates.append(template)

        except Exception as e:
            raise RuntimeError(f"Error loading template {template_file}: {e}")

    return tuple(templates)


# Load default templates from files
DEFAULT_TEMPLATES = load_templates_from_directory()


class PromptFactory:
    """Template-based prompt generation from concept combinations using Jinja2."""

    def __init__(
        self, templates: tuple[Template, ...] = None, templates_dir: str = None
    ):
        """Initialize PromptFactory with templates.

        Args:
            templates: Tuple of Jinja2 Template objects. If None, loads from templates_dir.
            templates_dir: Directory to load templates from. Defaults to "data/templates".
        """
        if templates is not None:
            self.templates = templates
        elif templates_dir is not None:
            self.templates = load_templates_from_directory(templates_dir)
        else:
            self.templates = DEFAULT_TEMPLATES

    def generate_prompt(
        self,
        concepts: List[Concept],
        level: str,
        template_idx: int = 0,
        strict: bool = True,
    ) -> str:
        """Generate prompt by combining concepts at specified granularity level."""
        if template_idx >= len(self.templates):
            raise IndexError(
                f"Template index {template_idx} out of range (0-{len(self.templates)-1})"
            )

        # Validate that all concepts can provide content at the specified level
        for concept in concepts:
            # This will raise ValueError if strict=True and level not available
            concept.get_description(level, strict=strict)

        # Create template context with concepts, level, and strict mode
        context = {"concepts": concepts, "level": level, "strict": strict}

        return self.templates[template_idx].render(context)

    def get_template_count(self) -> int:
        """Return the number of available templates."""
        return len(self.templates)


class PromptIterator:
    """Iterator for generating prompts across concept combinations and templates."""

    def __init__(
        self,
        prompt_factory: PromptFactory,
        concept_combinations: List[List[Concept]],
        level: str,
        strict: bool = True,
    ):
        self.prompt_factory = prompt_factory
        self.concept_combinations = concept_combinations
        self.level = level
        self.strict = strict
        self._current_combo_idx = 0
        self._current_template_idx = 0

    def __iter__(self) -> Iterator[Tuple[List[Concept], int, str]]:
        """Iterate over all combinations of concepts and templates."""
        for combo_idx, concepts in enumerate(self.concept_combinations):
            for template_idx in range(self.prompt_factory.get_template_count()):
                prompt = self.prompt_factory.generate_prompt(
                    concepts, self.level, template_idx, strict=self.strict
                )
                yield concepts, template_idx, prompt

    def generate_all(self) -> List[Tuple[List[Concept], int, str]]:
        """Generate all prompts as a list."""
        return list(self.__iter__())

    def get_total_count(self) -> int:
        """Get total number of prompts that will be generated."""
        return len(self.concept_combinations) * self.prompt_factory.get_template_count()
