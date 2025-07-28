from typing import List, Iterator, Tuple
from .concept import Concept


DEFAULT_TEMPLATES = (
    # Systematic analytical approach
    """Below are several concepts to work with:

{concepts}

Please systematically explore how these concepts might be combined or connected. For each potential combination:
1. Identify the core principles or mechanisms from each concept
2. Consider how they might interact, complement, or enhance each other
3. Generate specific novel applications, insights, or innovations

Provide a numbered list of your most promising ideas with brief explanations.""",
    
    # Creative synthesis approach
    """Here are some concepts for creative exploration:

{concepts}

Let your imagination flow and consider unexpected ways these concepts might combine. Think about:
- Novel applications that don't exist yet
- Surprising connections or analogies
- Creative solutions to existing problems
- New research directions or questions

Generate a diverse list of innovative possibilities, from practical to speculative.""",
    
    # Problem-solving focused approach
    """Consider the following concepts:

{concepts}

Think about current challenges, limitations, or unsolved problems in various fields. How might combining or applying these concepts lead to:
- Solutions to existing problems
- Improvements to current methods or systems
- New approaches that overcome known limitations
- Breakthrough innovations in any domain

List your most compelling ideas with explanations of how they address specific needs.""",
    
    # Research and discovery approach
    """Examine these concepts:

{concepts}

As a researcher or innovator, consider what new knowledge or discoveries might emerge from connecting these ideas. Focus on:
- Unexplored research questions that bridge these concepts
- Hypotheses about how they might interact
- New theoretical frameworks or models
- Experimental approaches that could test novel combinations

Provide a structured list of research-worthy ideas and their potential significance.""",
    
    # Application and implementation approach
    """Review these concepts:

{concepts}

Think practically about how these concepts could be implemented or applied in real-world contexts. Consider:
- Concrete products, services, or systems that could be built
- Ways to improve existing technologies or processes
- New business models or organizational approaches
- Practical benefits that could impact people's lives

Generate actionable ideas with clear implementation pathways."""
)


class PromptFactory:
    """Template-based prompt generation from concept combinations."""
    
    def __init__(self, templates: tuple[str, ...] = DEFAULT_TEMPLATES):
        self.templates = templates
    
    def generate_prompt(self, concepts: List[Concept], level: str, template_idx: int = 0) -> str:
        """Generate prompt by combining concepts at specified granularity level."""
        if template_idx >= len(self.templates):
            raise IndexError(f"Template index {template_idx} out of range (0-{len(self.templates)-1})")
        
        concept_texts = []
        for concept in concepts:
            text = getattr(concept, level)
            if text is None:
                raise ValueError(f"Concept '{concept.name}' has no content at level '{level}'")
            concept_texts.append(text)
        
        formatted_concepts = '\n'.join(f"- {text}" for text in concept_texts)
        return self.templates[template_idx].format(concepts=formatted_concepts)
    
    def get_template_count(self) -> int:
        """Return the number of available templates."""
        return len(self.templates)


class PromptIterator:
    """Iterator for generating prompts across concept combinations and templates."""
    
    def __init__(self, prompt_factory: PromptFactory, concept_combinations: List[List[Concept]], level: str):
        self.prompt_factory = prompt_factory
        self.concept_combinations = concept_combinations
        self.level = level
        self._current_combo_idx = 0
        self._current_template_idx = 0
    
    def __iter__(self) -> Iterator[Tuple[List[Concept], int, str]]:
        """Iterate over all combinations of concepts and templates."""
        for combo_idx, concepts in enumerate(self.concept_combinations):
            for template_idx in range(self.prompt_factory.get_template_count()):
                prompt = self.prompt_factory.generate_prompt(concepts, self.level, template_idx)
                yield concepts, template_idx, prompt
    
    def generate_all(self) -> List[Tuple[List[Concept], int, str]]:
        """Generate all prompts as a list."""
        return list(self.__iter__())
    
    def get_total_count(self) -> int:
        """Get total number of prompts that will be generated."""
        return len(self.concept_combinations) * self.prompt_factory.get_template_count()