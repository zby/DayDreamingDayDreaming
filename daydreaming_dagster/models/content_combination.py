from dataclasses import dataclass, field
from typing import List, Dict, Optional
from itertools import product

from .concept import Concept


@dataclass
class ContentCombination:
    """Resolved content for a combination of concepts, ready for template rendering."""
    contents: List[Dict[str, str]]  # [{"name": "...", "content": "..."}, ...]
    combo_id: str  # Unique identifier like "combo_001"
    concept_ids: List[str]  # List of concept_ids in this combination
    metadata: Dict[str, str] = field(default_factory=dict)  # Optional metadata like level used, strategy, etc.
    
    @classmethod
    def from_concepts(cls, concepts: List[Concept], level: str = "paragraph", combo_id: Optional[str] = None) -> "ContentCombination":
        """Current approach: single level with fallback for all concepts.

        Ordering note: The order of concepts in the rendered template is the
        order of the `concepts` argument. In the current pipeline, that order
        derives from the `concepts` asset, which preserves the row order of
        `data/1_raw/concepts_metadata.csv` after applying the `active`
        filter. As a result, concept combinations (and their template inputs)
        follow the CSV row order deterministically.
        """
        contents = []
        concept_ids = []
        for concept in concepts:
            content = cls._resolve_content(concept, level)
            contents.append({"name": concept.name, "content": content})
            concept_ids.append(concept.concept_id)
        
        # Generate combo_id if not provided
        if combo_id is None:
            combo_id = f"combo_{hash(tuple(concept_ids)) % 100000:05d}"
        
        return cls(
            contents=contents, 
            combo_id=combo_id,
            concept_ids=concept_ids,
            metadata={"strategy": "single_level", "level": level}
        )
    
    @classmethod
    def from_concepts_multi(cls, concepts: List[Concept]) -> List["ContentCombination"]:
        """Future: generate all level combinations for experimentation."""
        available_levels = cls._get_available_levels(concepts)
        concept_ids = [c.concept_id for c in concepts]
        
        combinations = []
        combo_counter = 1
        for level_combo in product(*[available_levels[c.concept_id] for c in concepts]):
            contents = []
            for concept, level in zip(concepts, level_combo):
                content = concept.descriptions[level]
                contents.append({"name": concept.name, "content": content})
            
            combo_id = f"combo_{combo_counter:03d}_multi"
            metadata = {
                "strategy": "multi_level", 
                "level_combination": level_combo
            }
            combinations.append(cls(
                contents=contents, 
                combo_id=combo_id,
                concept_ids=concept_ids,
                metadata=metadata
            ))
            combo_counter += 1
        
        return combinations
    
    @classmethod
    def from_concepts_filtered(cls, concepts: List[Concept], level_strategy: str) -> List["ContentCombination"]:
        """Future: smart filtering strategies to reduce combinatorial explosion."""
        concept_ids = [c.concept_id for c in concepts]
        
        if level_strategy == "uniform_levels":
            # All concepts use same level
            combinations = []
            for i, level in enumerate(["sentence", "paragraph", "article"]):
                combo_id = f"combo_{i+1:03d}_uniform_{level}"
                combo = cls.from_concepts(concepts, level, combo_id)
                combinations.append(combo)
            return combinations
        elif level_strategy == "progressive":
            # Systematically increase detail level
            combinations = []
            combo_counter = 1
            for base_level in ["sentence", "paragraph"]:
                for detail_concept_idx in range(len(concepts)):
                    contents = []
                    for i, concept in enumerate(concepts):
                        level = "article" if i == detail_concept_idx else base_level
                        content = cls._resolve_content(concept, level)
                        contents.append({"name": concept.name, "content": content})
                    
                    combo_id = f"combo_{combo_counter:03d}_prog"
                    metadata = {
                        "strategy": "progressive",
                        "base_level": base_level,
                        "detail_concept": detail_concept_idx
                    }
                    combinations.append(cls(
                        contents=contents, 
                        combo_id=combo_id,
                        concept_ids=concept_ids,
                        metadata=metadata
                    ))
                    combo_counter += 1
            return combinations
        else:
            raise ValueError(f"Unknown level strategy: {level_strategy}")
    
    @staticmethod
    def _resolve_content(concept: Concept, level: str) -> str:
        """Resolve content with fallback: requested → paragraph → sentence → name."""
        if level in concept.descriptions and concept.descriptions[level]:
            return concept.descriptions[level]
        
        # Fallback logic
        for fallback_level in ["paragraph", "sentence"]:
            if fallback_level in concept.descriptions and concept.descriptions[fallback_level]:
                return concept.descriptions[fallback_level]
        
        return concept.name  # Final fallback
    
    @staticmethod
    def _get_available_levels(concepts: List[Concept]) -> Dict[str, List[str]]:
        """Get available description levels for each concept."""
        available = {}
        for concept in concepts:
            available[concept.concept_id] = [
                level for level, content in concept.descriptions.items() 
                if content and content.strip()
            ]
        return available
