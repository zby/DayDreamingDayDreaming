import json
import os
from pathlib import Path
from typing import List, Iterator
from itertools import combinations

from .concept import Concept


class ConceptDB:
    """Registry for all available concepts with batch retrieval and combination iteration."""
    
    def __init__(self, concepts: List[Concept] = None):
        self._concepts = concepts or []
    
    def add_concept(self, concept: Concept) -> None:
        """Add a concept to the database."""
        self._concepts.append(concept)
    
    def get_concepts(self) -> List[Concept]:
        """Get all concepts in the database."""
        return self._concepts.copy()
    
    def get_concept(self, name: str) -> Concept:
        """Get a concept by name."""
        for concept in self._concepts:
            if concept.name == name:
                return concept
        raise KeyError(f"Concept '{name}' not found")
    
    def get_combinations(self, k: int) -> Iterator[List[Concept]]:
        """Generate all k-combinations of concepts."""
        for combo in combinations(self._concepts, k):
            yield list(combo)
    
    def save(self, manifest_path: str, articles_dir: str = "articles") -> None:
        """Save concepts to JSON manifest + article files."""
        manifest_path = Path(manifest_path)
        base_path = manifest_path.parent
        base_path.mkdir(parents=True, exist_ok=True)
        
        articles_path = base_path / articles_dir
        articles_path.mkdir(exist_ok=True)
        
        manifest = {
            "version": "1.0",
            "articles_dir": articles_dir,
            "concepts": []
        }
        
        for concept in self._concepts:
            concept_data = {
                "name": concept.name,
                "descriptions": concept.descriptions.copy()
            }
            
            # Handle article content - save to file if it exists
            if concept.descriptions.get('article'):
                article_file = f"{concept.name}.txt"
                article_file_path = articles_path / article_file
                with open(article_file_path, 'w', encoding='utf-8') as f:
                    f.write(concept.descriptions['article'])
                concept_data["article_path"] = f"{articles_dir}/{article_file}"
                # Remove article content from descriptions since it's in external file
                concept_data["descriptions"] = concept_data["descriptions"].copy()
                del concept_data["descriptions"]['article']
            elif concept.article_path:
                concept_data["article_path"] = concept.article_path
            
            manifest["concepts"].append(concept_data)
        
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2)
    
    @classmethod
    def load(cls, manifest_path: str) -> 'ConceptDB':
        """Load concepts from JSON manifest + article files."""
        manifest_path = Path(manifest_path)
        base_path = manifest_path.parent
        
        with open(manifest_path, 'r', encoding='utf-8') as f:
            manifest = json.load(f)
        
        articles_dir = manifest.get("articles_dir", "articles")
        
        concepts = []
        for concept_data in manifest["concepts"]:
            # Handle new format with descriptions dict
            descriptions = {}
            
            if "descriptions" in concept_data:
                # New format - use descriptions dict directly
                descriptions = concept_data["descriptions"].copy()
            else:
                # Backward compatibility - convert old format to new
                if concept_data.get("sentence"):
                    descriptions["sentence"] = concept_data["sentence"]
                if concept_data.get("paragraph"):
                    descriptions["paragraph"] = concept_data["paragraph"]
                if concept_data.get("article"):
                    descriptions["article"] = concept_data["article"]
            
            concept = Concept(
                name=concept_data["name"],
                descriptions=descriptions,
                article_path=concept_data.get("article_path")
            )
            
            # Load article content from external file if available
            if concept.article_path:
                article_full_path = base_path / concept.article_path
                if article_full_path.exists():
                    with open(article_full_path, 'r', encoding='utf-8') as f:
                        concept.descriptions['article'] = f.read()
            
            concepts.append(concept)
        
        return cls(concepts)
    
    def __len__(self) -> int:
        """Return number of concepts in database."""
        return len(self._concepts)