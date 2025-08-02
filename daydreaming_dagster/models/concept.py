from dataclasses import dataclass, field
from typing import Dict


@dataclass
class Concept:
    """Core concept with hierarchical descriptions for experiments."""
    
    concept_id: str
    name: str
    descriptions: Dict[str, str] = field(default_factory=dict)