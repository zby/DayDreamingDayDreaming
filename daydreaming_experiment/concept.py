from dataclasses import dataclass
from typing import Optional


@dataclass
class Concept:
    """Core concept with three granularity levels for experiments."""
    name: str
    sentence: Optional[str] = None      # 1-sentence summary
    paragraph: Optional[str] = None     # Multi-sentence description  
    article: Optional[str] = None       # Full comprehensive content
    article_path: Optional[str] = None  # External file for large articles