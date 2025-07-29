from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Concept:
    """Core concept with hierarchical descriptions for experiments."""

    # Canonical hierarchy from least detailed to most detailed
    DESCRIPTION_LEVELS = ["sentence", "paragraph", "article"]

    name: str
    descriptions: dict[str, str] = field(default_factory=dict)
    article_path: Optional[str] = None  # External file for large articles

    def get_description(self, level: str, strict: bool = True) -> str:
        """Get description at specified level with fallback to less detailed levels."""
        # Try requested level first
        if level in self.descriptions and self.descriptions[level]:
            return self.descriptions[level]

        if strict:
            raise ValueError(f"Concept '{self.name}' has no content at level '{level}'")

        # Validate requested level is known
        if level not in self.DESCRIPTION_LEVELS:
            raise ValueError(
                f"Unknown description level '{level}'. Valid levels: {self.DESCRIPTION_LEVELS}"
            )

        # Fallback: go backwards through hierarchy to find less detailed content
        requested_idx = self.DESCRIPTION_LEVELS.index(level)
        for i in range(requested_idx - 1, -1, -1):
            fallback_level = self.DESCRIPTION_LEVELS[i]
            if (
                fallback_level in self.descriptions
                and self.descriptions[fallback_level]
            ):
                return self.descriptions[fallback_level]

        # Final fallback to name
        return self.name

    # Backward compatibility properties
    @property
    def sentence(self) -> Optional[str]:
        """Backward compatibility for sentence access."""
        return self.descriptions.get("sentence")

    @property
    def paragraph(self) -> Optional[str]:
        """Backward compatibility for paragraph access."""
        return self.descriptions.get("paragraph")

    @property
    def article(self) -> Optional[str]:
        """Backward compatibility for article access."""
        return self.descriptions.get("article")
