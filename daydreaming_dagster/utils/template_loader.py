from pathlib import Path
from typing import Literal
import os

PHASE = Literal["links", "draft", "essay"]

def load_generation_template(template_name: str, phase: PHASE) -> str:
    """
    Load a generation template for a specific phase.
    
    Args:
        template_name: Name of the template (e.g., 'creative-synthesis-v7')
        phase: Phase of generation ('links' or 'essay')
        
    Returns:
        str: Template content
        
    Raises:
        FileNotFoundError: If template file doesn't exist for the specified phase
    """
    # Determine templates root; allow env override for tests
    templates_root = Path(os.environ.get("GEN_TEMPLATES_ROOT", "data/1_raw/generation_templates"))
    # Map phase to directory; "draft" is the new name (preferred). For legacy, accept "links".
    if phase == "essay":
        phase_dir = "essay"
        candidates = [templates_root / phase_dir / f"{template_name}.txt"]
    else:
        # New preferred location
        draft_path = templates_root / "draft" / f"{template_name}.txt"
        legacy_path = templates_root / "links" / f"{template_name}.txt"
        # Prefer draft; fall back to links during transition
        candidates = [draft_path, legacy_path]

    for path in candidates:
        if path.exists():
            return path.read_text(encoding="utf-8")
    # If none exist, raise with the first candidate path shown
    raise FileNotFoundError(f"Template not found for phase='{phase}': {candidates[0]}")
