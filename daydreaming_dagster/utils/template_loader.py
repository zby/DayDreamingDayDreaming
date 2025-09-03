from pathlib import Path
from typing import Literal
import os

PHASE = Literal["draft", "essay"]

def load_generation_template(template_name: str, phase: PHASE) -> str:
    """
    Load a generation template for a specific phase.
    
    Args:
        template_name: Name of the template (e.g., 'creative-synthesis-v7')
        phase: Phase of generation ('draft' or 'essay')
        
    Returns:
        str: Template content
        
    Raises:
        FileNotFoundError: If template file doesn't exist for the specified phase
    """
    # Determine templates root; allow env override for tests
    templates_root = Path(os.environ.get("GEN_TEMPLATES_ROOT", "data/1_raw/generation_templates"))
    if phase == "essay":
        path = templates_root / "essay" / f"{template_name}.txt"
    else:  # draft
        path = templates_root / "draft" / f"{template_name}.txt"
    if not path.exists():
        raise FileNotFoundError(f"Template not found for phase='{phase}': {path}")
    return path.read_text(encoding="utf-8")
