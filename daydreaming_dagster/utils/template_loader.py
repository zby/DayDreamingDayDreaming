from pathlib import Path
from typing import Literal

PHASE = Literal["links", "essay"]
TEMPLATES_ROOT = Path("data/1_raw/generation_templates")

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
    # Use phase directories: links/ (plural), essay/ (singular)
    phase_dir = "essay" if phase == "essay" else "links"
    path = TEMPLATES_ROOT / phase_dir / f"{template_name}.txt"
    if not path.exists():
        raise FileNotFoundError(f"Template not found for phase='{phase}': {path}")
    return path.read_text(encoding="utf-8")
