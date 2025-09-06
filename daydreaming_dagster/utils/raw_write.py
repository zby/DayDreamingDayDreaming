from pathlib import Path
from typing import Optional
from .versioned_files import save_versioned_text


def save_versioned_raw_text(base_dir: Path, task_id: str, text: str, logger=None) -> Optional[str]:
    """Save text to a versioned RAW file under base_dir and return the filepath."""
    return save_versioned_text(base_dir, task_id, text, ".txt", logger=logger)
