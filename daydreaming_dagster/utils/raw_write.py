from pathlib import Path
import os
import re
from typing import Optional


def save_versioned_raw_text(base_dir: Path, task_id: str, text: str, logger=None) -> Optional[str]:
    """Save text to a versioned RAW file under base_dir and return the filepath.

    Filenames follow the pattern: {task_id}_vN.txt where N increments.
    Returns the string path on success or None on failure.
    """
    try:
        base_dir.mkdir(parents=True, exist_ok=True)
        pattern = re.compile(rf"^{re.escape(task_id)}_v(\\d+)\\.txt$")
        current = 0
        for name in os.listdir(base_dir):
            m = pattern.match(name)
            if m:
                try:
                    v = int(m.group(1))
                    if v > current:
                        current = v
                except Exception:
                    # Skip non-integer matches silently
                    pass
        next_v = current + 1
        raw_path = base_dir / f"{task_id}_v{next_v}.txt"
        raw_path.write_text(text, encoding="utf-8")
        return str(raw_path)
    except Exception as e:
        if logger is not None:
            try:
                logger.info(f"Failed to save RAW text for {task_id}: {e}")
            except Exception:
                pass
        return None

