"""
Ensure `src` is on sys.path for local test runs without requiring installation.

This keeps `.venv/bin/pytest` working with an src/ layout.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).parent.resolve()
SRC = ROOT / "src"
if SRC.exists():
    sys.path.insert(0, str(SRC))

