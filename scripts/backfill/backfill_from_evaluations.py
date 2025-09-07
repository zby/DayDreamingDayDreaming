#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path


def main():
    # Ensure parent 'scripts' is on sys.path, then import and delegate
    here = Path(__file__).parent
    parent = here.parent
    if str(parent) not in sys.path:
        sys.path.append(str(parent))
    from backfill_from_evaluations import main as _main  # type: ignore

    return _main()


if __name__ == "__main__":
    raise SystemExit(main())

