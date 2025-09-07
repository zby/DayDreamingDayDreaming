#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path


def main():
    here = Path(__file__).parent
    parent = here.parent
    if str(parent) not in sys.path:
        sys.path.append(str(parent))
    from validate_parent_links import main as _main  # type: ignore

    return _main()


if __name__ == "__main__":
    raise SystemExit(main())

