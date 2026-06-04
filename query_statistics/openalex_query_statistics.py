#!/usr/bin/env python3
"""Launcher for OpenAlex query statistics."""

from __future__ import annotations

import sys
from pathlib import Path


if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from query_statistics.openalex import main


if __name__ == "__main__":
    main()
