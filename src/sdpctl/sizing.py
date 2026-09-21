from __future__ import annotations

from pathlib import Path


def apparent_size(path: str | Path) -> int:
    p = Path(path)
    if not p.exists():
        return 0
    if p.is_file():
        return p.stat().st_size
    total = 0
    for child in p.rglob("*"):
        try:
            if child.is_file():
                total += child.stat().st_size
        except FileNotFoundError:
            pass
    return total
