from __future__ import annotations
import ulid


def new_ulid() -> str:
    return str(ulid.new())
