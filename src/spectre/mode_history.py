from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class ModeTransition:
    from_mode: str
    to_mode: str
    task_id: str | None
    at: datetime


class ModeHistory:
    """Records every operating-mode transition driven by Tasking.

    This is the "illustrate the change of behaviour" piece: client.py calls
    `record()` right after a mode_change (or STOP/PAUSE revert) succeeds,
    which both (a) gives a distinct, greppable "MODE CHANGE: ..." log line
    separate from the generic TX/RX logging, and (b) lets
    messages.build_status surface the latest transition as a
    StatusReport.Status entry, so it's visible on the Fusion Node / C2 side
    too, not just in local logs.
    """

    def __init__(self, max_len: int = 100):
        self._max_len = max_len
        self._transitions: list[ModeTransition] = []

    def record(self, from_mode: str, to_mode: str, task_id: str | None) -> ModeTransition:
        transition = ModeTransition(
            from_mode=from_mode,
            to_mode=to_mode,
            task_id=task_id,
            at=datetime.now(timezone.utc),
        )
        self._transitions.append(transition)
        if len(self._transitions) > self._max_len:
            self._transitions.pop(0)
        return transition

    @property
    def transitions(self) -> list[ModeTransition]:
        return list(self._transitions)

    @property
    def last(self) -> ModeTransition | None:
        return self._transitions[-1] if self._transitions else None
