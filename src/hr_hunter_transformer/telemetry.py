from __future__ import annotations

from time import monotonic
from typing import Any


STAGE_ORDER = (
    "brief_normalized",
    "role_understood",
    "queries_planned",
    "retrieval_running",
    "extraction_running",
    "entity_resolution",
    "scoring",
    "verification",
    "finalizing",
    "completed",
)


class StageTelemetry:
    def __init__(self) -> None:
        self._start = monotonic()
        self._events: list[dict[str, Any]] = []

    def emit(self, stage: str, *, message: str = "", percent: int = 0, **metrics: Any) -> dict[str, Any]:
        event = {
            "stage": stage,
            "message": message,
            "percent": percent,
            "elapsed_seconds": int(max(0, monotonic() - self._start)),
            **metrics,
        }
        self._events.append(event)
        return event

    def events(self) -> list[dict[str, Any]]:
        return list(self._events)


def stage_percent(stage: str, *, completed: int = 0, total: int = 0) -> int:
    static_map = {
        "brief_normalized": 2,
        "role_understood": 6,
        "queries_planned": 10,
        "extraction_running": 72,
        "entity_resolution": 82,
        "scoring": 88,
        "verification": 94,
        "finalizing": 98,
        "completed": 100,
    }
    if stage == "retrieval_running":
        if total <= 0:
            return 16
        coverage = min(1.0, max(0.0, completed / max(1, total)))
        return max(16, min(68, 16 + int(round(coverage * 52))))
    return static_map.get(stage, 0)

