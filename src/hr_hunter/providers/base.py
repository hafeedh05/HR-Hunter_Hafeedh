from __future__ import annotations

from typing import Any, Dict, List

from hr_hunter.models import ProviderRunResult, SearchBrief, SearchSlice


class SearchProvider:
    name = "base"

    def __init__(self, settings: Dict[str, Any]):
        self.settings = settings

    def is_configured(self) -> bool:
        return True

    async def run(
        self,
        brief: SearchBrief,
        slices: List[SearchSlice],
        limit: int,
        dry_run: bool,
    ) -> ProviderRunResult:
        raise NotImplementedError
