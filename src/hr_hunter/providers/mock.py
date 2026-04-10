from __future__ import annotations

from typing import Any, Callable, Dict

from hr_hunter.models import CandidateProfile, ProviderRunResult, SearchBrief, SearchSlice
from hr_hunter.providers.base import SearchProvider


class MockProvider(SearchProvider):
    name = "mock"

    async def run(
        self,
        brief: SearchBrief,
        slices: list[SearchSlice],
        limit: int,
        dry_run: bool,
        exclude_queries: set[str] | None = None,
        progress_callback: Callable[[Dict[str, Any]], None] | None = None,
    ) -> ProviderRunResult:
        diagnostics = {
            "slice_count": len(slices),
            "message": "Mock provider returns fixture data only.",
        }
        if dry_run:
            return ProviderRunResult(
                provider_name=self.name,
                executed=False,
                dry_run=True,
                diagnostics=diagnostics,
            )

        candidates = [
            CandidateProfile(
                full_name="Jane Operator",
                current_title=brief.titles[0] if brief.titles else "Global Product Manager",
                current_company=brief.company_targets[0] if brief.company_targets else "Example Company",
                location_name=brief.geography.location_name,
                location_geo=(
                    f"{brief.geography.center_latitude},{brief.geography.center_longitude}"
                    if brief.geography.center_latitude is not None
                    and brief.geography.center_longitude is not None
                    else None
                ),
                linkedin_url="https://www.linkedin.com/in/jane-operator",
                source=self.name,
                source_url="https://www.linkedin.com/in/jane-operator",
                summary=brief.brief_summary,
                experience=[
                    {
                        "company": {"name": brief.company_targets[0] if brief.company_targets else "Example"},
                        "title": {"name": brief.titles[0] if brief.titles else "Global Product Manager"},
                        "start_date": "2011-01-01",
                    }
                ],
                industry="consumer packaged goods",
                raw={"fixture": True},
            )
        ]
        if progress_callback:
            progress_callback(
                {
                    "provider": self.name,
                    "stage": "retrieval",
                    "queries_total": 1,
                    "queries_completed": 1,
                    "raw_found": len(candidates),
                    "message": "Mock retrieval completed.",
                }
            )
        return ProviderRunResult(
            provider_name=self.name,
            executed=True,
            dry_run=False,
            request_count=1,
            candidate_count=len(candidates),
            candidates=candidates[:limit],
            diagnostics=diagnostics,
        )
