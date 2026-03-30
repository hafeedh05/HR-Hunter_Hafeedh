from __future__ import annotations

import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Dict, List

from hr_hunter.models import CandidateProfile, ProviderRunResult, SearchBrief, SearchRunReport
from hr_hunter.providers.mock import MockProvider
from hr_hunter.providers.pdl import PDLProvider
from hr_hunter.providers.scrapingbee import ScrapingBeeGoogleProvider
from hr_hunter.query_planner import build_search_slices
from hr_hunter.scoring import score_candidate, sort_candidates


PROVIDER_REGISTRY = {
    "mock": MockProvider,
    "pdl": PDLProvider,
    "scrapingbee_google": ScrapingBeeGoogleProvider,
}


def candidate_key(candidate: CandidateProfile) -> str:
    if candidate.linkedin_url:
        return candidate.linkedin_url.lower()

    parts = [
        candidate.full_name.lower(),
        candidate.current_company.lower(),
        candidate.current_title.lower(),
    ]
    return "|".join(parts)


def dedupe_candidates(candidates: List[CandidateProfile]) -> List[CandidateProfile]:
    deduped: Dict[str, CandidateProfile] = {}
    for candidate in candidates:
        key = candidate_key(candidate)
        existing = deduped.get(key)
        if existing is None or candidate.score > existing.score:
            deduped[key] = candidate
    return list(deduped.values())


class SearchEngine:
    async def run(
        self,
        brief: SearchBrief,
        provider_names: List[str],
        limit: int,
        dry_run: bool,
    ) -> SearchRunReport:
        slices = build_search_slices(brief)
        provider_results: List[ProviderRunResult] = []
        candidate_pool: List[CandidateProfile] = []

        for provider_name in provider_names:
            provider_class = PROVIDER_REGISTRY.get(provider_name)
            if provider_class is None:
                provider_results.append(
                    ProviderRunResult(
                        provider_name=provider_name,
                        executed=False,
                        dry_run=dry_run,
                        errors=[f"Unknown provider: {provider_name}"],
                    )
                )
                continue

            provider = provider_class(brief.provider_settings.get(provider_name, {}))
            result = await provider.run(brief, slices, limit, dry_run)
            provider_results.append(result)
            candidate_pool.extend(result.candidates)

            rescored_pool = [score_candidate(candidate, brief) for candidate in dedupe_candidates(candidate_pool)]
            candidate_pool = sort_candidates(rescored_pool)

            if not dry_run:
                accepted = [
                    candidate
                    for candidate in candidate_pool
                    if candidate.verification_status in {"verified", "review"}
                ]
                if len(accepted) >= limit:
                    break

        final_candidates = candidate_pool[:limit]
        summary = self._build_summary(brief, provider_results, final_candidates, dry_run)
        return SearchRunReport(
            run_id=f"{brief.id}-{uuid.uuid4().hex[:8]}",
            brief_id=brief.id,
            dry_run=dry_run,
            generated_at=datetime.now(timezone.utc).isoformat(),
            provider_results=provider_results,
            candidates=final_candidates,
            summary=summary,
        )

    def _build_summary(
        self,
        brief: SearchBrief,
        provider_results: List[ProviderRunResult],
        candidates: List[CandidateProfile],
        dry_run: bool,
    ) -> Dict[str, object]:
        verified = len([candidate for candidate in candidates if candidate.verification_status == "verified"])
        review = len([candidate for candidate in candidates if candidate.verification_status == "review"])
        rejected = len([candidate for candidate in candidates if candidate.verification_status == "reject"])
        return {
            "role_title": brief.role_title,
            "dry_run": dry_run,
            "provider_order": [result.provider_name for result in provider_results],
            "provider_errors": {
                result.provider_name: result.errors for result in provider_results if result.errors
            },
            "slice_count": len(build_search_slices(brief)),
            "candidate_count": len(candidates),
            "verified_count": verified,
            "review_count": review,
            "reject_count": rejected,
            "target_range": [brief.result_target_min, brief.result_target_max],
        }
