from __future__ import annotations

import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Dict, List, Set

from hr_hunter.identity import candidate_identity_keys, candidate_primary_key
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
    return candidate_primary_key(candidate)


def dedupe_candidates(candidates: List[CandidateProfile]) -> List[CandidateProfile]:
    deduped: List[CandidateProfile] = []
    for candidate in candidates:
        candidate_keys = candidate_identity_keys(candidate)
        matched_index = next(
            (
                index
                for index, existing in enumerate(deduped)
                if candidate_keys.intersection(candidate_identity_keys(existing))
            ),
            None,
        )
        if matched_index is None:
            deduped.append(candidate)
            continue

        existing = deduped[matched_index]
        if candidate.score > existing.score:
            deduped[matched_index] = candidate
    return deduped


class SearchEngine:
    async def run(
        self,
        brief: SearchBrief,
        provider_names: List[str],
        limit: int,
        dry_run: bool,
        exclude_candidate_keys: Set[str] | None = None,
        exclude_provider_queries: Dict[str, Set[str]] | None = None,
    ) -> SearchRunReport:
        slices = build_search_slices(brief)
        provider_results: List[ProviderRunResult] = []
        candidate_pool: List[CandidateProfile] = []
        exclude_candidate_keys = exclude_candidate_keys or set()
        exclude_provider_queries = exclude_provider_queries or {}
        excluded_seen_count = 0

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
            result = await provider.run(
                brief,
                slices,
                limit,
                dry_run,
                exclude_queries=exclude_provider_queries.get(provider_name, set()),
            )
            provider_results.append(result)
            candidate_pool.extend(result.candidates)

            rescored_pool = [score_candidate(candidate, brief) for candidate in dedupe_candidates(candidate_pool)]
            if exclude_candidate_keys:
                filtered_pool = [
                    candidate
                    for candidate in rescored_pool
                    if candidate_identity_keys(candidate).isdisjoint(exclude_candidate_keys)
                ]
                excluded_seen_count += len(rescored_pool) - len(filtered_pool)
                rescored_pool = filtered_pool
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
        summary = self._build_summary(
            brief,
            provider_results,
            final_candidates,
            dry_run,
            excluded_seen_count=excluded_seen_count,
        )
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
        excluded_seen_count: int = 0,
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
            "excluded_seen_count": excluded_seen_count,
        }
