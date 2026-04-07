from __future__ import annotations

import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Dict, List, Set

from hr_hunter.identity import candidate_identity_keys, candidate_primary_key
from hr_hunter.models import CandidateProfile, ProviderRunResult, SearchBrief, SearchRunReport
from hr_hunter.output import build_reporting_summary
from hr_hunter.providers.mock import MockProvider
from hr_hunter.providers.scrapingbee import ScrapingBeeGoogleProvider
from hr_hunter.query_planner import build_search_slices
from hr_hunter.ranker import RANKING_MODEL_VERSION, apply_learned_ranker, parse_learned_ranker_settings
from hr_hunter.reranker import DEFAULT_RERANKER_MODEL, parse_reranker_settings, rerank_candidates
from hr_hunter.scoring import score_candidate, sort_candidates
from hr_hunter.state import attach_registry_metadata, search_registry_memory


PROVIDER_REGISTRY = {
    "mock": MockProvider,
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
        memory_settings = brief.provider_settings.get("registry_memory", {})
        if not dry_run and bool(memory_settings.get("enabled", False)):
            memory_candidates = search_registry_memory(
                brief,
                limit=int(memory_settings.get("limit", limit) or limit),
            )
            if memory_candidates:
                provider_results.append(
                    ProviderRunResult(
                        provider_name="registry_memory",
                        executed=True,
                        dry_run=False,
                        request_count=0,
                        candidate_count=len(memory_candidates),
                        candidates=list(memory_candidates),
                        diagnostics={"message": "Loaded candidates from cross-search registry memory."},
                    )
                )
                candidate_pool.extend(memory_candidates)
                candidate_pool = sort_candidates([score_candidate(candidate, brief) for candidate in dedupe_candidates(candidate_pool)])

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
                candidate_pool = sort_candidates(rerank_candidates(brief, candidate_pool))
                candidate_pool = sort_candidates(apply_learned_ranker(brief, candidate_pool))

            if not dry_run:
                accepted = [
                    candidate
                    for candidate in candidate_pool
                    if candidate.verification_status in {"verified", "review"}
                ]
                if len(accepted) >= limit:
                    break

        final_candidates = attach_registry_metadata(candidate_pool[:limit])
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
        reranker_settings = parse_reranker_settings(brief)
        learned_ranker_settings = parse_learned_ranker_settings(brief)
        base_summary = {
            "role_title": brief.role_title,
            "dry_run": dry_run,
            "provider_order": [result.provider_name for result in provider_results],
            "provider_errors": {
                result.provider_name: result.errors for result in provider_results if result.errors
            },
            "slice_count": len(build_search_slices(brief)),
            "target_range": [brief.result_target_min, brief.result_target_max],
            "excluded_seen_count": excluded_seen_count,
            "anchor_weights": brief.anchor_weights,
            "company_match_mode": brief.company_match_mode,
            "location_targets": brief.location_targets,
            "years_mode": brief.years_mode,
            "years_target": brief.years_target,
            "years_tolerance": brief.years_tolerance,
            "jd_breakdown": brief.jd_breakdown,
            "ranking_model_version": RANKING_MODEL_VERSION,
            "reranker": {
                "enabled": reranker_settings.enabled,
                "model_name": reranker_settings.model_name if reranker_settings.enabled else DEFAULT_RERANKER_MODEL,
                "top_n": reranker_settings.top_n,
                "weight": reranker_settings.weight,
            },
            "learned_ranker": {
                "enabled": learned_ranker_settings.enabled,
                "model_dir": str(learned_ranker_settings.model_dir),
                "weight": learned_ranker_settings.weight,
            },
        }
        return build_reporting_summary(candidates, base_summary)
