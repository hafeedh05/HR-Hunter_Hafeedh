from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Set

from hr_hunter.identity import candidate_identity_keys, candidate_primary_key
from hr_hunter.models import CandidateProfile, ProviderRunResult, SearchBrief, SearchRunReport
from hr_hunter.output import build_reporting_summary, build_scope_progress_counts
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
        progress_callback: Callable[[Dict[str, Any]], None] | None = None,
    ) -> SearchRunReport:
        slices = build_search_slices(brief)
        provider_results: List[ProviderRunResult] = []
        candidate_pool: List[CandidateProfile] = []
        exclude_candidate_keys = exclude_candidate_keys or set()
        exclude_provider_queries = exclude_provider_queries or {}
        excluded_seen_count = 0
        provider_query_totals: Dict[str, int] = {}
        provider_query_completed: Dict[str, int] = {}
        provider_raw_found: Dict[str, int] = {}
        provider_queries_in_flight: Dict[str, int] = {}
        last_scope_counts: Dict[str, int] = {
            "in_scope_count": 0,
            "precise_in_scope_count": 0,
            "title_match_count": 0,
            "market_match_count": 0,
            "verified_count": 0,
            "review_count": 0,
            "reject_count": 0,
        }

        def emit_progress(payload: Dict[str, Any]) -> None:
            if not progress_callback:
                return
            try:
                progress_callback(dict(payload or {}))
            except Exception:
                # Progress telemetry must never break the search pipeline.
                return

        async def run_blocking_stage(
            *,
            stage: str,
            stage_label: str,
            message: str,
            percent: int,
            worker: Callable[[], Any],
            reranked_count: int = 0,
            finalized_count: int = 0,
            heartbeat_patch: Callable[[int], Dict[str, Any]] | None = None,
        ) -> Any:
            task = asyncio.create_task(asyncio.to_thread(worker))
            stage_started = time.monotonic()
            while not task.done():
                elapsed_stage = int(max(0.0, time.monotonic() - stage_started))
                payload: Dict[str, Any] = {
                    "stage": stage,
                    "stage_label": stage_label,
                    "message": message,
                    "queries_completed": sum(provider_query_completed.values()),
                    "queries_total": sum(provider_query_totals.values()),
                    "raw_found": sum(provider_raw_found.values()),
                    "queries_in_flight": sum(provider_queries_in_flight.values()),
                    "unique_after_dedupe": len(candidate_pool),
                    "reranked_count": reranked_count,
                    "finalized_count": finalized_count,
                    "percent": max(0, min(99, int(percent))),
                    "stage_elapsed_seconds": elapsed_stage,
                    **last_scope_counts,
                }
                if heartbeat_patch:
                    for key, value in heartbeat_patch(elapsed_stage).items():
                        if value is not None:
                            payload[key] = value
                emit_progress(payload)
                await asyncio.sleep(2.0)
            return await task

        emit_progress(
            {
                "stage": "retrieval",
                "stage_label": "Retrieval",
                "message": "Planning query slices.",
                "slice_count": len(slices),
                "queries_completed": 0,
                "queries_total": 0,
                "raw_found": 0,
                "unique_after_dedupe": 0,
                "reranked_count": 0,
                "finalized_count": 0,
                "round": 0,
                "percent": 5,
                **last_scope_counts,
            }
        )
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
                candidate_pool = sort_candidates(
                    [score_candidate(candidate, brief) for candidate in dedupe_candidates(candidate_pool)],
                    brief,
                )
                last_scope_counts = build_scope_progress_counts(candidate_pool)
                emit_progress(
                    {
                        "stage": "retrieval",
                        "stage_label": "Retrieval",
                        "message": "Loaded registry memory candidates.",
                        "queries_completed": 0,
                        "queries_total": 0,
                        "raw_found": len(memory_candidates),
                        "unique_after_dedupe": len(candidate_pool),
                        "reranked_count": 0,
                        "finalized_count": 0,
                        "percent": 10,
                        **last_scope_counts,
                    }
                )

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
            def _provider_progress(payload: Dict[str, Any], provider_default: str = provider_name) -> None:
                provider_key = str(payload.get("provider") or provider_default)
                provider_query_totals[provider_key] = max(
                    provider_query_totals.get(provider_key, 0),
                    int(payload.get("queries_total", 0) or 0),
                )
                provider_query_completed[provider_key] = max(
                    provider_query_completed.get(provider_key, 0),
                    int(payload.get("queries_completed", 0) or 0),
                )
                provider_raw_found[provider_key] = max(
                    provider_raw_found.get(provider_key, 0),
                    int(payload.get("raw_found", 0) or 0),
                )
                provider_queries_in_flight[provider_key] = max(
                    0,
                    int(payload.get("queries_in_flight", provider_queries_in_flight.get(provider_key, 0)) or 0),
                )
                emit_progress(
                    {
                        "stage": "retrieval",
                        "stage_label": "Retrieval",
                        "message": str(payload.get("message") or "Running retrieval queries."),
                        "queries_completed": sum(provider_query_completed.values()),
                        "queries_total": sum(provider_query_totals.values()),
                        "raw_found": sum(provider_raw_found.values()),
                        "queries_in_flight": sum(provider_queries_in_flight.values()),
                        "unique_after_dedupe": len(candidate_pool),
                        "reranked_count": 0,
                        "finalized_count": 0,
                        **last_scope_counts,
                        "percent": max(
                            10,
                            min(
                                65,
                                int(
                                    round(
                                        (
                                            sum(provider_query_completed.values())
                                            / max(1, sum(provider_query_totals.values()))
                                        )
                                        * 55
                                        + 10
                                    )
                                ),
                            ),
                        ),
                    }
                )
            result = await provider.run(
                brief,
                slices,
                limit,
                dry_run,
                exclude_queries=exclude_provider_queries.get(provider_name, set()),
                progress_callback=_provider_progress,
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
            candidate_pool = sort_candidates(rescored_pool, brief)
            last_scope_counts = build_scope_progress_counts(candidate_pool)
            emit_progress(
                {
                    "stage": "dedupe",
                    "stage_label": "Dedupe",
                    "message": f"Provider {provider_name} merged and deduped.",
                    "queries_completed": sum(provider_query_completed.values()),
                    "queries_total": sum(provider_query_totals.values()),
                    "raw_found": sum(provider_raw_found.values()),
                    "unique_after_dedupe": len(candidate_pool),
                    "reranked_count": 0,
                    "finalized_count": 0,
                    "percent": 70 if not dry_run else 90,
                    **last_scope_counts,
                }
            )

            if not dry_run:
                accepted = [
                    candidate
                    for candidate in candidate_pool
                    if candidate.verification_status in {"verified", "review"}
                ]
                if len(accepted) >= limit:
                    break

        reranked_count = 0
        rerank_target_count = 0
        if not dry_run and candidate_pool:
            for provider_key in list(provider_query_totals.keys()):
                completed_total = max(0, int(provider_query_completed.get(provider_key, 0) or 0))
                planned_total = max(0, int(provider_query_totals.get(provider_key, 0) or 0))
                if completed_total > 0 and planned_total > completed_total:
                    provider_query_totals[provider_key] = completed_total
            for provider_key in list(provider_queries_in_flight.keys()):
                provider_queries_in_flight[provider_key] = 0
            reranker_settings = parse_reranker_settings(brief)
            optimized_top_n = reranker_settings.top_n
            rerank_target_count = min(len(candidate_pool), max(1, int(optimized_top_n)))
            semantic_reranked_count = 0
            if reranker_settings.enabled:
                optimized_top_n = max(limit * 2, 220)
                optimized_top_n = min(optimized_top_n, reranker_settings.top_n, 500, len(candidate_pool))
                brief.provider_settings.setdefault("reranker", {})["top_n"] = max(1, int(optimized_top_n))
                rerank_target_count = min(len(candidate_pool), max(1, int(optimized_top_n)))
            else:
                rerank_target_count = min(len(candidate_pool), max(1, int(optimized_top_n)))

            def _semantic_rerank_progress(done: int, total: int) -> None:
                nonlocal semantic_reranked_count, rerank_target_count
                rerank_target_count = max(1, int(total or rerank_target_count or 1))
                semantic_reranked_count = max(semantic_reranked_count, int(done or 0))

            def _semantic_rerank_heartbeat(_: int) -> Dict[str, Any]:
                if rerank_target_count <= 0:
                    return {}
                coverage = min(1.0, max(0.0, semantic_reranked_count / rerank_target_count))
                return {
                    "reranked_count": semantic_reranked_count,
                    "rerank_target": rerank_target_count,
                    "queries_in_flight": 0,
                    "message": (
                        "Applying semantic reranker to top candidates. "
                        f"{semantic_reranked_count}/{rerank_target_count} scored."
                    ),
                    "percent": max(80, min(95, 80 + int(round(coverage * 15)))),
                }
            emit_progress(
                {
                    "stage": "rerank",
                    "stage_label": "Rerank",
                    "message": "Applying semantic reranker to top candidates.",
                    "queries_completed": sum(provider_query_completed.values()),
                    "queries_total": sum(provider_query_totals.values()),
                    "raw_found": sum(provider_raw_found.values()),
                    "unique_after_dedupe": len(candidate_pool),
                    "reranked_count": 0,
                    "finalized_count": 0,
                    "rerank_target": rerank_target_count,
                    "percent": 80,
                    **last_scope_counts,
                }
            )
            ranking_stage_message = "Applying semantic reranker to top candidates."
            candidate_pool = await run_blocking_stage(
                stage="rerank",
                stage_label="Rerank",
                message=ranking_stage_message,
                percent=80,
                worker=lambda: sort_candidates(
                    rerank_candidates(
                        brief,
                        candidate_pool,
                        progress_callback=_semantic_rerank_progress,
                    ),
                    brief,
                ),
                reranked_count=0,
                finalized_count=0,
                heartbeat_patch=_semantic_rerank_heartbeat,
            )
            if reranker_settings.enabled and rerank_target_count > 0:
                semantic_reranked_count = max(semantic_reranked_count, rerank_target_count)
            reranked_count = min(
                len(candidate_pool),
                max(semantic_reranked_count, int(brief.provider_settings.get("reranker", {}).get("top_n", 0) or 0)),
            )
            last_scope_counts = build_scope_progress_counts(candidate_pool)
            emit_progress(
                {
                    "stage": "rerank",
                    "stage_label": "Rerank",
                    "message": f"Semantic rerank completed for {reranked_count} candidates.",
                    "queries_completed": sum(provider_query_completed.values()),
                    "queries_total": sum(provider_query_totals.values()),
                    "raw_found": sum(provider_raw_found.values()),
                    "queries_in_flight": 0,
                    "unique_after_dedupe": len(candidate_pool),
                    "rerank_target": max(rerank_target_count, reranked_count),
                    "reranked_count": reranked_count,
                    "finalized_count": 0,
                    "percent": 92,
                    **last_scope_counts,
                }
            )
            candidate_pool = await run_blocking_stage(
                stage="rerank",
                stage_label="Rerank",
                message="Applying trained model from feedback.",
                percent=90,
                worker=lambda: sort_candidates(apply_learned_ranker(brief, candidate_pool), brief),
                reranked_count=reranked_count,
                finalized_count=0,
            )
            last_scope_counts = build_scope_progress_counts(candidate_pool)

        final_candidates = attach_registry_metadata(candidate_pool[:limit])
        last_scope_counts = build_scope_progress_counts(final_candidates)
        emit_progress(
                {
                    "stage": "rerank",
                    "stage_label": "Rerank",
                    "message": "Preparing verification shortlist.",
                    "queries_completed": sum(provider_query_completed.values()),
                    "queries_total": sum(provider_query_totals.values()),
                    "raw_found": sum(provider_raw_found.values()),
                    "queries_in_flight": 0,
                    "unique_after_dedupe": len(candidate_pool),
                    "reranked_count": reranked_count,
                    "finalized_count": 0,
                    "percent": 93 if not dry_run else 100,
                    **last_scope_counts,
                }
        )
        summary = self._build_summary(
            brief,
            provider_results,
            final_candidates,
            dry_run,
            excluded_seen_count=excluded_seen_count,
        )
        summary["pipeline_metrics"] = {
            "queries_completed": sum(provider_query_completed.values()),
            "queries_total": sum(provider_query_totals.values()),
            "raw_found": sum(provider_raw_found.values()),
            "unique_after_dedupe": len(candidate_pool),
            "rerank_target": max(rerank_target_count, reranked_count),
            "reranked_count": reranked_count,
            "finalized_count": len(final_candidates),
        }
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
