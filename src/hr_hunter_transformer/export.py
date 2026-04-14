from __future__ import annotations

from collections import Counter
from typing import Any

from hr_hunter_transformer.models import CandidateEntity, PipelineResult, QueryPlan, SearchBrief


def candidate_export_payload(candidate: CandidateEntity, ordinal: int) -> dict[str, Any]:
    top_evidence = candidate.evidence[:3]
    top_source = top_evidence[0] if top_evidence else None
    return {
        "rank_index": ordinal,
        "full_name": candidate.full_name,
        "current_title": candidate.current_title,
        "current_company": candidate.current_company,
        "current_location": candidate.current_location,
        "role_family": candidate.role_family,
        "final_score": round(candidate.score, 2),
        "verification_status": candidate.verification_status,
        "verification_confidence": round(candidate.verification_confidence, 4),
        "semantic_fit": round(candidate.semantic_fit, 4),
        "title_match": round(candidate.title_match_score, 4),
        "skill_match": round(candidate.skill_match_score, 4),
        "company_match": round(candidate.company_match_score, 4),
        "location_match": round(candidate.location_match_score, 4),
        "seniority_match": round(candidate.seniority_match_score, 4),
        "currentness": round(candidate.currentness_score, 4),
        "source_trust": round(candidate.source_trust_score, 4),
        "verification_notes": list(candidate.notes),
        "diagnostics": list(candidate.diagnostics),
        "top_source_domain": top_source.source_domain if top_source else "",
        "top_source_url": top_source.source_url if top_source else "",
    }


def build_run_summary(brief: SearchBrief, plan: QueryPlan, result: PipelineResult) -> dict[str, Any]:
    counts = Counter(candidate.verification_status for candidate in result.candidates)
    locations = Counter(candidate.current_location or "Unknown Location" for candidate in result.candidates)
    diagnostics = Counter()
    for candidate in result.candidates:
        diagnostics.update(candidate.diagnostics)
    return {
        "requested_candidate_limit": int(brief.target_count),
        "returned_candidate_count": int(len(result.candidates)),
        "candidate_count": int(len(result.candidates)),
        "verified_count": int(counts.get("verified", 0)),
        "review_count": int(counts.get("review", 0)),
        "reject_count": int(counts.get("reject", 0)),
        "role_family": plan.role_understanding.role_family,
        "role_subfamily": plan.role_understanding.role_subfamily,
        "execution_backend": "transformer_v2",
        "query_count": int(result.metrics.queries_completed or len(plan.queries)),
        "raw_found": int(result.metrics.raw_found),
        "unique_after_dedupe": int(result.metrics.unique_candidates),
        "quality_diagnostics": {
            "issues": [{"label": label, "count": count} for label, count in diagnostics.most_common()],
        },
        "top_locations": [{"location": name, "count": count} for name, count in locations.most_common(8)],
        "pipeline_metrics": {
            "queries_total": int(result.metrics.queries_planned or len(plan.queries)),
            "queries_completed": int(result.metrics.queries_completed or len(plan.queries)),
            "raw_found": int(result.metrics.raw_found),
            "unique_after_dedupe": int(result.metrics.unique_candidates),
            "reranked_count": int(result.metrics.unique_candidates),
            "rerank_target": int(min(brief.target_count, result.metrics.unique_candidates)),
            "finalized_count": int(len(result.candidates)),
        },
        "role_understanding": {
            "normalized_title": plan.role_understanding.normalized_title,
            "role_family": plan.role_understanding.role_family,
            "role_subfamily": plan.role_understanding.role_subfamily,
            "family_confidence": float(plan.role_understanding.family_confidence or 0.0),
            "title_variants": list(plan.role_understanding.title_variants or []),
            "adjacent_titles": list(plan.role_understanding.adjacent_titles or []),
            "inferred_skills": list(plan.role_understanding.inferred_skills or []),
            "seniority_hint": plan.role_understanding.seniority_hint,
            "search_complexity": plan.role_understanding.search_complexity,
        },
        "query_plan": [
            {
                "ordinal": index,
                "query_text": task.query_text,
                "query_type": task.query_type,
                "source_pack": task.source_pack,
                "page_budget": int(task.page_budget or 1),
            }
            for index, task in enumerate(plan.queries, start=1)
        ],
        "telemetry_events": list(result.telemetry_events or []),
    }
