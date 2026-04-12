from __future__ import annotations

from typing import List

from hr_hunter.briefing import normalize_text
from hr_hunter.features import build_candidate_features
from hr_hunter.models import CandidateProfile, SearchBrief
from hr_hunter.ranker import rank_candidate, status_from_score


def score_candidate(candidate: CandidateProfile, brief: SearchBrief) -> CandidateProfile:
    features = build_candidate_features(candidate, brief)
    rank_result = rank_candidate(features, brief)

    candidate.matched_titles = list(features.matched_titles)
    candidate.matched_companies = list(features.matched_companies)
    candidate.current_target_company_match = features.current_target_company_match
    candidate.target_company_history_match = features.target_company_history_match
    candidate.current_title_match = features.current_title_match
    candidate.industry_aligned = features.industry_aligned
    candidate.location_aligned = features.location_aligned
    candidate.matched_title_family = features.matched_title_family
    candidate.location_precision_bucket = features.location_bucket
    candidate.years_experience = features.years_experience
    candidate.years_experience_gap = features.years_experience_gap

    candidate.feature_scores = dict(features.feature_scores)
    candidate.anchor_scores = dict(rank_result.anchor_scores)
    candidate.title_similarity_score = features.feature_scores.get("title_similarity", 0.0)
    candidate.company_match_score = features.feature_scores.get("company_match", 0.0)
    candidate.location_match_score = features.feature_scores.get("location_match", 0.0)
    candidate.skill_overlap_score = features.feature_scores.get("skill_overlap", 0.0)
    candidate.industry_fit_score = features.feature_scores.get("industry_fit", 0.0)
    candidate.years_fit_score = features.feature_scores.get("years_fit", 0.0)
    candidate.current_function_fit = features.feature_scores.get("current_function_fit", 0.0)
    candidate.current_fmcg_fit = candidate.industry_fit_score
    candidate.parser_confidence = features.feature_scores.get("parser_confidence", 0.0)
    candidate.evidence_quality_score = features.feature_scores.get("evidence_quality", 0.0)
    candidate.semantic_similarity_score = features.feature_scores.get("semantic_similarity", 0.0)
    candidate.reranker_score = 0.0
    candidate.ranking_model_version = rank_result.ranking_model_version

    candidate.score = rank_result.score
    candidate.verification_status = status_from_score(candidate.score)
    candidate.cap_reasons = list(rank_result.cap_reasons)
    candidate.disqualifier_reasons = list(rank_result.disqualifier_reasons)
    candidate.verification_notes = list(dict.fromkeys([*features.notes, *rank_result.notes]))
    return candidate


def _is_title_market_priority_brief(brief: SearchBrief | None) -> bool:
    if brief is None:
        return False
    normalized_role_scope = " ".join(
        normalize_text(str(value))
        for value in [brief.role_title, *brief.titles]
        if normalize_text(str(value))
    )
    normalized_locations = {
        normalize_text(str(value))
        for value in [*brief.location_targets, brief.geography.location_name, brief.geography.country]
        if normalize_text(str(value))
    }
    return (
        not brief.company_targets
        and bool(brief.titles)
        and len(brief.titles) <= 3
        and len(normalized_locations) <= 4
        and not any(
            hint in normalized_role_scope
            for hint in ("ceo", "chief", "president", "managing director", "vice president", "vp")
        )
    )


def _market_bucket_rank(candidate: CandidateProfile) -> int:
    bucket = str(getattr(candidate, "location_precision_bucket", "") or "")
    if bucket in {
        "within_radius",
        "within_expanded_radius",
        "priority_target_location",
        "named_target_location",
        "secondary_target_location",
        "named_profile_location",
    }:
        return 0
    if bucket == "country_only":
        return 1
    if bucket == "unknown_location":
        return 3
    if bucket == "outside_target_area":
        return 4
    return 2


def _title_market_bucket(candidate: CandidateProfile) -> int:
    title_match = bool(getattr(candidate, "current_title_match", False))
    market_rank = _market_bucket_rank(candidate)
    if title_match and market_rank == 0:
        return 0
    if title_match and market_rank == 1:
        return 1
    if title_match:
        return 2
    if market_rank <= 1:
        return 3
    return 9


def sort_candidates(candidates: List[CandidateProfile], brief: SearchBrief | None = None) -> List[CandidateProfile]:
    status_rank = {"verified": 0, "review": 1, "reject": 2}
    if _is_title_market_priority_brief(brief):
        return sorted(
            candidates,
            key=lambda candidate: (
                _title_market_bucket(candidate),
                status_rank.get(candidate.verification_status, 9),
                -candidate.score,
                candidate.full_name.lower(),
            ),
        )
    return sorted(
        candidates,
        key=lambda candidate: (
            status_rank.get(candidate.verification_status, 9),
            -candidate.score,
            candidate.full_name.lower(),
        ),
    )
