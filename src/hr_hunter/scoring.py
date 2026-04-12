from __future__ import annotations

from typing import List

from hr_hunter.candidate_order import candidate_priority_sort_tuple, is_title_market_priority_brief
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
    return is_title_market_priority_brief(brief)


def sort_candidates(candidates: List[CandidateProfile], brief: SearchBrief | None = None) -> List[CandidateProfile]:
    return sorted(
        candidates,
        key=lambda candidate: candidate_priority_sort_tuple(candidate, brief, phase="final"),
    )
