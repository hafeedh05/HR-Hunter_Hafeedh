from __future__ import annotations

from hr_hunter.briefing import normalize_text
from hr_hunter.models import CandidateProfile, SearchBrief

STATUS_RANK = {"verified": 0, "review": 1, "reject": 2}
PRECISE_MARKET_BUCKETS = {
    "within_radius",
    "within_expanded_radius",
    "priority_target_location",
    "named_target_location",
    "secondary_target_location",
    "named_profile_location",
    "geo_distance",
    "current_evidence",
}
MARKET_MATCH_BUCKETS = {
    *PRECISE_MARKET_BUCKETS,
    "country_only",
    "text_aligned",
    "profile_text",
}
EXECUTIVE_TITLE_HINTS = ("ceo", "chief", "president", "managing director", "vice president", "vp")
PROFILE_SOURCE_PATH_HINTS = (
    "/in/",
    "/bio",
    "/speaker/",
    "/speakers/",
    "/profile",
    "/people/",
    "/person/",
    "/staff/",
    "/team/",
    "/our-team/",
    "/our-people/",
    "/leadership/",
    "/management/",
)


def _candidate_feature_score(
    candidate: CandidateProfile,
    *,
    feature_key: str,
    attribute_name: str,
) -> float:
    feature_scores = getattr(candidate, "feature_scores", None)
    if isinstance(feature_scores, dict) and feature_key in feature_scores:
        raw_value = feature_scores.get(feature_key)
        try:
            return float(raw_value or 0.0)
        except (TypeError, ValueError):
            pass
    try:
        return float(getattr(candidate, attribute_name, 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def is_title_market_priority_brief(brief: SearchBrief | None) -> bool:
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
        and not any(hint in normalized_role_scope for hint in EXECUTIVE_TITLE_HINTS)
    )


def candidate_market_bucket_rank(candidate: CandidateProfile) -> int:
    bucket = str(getattr(candidate, "location_precision_bucket", "") or "")
    if bucket in PRECISE_MARKET_BUCKETS:
        return 0
    if bucket == "country_only":
        return 1
    if bucket in {"text_aligned", "profile_text"}:
        return 2
    if bucket == "unknown_location":
        return 3
    if bucket == "outside_target_area":
        return 4
    return 2


def candidate_is_market_match(candidate: CandidateProfile) -> bool:
    bucket = str(getattr(candidate, "location_precision_bucket", "") or "")
    return bool(getattr(candidate, "location_aligned", False)) or bucket in MARKET_MATCH_BUCKETS


def candidate_is_precise_market_match(candidate: CandidateProfile) -> bool:
    bucket = str(getattr(candidate, "location_precision_bucket", "") or "")
    return bucket in PRECISE_MARKET_BUCKETS


def candidate_has_priority_fit(candidate: CandidateProfile) -> bool:
    parser_confidence = float(getattr(candidate, "parser_confidence", 0.0) or 0.0)
    anchor_supported = candidate_has_fit_anchor(candidate)
    return bool(
        getattr(candidate, "current_title_match", False)
        and candidate_is_market_match(candidate)
        and parser_confidence >= 0.35
        and anchor_supported
    )


def candidate_has_fit_anchor(candidate: CandidateProfile) -> bool:
    company_match_score = _candidate_feature_score(
        candidate,
        feature_key="company_match",
        attribute_name="company_match_score",
    )
    industry_fit_score = _candidate_feature_score(
        candidate,
        feature_key="industry_fit",
        attribute_name="industry_fit_score",
    )
    skill_overlap_score = _candidate_feature_score(
        candidate,
        feature_key="skill_overlap",
        attribute_name="skill_overlap_score",
    )
    semantic_similarity_score = _candidate_feature_score(
        candidate,
        feature_key="semantic_similarity",
        attribute_name="semantic_similarity_score",
    )
    evidence_quality_score = _candidate_feature_score(
        candidate,
        feature_key="evidence_quality",
        attribute_name="evidence_quality_score",
    )
    normalized_title = normalize_text(str(getattr(candidate, "current_title", "") or ""))
    executive_like = bool(
        getattr(candidate, "matched_title_family", "") == "executive"
        or any(hint in normalized_title for hint in EXECUTIVE_TITLE_HINTS)
    )
    if executive_like:
        return bool(
            company_match_score >= 0.28
            or industry_fit_score >= 0.35
            or (skill_overlap_score >= 0.18 and semantic_similarity_score >= 0.12)
        )
    return bool(
        company_match_score >= 0.28
        or industry_fit_score >= 0.35
        or skill_overlap_score >= 0.15
        or (semantic_similarity_score >= 0.12 and evidence_quality_score >= 0.45)
    )


def candidate_source_identity_rank(candidate: CandidateProfile) -> int:
    source_quality_score = float(getattr(candidate, "source_quality_score", 0.0) or 0.0)
    source_url = str(
        getattr(candidate, "linkedin_url", "")
        or getattr(candidate, "source_url", "")
        or ""
    ).lower()
    if "linkedin.com/in/" in source_url:
        return 0
    if any(hint in source_url for hint in PROFILE_SOURCE_PATH_HINTS):
        return 0
    if source_quality_score >= 0.45:
        return 1
    if source_url or source_quality_score >= 0.2:
        return 2
    return 3


def _candidate_priority_bucket(
    candidate: CandidateProfile,
    *,
    company_required: bool,
    title_market_priority: bool,
) -> int:
    title_match = bool(getattr(candidate, "current_title_match", False))
    current_company_match = bool(getattr(candidate, "current_target_company_match", False))
    history_company_match = bool(getattr(candidate, "target_company_history_match", False))
    precise_market_match = candidate_is_precise_market_match(candidate)
    market_match = candidate_is_market_match(candidate)
    anchor_supported = candidate_has_fit_anchor(candidate)
    strong_function_fit = float(getattr(candidate, "current_function_fit", 0.0) or 0.0) >= 0.6
    strong_skill_fit = _candidate_feature_score(
        candidate,
        feature_key="skill_overlap",
        attribute_name="skill_overlap_score",
    ) >= 0.18
    strong_evidence = (
        float(getattr(candidate, "parser_confidence", 0.0) or 0.0) >= 0.45
        or float(getattr(candidate, "evidence_quality_score", 0.0) or 0.0) >= 0.35
        or int(getattr(candidate, "current_role_proof_count", 0) or 0) > 0
    )

    if company_required:
        if current_company_match and title_match and precise_market_match:
            return 0
        if current_company_match and title_match and market_match:
            return 1
        if title_match and precise_market_match and anchor_supported:
            return 2
        if title_match and market_match and anchor_supported:
            return 3
        if current_company_match and precise_market_match:
            return 4
        if title_match and precise_market_match:
            return 5
        if title_match and market_match:
            return 6
        if current_company_match and title_match:
            return 7
        if history_company_match and title_match and market_match:
            return 8
        if title_match and strong_function_fit and anchor_supported:
            return 9
        if precise_market_match and strong_function_fit and strong_evidence:
            return 10
        if market_match and strong_function_fit and strong_evidence:
            return 11
        if title_match:
            return 12
        if market_match and strong_function_fit:
            return 13
        return 14

    if title_market_priority:
        if title_match and precise_market_match and anchor_supported:
            return 0
        if title_match and market_match and anchor_supported:
            return 1
        if title_match and precise_market_match:
            return 2
        if title_match and market_match:
            return 3
        if title_match:
            return 4
        if precise_market_match and strong_function_fit and strong_skill_fit:
            return 5
        if market_match and strong_function_fit:
            return 6
        return 14

    if title_match and precise_market_match and anchor_supported:
        return 0
    if title_match and market_match and anchor_supported:
        return 1
    if title_match and strong_function_fit and anchor_supported:
        return 2
    if precise_market_match and strong_function_fit and strong_skill_fit:
        return 3
    if market_match and strong_function_fit and strong_skill_fit:
        return 4
    if current_company_match and title_match:
        return 5
    if history_company_match and title_match and market_match:
        return 6
    if title_match and precise_market_match:
        return 7
    if title_match and market_match:
        return 8
    if title_match and strong_function_fit:
        return 9
    if precise_market_match and strong_evidence:
        return 10
    if market_match and strong_function_fit:
        return 11
    if title_match:
        return 12
    if market_match:
        return 13
    return 14


def candidate_priority_sort_tuple(
    candidate: CandidateProfile,
    brief: SearchBrief | None = None,
    *,
    phase: str = "final",
    company_required: bool | None = None,
) -> tuple[object, ...]:
    phase_name = str(phase or "final").strip().lower() or "final"
    company_required_flag = (
        bool(company_required)
        if company_required is not None
        else bool(getattr(brief, "company_targets", []))
    )
    title_market_priority = is_title_market_priority_brief(brief)
    bucket = _candidate_priority_bucket(
        candidate,
        company_required=company_required_flag,
        title_market_priority=title_market_priority,
    )
    status_rank = STATUS_RANK.get(str(getattr(candidate, "verification_status", "") or "").lower(), 9)
    current_employment_confirmed = 0 if getattr(candidate, "current_employment_confirmed", False) else 1
    precise_location_confirmed = 0 if getattr(candidate, "precise_location_confirmed", False) else 1
    current_location_confirmed = 0 if getattr(candidate, "current_location_confirmed", False) else 1
    current_company_confirmed = 0 if getattr(candidate, "current_company_confirmed", False) else 1
    current_title_confirmed = 0 if getattr(candidate, "current_title_confirmed", False) else 1
    current_role_proof_count = int(getattr(candidate, "current_role_proof_count", 0) or 0)
    current_function_fit = float(getattr(candidate, "current_function_fit", 0.0) or 0.0)
    skill_overlap_score = _candidate_feature_score(
        candidate,
        feature_key="skill_overlap",
        attribute_name="skill_overlap_score",
    )
    industry_fit_score = _candidate_feature_score(
        candidate,
        feature_key="industry_fit",
        attribute_name="industry_fit_score",
    )
    company_match_score = _candidate_feature_score(
        candidate,
        feature_key="company_match",
        attribute_name="company_match_score",
    )
    location_match_score = _candidate_feature_score(
        candidate,
        feature_key="location_match",
        attribute_name="location_match_score",
    )
    evidence_quality_score = _candidate_feature_score(
        candidate,
        feature_key="evidence_quality",
        attribute_name="evidence_quality_score",
    )
    source_quality_score = float(getattr(candidate, "source_quality_score", 0.0) or 0.0)
    parser_confidence = float(getattr(candidate, "parser_confidence", 0.0) or 0.0)
    score = float(getattr(candidate, "score", 0.0) or 0.0)
    reranker_score = float(getattr(candidate, "reranker_score", 0.0) or 0.0)
    market_rank = candidate_market_bucket_rank(candidate)
    source_identity_rank = candidate_source_identity_rank(candidate)
    name_key = str(getattr(candidate, "full_name", "") or "").lower()

    if phase_name == "rerank":
        return (
            bucket,
            market_rank,
            -industry_fit_score,
            -company_match_score,
            -current_function_fit,
            -skill_overlap_score,
            source_identity_rank,
            -source_quality_score,
            -location_match_score,
            -parser_confidence,
            -evidence_quality_score,
            status_rank,
            -score,
            name_key,
        )
    if phase_name == "verification":
        return (
            bucket,
            -industry_fit_score,
            -company_match_score,
            -current_function_fit,
            -skill_overlap_score,
            source_identity_rank,
            -source_quality_score,
            current_employment_confirmed,
            precise_location_confirmed,
            current_location_confirmed,
            current_company_confirmed,
            current_title_confirmed,
            -current_role_proof_count,
            market_rank,
            -evidence_quality_score,
            -parser_confidence,
            status_rank,
            -score,
            name_key,
        )
    return (
        bucket,
        status_rank,
        -industry_fit_score,
        -company_match_score,
        -current_function_fit,
        -skill_overlap_score,
        source_identity_rank,
        -source_quality_score,
        current_employment_confirmed,
        precise_location_confirmed,
        current_location_confirmed,
        current_company_confirmed,
        current_title_confirmed,
        -current_role_proof_count,
        market_rank,
        -location_match_score,
        -evidence_quality_score,
        -parser_confidence,
        -score,
        -reranker_score,
        name_key,
    )
