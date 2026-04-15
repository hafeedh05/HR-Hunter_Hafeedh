from __future__ import annotations

from dataclasses import dataclass

from hr_hunter_transformer.family_learning import family_learning_stats
from hr_hunter_transformer.models import CandidateEntity, SearchBrief


VERIFIED_THRESHOLD = 0.78
REVIEW_THRESHOLD = 0.45
VERIFIED_SCORE_FLOOR = 72.0
REVIEW_SCORE_FLOOR = 52.0
DENSE_ROLE_FAMILIES = {
    "data",
    "supply_chain",
    "finance",
    "marketing",
    "operations_process",
    "sales_business_development",
    "customer_service_success",
    "procurement_sourcing",
    "design_architecture",
    "retail_merchandising",
    "real_estate_property",
}


@dataclass(frozen=True)
class VerificationProfile:
    verified_threshold: float = VERIFIED_THRESHOLD
    review_threshold: float = REVIEW_THRESHOLD
    verified_score_floor: float = VERIFIED_SCORE_FLOOR
    review_score_floor: float = REVIEW_SCORE_FLOOR
    company_inference_floor: float = 0.55
    location_floor: float = 0.45
    require_company_for_verified: bool = False


VERIFICATION_PROFILES: dict[str, VerificationProfile] = {
    "executive": VerificationProfile(verified_threshold=0.75, verified_score_floor=69.5, company_inference_floor=0.52, location_floor=0.35, require_company_for_verified=True),
    "technical_ai": VerificationProfile(verified_threshold=0.76, verified_score_floor=69.0, company_inference_floor=0.5, location_floor=0.3),
    "supply_chain": VerificationProfile(verified_threshold=0.75, verified_score_floor=67.5, company_inference_floor=0.5, location_floor=0.42),
    "finance": VerificationProfile(verified_threshold=0.76, verified_score_floor=68.0, company_inference_floor=0.52, location_floor=0.42),
    "marketing": VerificationProfile(verified_threshold=0.75, verified_score_floor=67.0, company_inference_floor=0.48, location_floor=0.4),
    "design_architecture": VerificationProfile(verified_threshold=0.77, verified_score_floor=70.0, company_inference_floor=0.5, location_floor=0.42),
    "hr_talent": VerificationProfile(verified_threshold=0.77, verified_score_floor=68.0, company_inference_floor=0.48, location_floor=0.38),
    "admin_office_support": VerificationProfile(verified_threshold=0.78, verified_score_floor=68.0, company_inference_floor=0.46, location_floor=0.36),
    "data": VerificationProfile(verified_threshold=0.75, verified_score_floor=66.5, company_inference_floor=0.42, location_floor=0.32),
    "education_training": VerificationProfile(verified_threshold=0.76, verified_score_floor=67.0, company_inference_floor=0.42, location_floor=0.34),
    "engineering_non_it": VerificationProfile(verified_threshold=0.77, verified_score_floor=68.0, company_inference_floor=0.48, location_floor=0.36),
    "legal_compliance": VerificationProfile(verified_threshold=0.77, verified_score_floor=68.0, company_inference_floor=0.5, location_floor=0.36),
    "product_management": VerificationProfile(verified_threshold=0.77, verified_score_floor=68.0, company_inference_floor=0.48, location_floor=0.34),
    "project_program_management": VerificationProfile(verified_threshold=0.77, verified_score_floor=68.0, company_inference_floor=0.48, location_floor=0.36),
    "public_sector_government": VerificationProfile(verified_threshold=0.78, verified_score_floor=69.0, company_inference_floor=0.5, location_floor=0.34, require_company_for_verified=True),
    "research_development": VerificationProfile(verified_threshold=0.77, verified_score_floor=68.0, company_inference_floor=0.44, location_floor=0.32),
    "healthcare_medical": VerificationProfile(verified_threshold=0.75, verified_score_floor=66.5, company_inference_floor=0.4, location_floor=0.32),
}


def _family_verification_profile(role_family: str) -> VerificationProfile:
    profile = VERIFICATION_PROFILES.get(role_family, VerificationProfile())
    stats = family_learning_stats(role_family)
    verified_threshold = profile.verified_threshold
    review_threshold = profile.review_threshold
    verified_score_floor = profile.verified_score_floor
    review_score_floor = profile.review_score_floor
    if stats and stats.run_count >= 2:
        if stats.average_verified_rate < 0.12 and stats.average_review_rate > 0.7:
            verified_threshold -= 0.03
            verified_score_floor -= 2.0
        if stats.positive_feedback_rate > 0.55:
            verified_threshold -= 0.015
            review_threshold -= 0.01
        if stats.average_reject_rate > 0.24 or stats.negative_feedback_rate > 0.55:
            verified_threshold += 0.015
            review_threshold += 0.01
    return VerificationProfile(
        verified_threshold=max(0.68, min(0.82, round(verified_threshold, 3))),
        review_threshold=max(0.38, min(0.55, round(review_threshold, 3))),
        verified_score_floor=max(64.0, min(74.0, round(verified_score_floor, 2))),
        review_score_floor=max(48.0, min(56.0, round(review_score_floor, 2))),
        company_inference_floor=profile.company_inference_floor,
        location_floor=profile.location_floor,
        require_company_for_verified=profile.require_company_for_verified,
    )


def compute_verification_confidence(entity: CandidateEntity, brief: SearchBrief) -> float:
    current_role = min(1.0, 0.35 + min(0.45, entity.current_role_proof_count * 0.18)) if entity.current_role_proof_count else 0.0
    title = entity.title_match_score
    company = entity.company_match_score if entity.current_company_confirmed else entity.company_match_score * 0.6
    location = entity.location_match_score if entity.current_location_confirmed else entity.location_match_score * 0.7
    freshness = max((record.freshness_confidence for record in entity.evidence), default=0.45)
    if freshness <= 0:
        freshness = 0.45
    source = entity.source_trust_score
    confidence = (
        0.28 * current_role
        + 0.20 * title
        + 0.14 * company
        + 0.12 * location
        + 0.12 * freshness
        + 0.14 * source
    )
    return round(min(1.0, max(0.0, confidence)), 4)


def verify_candidate(entity: CandidateEntity, brief: SearchBrief) -> CandidateEntity:
    profile = _family_verification_profile(entity.role_family)
    entity.verification_confidence = compute_verification_confidence(entity, brief)
    diagnostics: list[str] = []
    max_company_confidence = max((record.company_confidence for record in entity.evidence), default=0.0)
    max_location_confidence = max((record.location_confidence for record in entity.evidence), default=0.0)
    if entity.title_match_score < 0.35:
        diagnostics.append("title_mismatch")
    if brief.countries and entity.location_match_score < 0.35:
        diagnostics.append("geo_mismatch")
    if entity.current_role_proof_count <= 0:
        diagnostics.append("weak_current_role_proof")
    if not entity.current_company_confirmed:
        diagnostics.append("missing_current_company_confirmation")
    company_ready_for_verified = entity.current_company_confirmed or max(entity.company_match_score, max_company_confidence) >= profile.company_inference_floor
    dense_role_verified = (
        entity.role_family in DENSE_ROLE_FAMILIES
        and entity.verification_confidence >= 0.58
        and entity.score >= max(65.5, profile.verified_score_floor - 2.5)
        and entity.title_match_score >= 0.65
        and max(entity.location_match_score, max_location_confidence) >= profile.location_floor
        and entity.current_role_proof_count >= 1
        and company_ready_for_verified
        and "title_mismatch" not in diagnostics
        and "geo_mismatch" not in diagnostics
    )
    if (
        entity.verification_confidence >= profile.verified_threshold
        and entity.score >= profile.verified_score_floor
        and (not profile.require_company_for_verified or company_ready_for_verified)
    ):
        entity.verification_status = "verified"
    elif dense_role_verified:
        entity.verification_status = "verified"
        if "missing_current_company_confirmation" in diagnostics:
            diagnostics.remove("missing_current_company_confirmation")
            diagnostics.append("dense_role_company_inferred")
    elif entity.verification_confidence >= profile.review_threshold and entity.score >= profile.review_score_floor and "title_mismatch" not in diagnostics:
        entity.verification_status = "review"
    else:
        entity.verification_status = "reject"
    entity.diagnostics = diagnostics
    return entity


def verify_candidates(candidates: list[CandidateEntity], brief: SearchBrief) -> list[CandidateEntity]:
    return [verify_candidate(candidate, brief) for candidate in candidates]
