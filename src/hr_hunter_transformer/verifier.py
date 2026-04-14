from __future__ import annotations

from hr_hunter_transformer.models import CandidateEntity, SearchBrief


VERIFIED_THRESHOLD = 0.78
REVIEW_THRESHOLD = 0.45
VERIFIED_SCORE_FLOOR = 72.0
REVIEW_SCORE_FLOOR = 52.0
DENSE_ROLE_FAMILIES = {
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
    dense_role_verified = (
        entity.role_family in DENSE_ROLE_FAMILIES
        and entity.verification_confidence >= 0.58
        and entity.score >= 65.5
        and entity.title_match_score >= 0.65
        and max(entity.location_match_score, max_location_confidence) >= 0.45
        and entity.current_role_proof_count >= 1
        and (entity.current_company_confirmed or max(entity.company_match_score, max_company_confidence) >= 0.55)
        and "title_mismatch" not in diagnostics
        and "geo_mismatch" not in diagnostics
    )
    if entity.verification_confidence >= VERIFIED_THRESHOLD and entity.score >= VERIFIED_SCORE_FLOOR:
        entity.verification_status = "verified"
    elif dense_role_verified:
        entity.verification_status = "verified"
        if "missing_current_company_confirmation" in diagnostics:
            diagnostics.remove("missing_current_company_confirmation")
            diagnostics.append("dense_role_company_inferred")
    elif entity.verification_confidence >= REVIEW_THRESHOLD and entity.score >= REVIEW_SCORE_FLOOR and "title_mismatch" not in diagnostics:
        entity.verification_status = "review"
    else:
        entity.verification_status = "reject"
    entity.diagnostics = diagnostics
    return entity


def verify_candidates(candidates: list[CandidateEntity], brief: SearchBrief) -> list[CandidateEntity]:
    return [verify_candidate(candidate, brief) for candidate in candidates]
