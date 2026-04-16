from __future__ import annotations

from dataclasses import dataclass
import re

from hr_hunter_transformer.company_quality import company_quality_score, looks_like_bad_company
from hr_hunter_transformer.family_learning import family_learning_stats
from hr_hunter_transformer.models import CandidateEntity, SearchBrief
from hr_hunter_transformer.role_profiles import normalize_text
from hr_hunter_transformer.title_matching import (
    adjacent_title_gap,
    best_requested_title_coverage,
    best_requested_title_precision,
    canonical_title_coverage,
    canonical_title_precision,
    blended_title_precision,
)


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
STRICT_TITLE_PRECISION_FAMILIES = {"design_architecture", "executive"}
COMPANY_SUFFIX_TOKENS = {
    "co",
    "company",
    "corp",
    "corporation",
    "group",
    "holdings",
    "inc",
    "limited",
    "llc",
    "ltd",
    "plc",
}
GENERIC_COMPANY_LITERALS = {
    "board",
    "confidential",
    "growth",
    "leadership",
    "management",
    "middle",
    "strategy",
    "transform",
}
LOCATION_LIKE_COMPANY_LITERALS = {
    "abu dhabi",
    "bahrain",
    "doha",
    "dubai",
    "jeddah",
    "kuwait",
    "malaysia",
    "mena",
    "middle east",
    "qatar",
    "riyadh",
    "saudi arabia",
    "uae",
    "united arab emirates",
}

EXECUTIVE_TITLE_MARKERS = (
    "ceo",
    "chief executive officer",
    "president",
    "managing director",
    "general manager",
    "group ceo",
    "regional ceo",
    "brand president",
    "country manager",
    "vice president",
)


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


def _executive_company_ready(entity: CandidateEntity, max_company_confidence: float) -> bool:
    cleaned_company = str(entity.current_company or "").strip()
    normalized_company = normalize_text(cleaned_company)
    company_quality = max(entity.company_quality_score, company_quality_score(cleaned_company, entity.current_title, entity.role_family))
    if not cleaned_company or looks_like_bad_company(cleaned_company, entity.current_title):
        return False
    company_tokens = [token for token in re.split(r"[^a-z0-9]+", normalized_company) if token]
    if normalized_company in GENERIC_COMPANY_LITERALS or normalized_company in LOCATION_LIKE_COMPANY_LITERALS:
        return False
    if any(
        phrase in normalized_company
        for phrase in (
            "work experience",
            "find jobs",
            "chairman and acting ceo",
            "vice chairman",
            "leading strategy",
        )
    ):
        return False
    if len(company_tokens) >= 5 and not any(token in COMPANY_SUFFIX_TOKENS for token in company_tokens):
        return False
    strong_company_signal = max(
        entity.company_match_score,
        entity.company_consensus_score,
        max_company_confidence,
    )
    return bool(
        company_quality >= 0.52
        and
        strong_company_signal >= 0.45
        and (
            (entity.company_support_count >= 2 and entity.company_consensus_score >= 0.6)
            or (entity.current_company_confirmed and max_company_confidence >= 0.72)
            or entity.company_match_score >= 0.78
        )
    )


def _looks_like_executive_title(title: str) -> bool:
    normalized_title = normalize_text(title)
    return any(marker in normalized_title for marker in EXECUTIVE_TITLE_MARKERS)


def compute_verification_confidence(entity: CandidateEntity, brief: SearchBrief) -> float:
    current_role = min(1.0, 0.35 + min(0.45, entity.current_role_proof_count * 0.18)) if entity.current_role_proof_count else 0.0
    title = max(entity.title_match_score, blended_title_precision(entity.current_title, brief))
    company_quality = max(entity.company_quality_score, company_quality_score(entity.current_company, entity.current_title, entity.role_family))
    company = entity.company_match_score if entity.current_company_confirmed else max(entity.company_match_score * 0.7, entity.company_consensus_score * 0.55)
    company *= 0.8 + (0.2 * company_quality)
    location = entity.location_match_score if entity.current_location_confirmed else entity.location_match_score * 0.7
    freshness = max((record.freshness_confidence for record in entity.evidence), default=0.45)
    if freshness <= 0:
        freshness = 0.45
    source = entity.source_trust_score
    company_consensus = entity.company_consensus_score
    industry = entity.industry_match_score
    conflict_penalty = min(0.18, entity.evidence_conflict_score * 0.18)
    confidence = (
        0.28 * current_role
        + 0.20 * title
        + 0.14 * company
        + 0.12 * location
        + 0.12 * freshness
        + 0.14 * source
        + 0.05 * company_consensus
        + 0.04 * industry
        - (conflict_penalty * 0.7)
    )
    if entity.role_family in DENSE_ROLE_FAMILIES and entity.current_role_proof_count >= 2:
        confidence += 0.02 * min(1.0, entity.skill_match_score + entity.location_match_score)
    return round(min(1.0, max(0.0, confidence)), 4)


def verify_candidate(entity: CandidateEntity, brief: SearchBrief) -> CandidateEntity:
    profile = _family_verification_profile(entity.role_family)
    entity.verification_confidence = compute_verification_confidence(entity, brief)
    diagnostics: list[str] = []
    max_company_confidence = max((record.company_confidence for record in entity.evidence), default=0.0)
    max_location_confidence = max((record.location_confidence for record in entity.evidence), default=0.0)
    requested_title_precision = best_requested_title_precision(entity.current_title, brief)
    requested_title_coverage = best_requested_title_coverage(entity.current_title, brief)
    primary_title_signal = canonical_title_precision(entity.current_title, brief)
    canonical_coverage = canonical_title_coverage(entity.current_title, brief)
    title_gap = adjacent_title_gap(entity.current_title, brief)
    strong_requested_title = requested_title_precision >= 0.84 and requested_title_coverage >= 0.84
    company_quality = max(entity.company_quality_score, company_quality_score(entity.current_company, entity.current_title, entity.role_family))
    if entity.title_match_score < 0.35:
        diagnostics.append("title_mismatch")
    if entity.role_family in STRICT_TITLE_PRECISION_FAMILIES and requested_title_precision < 0.34:
        diagnostics.append("adjacent_title_leakage")
    if brief.countries and entity.location_match_score < 0.35:
        diagnostics.append("geo_mismatch")
    if entity.evidence_conflict_score >= 0.5:
        diagnostics.append("identity_conflict")
    if entity.current_role_proof_count <= 0:
        diagnostics.append("weak_current_role_proof")
    if not entity.current_company_confirmed:
        diagnostics.append("missing_current_company_confirmation")
    if company_quality < 0.38:
        diagnostics.append("weak_company_identity")
    company_ready_for_verified = entity.current_company_confirmed or max(
        entity.company_match_score,
        max_company_confidence,
        entity.company_consensus_score,
    ) >= profile.company_inference_floor
    company_ready_for_verified = company_ready_for_verified and company_quality >= max(0.35, profile.company_inference_floor - 0.08)
    executive_ready_for_verified = True
    executive_verified_override = False
    architecture_ready_for_verified = True
    if entity.role_family == "executive":
        brief_company_signal = 1.0 if entity.company_match else 0.75 if entity.peer_company_match else 0.0
        brief_relevance_signal = max(
            brief_company_signal,
            entity.industry_match_score,
            min(1.0, 0.75 * entity.skill_match_score + 0.25 * entity.location_match_score),
        )
        canonical_ready = (
            primary_title_signal >= 0.42
            and brief_relevance_signal >= 0.4
            and max(entity.location_match_score, max_location_confidence) >= 0.35
        )
        adjacent_ready = (
            requested_title_precision >= 0.72
            and title_gap >= 0.32
            and entity.current_role_proof_count >= 2
            and max(entity.location_match_score, max_location_confidence) >= 0.45
            and brief_relevance_signal >= 0.62
            and (
                entity.company_match
                or entity.peer_company_match
                or (entity.industry_match_score >= 0.34 and entity.skill_match_score >= 0.5)
            )
        )
        executive_ready_for_verified = (
            requested_title_precision >= 0.42
            and entity.current_role_proof_count >= 1
            and _executive_company_ready(entity, max_company_confidence)
            and (canonical_ready or adjacent_ready)
        )
        exact_company_exec_fast_track = (
            entity.company_match
            and _looks_like_executive_title(entity.current_title)
            and entity.current_role_proof_count >= 1
            and company_quality >= 0.4
            and max(entity.location_match_score, max_location_confidence) >= 0.0
            and entity.score >= max(54.0, profile.verified_score_floor - 15.0)
            and entity.verification_confidence >= max(0.38, profile.review_threshold - 0.05)
            and (
                primary_title_signal >= 0.34
                or requested_title_precision >= 0.1
                or requested_title_coverage >= 0.4
            )
        )
        peer_company_exec_fast_track = (
            entity.peer_company_match
            and _looks_like_executive_title(entity.current_title)
            and entity.current_role_proof_count >= 1
            and company_quality >= 0.4
            and max(entity.location_match_score, max_location_confidence) >= 0.0
            and entity.score >= max(55.0, profile.verified_score_floor - 14.0)
            and entity.verification_confidence >= max(0.4, profile.review_threshold - 0.04)
            and (
                requested_title_precision >= 0.12
                or requested_title_coverage >= 0.45
                or primary_title_signal >= 0.34
            )
        )
        executive_ready_for_verified = executive_ready_for_verified or exact_company_exec_fast_track or peer_company_exec_fast_track
        executive_verified_override = exact_company_exec_fast_track or peer_company_exec_fast_track
        if not executive_ready_for_verified:
            diagnostics.append("weak_company_or_industry_signals")
    elif entity.role_family == "design_architecture":
        adjacent_architecture = title_gap >= 0.22 and canonical_coverage < 0.72 and not strong_requested_title
        architecture_relevance = max(
            entity.skill_match_score,
            entity.industry_match_score,
            min(
                1.0,
                0.42 * entity.skill_match_score
                + 0.18 * entity.industry_match_score
                + 0.2 * max(entity.company_match_score, entity.company_consensus_score)
                + 0.2 * entity.currentness_score,
            ),
            min(1.0, 0.7 * entity.skill_match_score + 0.3 * entity.semantic_fit),
        )
        strong_requested_architecture = (
            strong_requested_title
            and company_ready_for_verified
            and company_quality >= 0.42
            and entity.current_role_proof_count >= 1
            and max(entity.location_match_score, max_location_confidence) >= 0.35
        )
        if adjacent_architecture and architecture_relevance < 0.28 and not strong_requested_architecture:
            diagnostics.append("generic_title_match")
        architecture_ready_for_verified = (
            requested_title_precision >= 0.42
            and max(requested_title_coverage, canonical_coverage) >= 0.58
            and entity.current_role_proof_count >= 1
            and company_quality >= 0.42
            and (strong_requested_architecture or not adjacent_architecture or architecture_relevance >= 0.28)
        )
    dense_role_verified = (
        entity.role_family in DENSE_ROLE_FAMILIES
        and entity.verification_confidence >= 0.58
        and entity.score >= max(64.5, profile.verified_score_floor - 3.0)
        and entity.title_match_score >= 0.65
        and company_quality >= 0.4
        and architecture_ready_for_verified
        and max(entity.location_match_score, max_location_confidence) >= profile.location_floor
        and entity.current_role_proof_count >= 1
        and company_ready_for_verified
        and "title_mismatch" not in diagnostics
        and "adjacent_title_leakage" not in diagnostics
        and "geo_mismatch" not in diagnostics
        and "identity_conflict" not in diagnostics
    )
    if (
        (
            entity.verification_confidence >= profile.verified_threshold
            and entity.score >= profile.verified_score_floor
            and company_ready_for_verified
            and executive_ready_for_verified
            and architecture_ready_for_verified
        )
        or (
            executive_verified_override
            and company_ready_for_verified
            and architecture_ready_for_verified
        )
    ) and "adjacent_title_leakage" not in diagnostics and "identity_conflict" not in diagnostics:
        entity.verification_status = "verified"
    elif dense_role_verified:
        entity.verification_status = "verified"
        if "missing_current_company_confirmation" in diagnostics:
            diagnostics.remove("missing_current_company_confirmation")
            diagnostics.append("dense_role_company_inferred")
    elif (
        entity.verification_confidence >= profile.review_threshold
        and entity.score >= profile.review_score_floor
        and "title_mismatch" not in diagnostics
        and entity.evidence_conflict_score < 0.78
    ):
        entity.verification_status = "review"
    else:
        entity.verification_status = "reject"
    entity.diagnostics = diagnostics
    return entity


def verify_candidates(candidates: list[CandidateEntity], brief: SearchBrief) -> list[CandidateEntity]:
    return [verify_candidate(candidate, brief) for candidate in candidates]
