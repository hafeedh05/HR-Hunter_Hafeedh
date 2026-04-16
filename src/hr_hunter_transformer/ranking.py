from __future__ import annotations

from hr_hunter_transformer.company_quality import company_quality_score
from hr_hunter_transformer.calibration import TransformerCalibrationModel, candidate_feature_map
from hr_hunter_transformer.models import CandidateEntity, SearchBrief
from hr_hunter_transformer.role_profiles import TECHNICAL_SOURCES, infer_role_family, normalize_text
from hr_hunter_transformer.title_matching import (
    adjacent_title_gap,
    best_requested_title_coverage,
    blended_title_precision,
    canonical_title_coverage,
    canonical_title_precision,
)
from hr_hunter_transformer.transformer_ranker import TransformerScorer


BASE_SCORE_WEIGHTS: dict[str, float] = {
    "semantic": 0.24,
    "title": 0.18,
    "skill": 0.12,
    "company": 0.08,
    "location": 0.08,
    "seniority": 0.06,
    "currentness": 0.10,
    "source": 0.06,
    "verification": 0.08,
}


FAMILY_EVIDENCE_BONUSES: dict[str, dict[str, float]] = {
    "default": {
        "company": 2.0,
        "industry": 1.5,
        "consensus": 0.8,
        "required": 0.8,
        "conflict_scale": 0.55,
    },
    "supply_chain": {
        "company": 3.4,
        "industry": 2.6,
        "consensus": 1.1,
        "required": 1.2,
        "conflict_scale": 0.45,
    },
    "design_architecture": {
        "company": 3.2,
        "industry": 2.8,
        "consensus": 1.1,
        "required": 1.0,
        "conflict_scale": 0.45,
    },
    "executive": {
        "company": 4.2,
        "industry": 3.0,
        "consensus": 1.4,
        "required": 0.8,
        "conflict_scale": 0.65,
    },
}


def _keyword_signal(entity: CandidateEntity, keywords: list[str], *, cap: int) -> tuple[float, int]:
    targets = {normalize_text(keyword) for keyword in keywords if normalize_text(keyword)}
    if not targets:
        return 0.0, 0
    supporting = {
        normalize_text(keyword)
        for record in entity.evidence
        for keyword in record.supporting_keywords
        if normalize_text(keyword)
    }
    matched = supporting & targets
    return round(min(1.0, len(matched) / max(1, min(cap, len(targets)))), 4), len(matched)


def _evidence_bonus_weights(role_family: str) -> dict[str, float]:
    resolved = dict(FAMILY_EVIDENCE_BONUSES["default"])
    resolved.update(FAMILY_EVIDENCE_BONUSES.get(role_family, {}))
    return resolved


class VerificationAwareRanker:
    def __init__(
        self,
        transformer_scorer: TransformerScorer | None = None,
        calibration_model: TransformerCalibrationModel | None = None,
    ) -> None:
        self.transformer_scorer = transformer_scorer
        self.calibration_model = calibration_model

    def score(self, entity: CandidateEntity, brief: SearchBrief) -> CandidateEntity:
        requested_family = infer_role_family(brief.role_title, *brief.titles)
        bonus_weights = _evidence_bonus_weights(requested_family)
        requested_title_signal = blended_title_precision(entity.current_title, brief)
        canonical_title_signal = canonical_title_precision(entity.current_title, brief)
        requested_title_coverage = best_requested_title_coverage(entity.current_title, brief)
        canonical_coverage = canonical_title_coverage(entity.current_title, brief)
        title_gap = adjacent_title_gap(entity.current_title, brief)
        strong_requested_title = requested_title_signal >= 0.84 and requested_title_coverage >= 0.84
        title_strength = min(1.0, (0.72 * requested_title_signal) + (0.28 * requested_title_coverage))
        entity.title_match_score = max(entity.title_match_score, title_strength if entity.title_match else title_strength * 0.7)
        entity.company_match_score = max(entity.company_match_score, 1.0 if entity.company_match else 0.0)
        entity.company_quality_score = max(entity.company_quality_score, company_quality_score(entity.current_company, entity.current_title, entity.role_family))
        entity.location_match_score = max(entity.location_match_score, 1.0 if entity.location_match else 0.0)
        entity.seniority_match_score = 0.78 if "manager" in normalize_text(entity.current_title or brief.role_title) else 0.55
        required_score, required_count = _keyword_signal(entity, brief.required_keywords, cap=4)
        preferred_score, _ = _keyword_signal(entity, brief.preferred_keywords, cap=6)
        industry_score, industry_count = _keyword_signal(entity, brief.industry_keywords, cap=4)
        entity.skill_match_score = round(max(entity.skill_match_score, min(1.0, 0.72 * required_score + 0.28 * preferred_score)), 4)
        entity.industry_match_score = round(max(entity.industry_match_score, industry_score), 4)
        entity.currentness_score = max(entity.currentness_score, 0.35 if entity.current_role_proof_count else 0.0)
        if any(domain in TECHNICAL_SOURCES for domain in entity.source_domains):
            entity.source_trust_score = max(entity.source_trust_score, 0.72)
        if self.transformer_scorer is not None:
            self.transformer_scorer.score(brief, entity)
        entity.semantic_fit = entity.semantic_similarity
        company_signal = round(
            min(
                1.0,
                0.6 * entity.company_match_score
                + 0.25 * entity.company_consensus_score
                + 0.15 * min(1.0, entity.company_support_count / 3.0),
            ),
            4,
        )
        calibrated_probability = self.calibration_model.predict_probability(candidate_feature_map(entity)) if self.calibration_model else 0.0
        calibration_signal = max(calibrated_probability, entity.semantic_similarity)
        company_fit = max(entity.company_match_score, company_signal)
        company_fit = round(company_fit * (0.82 + (0.18 * max(0.0, min(1.0, entity.company_quality_score)))), 4)
        conflict_penalty = min(0.18, entity.evidence_conflict_score * 0.18)
        baseline_verification_confidence = (
            0.32 * entity.currentness_score
            + 0.18 * entity.title_match_score
            + 0.12 * company_fit
            + 0.12 * entity.location_match_score
            + 0.10 * entity.source_trust_score
            + 0.14 * calibration_signal
            + 0.05 * entity.company_consensus_score
            + 0.04 * entity.industry_match_score
            - (conflict_penalty * 0.7)
        )
        if requested_family in {"supply_chain", "design_architecture"} and entity.current_role_proof_count >= 2:
            baseline_verification_confidence += 0.02 * min(1.0, entity.skill_match_score + entity.location_match_score)
        entity.verification_confidence = round(min(1.0, max(0.0, baseline_verification_confidence)), 4)
        base_score = round(
            100
            * (
                BASE_SCORE_WEIGHTS["semantic"] * entity.semantic_fit
                + BASE_SCORE_WEIGHTS["title"] * entity.title_match_score
                + BASE_SCORE_WEIGHTS["skill"] * entity.skill_match_score
                + BASE_SCORE_WEIGHTS["company"] * company_fit
                + BASE_SCORE_WEIGHTS["location"] * entity.location_match_score
                + BASE_SCORE_WEIGHTS["seniority"] * entity.seniority_match_score
                + BASE_SCORE_WEIGHTS["currentness"] * entity.currentness_score
                + BASE_SCORE_WEIGHTS["source"] * entity.source_trust_score
                + BASE_SCORE_WEIGHTS["verification"] * entity.verification_confidence
            ),
            2,
        )
        evidence_bonus = (
            bonus_weights["company"] * company_signal
            + bonus_weights["industry"] * entity.industry_match_score
            + bonus_weights["consensus"] * entity.company_consensus_score
            + bonus_weights["required"] * required_score
        )
        entity.score = round(base_score + evidence_bonus, 2)
        if requested_family in {"supply_chain", "design_architecture"} and entity.company_quality_score < 0.42 and not entity.company_match:
            entity.score = round(entity.score - min(4.5, (0.42 - entity.company_quality_score) * 8.0), 2)
        if requested_family == "design_architecture":
            adjacent_architecture = title_gap >= 0.22 and canonical_coverage < 0.72 and not strong_requested_title
            architecture_relevance = max(
                entity.skill_match_score,
                entity.industry_match_score,
                min(
                    1.0,
                    0.5 * entity.skill_match_score
                    + 0.18 * entity.industry_match_score
                    + 0.18 * company_fit
                    + 0.14 * entity.currentness_score,
                ),
                min(1.0, 0.7 * entity.skill_match_score + 0.3 * entity.semantic_fit),
            )
            if adjacent_architecture and architecture_relevance < 0.3:
                entity.score = round(entity.score - min(4.0, (0.3 - architecture_relevance) * 10.0), 2)
        if requested_family == "executive":
            brief_company_signal = 1.0 if entity.company_match else 0.55 if entity.peer_company_match else 0.0
            brief_relevance = max(
                brief_company_signal,
                entity.industry_match_score,
                min(1.0, 0.75 * entity.skill_match_score + 0.25 * entity.location_match_score),
            )
            relevance_floor = 0.55 if title_gap >= 0.32 else 0.4
            if brief_relevance < relevance_floor:
                entity.score = round(entity.score - min(5.4, (relevance_floor - brief_relevance) * 12.0), 2)
            if entity.company_match:
                entity.score = round(entity.score + 5.0, 2)
            elif entity.peer_company_match:
                entity.score = round(entity.score + 2.0, 2)
        if conflict_penalty:
            entity.score = round(entity.score * max(0.88, 1.0 - (conflict_penalty * bonus_weights["conflict_scale"])), 2)
        notes: list[str] = []
        if entity.role_family == requested_family:
            notes.append("role_family_match")
        if entity.title_match:
            notes.append("title_match")
        if canonical_title_signal >= 0.42:
            notes.append("primary_title_match")
        if entity.company_support_count >= 2 and entity.company_consensus_score >= 0.66:
            notes.append("company_consensus")
        if entity.current_role_proof_count:
            notes.append("current_role_proof")
        if entity.company_quality_score >= 0.56:
            notes.append("strong_company_identity")
        if industry_count >= 1:
            notes.append("industry_signal")
        if required_count >= 2:
            notes.append("required_skill_overlap")
        if calibrated_probability >= 0.55:
            notes.append("feedback_calibration")
        if entity.semantic_fit >= 0.45:
            notes.append("semantic_fit")
        if requested_family in {"supply_chain", "design_architecture"} and entity.company_quality_score < 0.42:
            notes.append("weak_company_identity")
        if requested_family == "design_architecture" and title_gap >= 0.22 and canonical_coverage < 0.72 and not strong_requested_title:
            notes.append("generic_title_risk")
        if requested_family == "executive":
            brief_company_signal = 1.0 if entity.company_match else 0.75 if entity.peer_company_match else 0.0
            brief_relevance = max(
                brief_company_signal,
                entity.industry_match_score,
                min(1.0, 0.75 * entity.skill_match_score + 0.25 * entity.location_match_score),
            )
            if brief_relevance < (0.55 if title_gap >= 0.32 else 0.4):
                notes.append("weak_company_or_industry_signals")
        if requested_family == "executive" and title_gap >= 0.32:
            notes.append("adjacent_title_risk")
        if entity.evidence_conflict_score >= 0.35:
            notes.append("identity_conflict")
        entity.notes = notes
        return entity

    def rank(self, entities: list[CandidateEntity], brief: SearchBrief) -> list[CandidateEntity]:
        scored = [self.score(entity, brief) for entity in entities]
        return sorted(
            scored,
            key=lambda entity: (
                -entity.score,
                -entity.verification_confidence,
                -entity.company_consensus_score,
                -entity.industry_match_score,
                -entity.semantic_fit,
                normalize_text(entity.full_name),
            ),
        )
