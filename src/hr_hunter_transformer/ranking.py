from __future__ import annotations

from hr_hunter_transformer.calibration import TransformerCalibrationModel, candidate_feature_map
from hr_hunter_transformer.models import CandidateEntity, SearchBrief
from hr_hunter_transformer.role_profiles import TECHNICAL_SOURCES, infer_role_family, normalize_text
from hr_hunter_transformer.transformer_ranker import TransformerScorer


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
        entity.title_match_score = max(entity.title_match_score, 1.0 if entity.title_match else 0.0)
        entity.company_match_score = max(entity.company_match_score, 1.0 if entity.company_match else 0.0)
        entity.location_match_score = max(entity.location_match_score, 1.0 if entity.location_match else 0.0)
        entity.seniority_match_score = 0.78 if "manager" in normalize_text(entity.current_title or brief.role_title) else 0.55
        entity.skill_match_score = round(
            min(1.0, len({keyword.lower() for keyword in brief.required_keywords if keyword} & {keyword.lower() for record in entity.evidence for keyword in record.supporting_keywords}) / max(1, len(brief.required_keywords[:4]))),
            4,
        )
        entity.currentness_score = max(entity.currentness_score, 0.35 if entity.current_role_proof_count else 0.0)
        if any(domain in TECHNICAL_SOURCES for domain in entity.source_domains):
            entity.source_trust_score = max(entity.source_trust_score, 0.72)
        if self.transformer_scorer is not None:
            self.transformer_scorer.score(brief, entity)
        entity.semantic_fit = entity.semantic_similarity
        calibrated_probability = self.calibration_model.predict_probability(candidate_feature_map(entity)) if self.calibration_model else 0.0
        baseline_verification_confidence = (
            0.34 * entity.currentness_score
            + 0.18 * entity.title_match_score
            + 0.12 * entity.company_match_score
            + 0.12 * entity.location_match_score
            + 0.10 * entity.source_trust_score
            + 0.14 * max(calibrated_probability, entity.semantic_similarity)
        )
        entity.verification_confidence = round(min(1.0, max(0.0, baseline_verification_confidence)), 4)
        entity.score = round(
            100
            * (
                0.24 * entity.semantic_fit
                + 0.18 * entity.title_match_score
                + 0.12 * entity.skill_match_score
                + 0.08 * entity.company_match_score
                + 0.08 * entity.location_match_score
                + 0.06 * entity.seniority_match_score
                + 0.10 * entity.currentness_score
                + 0.06 * entity.source_trust_score
                + 0.08 * entity.verification_confidence
            ),
            2,
        )
        notes: list[str] = []
        if entity.role_family == requested_family:
            notes.append("role_family_match")
        if entity.title_match:
            notes.append("title_match")
        if entity.current_role_proof_count:
            notes.append("current_role_proof")
        if calibrated_probability:
            notes.append("feedback_calibration")
        if entity.semantic_fit >= 0.45:
            notes.append("semantic_fit")
        entity.notes = notes
        return entity

    def rank(self, entities: list[CandidateEntity], brief: SearchBrief) -> list[CandidateEntity]:
        scored = [self.score(entity, brief) for entity in entities]
        return sorted(scored, key=lambda entity: (-entity.score, -entity.verification_confidence, -entity.semantic_fit, normalize_text(entity.full_name)))

