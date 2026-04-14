from __future__ import annotations

import json
import math
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from hr_hunter.feedback import load_ranker_training_rows
from hr_hunter_transformer.models import CandidateEntity
from hr_hunter_transformer.role_profiles import TECHNICAL_SOURCES


def _default_report_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "output" / "search"


def _sigmoid(value: float) -> float:
    bounded = max(-30.0, min(30.0, value))
    return 1.0 / (1.0 + math.exp(-bounded))


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


@dataclass(slots=True)
class TransformerCalibrationModel:
    weights: dict[str, float]
    bias: float
    training_rows: int
    positive_rows: int

    def predict_probability(self, features: dict[str, float]) -> float:
        score = self.bias
        for key, value in features.items():
            score += self.weights.get(key, 0.0) * float(value)
        return _sigmoid(score)


def candidate_feature_map(entity: CandidateEntity) -> dict[str, float]:
    max_confidence = max((record.confidence for record in entity.evidence), default=0.0)
    keyword_support = max((len(record.supporting_keywords) for record in entity.evidence), default=0)
    return {
        "title_match": 1.0 if entity.title_match else 0.0,
        "company_match": 1.0 if entity.company_match else 0.0,
        "location_match": 1.0 if entity.location_match else 0.0,
        "current_role_proof_count": min(5.0, float(entity.current_role_proof_count or 0)),
        "current_company_confirmed": 1.0 if entity.current_company_confirmed else 0.0,
        "current_title_confirmed": 1.0 if entity.current_title_confirmed else 0.0,
        "current_location_confirmed": 1.0 if entity.current_location_confirmed else 0.0,
        "semantic_similarity": float(entity.semantic_similarity or 0.0),
        "source_count": float(len(entity.source_domains)),
        "technical_source": 1.0 if any(domain in TECHNICAL_SOURCES for domain in entity.source_domains) else 0.0,
        "max_evidence_confidence": float(max_confidence),
        "keyword_support": float(keyword_support),
    }


def _training_feature_map(row: dict[str, Any]) -> dict[str, float]:
    feature_json = row.get("feature_json") if isinstance(row.get("feature_json"), dict) else {}
    title_match = max(
        _safe_float(feature_json.get("current_title_match")),
        1.0 if _safe_float(feature_json.get("title_similarity_score")) >= 0.6 else 0.0,
    )
    company_match = max(
        _safe_float(feature_json.get("current_target_company_match")),
        1.0 if _safe_float(feature_json.get("company_match_score")) >= 0.6 else 0.0,
    )
    location_match = max(
        _safe_float(feature_json.get("current_location_confirmed")),
        1.0 if _safe_float(feature_json.get("location_match_score")) >= 0.5 else 0.0,
    )
    return {
        "title_match": title_match,
        "company_match": company_match,
        "location_match": location_match,
        "current_role_proof_count": min(5.0, _safe_float(feature_json.get("current_role_proof_count"))),
        "current_company_confirmed": _safe_float(feature_json.get("current_company_confirmed")),
        "current_title_confirmed": _safe_float(feature_json.get("current_title_confirmed")),
        "current_location_confirmed": _safe_float(feature_json.get("current_location_confirmed")),
        "semantic_similarity": _safe_float(feature_json.get("semantic_similarity_score")),
        "source_count": min(5.0, _safe_float(feature_json.get("source_quality_score")) * 4.0),
        "technical_source": 1.0 if any(token in json.dumps(feature_json).lower() for token in ("github", "gitlab", "huggingface", "kaggle")) else 0.0,
        "max_evidence_confidence": _safe_float(feature_json.get("evidence_quality_score")),
        "keyword_support": min(6.0, _safe_float(feature_json.get("skill_overlap_score")) * 6.0),
    }


def _report_candidate_feature_map(candidate: dict[str, Any]) -> dict[str, float]:
    feature_scores = candidate.get("feature_scores") if isinstance(candidate.get("feature_scores"), dict) else {}
    evidence_records = candidate.get("evidence_records") if isinstance(candidate.get("evidence_records"), list) else []
    max_confidence = max((_safe_float(record.get("confidence")) for record in evidence_records if isinstance(record, dict)), default=0.0)
    keyword_support = max((len(record.get("supporting_keywords") or []) for record in evidence_records if isinstance(record, dict)), default=0)
    source_domains = {
        str(record.get("source_domain") or "").lower().strip()
        for record in evidence_records
        if isinstance(record, dict)
    }
    return {
        "title_match": 1.0
        if (
            _safe_float(candidate.get("current_title_match"))
            or _safe_float(feature_scores.get("title_similarity")) >= 0.7
        )
        else 0.0,
        "company_match": 1.0
        if (
            _safe_float(candidate.get("current_target_company_match"))
            or _safe_float(feature_scores.get("company_match")) >= 0.7
        )
        else 0.0,
        "location_match": 1.0
        if (
            _safe_float(candidate.get("current_location_confirmed"))
            or _safe_float(feature_scores.get("location_match")) >= 0.6
        )
        else 0.0,
        "current_role_proof_count": min(5.0, _safe_float(candidate.get("current_role_proof_count"))),
        "current_company_confirmed": _safe_float(candidate.get("current_company_confirmed")),
        "current_title_confirmed": _safe_float(candidate.get("current_title_confirmed")),
        "current_location_confirmed": _safe_float(candidate.get("current_location_confirmed")),
        "semantic_similarity": _safe_float(candidate.get("semantic_similarity_score"))
        or _safe_float(feature_scores.get("semantic_similarity")),
        "source_count": min(6.0, float(len(source_domains))),
        "technical_source": 1.0 if any(domain in TECHNICAL_SOURCES for domain in source_domains) else 0.0,
        "max_evidence_confidence": max_confidence,
        "keyword_support": float(keyword_support) if keyword_support else min(6.0, _safe_float(feature_scores.get("skill_overlap")) * 6.0),
    }


def _iter_bootstrap_training_rows(report_dir: Path | None = None) -> list[tuple[dict[str, float], float]]:
    base_dir = (report_dir or _default_report_dir()).expanduser().resolve()
    if not base_dir.exists():
        return []
    prepared: list[tuple[dict[str, float], float]] = []
    for report_path in sorted(base_dir.glob("*.json")):
        try:
            payload = json.loads(report_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        candidates = payload.get("candidates") if isinstance(payload.get("candidates"), list) else []
        if len(candidates) < 20:
            continue
        counts = {"verified": 0, "review": 0, "reject": 0}
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            status = str(candidate.get("verification_status") or "reject").lower().strip()
            if status in counts:
                counts[status] += 1
        if counts["verified"] == 0 and counts["reject"] == 0:
            continue
        if counts["verified"] >= max(20, int(len(candidates) * 0.98)) and counts["reject"] == 0:
            continue
        if counts["reject"] >= max(20, int(len(candidates) * 0.98)) and counts["verified"] == 0:
            continue
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            status = str(candidate.get("verification_status") or "reject").lower().strip()
            if status == "verified":
                prepared.append((_report_candidate_feature_map(candidate), 1.0))
            elif status == "reject":
                prepared.append((_report_candidate_feature_map(candidate), 0.0))
            elif status == "review":
                proof_count = _safe_float(candidate.get("current_role_proof_count"))
                title_confirmed = _safe_float(candidate.get("current_title_confirmed"))
                location_confirmed = _safe_float(candidate.get("current_location_confirmed"))
                semantic_similarity = _safe_float(candidate.get("semantic_similarity_score"))
                if proof_count >= 2 and title_confirmed and (location_confirmed or semantic_similarity >= 0.45):
                    prepared.append((_report_candidate_feature_map(candidate), 1.0))
    return prepared


def train_transformer_calibration_model(
    db_path: Path | None = None,
    report_dir: Path | None = None,
) -> TransformerCalibrationModel | None:
    rows = load_ranker_training_rows(db_path)
    prepared: list[tuple[dict[str, float], float]] = []
    positive_rows = 0
    for row in rows:
        label = int(row.get("label", 0) or 0)
        target = 1.0 if label >= 3 else 0.0
        positive_rows += int(target > 0)
        prepared.append((_training_feature_map(row), target))
    bootstrap_rows = _iter_bootstrap_training_rows(report_dir)
    positive_rows += sum(1 for _, target in bootstrap_rows if target > 0)
    prepared.extend(bootstrap_rows)
    if positive_rows < 5 or positive_rows >= len(prepared):
        return None

    feature_names = sorted({key for features, _ in prepared for key in features})
    weights = {name: 0.0 for name in feature_names}
    bias = 0.0
    learning_rate = 0.08
    for _ in range(180):
        grad = {name: 0.0 for name in feature_names}
        bias_grad = 0.0
        for features, target in prepared:
            score = bias + sum(weights[name] * features.get(name, 0.0) for name in feature_names)
            prediction = _sigmoid(score)
            error = prediction - target
            bias_grad += error
            for name in feature_names:
                grad[name] += error * features.get(name, 0.0)
        scale = 1.0 / max(1, len(prepared))
        bias -= learning_rate * bias_grad * scale
        for name in feature_names:
            weights[name] -= learning_rate * grad[name] * scale

    return TransformerCalibrationModel(
        weights=weights,
        bias=bias,
        training_rows=len(prepared),
        positive_rows=positive_rows,
    )


@lru_cache(maxsize=1)
def load_transformer_calibration_model(db_path_str: str = "", report_dir_str: str = "") -> TransformerCalibrationModel | None:
    db_path = Path(db_path_str).expanduser().resolve() if db_path_str else None
    report_dir = Path(report_dir_str).expanduser().resolve() if report_dir_str else None
    return train_transformer_calibration_model(db_path, report_dir)
