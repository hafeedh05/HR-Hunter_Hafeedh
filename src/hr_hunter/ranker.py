from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from hr_hunter.config import resolve_ranker_model_dir
from hr_hunter.features import FeatureBuildResult
from hr_hunter.briefing import normalize_text
from hr_hunter.models import CandidateProfile, SearchBrief
from hr_hunter.state import record_model_version


RANKING_MODEL_VERSION = "heuristic-anchor-ranker-v2"
LEARNED_RANKING_MODEL_PREFIX = "lightgbm-lambdarank"
LEARNED_RANKER_MODEL_FILENAME = "model.txt"
LEARNED_RANKER_METADATA_FILENAME = "metadata.json"
LEARNED_RANKER_FEATURES = [
    "title_similarity",
    "company_match",
    "location_match",
    "skill_overlap",
    "industry_fit",
    "years_fit",
    "current_function_fit",
    "parser_confidence",
    "evidence_quality",
    "employment_status",
    "semantic_similarity",
    "source_quality_score",
    "reranker_score",
    "heuristic_score",
    "years_experience_gap",
    "anchor_title_similarity",
    "anchor_company_match",
    "anchor_location_match",
    "anchor_skill_overlap",
    "anchor_industry_fit",
    "anchor_years_fit",
    "anchor_current_function_fit",
    "anchor_parser_confidence",
    "anchor_evidence_quality",
    "anchor_semantic_similarity",
]


@dataclass
class RankResult:
    score: float
    anchor_scores: Dict[str, float]
    cap_reasons: List[str] = field(default_factory=list)
    disqualifier_reasons: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    ranking_model_version: str = RANKING_MODEL_VERSION


@dataclass
class LearnedRankerSettings:
    enabled: bool = False
    model_dir: Path = Path("output/models/ranker/latest")
    weight: float = 0.7


def status_from_score(score: float) -> str:
    if score >= 70.0:
        return "verified"
    if score >= 50.0:
        return "review"
    return "reject"


def parse_learned_ranker_settings(brief: SearchBrief) -> LearnedRankerSettings:
    config = brief.provider_settings.get("learned_ranker", {})
    if not isinstance(config, dict):
        config = {}
    return LearnedRankerSettings(
        enabled=bool(config.get("enabled", False)),
        model_dir=resolve_ranker_model_dir(str(config.get("model_dir")) if config.get("model_dir") else None),
        weight=min(1.0, max(0.0, float(config.get("weight", 0.7) or 0.7))),
    )


def normalize_anchor_weights(
    feature_scores: Dict[str, float],
    anchor_weights: Dict[str, float],
) -> Dict[str, float]:
    relevant = {
        name: max(0.0, float(anchor_weights.get(name, 0.0)))
        for name in feature_scores
        if float(anchor_weights.get(name, 0.0)) > 0.0
    }
    total = sum(relevant.values())
    if total <= 0.0:
        fallback_total = float(len(feature_scores)) or 1.0
        return {name: round(1.0 / fallback_total, 6) for name in feature_scores}
    return {name: round(weight / total, 6) for name, weight in relevant.items()}


def _weighted_anchor_scores(
    feature_scores: Dict[str, float],
    anchor_weights: Dict[str, float],
) -> Tuple[float, Dict[str, float]]:
    normalized = normalize_anchor_weights(feature_scores, anchor_weights)
    anchor_scores = {
        name: round(feature_scores.get(name, 0.0) * normalized.get(name, 0.0) * 100.0, 2)
        for name in feature_scores
    }
    return round(sum(anchor_scores.values()), 2), anchor_scores


def _is_focused_precision_brief(brief: SearchBrief) -> bool:
    retrieval_settings = dict(brief.provider_settings.get("retrieval", {}) or {})
    normalized_locations = {
        normalize_text(str(value))
        for value in [*brief.location_targets, brief.geography.location_name, brief.geography.country]
        if normalize_text(str(value))
    }
    return (
        not bool(retrieval_settings.get("include_broad_slice", True))
        and not bool(retrieval_settings.get("include_discovery_slices", True))
        and (not bool(retrieval_settings.get("include_history_slices", False)) or not brief.company_targets)
        and bool(brief.titles)
        and len(brief.titles) <= 3
        and len(normalized_locations) <= 4
    )


def rank_candidate(features: FeatureBuildResult, brief: SearchBrief) -> RankResult:
    base_score, anchor_scores = _weighted_anchor_scores(features.feature_scores, brief.anchor_weights)
    score = base_score
    notes: List[str] = []
    cap_reasons: List[str] = []
    disqualifier_reasons = list(features.disqualifier_reasons)
    max_score = 100.0

    location_weight = float(brief.anchor_weights.get("location_match", 0.0))
    company_weight = float(brief.anchor_weights.get("company_match", 0.0))
    title_weight = float(brief.anchor_weights.get("title_similarity", 0.0))
    industry_weight = float(brief.anchor_weights.get("industry_fit", 0.0))
    company_mode = brief.company_match_mode
    employment_mode = brief.employment_status_mode
    normalized_location_targets = {
        normalize_text(str(value))
        for value in [*brief.location_targets, brief.geography.location_name, brief.geography.country]
        if normalize_text(str(value))
    }
    broad_location_scope = len(normalized_location_targets) >= 6
    outside_location_penalty = 10.0 if broad_location_scope else 25.0
    imprecise_location_penalty = 4.0 if broad_location_scope else 10.0
    focused_precision_brief = _is_focused_precision_brief(brief)
    has_specific_location_target = any(
        normalize_text(str(value))
        and normalize_text(str(value)) != normalize_text(brief.geography.country)
        for value in [brief.geography.location_name, *brief.location_targets]
    )
    skill_overlap_score = float(features.feature_scores.get("skill_overlap", 0.0) or 0.0)
    current_function_fit_score = float(features.feature_scores.get("current_function_fit", 0.0) or 0.0)

    if brief.company_targets:
        if company_mode == "current_only":
            if features.current_target_company_match:
                score += 6.0
                notes.append("ranker_bonus: current_target_company_match")
            else:
                score -= min(16.0, company_weight * 16.0 or 14.0)
                notes.append("ranker_penalty: current_target_company_missing")
        elif company_mode == "past_only":
            if features.target_company_history_match:
                score += 4.0
                notes.append("ranker_bonus: target_company_history_match")
            else:
                score -= min(14.0, company_weight * 14.0 or 12.0)
                notes.append("ranker_penalty: target_company_history_missing")
        else:
            broad_company_scope = broad_location_scope or len(brief.company_targets) >= 8
            if features.current_target_company_match:
                score += 6.0
                notes.append("ranker_bonus: current_target_company_match")
            elif features.target_company_history_match:
                if broad_company_scope:
                    score += 2.0
                    notes.append("ranker_bonus: target_company_history_match")
                else:
                    score -= 8.0
                    notes.append("ranker_penalty: target_company_history_only")
            else:
                if broad_company_scope:
                    score -= min(8.0, company_weight * 8.0 or 6.0)
                else:
                    score -= min(14.0, company_weight * 14.0)
                notes.append("ranker_penalty: target_company_missing")

    if features.current_title_match:
        score += 5.0
        notes.append("ranker_bonus: current_title_match")
    elif brief.titles or brief.title_keywords:
        score -= min(16.0, title_weight * 16.0)
        notes.append("ranker_penalty: current_title_not_aligned")

    if features.location_bucket == "outside_target_area":
        score -= outside_location_penalty
        notes.append("ranker_penalty: outside_target_area")
        if location_weight >= 0.7:
            max_score = min(max_score, 45.0)
            cap_reasons.append("outside_target_area")
    elif features.location_bucket in {"country_only", "unknown_location"}:
        score -= imprecise_location_penalty
        notes.append("ranker_penalty: imprecise_location")
        if location_weight >= 0.7:
            max_score = min(max_score, 69.0)
            cap_reasons.append("precise_location_required")

    if features.off_function_blocked:
        score -= 18.0
        max_score = min(max_score, 69.0)
        cap_reasons.append("current_function_review")
        notes.append("ranker_penalty: off_function_current_role")

    if focused_precision_brief:
        if not features.current_title_match:
            score -= 12.0
            max_score = min(max_score, 45.0)
            if "title_alignment_required" not in cap_reasons:
                cap_reasons.append("title_alignment_required")
            notes.append("ranker_penalty: focused_title_mismatch")

        if brief.required_keywords:
            if skill_overlap_score <= 0.0:
                score -= 20.0
                max_score = min(max_score, 35.0)
                cap_reasons.append("required_skills_missing")
                notes.append("ranker_penalty: required_skills_missing")
            elif skill_overlap_score < 0.5:
                score -= 10.0
                max_score = min(max_score, 59.0)
                cap_reasons.append("required_skills_partial")
                notes.append("ranker_penalty: required_skills_partial")

        if has_specific_location_target and features.location_bucket in {"country_only", "unknown_location"}:
            score -= 10.0
            max_score = min(max_score, 59.0)
            if "precise_location_required" not in cap_reasons:
                cap_reasons.append("precise_location_required")
            notes.append("ranker_penalty: focused_location_precision")

        if current_function_fit_score < 0.45:
            score -= 12.0
            max_score = min(max_score, 49.0)
            cap_reasons.append("current_function_alignment_required")
            notes.append("ranker_penalty: focused_function_mismatch")

    if features.exclude_hits:
        score -= min(36.0, float(len(features.exclude_hits) * 14))
        max_score = min(max_score, 35.0)
        cap_reasons.append("hard_exclude")
        disqualifier_reasons.extend(features.exclude_hits)
        notes.append("ranker_penalty: excluded_terms")

    if features.low_seniority_hits and (brief.minimum_years_experience or 0) >= 6:
        score -= min(15.0, float(features.low_seniority_hits * 6))
        notes.append("ranker_penalty: low_seniority")

    parser_confidence = features.feature_scores.get("parser_confidence", 0.0)
    if parser_confidence < 0.25:
        score -= 18.0
        max_score = min(max_score, 49.0)
        cap_reasons.append("parser_confidence_too_low")
        notes.append("ranker_penalty: parser_confidence_low")
    elif parser_confidence < 0.45:
        score -= 8.0
        notes.append("ranker_penalty: parser_confidence_soft")

    if brief.industry_keywords and features.feature_scores.get("industry_fit", 0.0) <= 0.0:
        score -= min(14.0, industry_weight * 14.0 or 12.0)
        notes.append("ranker_penalty: industry_fit_missing")

    if employment_mode != "any":
        if features.employment_status_match:
            if employment_mode == "open_to_work_signal":
                score += 4.0
                notes.append("ranker_bonus: open_to_work_signal")
            elif employment_mode == "not_currently_employed":
                score += 3.0
                notes.append("ranker_bonus: not_currently_employed")
            else:
                score += 2.0
                notes.append("ranker_bonus: employment_status_match")
        else:
            score -= 22.0
            max_score = min(max_score, 35.0)
            cap_reasons.append("employment_status_required")
            notes.append("ranker_penalty: employment_status_missing")

    if brief.company_targets:
        if company_mode == "current_only" and not features.current_target_company_match and company_weight >= 0.75:
            max_score = min(max_score, 69.0)
            cap_reasons.append("current_target_company_required")
        elif company_mode == "past_only" and not features.target_company_history_match and company_weight >= 0.75:
            max_score = min(max_score, 69.0)
            cap_reasons.append("target_company_history_required")
        elif (
            company_mode == "both"
            and features.target_company_history_match
            and not features.current_target_company_match
            and company_weight >= 0.75
            and not (broad_location_scope or len(brief.company_targets) >= 8)
        ):
            max_score = min(max_score, 69.0)
            cap_reasons.append("current_target_company_required")
    if (brief.titles or brief.title_keywords) and not features.current_title_match and title_weight >= 0.75:
        max_score = min(max_score, 69.0)
        if "title_alignment_required" not in cap_reasons:
            cap_reasons.append("title_alignment_required")

    if (
        features.current_target_company_match
        and features.current_title_match
        and features.location_aligned
    ):
        score += 8.0
        notes.append("ranker_bonus: anchor_triple_match")

    score = round(min(max(score, 0.0), max_score), 2)
    return RankResult(
        score=score,
        anchor_scores=anchor_scores,
        cap_reasons=list(dict.fromkeys(cap_reasons)),
        disqualifier_reasons=list(dict.fromkeys(disqualifier_reasons)),
        notes=list(dict.fromkeys(notes)),
    )


def _model_artifact_paths(model_dir: Path) -> tuple[Path, Path]:
    return (
        model_dir / LEARNED_RANKER_MODEL_FILENAME,
        model_dir / LEARNED_RANKER_METADATA_FILENAME,
    )


def build_learned_feature_map(candidate: CandidateProfile, brief: SearchBrief) -> Dict[str, float]:
    feature_scores = dict(candidate.feature_scores or {})
    anchor_weights = dict(brief.anchor_weights or {})
    years_gap = candidate.years_experience_gap
    return {
        "title_similarity": float(feature_scores.get("title_similarity", candidate.title_similarity_score)),
        "company_match": float(feature_scores.get("company_match", candidate.company_match_score)),
        "location_match": float(feature_scores.get("location_match", candidate.location_match_score)),
        "skill_overlap": float(feature_scores.get("skill_overlap", candidate.skill_overlap_score)),
        "industry_fit": float(feature_scores.get("industry_fit", candidate.industry_fit_score)),
        "years_fit": float(feature_scores.get("years_fit", candidate.years_fit_score)),
        "current_function_fit": float(feature_scores.get("current_function_fit", candidate.current_function_fit)),
        "parser_confidence": float(feature_scores.get("parser_confidence", candidate.parser_confidence)),
        "evidence_quality": float(feature_scores.get("evidence_quality", candidate.evidence_quality_score)),
        "employment_status": float(feature_scores.get("employment_status", 0.0)),
        "semantic_similarity": float(feature_scores.get("semantic_similarity", candidate.semantic_similarity_score)),
        "source_quality_score": float(candidate.source_quality_score),
        "reranker_score": float(candidate.reranker_score),
        "heuristic_score": float(candidate.score),
        "years_experience_gap": abs(float(years_gap)) if years_gap is not None else 0.0,
        "anchor_title_similarity": float(anchor_weights.get("title_similarity", 0.0)),
        "anchor_company_match": float(anchor_weights.get("company_match", 0.0)),
        "anchor_location_match": float(anchor_weights.get("location_match", 0.0)),
        "anchor_skill_overlap": float(anchor_weights.get("skill_overlap", 0.0)),
        "anchor_industry_fit": float(anchor_weights.get("industry_fit", 0.0)),
        "anchor_years_fit": float(anchor_weights.get("years_fit", 0.0)),
        "anchor_current_function_fit": float(anchor_weights.get("current_function_fit", 0.0)),
        "anchor_parser_confidence": float(anchor_weights.get("parser_confidence", 0.0)),
        "anchor_evidence_quality": float(anchor_weights.get("evidence_quality", 0.0)),
        "anchor_semantic_similarity": float(anchor_weights.get("semantic_similarity", 0.0)),
    }


def _row_feature_vector(feature_map: Dict[str, Any]) -> List[float]:
    return [float(feature_map.get(name, 0.0) or 0.0) for name in LEARNED_RANKER_FEATURES]


def cap_candidate_score(score: float, candidate: CandidateProfile) -> float:
    cap = 100.0
    if {"hard_exclude", "employment_status_required"}.intersection(set(candidate.cap_reasons)):
        cap = min(cap, 35.0)
    if "outside_target_area" in candidate.cap_reasons:
        cap = min(cap, 45.0)
    if "parser_confidence_too_low" in candidate.cap_reasons:
        cap = min(cap, 49.0)
    if {
        "precise_location_required",
        "current_function_review",
        "current_target_company_required",
        "target_company_history_required",
        "title_alignment_required",
    }.intersection(set(candidate.cap_reasons)):
        cap = min(cap, 69.0)
    return min(score, cap)


def train_learned_ranker(
    training_rows: List[Dict[str, Any]],
    *,
    model_dir: Path | None = None,
    n_estimators: int = 80,
    learning_rate: float = 0.1,
    num_leaves: int = 31,
) -> Dict[str, Any]:
    try:
        from lightgbm import LGBMRanker
    except ImportError as exc:  # pragma: no cover - runtime dependency
        raise RuntimeError("LightGBM is not installed. Run `uv sync --extra ranker`.") from exc

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in training_rows:
        grouped.setdefault(str(row["query_id"]), []).append(row)

    feature_matrix: List[List[float]] = []
    labels: List[int] = []
    groups: List[int] = []
    used_queries = 0
    for query_id, rows in grouped.items():
        if len(rows) < 2:
            continue
        unique_labels = {int(row["label"]) for row in rows}
        if len(unique_labels) < 2:
            continue
        used_queries += 1
        groups.append(len(rows))
        for row in rows:
            feature_payload = row.get("feature_json", {})
            feature_matrix.append(_row_feature_vector(feature_payload))
            labels.append(int(row["label"]))

    if not feature_matrix or not groups:
        raise ValueError("Not enough feedback diversity to train a LambdaRank model yet.")

    resolved_model_dir = resolve_ranker_model_dir(str(model_dir) if model_dir else None)
    ranker = LGBMRanker(
        objective="lambdarank",
        metric="ndcg",
        n_estimators=n_estimators,
        learning_rate=learning_rate,
        num_leaves=num_leaves,
        random_state=42,
        min_data_in_leaf=1,
    )
    ranker.fit(feature_matrix, labels, group=groups)

    resolved_model_dir.mkdir(parents=True, exist_ok=True)
    model_path, metadata_path = _model_artifact_paths(resolved_model_dir)
    ranker.booster_.save_model(str(model_path))
    metadata = {
        "model_version": f"{LEARNED_RANKING_MODEL_PREFIX}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "feature_names": LEARNED_RANKER_FEATURES,
        "query_count": used_queries,
        "training_row_count": len(feature_matrix),
        "n_estimators": n_estimators,
        "learning_rate": learning_rate,
        "num_leaves": num_leaves,
    }
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    record_model_version(
        model_type="learned_ranker",
        model_version=str(metadata["model_version"]),
        model_dir=str(resolved_model_dir),
        metadata=metadata,
    )
    return {
        "model_dir": str(resolved_model_dir),
        "model_path": str(model_path),
        "metadata_path": str(metadata_path),
        **metadata,
    }


def _load_learned_ranker(model_dir: Path) -> tuple[Any, Dict[str, Any]]:
    try:
        import lightgbm as lgb
    except ImportError as exc:  # pragma: no cover - runtime dependency
        raise RuntimeError("LightGBM is not installed. Run `uv sync --extra ranker`.") from exc

    model_path, metadata_path = _model_artifact_paths(model_dir)
    if not model_path.exists() or not metadata_path.exists():
        raise FileNotFoundError(f"No trained ranker artifacts found in {model_dir}")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    booster = lgb.Booster(model_file=str(model_path))
    return booster, metadata


def apply_learned_ranker(brief: SearchBrief, candidates: List[CandidateProfile]) -> List[CandidateProfile]:
    settings = parse_learned_ranker_settings(brief)
    if not settings.enabled or not candidates:
        return list(candidates)

    try:
        booster, metadata = _load_learned_ranker(settings.model_dir)
    except Exception as exc:  # pragma: no cover - runtime fallback
        for candidate in candidates:
            candidate.verification_notes = list(dict.fromkeys([*candidate.verification_notes, f"learned_ranker:unavailable:{exc}"]))
        return list(candidates)

    feature_matrix = [
        _row_feature_vector(build_learned_feature_map(candidate, brief))
        for candidate in candidates
    ]
    raw_predictions = list(booster.predict(feature_matrix))
    if not raw_predictions:
        return list(candidates)

    min_prediction = min(raw_predictions)
    max_prediction = max(raw_predictions)
    prediction_range = max_prediction - min_prediction
    for candidate, raw_prediction in zip(candidates, raw_predictions):
        if prediction_range <= 1e-9:
            normalized_prediction = candidate.score
        else:
            normalized_prediction = ((raw_prediction - min_prediction) / prediction_range) * 100.0
        blended_score = (candidate.score * (1.0 - settings.weight)) + (normalized_prediction * settings.weight)
        candidate.score = round(max(0.0, cap_candidate_score(blended_score, candidate)), 2)
        candidate.ranking_model_version = str(metadata.get("model_version", LEARNED_RANKING_MODEL_PREFIX))
        candidate.verification_status = status_from_score(candidate.score)
        candidate.verification_notes = list(
            dict.fromkeys([*candidate.verification_notes, f"learned_ranker:{candidate.ranking_model_version}"])
        )
    return list(candidates)
