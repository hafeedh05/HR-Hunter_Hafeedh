from __future__ import annotations

import math
from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable, List, Sequence

from hr_hunter.briefing import unique_preserving_order
from hr_hunter.models import CandidateProfile, SearchBrief
from hr_hunter.ranker import cap_candidate_score, status_from_score


DEFAULT_RERANKER_MODEL = "disabled"
PLANNED_RERANKER_MODEL = "BAAI/bge-reranker-v2-m3"
DEFAULT_RERANKER_TOP_N = 40
DEFAULT_RERANKER_WEIGHT = 0.35


@dataclass
class RerankerResult:
    score: float = 0.0
    model_name: str = DEFAULT_RERANKER_MODEL
    enabled: bool = False
    error: str = ""


@dataclass
class RerankerSettings:
    enabled: bool = False
    model_name: str = PLANNED_RERANKER_MODEL
    top_n: int = DEFAULT_RERANKER_TOP_N
    weight: float = DEFAULT_RERANKER_WEIGHT
    device: str | None = None


def _safe_float(value: object, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_reranker_settings(brief: SearchBrief) -> RerankerSettings:
    config = brief.provider_settings.get("reranker", {})
    if not isinstance(config, dict):
        config = {}
    return RerankerSettings(
        enabled=bool(config.get("enabled", False)),
        model_name=str(config.get("model_name", PLANNED_RERANKER_MODEL)).strip() or PLANNED_RERANKER_MODEL,
        top_n=max(1, int(config.get("top_n", DEFAULT_RERANKER_TOP_N) or DEFAULT_RERANKER_TOP_N)),
        weight=min(1.0, max(0.0, _safe_float(config.get("weight", DEFAULT_RERANKER_WEIGHT), DEFAULT_RERANKER_WEIGHT))),
        device=str(config.get("device")).strip() if config.get("device") else None,
    )


def build_brief_text(brief: SearchBrief) -> str:
    location_targets = unique_preserving_order(
        [
            *brief.location_targets,
            brief.geography.location_name,
            brief.geography.country,
            *brief.geography.location_hints,
        ]
    )
    parts = [
        brief.role_title,
        brief.brief_summary,
        brief.document_text,
        "Titles: " + ", ".join(brief.titles) if brief.titles else "",
        "Target companies: " + ", ".join(brief.company_targets) if brief.company_targets else "",
        f"Company match mode: {brief.company_match_mode}" if brief.company_targets else "",
        "Hiring company: " + brief.hiring_company_name if brief.hiring_company_name else "",
        "Candidate interest required" if brief.candidate_interest_required else "",
        "Required skills: " + ", ".join(brief.required_keywords) if brief.required_keywords else "",
        "Preferred skills: " + ", ".join(brief.preferred_keywords) if brief.preferred_keywords else "",
        "Industry: " + ", ".join(brief.industry_keywords) if brief.industry_keywords else "",
        "Location: " + ", ".join(location_targets)
        if location_targets
        else "",
        (
            f"Experience range: {brief.minimum_years_experience or 0}"
            f"-{brief.maximum_years_experience if brief.maximum_years_experience is not None else 'any'} years"
        )
        if brief.minimum_years_experience is not None or brief.maximum_years_experience is not None
        else "",
        f"Years mode: {brief.years_mode}" if brief.years_mode else "",
    ]
    return "\n".join(part.strip() for part in parts if part and part.strip())


def build_candidate_text(candidate: CandidateProfile) -> str:
    experience_lines: List[str] = []
    for item in candidate.experience[:8]:
        title = ""
        raw_title = item.get("title") or item.get("role") or item.get("headline")
        if isinstance(raw_title, dict):
            title = str(raw_title.get("name", "")).strip()
        elif raw_title:
            title = str(raw_title).strip()
        company = item.get("company")
        if isinstance(company, dict):
            company = company.get("name")
        elif not company:
            company = item.get("company_name")
        company_text = str(company).strip() if company else ""
        if title or company_text:
            experience_lines.append(" - ".join(value for value in [title, company_text] if value))
        summary = str(item.get("summary") or item.get("description") or "").strip()
        if summary:
            experience_lines.append(summary)

    parts = [
        candidate.full_name,
        candidate.current_title,
        candidate.current_company,
        candidate.location_name,
        candidate.summary,
        candidate.industry or "",
        f"Years experience: {candidate.years_experience}" if candidate.years_experience is not None else "",
        "Matched titles: " + ", ".join(candidate.matched_titles) if candidate.matched_titles else "",
        "Matched companies: " + ", ".join(candidate.matched_companies) if candidate.matched_companies else "",
        "Experience:\n" + "\n".join(unique_preserving_order(experience_lines)) if experience_lines else "",
    ]
    return "\n".join(part.strip() for part in parts if part and part.strip())


class BaseRerankerBackend:
    model_name: str

    def score_pairs(self, pairs: Sequence[tuple[str, str]]) -> List[float]:
        raise NotImplementedError


class TransformersCrossEncoderBackend(BaseRerankerBackend):
    def __init__(self, model_name: str, device: str | None = None):
        try:
            import torch
            from transformers import AutoModelForSequenceClassification, AutoTokenizer
        except ImportError as exc:  # pragma: no cover - exercised through fallback behavior
            raise RuntimeError(
                "Reranker dependencies are missing. Run `uv sync --extra reranker`."
            ) from exc

        self.model_name = model_name
        self._torch = torch
        self._tokenizer = AutoTokenizer.from_pretrained(model_name)
        model = AutoModelForSequenceClassification.from_pretrained(model_name)
        selected_device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self._device = selected_device
        self._model = model.to(selected_device)
        self._model.eval()

    def score_pairs(self, pairs: Sequence[tuple[str, str]]) -> List[float]:
        if not pairs:
            return []
        tokenizer = self._tokenizer
        torch = self._torch
        encoded = tokenizer(
            [query for query, _ in pairs],
            [document for _, document in pairs],
            padding=True,
            truncation=True,
            max_length=1024,
            return_tensors="pt",
        )
        encoded = {key: value.to(self._device) for key, value in encoded.items()}
        with torch.no_grad():
            logits = self._model(**encoded).logits
        if getattr(logits, "ndim", 1) > 1:
            logits = logits[:, 0]
        raw_scores = logits.detach().cpu().tolist()
        return [float(1.0 / (1.0 + math.exp(-score))) for score in raw_scores]


@lru_cache(maxsize=4)
def _load_backend(model_name: str, device: str | None) -> BaseRerankerBackend:
    return TransformersCrossEncoderBackend(model_name=model_name, device=device)


def rerank_candidate(brief: SearchBrief, candidate: CandidateProfile) -> RerankerResult:
    settings = parse_reranker_settings(brief)
    if not settings.enabled:
        return RerankerResult()
    try:
        backend = _load_backend(settings.model_name, settings.device)
        score = backend.score_pairs([(build_brief_text(brief), build_candidate_text(candidate))])[0]
        return RerankerResult(score=round(score, 4), model_name=settings.model_name, enabled=True)
    except Exception as exc:  # pragma: no cover - runtime fallback
        return RerankerResult(
            score=0.0,
            model_name=settings.model_name,
            enabled=False,
            error=str(exc),
        )


def rerank_candidates(
    brief: SearchBrief,
    candidates: Sequence[CandidateProfile],
) -> List[CandidateProfile]:
    settings = parse_reranker_settings(brief)
    if not settings.enabled or not candidates:
        for candidate in candidates:
            if not candidate.ranking_model_version:
                candidate.ranking_model_version = "heuristic-anchor-ranker-v1"
        return list(candidates)

    rerank_window = list(candidates[: settings.top_n])
    passthrough = list(candidates[settings.top_n :])
    try:
        backend = _load_backend(settings.model_name, settings.device)
        query_text = build_brief_text(brief)
        pair_scores = backend.score_pairs(
            [(query_text, build_candidate_text(candidate)) for candidate in rerank_window]
        )
        for candidate, score in zip(rerank_window, pair_scores):
            candidate.reranker_score = round(float(score), 4)
            candidate.ranking_model_version = settings.model_name
            blended = (candidate.score * (1.0 - settings.weight)) + ((score * 100.0) * settings.weight)
            candidate.score = round(min(max(cap_candidate_score(blended, candidate), 0.0), 100.0), 2)
            candidate.verification_status = status_from_score(candidate.score)
            candidate.verification_notes = list(
                dict.fromkeys([*candidate.verification_notes, f"reranker:{settings.model_name}"])
            )
        rerank_window.sort(
            key=lambda candidate: (
                -candidate.score,
                -candidate.reranker_score,
                candidate.full_name.lower(),
            )
        )
    except Exception as exc:  # pragma: no cover - runtime fallback
        for candidate in rerank_window:
            candidate.reranker_score = 0.0
            candidate.ranking_model_version = f"heuristic-anchor-ranker-v1 (reranker unavailable: {exc})"
            candidate.verification_notes = list(
                dict.fromkeys([*candidate.verification_notes, "reranker:unavailable"])
            )
    return [*rerank_window, *passthrough]
