from __future__ import annotations

import math
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Callable, Iterable, List, Sequence

from hr_hunter.briefing import unique_preserving_order
from hr_hunter.candidate_order import candidate_priority_sort_tuple
from hr_hunter.models import CandidateProfile, SearchBrief
from hr_hunter.ranker import cap_candidate_score, status_from_score


DEFAULT_RERANKER_MODEL = "disabled"
PLANNED_RERANKER_MODEL = "BAAI/bge-reranker-v2-m3"
DEFAULT_RERANKER_TOP_N = 40
DEFAULT_RERANKER_WEIGHT = 0.35
RERANKER_MAX_LENGTH = 384
RERANKER_CPU_BATCH_SIZE = 12
RERANKER_GPU_BATCH_SIZE = 32
MAX_BRIEF_SUMMARY_CHARS = 800
MAX_BRIEF_DOC_CHARS = 1200
MAX_CANDIDATE_SUMMARY_CHARS = 520
MAX_EXPERIENCE_SUMMARY_CHARS = 180
DEFAULT_MIN_TOTAL_MEMORY_GB = 3.0
DEFAULT_MIN_AVAILABLE_MEMORY_GB = 1.25


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


def _memory_snapshot_bytes() -> tuple[int | None, int | None]:
    meminfo: dict[str, int] = {}
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as handle:
            for line in handle:
                key, _, raw_value = line.partition(":")
                amount = raw_value.strip().split()[0] if raw_value.strip() else ""
                if amount.isdigit():
                    meminfo[key] = int(amount) * 1024
    except OSError:
        meminfo = {}
    total_bytes = meminfo.get("MemTotal")
    available_bytes = meminfo.get("MemAvailable")
    if total_bytes is None:
        try:
            page_size = int(os.sysconf("SC_PAGE_SIZE"))
            page_count = int(os.sysconf("SC_PHYS_PAGES"))
            total_bytes = page_size * page_count
        except (AttributeError, OSError, ValueError):
            total_bytes = None
    return total_bytes, available_bytes


def _format_gib(value_bytes: int | None) -> str:
    if value_bytes is None or value_bytes <= 0:
        return "unknown"
    return f"{value_bytes / (1024 ** 3):.2f} GiB"


def _low_memory_reranker_override_enabled() -> bool:
    return str(os.getenv("HR_HUNTER_RERANKER_ALLOW_LOW_MEMORY", "false")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _ensure_transformer_reranker_memory_budget(device: str | None) -> None:
    if "cuda" in str(device or "").lower() or _low_memory_reranker_override_enabled():
        return
    total_bytes, available_bytes = _memory_snapshot_bytes()
    min_total_bytes = max(
        0,
        int(
            _safe_float(
                os.getenv("HR_HUNTER_RERANKER_MIN_TOTAL_MEMORY_GB", DEFAULT_MIN_TOTAL_MEMORY_GB),
                DEFAULT_MIN_TOTAL_MEMORY_GB,
            )
            * (1024 ** 3)
        ),
    )
    min_available_bytes = max(
        0,
        int(
            _safe_float(
                os.getenv("HR_HUNTER_RERANKER_MIN_AVAILABLE_MEMORY_GB", DEFAULT_MIN_AVAILABLE_MEMORY_GB),
                DEFAULT_MIN_AVAILABLE_MEMORY_GB,
            )
            * (1024 ** 3)
        ),
    )
    if total_bytes is not None and total_bytes < min_total_bytes:
        raise RuntimeError(
            "Transformer reranker skipped on low-memory host "
            f"(total={_format_gib(total_bytes)}, available={_format_gib(available_bytes)}, "
            f"requires at least {min_total_bytes / (1024 ** 3):.2f} GiB total RAM)."
        )
    if available_bytes is not None and available_bytes < min_available_bytes:
        raise RuntimeError(
            "Transformer reranker skipped because available memory is too low "
            f"(total={_format_gib(total_bytes)}, available={_format_gib(available_bytes)}, "
            f"requires at least {min_available_bytes / (1024 ** 3):.2f} GiB free RAM)."
        )


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
        _clip_text(brief.brief_summary, MAX_BRIEF_SUMMARY_CHARS),
        _clip_text(brief.document_text, MAX_BRIEF_DOC_CHARS),
        "Titles: " + ", ".join(brief.titles) if brief.titles else "",
        "Target companies: " + ", ".join(brief.company_targets) if brief.company_targets else "",
        f"Company match mode: {brief.company_match_mode}" if brief.company_targets else "",
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
        summary = _clip_text(str(item.get("summary") or item.get("description") or "").strip(), MAX_EXPERIENCE_SUMMARY_CHARS)
        if summary:
            experience_lines.append(summary)

    parts = [
        candidate.full_name,
        candidate.current_title,
        candidate.current_company,
        candidate.location_name,
        _clip_text(candidate.summary, MAX_CANDIDATE_SUMMARY_CHARS),
        candidate.industry or "",
        f"Years experience: {candidate.years_experience}" if candidate.years_experience is not None else "",
        "Matched titles: " + ", ".join(candidate.matched_titles) if candidate.matched_titles else "",
        "Matched companies: " + ", ".join(candidate.matched_companies) if candidate.matched_companies else "",
        "Experience:\n" + "\n".join(unique_preserving_order(experience_lines)) if experience_lines else "",
    ]
    return "\n".join(part.strip() for part in parts if part and part.strip())


class BaseRerankerBackend:
    model_name: str

    def score_pairs(
        self,
        pairs: Sequence[tuple[str, str]],
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> List[float]:
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
        allow_download = str(os.getenv("HR_HUNTER_RERANKER_ALLOW_DOWNLOAD", "false")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        selected_device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        _ensure_transformer_reranker_memory_budget(selected_device)
        self._tokenizer = AutoTokenizer.from_pretrained(
            model_name,
            local_files_only=not allow_download,
        )
        model = AutoModelForSequenceClassification.from_pretrained(
            model_name,
            local_files_only=not allow_download,
            low_cpu_mem_usage=True,
        )
        self._device = selected_device
        self._model = model.to(selected_device)
        self._model.eval()

    def score_pairs(
        self,
        pairs: Sequence[tuple[str, str]],
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> List[float]:
        if not pairs:
            return []
        tokenizer = self._tokenizer
        torch = self._torch
        batch_size = RERANKER_GPU_BATCH_SIZE if "cuda" in str(self._device).lower() else RERANKER_CPU_BATCH_SIZE
        all_scores: List[float] = []
        total_pairs = len(pairs)
        if progress_callback:
            progress_callback(0, total_pairs)
        for offset in range(0, len(pairs), batch_size):
            batch = pairs[offset : offset + batch_size]
            encoded = tokenizer(
                [query for query, _ in batch],
                [document for _, document in batch],
                padding=True,
                truncation=True,
                max_length=RERANKER_MAX_LENGTH,
                return_tensors="pt",
            )
            encoded = {key: value.to(self._device) for key, value in encoded.items()}
            with torch.no_grad():
                logits = self._model(**encoded).logits
            if getattr(logits, "ndim", 1) > 1:
                logits = logits[:, 0]
            raw_scores = logits.detach().cpu().tolist()
            all_scores.extend(float(1.0 / (1.0 + math.exp(-score))) for score in raw_scores)
            if progress_callback:
                progress_callback(min(total_pairs, offset + len(batch)), total_pairs)
        return all_scores


def _clip_text(value: str | None, max_chars: int) -> str:
    text = str(value or "").strip()
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 1)].rstrip() + "…"


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
    progress_callback: Callable[[int, int], None] | None = None,
) -> List[CandidateProfile]:
    settings = parse_reranker_settings(brief)
    if not settings.enabled or not candidates:
        for candidate in candidates:
            if not candidate.ranking_model_version:
                candidate.ranking_model_version = "heuristic-anchor-ranker-v1"
        return list(candidates)

    ordered_candidates = sorted(
        list(candidates),
        key=lambda candidate: candidate_priority_sort_tuple(candidate, brief, phase="rerank"),
    )
    rerank_window = list(ordered_candidates[: settings.top_n])
    passthrough = list(ordered_candidates[settings.top_n :])
    try:
        backend = _load_backend(settings.model_name, settings.device)
        query_text = build_brief_text(brief)
        pairs = [(query_text, build_candidate_text(candidate)) for candidate in rerank_window]
        pair_scores: List[float] = []
        total_pairs = len(pairs)
        if progress_callback:
            progress_callback(0, total_pairs)
        if total_pairs:
            backend_device = str(getattr(backend, "_device", "")).lower()
            chunk_size = 80 if "cuda" in backend_device else 24
            completed_pairs = 0
            for offset in range(0, total_pairs, chunk_size):
                chunk = pairs[offset : offset + chunk_size]
                chunk_progress = None
                if progress_callback:
                    def _chunk_progress(done: int, _total: int, *, base_offset: int = offset) -> None:
                        progress_callback(min(total_pairs, max(0, base_offset + int(done or 0))), total_pairs)
                    chunk_progress = _chunk_progress
                try:
                    chunk_scores = backend.score_pairs(chunk, progress_callback=chunk_progress)
                except TypeError:
                    chunk_scores = backend.score_pairs(chunk)
                pair_scores.extend(chunk_scores)
                completed_pairs += len(chunk)
                if progress_callback:
                    progress_callback(min(total_pairs, completed_pairs), total_pairs)
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
        total_pairs = len(rerank_window)
        if progress_callback:
            progress_callback(total_pairs, total_pairs)
        for candidate in rerank_window:
            lexical_proxy_score = max(
                0.0,
                min(
                    1.0,
                    (candidate.semantic_similarity_score * 0.6)
                    + (candidate.title_similarity_score * 0.25)
                    + (candidate.skill_overlap_score * 0.15),
                ),
            )
            candidate.reranker_score = round(lexical_proxy_score, 4)
            candidate.ranking_model_version = "fallback-lexical-reranker-v1"
            blended = (candidate.score * (1.0 - settings.weight)) + ((lexical_proxy_score * 100.0) * settings.weight)
            candidate.score = round(min(max(cap_candidate_score(blended, candidate), 0.0), 100.0), 2)
            candidate.verification_status = status_from_score(candidate.score)
            candidate.verification_notes = list(
                dict.fromkeys([*candidate.verification_notes, f"reranker:fallback ({exc})"])
            )
    return [*rerank_window, *passthrough]
