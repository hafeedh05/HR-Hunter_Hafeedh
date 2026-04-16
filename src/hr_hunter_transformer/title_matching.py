from __future__ import annotations

import re

from hr_hunter_transformer.models import SearchBrief
from hr_hunter_transformer.role_profiles import normalize_text


TITLE_TOKEN_STOPWORDS = {
    "a",
    "an",
    "and",
    "assistant",
    "associate",
    "chief",
    "country",
    "deputy",
    "global",
    "group",
    "head",
    "jr",
    "junior",
    "lead",
    "principal",
    "regional",
    "senior",
    "sr",
    "the",
    "vp",
}
TITLE_TOKEN_EXPANSIONS = {
    "ceo": ("chief", "executive", "officer"),
    "cfo": ("chief", "financial", "officer"),
    "coo": ("chief", "operating", "officer"),
    "cto": ("chief", "technology", "officer"),
    "cio": ("chief", "information", "officer"),
    "cmo": ("chief", "marketing", "officer"),
    "chro": ("chief", "human", "resources", "officer"),
    "md": ("managing", "director"),
    "gm": ("general", "manager"),
}


def title_tokens(value: str) -> set[str]:
    normalized = normalize_text(value)
    if not normalized:
        return set()
    expanded_parts: list[str] = []
    for token in re.split(r"[^a-z0-9]+", normalized):
        token = token.strip()
        if not token:
            continue
        expansion = TITLE_TOKEN_EXPANSIONS.get(token)
        if expansion:
            expanded_parts.extend(expansion)
        else:
            expanded_parts.append(token)
    return {
        token
        for token in expanded_parts
        if token and token not in TITLE_TOKEN_STOPWORDS and len(token) > 2
    }


def title_precision(candidate_title: str, target_title: str) -> float:
    candidate = title_tokens(candidate_title)
    target = title_tokens(target_title)
    if not candidate or not target:
        return 0.0
    overlap = candidate & target
    return round((2 * len(overlap)) / max(1, len(candidate) + len(target)), 4)


def title_coverage(candidate_title: str, target_title: str) -> float:
    candidate = title_tokens(candidate_title)
    target = title_tokens(target_title)
    if not candidate or not target:
        return 0.0
    overlap = candidate & target
    return round(len(overlap) / max(1, len(target)), 4)


def best_requested_title_precision(candidate_title: str, brief: SearchBrief) -> float:
    requested_titles = [brief.role_title, *brief.titles]
    best = 0.0
    for requested_title in requested_titles:
        best = max(best, title_precision(candidate_title, requested_title))
    return round(best, 4)


def best_requested_title_coverage(candidate_title: str, brief: SearchBrief) -> float:
    requested_titles = [brief.role_title, *brief.titles]
    best = 0.0
    for requested_title in requested_titles:
        best = max(best, title_coverage(candidate_title, requested_title))
    return round(best, 4)


def canonical_title_precision(candidate_title: str, brief: SearchBrief) -> float:
    return title_precision(candidate_title, brief.role_title)


def canonical_title_coverage(candidate_title: str, brief: SearchBrief) -> float:
    return title_coverage(candidate_title, brief.role_title)


def blended_title_precision(candidate_title: str, brief: SearchBrief) -> float:
    requested = best_requested_title_precision(candidate_title, brief)
    canonical = canonical_title_precision(candidate_title, brief)
    return round(min(1.0, (0.72 * requested) + (0.28 * canonical)), 4)


def adjacent_title_gap(candidate_title: str, brief: SearchBrief) -> float:
    requested = best_requested_title_precision(candidate_title, brief)
    canonical = canonical_title_precision(candidate_title, brief)
    return round(max(0.0, requested - canonical), 4)
