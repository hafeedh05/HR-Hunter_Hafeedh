from __future__ import annotations

import re

from hr_hunter_transformer.taxonomy import family_hints_map, load_taxonomy, resolve_subfamily

ROLE_FAMILIES: dict[str, tuple[str, ...]] = family_hints_map()


TECHNICAL_SOURCES = {
    "github.com",
    "gitlab.com",
    "huggingface.co",
    "kaggle.com",
    "stackoverflow.com",
    "dev.to",
    "medium.com",
}

PROFESSIONAL_SOURCES = {
    "linkedin.com",
    "ae.linkedin.com",
    "sa.linkedin.com",
    "in.linkedin.com",
    "pk.linkedin.com",
    "people.bayt.com",
    "theorg.com",
    "rocketreach.co",
    "signalhire.com",
    "apollo.io",
    "contactout.com",
    "wellfound.com",
    "clenchedfist.co",
    "navitalglobal.com",
    "behance.net",
    "archinect.com",
    "architizer.com",
}


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9+/#&.-]+", " ", str(value or "").lower())).strip()


def infer_role_family_with_confidence(*values: str) -> tuple[str, float]:
    haystack = " ".join(normalize_text(value) for value in values if str(value).strip())
    if not haystack:
        return "other", 0.0
    haystack_tokens = set(token for token in haystack.split(" ") if token)
    best_family = "other"
    best_score = 0.0
    for family, hints in ROLE_FAMILIES.items():
        family_score = 0.0
        for hint in hints:
            normalized_hint = normalize_text(hint)
            if not normalized_hint:
                continue
            if normalized_hint in haystack:
                family_score = max(family_score, 1.0 if normalized_hint == haystack else 0.92)
                continue
            hint_tokens = set(token for token in normalized_hint.split(" ") if token)
            if not hint_tokens:
                continue
            overlap = len(haystack_tokens & hint_tokens)
            if overlap <= 0:
                continue
            family_score = max(
                family_score,
                min(0.88, 0.34 + 0.54 * (overlap / max(1, len(hint_tokens)))),
            )
        if family_score > best_score:
            best_family = family
            best_score = family_score
    if best_score < 0.34:
        return "other", round(best_score, 4)
    return best_family, round(best_score, 4)


def infer_role_family(*values: str) -> str:
    return infer_role_family_with_confidence(*values)[0]


def role_family_hints(role_family: str) -> tuple[str, ...]:
    return ROLE_FAMILIES.get(role_family, ())


def role_subfamily(role_family: str, title_value: str) -> str:
    return resolve_subfamily(role_family, title_value, normalize_text)


def title_variants(role_family: str, role_title: str, titles: list[str] | tuple[str, ...]) -> list[str]:
    taxonomy = load_taxonomy()
    variants: list[str] = []
    for value in [role_title, *titles]:
        normalized = normalize_text(value)
        if value and normalized and normalized not in {normalize_text(existing) for existing in variants}:
            variants.append(str(value))
    for aliases in taxonomy.get(role_family, {}).values():
        for alias in aliases:
            normalized = normalize_text(alias)
            if normalized and normalized not in {normalize_text(existing) for existing in variants}:
                variants.append(alias)
    return variants
