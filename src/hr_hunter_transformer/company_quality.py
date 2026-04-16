from __future__ import annotations

import re

from hr_hunter_transformer.role_profiles import normalize_text


COMPANY_HINT_TOKENS = {
    "associates",
    "capital",
    "consulting",
    "consultants",
    "contracting",
    "corp",
    "corporation",
    "developments",
    "engineering",
    "enterprise",
    "enterprises",
    "global",
    "group",
    "holdings",
    "inc",
    "industries",
    "international",
    "llc",
    "limited",
    "ltd",
    "partners",
    "properties",
    "retail",
    "solutions",
    "studio",
    "technologies",
    "technology",
    "trading",
}
GENERIC_DESCRIPTOR_TOKENS = {
    "architecture",
    "bachelor",
    "bachelors",
    "browse",
    "career",
    "careers",
    "college",
    "degree",
    "design",
    "decor",
    "education",
    "furniture",
    "home",
    "interiors",
    "follow",
    "interior",
    "job",
    "jobs",
    "lifestyle",
    "profile",
    "project",
    "projects",
    "school",
    "team",
    "university",
    "view",
}
GENERIC_COMPANY_LITERALS = {
    "@",
    "&",
    "+",
    "at",
    "board",
    "ceo",
    "chief executive officer",
    "co",
    "co.",
    "company",
    "confidential",
    "current",
    "dr",
    "educational",
    "experience",
    "growth",
    "he",
    "including",
    "intern",
    "leadership",
    "management",
    "middle",
    "mr",
    "mrs",
    "ms",
    "present",
    "profile",
    "she",
    "strategy",
    "transform",
    "we",
}
MONTH_ONLY_LITERALS = {
    "jan",
    "feb",
    "mar",
    "apr",
    "may",
    "jun",
    "jul",
    "aug",
    "sep",
    "oct",
    "nov",
    "dec",
}
AFFILIATION_TOKENS = {
    "advisor",
    "advisory",
    "consultant",
    "consultants",
    "faculty",
    "freelance",
    "lecturer",
    "student",
    "trainer",
}
EDUCATION_ORG_TOKENS = {
    "academy",
    "campus",
    "college",
    "institute",
    "school",
    "university",
}
LOCATION_LIKE_COMPANY_LITERALS = {
    "abu dhabi",
    "bahrain",
    "doha",
    "dubai",
    "gcc",
    "jeddah",
    "kuwait",
    "malaysia",
    "mea",
    "mena",
    "middle east",
    "qatar",
    "riyadh",
    "saudi arabia",
    "uae",
    "united arab emirates",
}
ROLE_LIKE_TOKENS = {
    "accountant",
    "analyst",
    "architect",
    "architecture",
    "board",
    "chairman",
    "designer",
    "director",
    "engineer",
    "executive",
    "lead",
    "leader",
    "leadership",
    "logistics",
    "management",
    "manager",
    "officer",
    "operations",
    "planner",
    "planning",
    "president",
    "process",
    "procurement",
    "project",
    "projects",
    "specialist",
    "strategy",
    "supply",
    "vice",
}
SOFT_BAD_PHRASES = (
    "bachelors in",
    "browse jobs",
    "find jobs",
    "follow ",
    "view manager",
    "view org chart",
    "view profile",
    "work experience",
    "complete logistics and supply chain process",
    "leading strategy",
)
HARD_BAD_PHRASES = (
    "chairman and acting ceo",
    "manager at",
    "engineer at",
    "planner at",
    " is a ",
)
MONTH_YEAR_RE = re.compile(r"^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(-\d{2}|\s+\d{2,4})$", re.IGNORECASE)


def sanitize_company_name(value: str) -> str:
    cleaned = str(value or "").strip()
    cleaned = re.sub(r"^[\s\-–—|,.;:]+", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def company_tokens(value: str) -> list[str]:
    normalized = normalize_text(value)
    return [token for token in re.split(r"[^a-z0-9]+", normalized) if token]


def looks_like_bad_company(value: str, current_title: str = "") -> bool:
    raw_value = str(value or "").strip()
    has_fragment_prefix = bool(re.match(r"^[\s\-–—|,.;:]+[A-Za-z0-9]", raw_value))
    cleaned = sanitize_company_name(value)
    lowered = normalize_text(cleaned)
    if not cleaned:
        return True
    if lowered in GENERIC_COMPANY_LITERALS or lowered in LOCATION_LIKE_COMPANY_LITERALS or lowered in MONTH_ONLY_LITERALS:
        return True
    if len(cleaned) <= 2:
        return True
    if re.fullmatch(r"\d{4}", cleaned):
        return True
    if MONTH_YEAR_RE.fullmatch(cleaned):
        return True
    if re.fullmatch(r"[()@&+\-./]+", cleaned):
        return True
    if cleaned.count("(") != cleaned.count(")"):
        return True
    if cleaned.endswith(("&", "+", "/", "|", "-", "–", "—")):
        return True
    if re.search(r"[(][A-Za-z]{0,3}$", cleaned):
        return True
    if lowered.startswith(("i ", "we ")):
        return True
    if current_title and lowered == normalize_text(current_title):
        return True
    if any(phrase in lowered for phrase in HARD_BAD_PHRASES):
        return True
    if any(phrase in lowered for phrase in ("chief executive officer", "machine learning engineer", "ai engineer", "llm engineer")):
        return True
    tokens = company_tokens(cleaned)
    if not tokens:
        return True
    if "including" in tokens:
        return True
    if cleaned.startswith("/"):
        return True
    if any(token in EDUCATION_ORG_TOKENS for token in tokens) and any(token in AFFILIATION_TOKENS for token in tokens):
        return True
    hint_count = sum(1 for token in tokens if token in COMPANY_HINT_TOKENS)
    role_like_count = sum(1 for token in tokens if token in ROLE_LIKE_TOKENS)
    if has_fragment_prefix and hint_count == 0 and len(tokens) <= 2:
        return True
    if any(token in AFFILIATION_TOKENS for token in tokens) and len(tokens) <= 2 and hint_count == 0:
        return True
    descriptor_count = sum(1 for token in tokens if token in GENERIC_DESCRIPTOR_TOKENS)
    if any(token in EDUCATION_ORG_TOKENS for token in tokens) and len(tokens) >= 6 and hint_count == 0 and descriptor_count >= 1:
        return True
    distinctive_tokens = [
        token
        for token in tokens
        if token not in GENERIC_DESCRIPTOR_TOKENS
        and token not in ROLE_LIKE_TOKENS
        and token not in COMPANY_HINT_TOKENS
    ]
    if not distinctive_tokens and hint_count == 0:
        return True
    if all(len(token) == 1 for token in tokens) and hint_count == 0:
        return True
    if role_like_count >= max(2, len(tokens) - 1) and hint_count == 0:
        return True
    return False


def company_quality_score(value: str, current_title: str = "", role_family: str = "") -> float:
    cleaned = sanitize_company_name(value)
    if looks_like_bad_company(value, current_title):
        return 0.0

    tokens = company_tokens(cleaned)
    if not tokens:
        return 0.0

    lowered = normalize_text(cleaned)
    score = 0.96
    hint_count = sum(1 for token in tokens if token in COMPANY_HINT_TOKENS)
    role_like_count = sum(1 for token in tokens if token in ROLE_LIKE_TOKENS)
    role_ratio = role_like_count / max(1, len(tokens))
    descriptor_count = sum(1 for token in tokens if token in GENERIC_DESCRIPTOR_TOKENS)
    distinctive_tokens = [
        token
        for token in tokens
        if token not in GENERIC_DESCRIPTOR_TOKENS
        and token not in ROLE_LIKE_TOKENS
        and token not in COMPANY_HINT_TOKENS
    ]

    if len(tokens) == 1:
        token = tokens[0]
        if len(token) <= 3 and not cleaned.isupper():
            score -= 0.28
        if "." in cleaned and not cleaned.replace(".", "").isupper():
            score -= 0.18

    if len(tokens) >= 5 and hint_count == 0:
        score -= 0.18
    if role_ratio >= 0.5 and hint_count == 0:
        score -= 0.28
    elif role_ratio >= 0.34 and hint_count == 0:
        score -= 0.14
    if descriptor_count >= max(2, len(tokens) - 1) and hint_count == 0:
        score -= 0.34
    elif descriptor_count >= 2 and not distinctive_tokens and hint_count == 0:
        score -= 0.22
    if all(len(token) == 1 for token in tokens) and hint_count == 0:
        score -= 0.5

    if any(phrase in lowered for phrase in SOFT_BAD_PHRASES):
        score -= 0.5
    if any(token in EDUCATION_ORG_TOKENS for token in tokens) and any(token in AFFILIATION_TOKENS for token in tokens):
        score -= 0.42
    if re.search(r"[^A-Za-z0-9&()'./,\- ]", cleaned):
        score -= 0.12
    if cleaned.startswith(("-", "–", "—", "|")):
        score -= 0.1
    if hint_count:
        score += min(0.06, hint_count * 0.02)

    if role_family in {"executive", "design_architecture"} and role_ratio >= 0.34 and hint_count == 0:
        score -= 0.08
    if role_family == "supply_chain" and role_ratio >= 0.5 and hint_count == 0:
        score -= 0.08

    return round(max(0.0, min(1.0, score)), 4)
