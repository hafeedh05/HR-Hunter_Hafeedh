from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

from hr_hunter.briefing import merge_company_aliases, normalize_text, unique_preserving_order
from hr_hunter.geo import distance_from_center
from hr_hunter.models import CandidateProfile, SearchBrief


TITLE_FAMILY_KEYWORDS = {
    "executive": [
        "chief executive officer",
        "ceo",
        "managing director",
        "md",
        "president",
        "group ceo",
        "regional ceo",
        "country ceo",
        "general manager",
    ],
    "brand": ["brand manager", "brand lead", "brand director", "head of brand"],
    "category": ["category manager", "category lead", "category director"],
    "marketing": [
        "digital marketing",
        "growth marketing",
        "performance marketing",
        "paid media",
        "demand generation",
        "lifecycle marketing",
        "crm marketing",
        "marketing manager",
        "marketing director",
        "director of marketing",
        "head of marketing",
    ],
    "product_marketing": [
        "product marketing",
        "product marketing manager",
        "product marketing director",
        "head of product marketing",
    ],
    "portfolio": ["portfolio manager", "product portfolio", "portfolio lead", "portfolio director"],
    "innovation": ["innovation manager", "innovation lead", "commercialization manager", "proposition manager"],
    "shopper_marketing": [
        "shopper marketing manager",
        "customer marketing manager",
        "trade marketing manager",
        "commercial category manager",
        "category and insights manager",
    ],
    "product": ["product manager", "global product", "product director"],
}
GENERIC_PRODUCT_FAMILY = {"product"}
TITLE_TOKEN_STOPWORDS = {
    "and",
    "director",
    "global",
    "head",
    "lead",
    "manager",
    "of",
    "senior",
    "vice",
}
NON_PERSON_NAME_TOKENS = {
    "about",
    "blog",
    "brand",
    "careers",
    "company",
    "contact",
    "free",
    "info",
    "leadership",
    "management",
    "our",
    "people",
    "profile",
    "speaker",
    "speakers",
    "staff",
    "team",
}
TITLE_PROXIMITY_STOPWORDS = {
    "a",
    "an",
    "and",
    "director",
    "for",
    "global",
    "head",
    "lead",
    "manager",
    "of",
    "senior",
    "the",
    "to",
}
TITLE_ALIAS_EXPANSIONS = {
    "ceo": {"chief", "executive", "officer"},
    "chief executive officer": {"ceo"},
    "md": {"managing", "director"},
    "managing director": {"md"},
    "gm": {"general", "manager"},
    "general manager": {"gm"},
}
LOW_SENIORITY_KEYWORDS = [
    "assistant",
    "associate",
    "specialist",
    "coordinator",
    "executive",
]
OFF_FUNCTION_KEYWORDS = [
    "sales",
    "customer development",
    "content",
    "digital",
    "ecommerce",
    "communications",
    "commercial excellence",
    "specialist",
    "coordinator",
]
KEYWORD_MATCH_STOPWORDS = {
    "a",
    "an",
    "and",
    "for",
    "in",
    "of",
    "on",
    "the",
    "to",
    "with",
}
SKILL_KEYWORD_VARIANTS = {
    "ga4": ["google analytics 4", "google analytics"],
    "google analytics 4": ["ga4", "google analytics"],
    "meta ads": ["facebook ads", "instagram ads", "paid social"],
    "google ads": ["google adwords", "search ads", "ppc"],
    "campaign optimisation": ["campaign optimization", "campaign management"],
    "campaign optimization": ["campaign optimisation", "campaign management"],
    "lead generation": ["demand generation", "pipeline generation"],
    "stakeholder management": ["stakeholder engagement", "cross functional collaboration", "cross-functional collaboration"],
    "ecommerce": ["e-commerce", "online retail", "marketplace"],
    "e-commerce": ["ecommerce", "online retail", "marketplace"],
}
INDUSTRY_KEYWORD_VARIANTS = {
    "consumer": ["consumer goods", "b2c", "beauty", "cosmetics", "lifestyle"],
    "consumer goods": ["consumer", "fmcg", "cpg"],
    "fmcg": ["consumer goods", "cpg", "consumer"],
    "cpg": ["consumer goods", "fmcg", "consumer"],
    "ecommerce": ["e-commerce", "online retail", "marketplace", "digital commerce"],
    "e-commerce": ["ecommerce", "online retail", "marketplace", "digital commerce"],
    "home furnishings": ["furniture", "interiors", "home decor", "homeware"],
    "premium retail": ["luxury retail", "design led retail", "design-led retail"],
}
GENERIC_COMPANY_FRAGMENT_TOKENS = {
    *LOW_SENIORITY_KEYWORDS,
    "acquisition",
    "ads",
    "adwords",
    "analytics",
    "automation",
    "brand",
    "business",
    "campaign",
    "commerce",
    "communications",
    "content",
    "coordinator",
    "crm",
    "customer",
    "data",
    "demand",
    "designer",
    "developer",
    "digital",
    "display",
    "ecommerce",
    "email",
    "engagement",
    "engineer",
    "ga4",
    "generation",
    "google",
    "growth",
    "insights",
    "lead",
    "leadership",
    "lifecycle",
    "manager",
    "marketing",
    "media",
    "meta",
    "paid",
    "performance",
    "ppc",
    "product",
    "reporting",
    "retention",
    "sales",
    "scientist",
    "search",
    "seo",
    "social",
    "specialist",
    "strategy",
}
COMPANY_ENTITY_HINTS = {
    "bank",
    "brands",
    "capital",
    "company",
    "corp",
    "corporation",
    "cosmetics",
    "furniture",
    "global",
    "group",
    "holdings",
    "home",
    "homes",
    "industries",
    "interiors",
    "labs",
    "limited",
    "ltd",
    "retail",
    "studio",
    "studios",
    "technologies",
    "technology",
    "ventures",
}
TITLE_ROLE_SIGNAL_MAP = {
    "executive": [
        "chief executive officer",
        "ceo",
        "managing director",
        "president",
        "group ceo",
        "country ceo",
        "general manager",
    ],
    "brand": ["brand"],
    "category": ["category", "category development", "category insights"],
    "marketing": [
        "digital marketing",
        "growth marketing",
        "performance marketing",
        "paid media",
        "demand generation",
        "marketing",
    ],
    "product_marketing": ["product marketing"],
    "portfolio": ["portfolio", "commercialization", "proposition"],
    "innovation": ["innovation", "commercialization", "proposition"],
    "shopper_marketing": [
        "shopper marketing",
        "customer marketing",
        "trade marketing",
        "commercial category",
        "category and insights",
    ],
    "product": ["product", "product development"],
}
ADJACENT_TITLE_FAMILY_MAP = {
    "brand": {"category", "innovation", "product_marketing", "shopper_marketing"},
    "category": {"brand", "innovation", "product_marketing", "shopper_marketing"},
    "marketing": {"product_marketing", "shopper_marketing"},
    "innovation": {"brand", "category", "portfolio", "product", "product_marketing"},
    "portfolio": {"innovation", "product", "product_marketing"},
    "product": {"innovation", "portfolio", "product_marketing"},
    "product_marketing": {"brand", "category", "marketing", "portfolio", "product", "shopper_marketing"},
    "shopper_marketing": {"brand", "category", "marketing", "product_marketing"},
}
SEMANTIC_STOPWORDS = {
    "a",
    "an",
    "and",
    "at",
    "for",
    "in",
    "of",
    "on",
    "the",
    "to",
    "with",
}
YEAR_PATTERN = re.compile(r"(19|20)\d{2}")
OPEN_TO_WORK_PHRASES = [
    "open to work",
    "open for work",
    "actively looking",
    "actively seeking",
    "available immediately",
    "available for work",
    "currently seeking",
    "looking for opportunities",
    "seeking opportunities",
    "seeking new opportunities",
    "between roles",
]
COMPANY_TOKEN_STOPWORDS = {
    "and",
    "co",
    "company",
    "corp",
    "corporation",
    "group",
    "holding",
    "holdings",
    "inc",
    "limited",
    "llc",
    "ltd",
    "plc",
    "sa",
    "the",
}


@dataclass
class FeatureBuildResult:
    feature_scores: Dict[str, float]
    notes: List[str] = field(default_factory=list)
    matched_titles: List[str] = field(default_factory=list)
    matched_companies: List[str] = field(default_factory=list)
    matched_title_family: str = ""
    location_bucket: str = "unknown_location"
    current_target_company_match: bool = False
    target_company_history_match: bool = False
    current_title_match: bool = False
    industry_aligned: bool = False
    location_aligned: bool = False
    off_function_blocked: bool = False
    disqualifier_reasons: List[str] = field(default_factory=list)
    exclude_hits: List[str] = field(default_factory=list)
    low_seniority_hits: int = 0
    years_experience: Optional[float] = None
    years_experience_gap: Optional[float] = None
    employment_status_match: bool = True
    employment_state: str = "unknown"


def parse_year(value: object) -> Optional[int]:
    if value is None:
        return None
    match = YEAR_PATTERN.search(str(value))
    if not match:
        return None
    return int(match.group(0))


def derive_years_experience(candidate: CandidateProfile) -> Optional[float]:
    if candidate.years_experience is not None:
        return candidate.years_experience

    starts: List[int] = []
    latest_end = datetime.now(timezone.utc).year
    for item in candidate.experience:
        start_year = parse_year(item.get("start_date") or item.get("start"))
        end_year = parse_year(item.get("end_date") or item.get("end"))
        if start_year:
            starts.append(start_year)
        if end_year:
            latest_end = max(latest_end, end_year)

    if not starts:
        return None

    candidate.years_experience = float(max(0, latest_end - min(starts)))
    return candidate.years_experience


def experience_text_parts(candidate: CandidateProfile) -> List[str]:
    parts: List[str] = [
        candidate.current_title,
        candidate.current_company,
        candidate.summary,
        candidate.industry or "",
    ]
    for item in candidate.experience:
        for key in ("title", "role", "headline", "summary", "description", "industry"):
            value = item.get(key)
            if value:
                parts.append(str(value))
        company = item.get("company")
        if isinstance(company, dict):
            company = company.get("name")
        elif not company:
            company = item.get("company_name")
        if company:
            parts.append(str(company))
    return parts


def _flatten_text_values(value: object) -> List[str]:
    parts: List[str] = []
    if isinstance(value, str):
        if value.strip():
            parts.append(value)
        return parts
    if isinstance(value, dict):
        for nested in value.values():
            parts.extend(_flatten_text_values(nested))
        return parts
    if isinstance(value, list):
        for nested in value:
            parts.extend(_flatten_text_values(nested))
        return parts
    if value is not None:
        parts.append(str(value))
    return parts


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "present", "current"}


def candidate_text_parts(candidate: CandidateProfile) -> List[str]:
    parts = experience_text_parts(candidate)
    for evidence in candidate.evidence_records:
        parts.extend(
            [
                evidence.title,
                evidence.snippet,
                evidence.source_domain,
                evidence.location_match_text,
            ]
        )
        parts.extend(_flatten_text_values(evidence.raw))
    parts.extend(_flatten_text_values(candidate.raw))
    return [part for part in parts if str(part).strip()]


def has_current_employment(candidate: CandidateProfile) -> bool:
    if candidate.current_employment_confirmed:
        return True
    if candidate.current_company.strip():
        return True
    if any(record.current_employment_signal for record in candidate.evidence_records):
        return True
    for item in candidate.experience:
        if _truthy(item.get("is_current")) or _truthy(item.get("current")):
            return True
        end_value = normalize_text(str(item.get("end_date") or item.get("end") or ""))
        if end_value in {"present", "current", "now", "today"}:
            return True
    return False


def has_open_to_work_signal(candidate: CandidateProfile, text_parts: List[str]) -> bool:
    haystack = normalize_text(" ".join(text_parts))
    return any(normalize_text(phrase) in haystack for phrase in OPEN_TO_WORK_PHRASES)


def evaluate_employment_status(
    candidate: CandidateProfile,
    brief: SearchBrief,
    text_parts: List[str],
) -> tuple[float, bool, str, List[str]]:
    notes: List[str] = []
    mode = brief.employment_status_mode
    current_employment = has_current_employment(candidate)
    open_to_work = has_open_to_work_signal(candidate, text_parts)

    if current_employment:
        state = "currently_employed"
    elif open_to_work:
        state = "open_to_work_signal"
    elif candidate.current_company.strip():
        state = "currently_employed"
    else:
        state = "not_currently_employed"

    if mode == "any":
        return 0.0, True, state, notes
    if mode == "currently_employed":
        if current_employment:
            notes.append("employment_status: currently_employed")
            return 1.0, True, state, notes
        notes.append("employment_status: current_employment_missing")
        return 0.0, False, state, notes
    if mode == "open_to_work_signal":
        if open_to_work:
            notes.append("employment_status: open_to_work_signal")
            return 1.0, True, state, notes
        notes.append("employment_status: open_to_work_missing")
        return 0.0, False, state, notes
    if not current_employment:
        if open_to_work:
            notes.append("employment_status: open_to_work_signal")
            return 1.0, True, state, notes
        notes.append("employment_status: inferred_not_currently_employed")
        return 0.82, True, state, notes
    notes.append("employment_status: currently_employed")
    return 0.0, False, state, notes


def title_families(value: str) -> set[str]:
    normalized = normalize_text(value)
    return {
        family
        for family, keywords in TITLE_FAMILY_KEYWORDS.items()
        if any(normalize_text(keyword) in normalized for keyword in keywords)
    }


def brief_target_title_families(brief: SearchBrief) -> set[str]:
    families: set[str] = set()
    for value in [*brief.titles, *brief.title_keywords]:
        families.update(title_families(value))
    return families


def brief_location_targets(brief: SearchBrief) -> List[str]:
    return unique_preserving_order(
        [
            *brief.location_targets,
            brief.geography.location_name,
            brief.geography.country,
            *brief.geography.location_hints,
        ]
    )


def location_priority_rank(target_locations: List[str], hint: str) -> int:
    normalized_hint = normalize_text(hint)
    if not normalized_hint:
        return len(target_locations)
    for index, value in enumerate(target_locations):
        if normalize_text(value) == normalized_hint:
            return index
    return len(target_locations)


def title_signal_tokens(value: str) -> set[str]:
    return {
        token
        for token in _expanded_title_tokens(value)
        if token and token not in TITLE_TOKEN_STOPWORDS
    }


def _expanded_title_tokens(value: str) -> set[str]:
    normalized = normalize_text(value)
    tokens = {
        token
        for token in normalized.split()
        if token and token not in TITLE_PROXIMITY_STOPWORDS
    }
    expanded = set(tokens)
    for phrase, extras in TITLE_ALIAS_EXPANSIONS.items():
        normalized_phrase = normalize_text(phrase)
        phrase_tokens = {
            token
            for token in normalized_phrase.split()
            if token and token not in TITLE_PROXIMITY_STOPWORDS
        }
        if not phrase_tokens:
            continue
        if normalized_phrase in normalized or phrase_tokens.issubset(tokens):
            expanded.update(
                token
                for token in extras
                if token and token not in TITLE_PROXIMITY_STOPWORDS
            )
    return expanded


def title_similarity(title: str, options: Iterable[str]) -> tuple[float, List[str]]:
    current_tokens = _expanded_title_tokens(title)
    if not current_tokens:
        return 0.0, []

    best_score = 0.0
    best_matches: List[str] = []
    for option in options:
        option_tokens = _expanded_title_tokens(option)
        if not option_tokens:
            continue
        overlap = current_tokens.intersection(option_tokens)
        if not overlap:
            continue
        score = len(overlap) / len(current_tokens.union(option_tokens))
        if len(overlap) >= 2:
            score += 0.2
        score = min(score, 1.0)
        if score > best_score + 1e-9:
            best_score = score
            best_matches = [option]
        elif abs(score - best_score) <= 1e-9:
            best_matches.append(option)
    return round(best_score, 3), unique_preserving_order(best_matches)


def company_text_matches(value: str, aliases: Iterable[str]) -> bool:
    normalized_value = normalize_text(value)
    if not normalized_value:
        return False
    value_tokens = {
        token
        for token in re.split(r"[^a-z0-9]+", normalized_value)
        if token and len(token) > 2 and token not in COMPANY_TOKEN_STOPWORDS
    }
    for alias in aliases:
        normalized_alias = normalize_text(alias)
        if not normalized_alias:
            continue
        if normalized_value == normalized_alias:
            return True
        alias_tokens = {
            token
            for token in re.split(r"[^a-z0-9]+", normalized_alias)
            if token and len(token) > 2 and token not in COMPANY_TOKEN_STOPWORDS
        }
        if len(alias_tokens) >= 2 and alias_tokens.issubset(value_tokens):
            return True
        if len(alias_tokens) == 1:
            token = next(iter(alias_tokens))
            if token in value_tokens and len(token) >= 6:
                return True
    return False


def extract_experience_companies(candidate: CandidateProfile) -> List[str]:
    companies = []
    for item in candidate.experience:
        company = item.get("company")
        if isinstance(company, dict):
            company = company.get("name")
        elif not company:
            company = item.get("company_name")
        if company:
            companies.append(str(company))
    return companies


def best_company_match(
    current_company: str,
    experience_companies: Iterable[str],
    aliases: Dict[str, List[str]],
    match_mode: str = "both",
) -> Dict[str, object]:
    result = {
        "score": 0.0,
        "matches": [],
        "current_match": False,
        "history_match": False,
        "history_matches": [],
    }
    current_allowed = match_mode in {"both", "current_only"}
    history_allowed = match_mode in {"both", "past_only"}

    for company, alias_values in aliases.items():
        if current_allowed and current_company and company_text_matches(current_company, alias_values):
            result["score"] = max(result["score"], 1.0)
            result["matches"] = [company]
            result["current_match"] = True
            continue

        if history_allowed and any(
            company_text_matches(experience_company, alias_values)
            for experience_company in experience_companies
        ):
            result["score"] = max(result["score"], 0.45)
            result["history_match"] = True
            if company not in result["history_matches"]:
                result["history_matches"].append(company)

    if not result["matches"] and result["history_matches"]:
        result["matches"] = list(result["history_matches"])

    return result


def keyword_hits(text_parts: Iterable[str], keywords: Iterable[str]) -> int:
    haystack = normalize_text(" ".join(part for part in text_parts if part))
    hits = 0
    for keyword in keywords:
        normalized_keyword = normalize_text(keyword)
        if normalized_keyword and normalized_keyword in haystack:
            hits += 1
    return hits


def _normalized_keyword_variants(keyword: str, variant_map: Dict[str, List[str]]) -> List[str]:
    normalized = normalize_text(keyword)
    if not normalized:
        return []
    variants = [normalized]
    variants.extend(normalize_text(value) for value in variant_map.get(normalized, []))
    if "optimisation" in normalized:
        variants.append(normalized.replace("optimisation", "optimization"))
    if "optimization" in normalized:
        variants.append(normalized.replace("optimization", "optimisation"))
    if "ecommerce" in normalized:
        variants.append(normalized.replace("ecommerce", "e commerce"))
    if "e commerce" in normalized:
        variants.append(normalized.replace("e commerce", "ecommerce"))
    return unique_preserving_order([value for value in variants if value])


def _keyword_match_strength(
    haystack: str,
    haystack_tokens: set[str],
    keyword: str,
    *,
    variant_map: Dict[str, List[str]],
) -> float:
    best = 0.0
    for variant in _normalized_keyword_variants(keyword, variant_map):
        if variant in haystack:
            return 1.0
        variant_tokens = [
            token
            for token in variant.split()
            if token and token not in KEYWORD_MATCH_STOPWORDS
        ]
        if not variant_tokens:
            continue
        overlap = len(set(variant_tokens).intersection(haystack_tokens))
        if len(variant_tokens) == 1:
            if overlap:
                best = max(best, 0.8)
            continue
        ratio = overlap / len(set(variant_tokens))
        if ratio >= 1.0:
            best = max(best, 0.85)
        elif len(variant_tokens) >= 3 and ratio >= 0.67:
            best = max(best, 0.55)
    return round(best, 3)


def lexical_similarity(brief: SearchBrief, candidate: CandidateProfile, text_parts: List[str]) -> float:
    brief_text = " ".join(
        [
            brief.role_title,
            brief.brief_summary,
            brief.document_text,
            *brief.titles,
            *brief.title_keywords,
            *brief.required_keywords,
            *brief.preferred_keywords,
            *brief.industry_keywords,
        ]
    )
    candidate_text = " ".join(text_parts)
    brief_tokens = {
        token
        for token in normalize_text(brief_text).split()
        if len(token) > 2 and token not in SEMANTIC_STOPWORDS
    }
    candidate_tokens = {
        token
        for token in normalize_text(candidate_text).split()
        if len(token) > 2 and token not in SEMANTIC_STOPWORDS
    }
    if not brief_tokens or not candidate_tokens:
        return 0.0
    overlap = len(brief_tokens.intersection(candidate_tokens))
    return round(min(1.0, overlap / len(brief_tokens)), 3)


def current_role_signal_phrases(brief: SearchBrief) -> List[str]:
    phrases: List[str] = []
    families = brief_target_title_families(brief)
    for family in families:
        phrases.extend(TITLE_ROLE_SIGNAL_MAP.get(family, []))
        if brief.allow_adjacent_titles:
            for adjacent_family in ADJACENT_TITLE_FAMILY_MAP.get(family, set()):
                phrases.extend(TITLE_ROLE_SIGNAL_MAP.get(adjacent_family, []))
    normalized_targets = [normalize_text(value) for value in [*brief.titles, *brief.title_keywords]]
    if any("product marketing" in value for value in normalized_targets):
        phrases.append("product marketing")
    return unique_preserving_order(phrases)


def adjacent_family_overlap(candidate_families: set[str], target_families: set[str]) -> set[str]:
    matches: set[str] = set()
    for family in candidate_families:
        matches.update(ADJACENT_TITLE_FAMILY_MAP.get(family, set()).intersection(target_families))
    return matches


def evaluate_current_function_fit(
    candidate: CandidateProfile,
    brief: SearchBrief,
    candidate_title_families: set[str],
    target_title_families: set[str],
    title_similarity_score: float,
) -> tuple[float, bool, List[str], List[str], set[str], set[str]]:
    normalized_title = normalize_text(candidate.current_title)
    target_role_text = normalize_text(" ".join([brief.role_title, *brief.titles, *brief.title_keywords]))
    target_signals = current_role_signal_phrases(brief)
    has_target_role_signal = any(signal in normalized_title for signal in target_signals if signal)
    family_overlap = candidate_title_families.intersection(target_title_families)
    adjacent_overlap = (
        adjacent_family_overlap(candidate_title_families, target_title_families)
        if brief.allow_adjacent_titles
        else set()
    )
    strict_title_scope = bool(not brief.allow_adjacent_titles and target_title_families)
    disqualifiers = [
        keyword
        for keyword in OFF_FUNCTION_KEYWORDS
        if keyword in normalized_title and keyword not in target_role_text
    ]
    notes: List[str] = []

    if disqualifiers and not has_target_role_signal:
        notes.append(f"current_function_fit: blocked ({', '.join(disqualifiers)})")
        return 0.0, True, notes, disqualifiers, family_overlap, adjacent_overlap
    if family_overlap.difference(GENERIC_PRODUCT_FAMILY):
        if strict_title_scope and title_similarity_score < 0.8:
            notes.append("current_function_fit: family_overlap_but_strict_scope")
            return 0.45, False, notes, disqualifiers, family_overlap, adjacent_overlap
        notes.append("current_function_fit: strong")
        return 1.0, False, notes, disqualifiers, family_overlap, adjacent_overlap
    if adjacent_overlap:
        notes.append("current_function_fit: adjacent_family")
        return 0.8, False, notes, disqualifiers, family_overlap, adjacent_overlap
    if title_similarity_score >= 0.6:
        if not brief.allow_adjacent_titles and target_title_families and not family_overlap:
            notes.append("current_function_fit: title_overlap_but_strict_scope")
            return 0.45, False, notes, disqualifiers, family_overlap, adjacent_overlap
        notes.append("current_function_fit: title_overlap")
        return 0.72, False, notes, disqualifiers, family_overlap, adjacent_overlap
    if family_overlap:
        notes.append("current_function_fit: generic_family")
        return 0.55, False, notes, disqualifiers, family_overlap, adjacent_overlap
    if has_target_role_signal:
        notes.append("current_function_fit: retained_role_signal")
        return 0.45, False, notes, disqualifiers, family_overlap, adjacent_overlap
    if candidate.current_title:
        notes.append("current_function_fit: weak")
    return 0.15, False, notes, disqualifiers, family_overlap, adjacent_overlap


def evaluate_location_match(candidate: CandidateProfile, brief: SearchBrief) -> tuple[float, str, bool, List[str]]:
    notes: List[str] = []
    candidate.distance_miles = distance_from_center(brief.geography, candidate.location_geo)

    if candidate.distance_miles is not None:
        if candidate.distance_miles <= brief.geography.radius_miles:
            notes.append("location_match: within_radius")
            return 1.0, "within_radius", True, notes
        if brief.geography.radius_miles and candidate.distance_miles <= brief.geography.radius_miles * 2:
            notes.append("location_match: within_expanded_radius")
            return 0.72, "within_expanded_radius", True, notes
        notes.append("location_match: outside_target_area")
        return 0.0, "outside_target_area", False, notes

    location_haystack = normalize_text(" ".join([candidate.location_name, candidate.summary]))
    target_locations = brief_location_targets(brief)
    specific_hints = [
        hint
        for hint in target_locations
        if normalize_text(hint)
        and normalize_text(hint) != normalize_text(brief.geography.country)
    ]
    precise_market_required = bool(specific_hints)
    matched_hint = next(
        (hint for hint in specific_hints if normalize_text(hint) in location_haystack),
        "",
    )
    if matched_hint:
        priority_index = location_priority_rank(target_locations, matched_hint)
        if priority_index <= 3:
            notes.append(f"location_match: priority_target_location ({matched_hint})")
            return 0.92, "priority_target_location", True, notes
        if priority_index <= 9:
            notes.append(f"location_match: named_target_location ({matched_hint})")
            return 0.82, "named_target_location", True, notes
        notes.append(f"location_match: secondary_target_location ({matched_hint})")
        return 0.7, "secondary_target_location", True, notes
    country_targets = unique_preserving_order([brief.geography.country])
    matched_country = next(
        (hint for hint in country_targets if normalize_text(hint) and normalize_text(hint) in location_haystack),
        "",
    )
    if matched_country:
        if candidate.location_name and normalize_text(candidate.location_name) != normalize_text(brief.geography.country):
            notes.append("location_match: named_profile_location")
            return 0.68, "named_profile_location", True, notes
        if not precise_market_required:
            notes.append("location_match: country_only")
            return 0.72, "country_only", True, notes
        notes.append("location_match: country_only")
        return 0.4, "country_only", False, notes
    if candidate.location_name:
        notes.append("location_match: outside_target_area")
        return 0.0, "outside_target_area", False, notes
    notes.append("location_match: unknown_location")
    return 0.0, "unknown_location", False, notes


def evaluate_skill_overlap(candidate: CandidateProfile, brief: SearchBrief, text_parts: List[str]) -> tuple[float, List[str]]:
    notes: List[str] = []
    required = brief.required_keywords
    optional = unique_preserving_order(
        [
            *brief.preferred_keywords,
            *brief.portfolio_keywords,
            *brief.commercial_keywords,
            *brief.leadership_keywords,
            *brief.scope_keywords,
        ]
    )
    if not required and not optional:
        return 0.5, notes

    haystack = normalize_text(" ".join(part for part in text_parts if part))
    haystack_tokens = set(haystack.split())
    required_scores = [
        _keyword_match_strength(haystack, haystack_tokens, keyword, variant_map=SKILL_KEYWORD_VARIANTS)
        for keyword in required
    ]
    optional_scores = [
        _keyword_match_strength(haystack, haystack_tokens, keyword, variant_map=SKILL_KEYWORD_VARIANTS)
        for keyword in optional
    ]
    required_ratio = (sum(required_scores) / len(required_scores)) if required_scores else 0.0
    optional_ratio = (sum(optional_scores) / len(optional_scores)) if optional_scores else 0.0

    if required and optional:
        score = (required_ratio * 0.7) + (optional_ratio * 0.3)
    elif required:
        score = required_ratio
    else:
        score = optional_ratio

    score = round(min(1.0, score), 3)
    if score > 0.0:
        notes.append("skill_overlap: matched")
    return score, notes


def evaluate_industry_fit(candidate: CandidateProfile, brief: SearchBrief, text_parts: List[str]) -> tuple[float, bool, List[str]]:
    notes: List[str] = []
    if not brief.industry_keywords:
        return 0.5, False, notes

    current_parts = [candidate.current_title, candidate.current_company, candidate.summary, candidate.industry or ""]
    current_haystack = normalize_text(" ".join(part for part in current_parts if part))
    current_tokens = set(current_haystack.split())
    history_haystack = normalize_text(" ".join(part for part in text_parts if part))
    history_tokens = set(history_haystack.split())
    current_scores = [
        _keyword_match_strength(current_haystack, current_tokens, keyword, variant_map=INDUSTRY_KEYWORD_VARIANTS)
        for keyword in brief.industry_keywords
    ]
    history_scores = [
        _keyword_match_strength(history_haystack, history_tokens, keyword, variant_map=INDUSTRY_KEYWORD_VARIANTS)
        for keyword in brief.industry_keywords
    ]
    current_strength = sum(current_scores)
    history_strength = sum(history_scores)
    strong_current_hits = sum(1 for score in current_scores if score >= 0.75)
    if strong_current_hits >= 2 or current_strength >= 1.6:
        notes.append("industry_fit: strong_current")
        return 1.0, True, notes
    if strong_current_hits >= 1 or current_strength >= 0.85:
        notes.append("industry_fit: current_signal")
        return 0.75, True, notes
    if any(score >= 0.75 for score in history_scores) or history_strength >= 0.85:
        notes.append("industry_fit: historical_signal")
        return 0.55, True, notes
    notes.append("industry_fit: missing")
    return 0.0, False, notes


def evaluate_years_fit(candidate: CandidateProfile, brief: SearchBrief) -> tuple[float, Optional[float], Optional[float], List[str]]:
    notes: List[str] = []
    years_experience = derive_years_experience(candidate)
    if brief.minimum_years_experience is None and brief.maximum_years_experience is None:
        return 0.5, years_experience, None, notes
    if years_experience is None:
        notes.append("years_fit: missing")
        return 0.0, None, None, notes

    gap: Optional[float] = None
    score = 1.0
    if brief.minimum_years_experience is not None and years_experience < brief.minimum_years_experience:
        gap = float(brief.minimum_years_experience - years_experience)
        if gap <= 2:
            score = min(score, 0.45)
            notes.append("years_fit: near_floor")
        else:
            score = 0.0
            notes.append("years_fit: below_floor")
    if brief.maximum_years_experience is not None and years_experience > brief.maximum_years_experience:
        gap = float(years_experience - brief.maximum_years_experience)
        if gap <= 2:
            score = min(score, 0.7)
            notes.append("years_fit: near_ceiling")
        else:
            score = min(score, 0.35)
            notes.append("years_fit: above_ceiling")
    if gap is None:
        notes.append("years_fit: in_range")
    return round(score, 3), years_experience, gap, notes


def looks_like_non_company_fragment(value: str, brief: SearchBrief | None = None) -> bool:
    normalized = normalize_text(value)
    if not normalized:
        return False

    allowed_companies = set()
    keyword_values = set()
    if brief is not None:
        allowed_companies = {
            normalize_text(company)
            for company in [
                *brief.company_targets,
                *[alias for aliases in brief.company_aliases.values() for alias in aliases],
            ]
            if normalize_text(company)
        }
        if normalized in allowed_companies:
            return False
        keyword_values = {
            normalize_text(keyword)
            for keyword in [
                brief.role_title,
                *brief.titles,
                *brief.title_keywords,
                *brief.required_keywords,
                *brief.preferred_keywords,
                *brief.portfolio_keywords,
                *brief.commercial_keywords,
                *brief.leadership_keywords,
                *brief.scope_keywords,
            ]
            if normalize_text(keyword)
        }
        if normalized in keyword_values:
            return True

    tokens = [token for token in re.split(r"[^a-z0-9]+", normalized) if token]
    if not tokens or len(tokens) > 5:
        return False
    if any(token in COMPANY_ENTITY_HINTS for token in tokens):
        return False

    generic_hits = sum(1 for token in tokens if token in GENERIC_COMPANY_FRAGMENT_TOKENS)
    if generic_hits == len(tokens):
        return True
    if generic_hits >= max(2, len(tokens) - 1):
        return True
    return False


def looks_like_person_name(value: str) -> bool:
    tokens = [token for token in re.split(r"[^A-Za-z'’.-]+", value.strip()) if token]
    if len(tokens) < 2 or len(tokens) > 5:
        return False
    lowered = {token.lower().strip(".") for token in tokens}
    if lowered.intersection(NON_PERSON_NAME_TOKENS):
        return False
    if lowered.intersection(COMPANY_ENTITY_HINTS):
        return False
    return len([token for token in tokens if any(char.isalpha() for char in token)]) >= 2


def title_looks_like_company_label(
    current_title: str,
    current_company: str,
    brief: SearchBrief | None = None,
) -> bool:
    normalized_title = normalize_text(current_title)
    if not normalized_title:
        return False
    title_tokens = set(normalized_title.split())
    role_tokens = {
        "ceo",
        "chief",
        "executive",
        "officer",
        "president",
        "managing",
        "director",
        "general",
        "manager",
        "head",
        "lead",
        "founder",
        "chairman",
        "chairwoman",
        "partner",
        "principal",
        "owner",
    }
    company_candidates = unique_preserving_order(
        [
            current_company,
            *(brief.company_targets if brief else []),
            *(getattr(brief, "sourcing_company_targets", []) if brief else []),
        ]
    )
    if any(company_text_matches(current_title, [company]) for company in company_candidates if company):
        if not title_tokens.intersection(role_tokens):
            return True
    company_label_tokens = {"experience", "furniture", "home", "homes", "interiors", "properties", "retail"}
    if title_tokens.intersection(company_label_tokens) and not title_tokens.intersection(role_tokens):
        return True
    return False


def evaluate_parser_confidence(candidate: CandidateProfile, brief: SearchBrief | None = None) -> tuple[float, List[str]]:
    notes: List[str] = []
    score = 0.0
    if candidate.current_title:
        score += 0.3
    if candidate.current_company:
        score += 0.3
    if candidate.location_name or candidate.location_geo:
        score += 0.15
    if candidate.summary:
        score += 0.1
    if candidate.source_url or candidate.linkedin_url:
        score += 0.1
    if candidate.raw:
        score += 0.05

    if candidate.full_name and not looks_like_person_name(candidate.full_name):
        score -= 0.45
        notes.append("parser_confidence: invalid_person_name")
    normalized_title = normalize_text(candidate.current_title)
    normalized_company = normalize_text(candidate.current_company)
    if normalized_title and normalized_title == normalized_company:
        score -= 0.5
        notes.append("parser_confidence: title_equals_company")
    if candidate.current_title and title_looks_like_company_label(candidate.current_title, candidate.current_company, brief):
        score -= 0.45
        notes.append("parser_confidence: title_looks_like_company")
    if normalized_company and looks_like_non_company_fragment(candidate.current_company, brief):
        score -= 0.35
        notes.append("parser_confidence: invalid_company_fragment")

    score = round(min(max(score, 0.0), 1.0), 3)
    if score > 0.0:
        notes.append("parser_confidence: structured_fields_present")
    return score, notes


def evaluate_evidence_quality(candidate: CandidateProfile) -> tuple[float, List[str]]:
    notes: List[str] = []
    score = 0.0
    if candidate.linkedin_url:
        score += 0.3
    if candidate.source_url:
        score += 0.2
    if candidate.current_company_confirmed or candidate.current_title_confirmed:
        score += 0.15
    if candidate.current_location_confirmed:
        score += 0.1
    if candidate.current_employment_confirmed:
        score += 0.15
    if candidate.evidence_records:
        score += min(0.1, 0.03 * len(candidate.evidence_records))
    score = round(min(score, 1.0), 3)
    if score > 0.0:
        notes.append("evidence_quality: public_signal_present")
    return score, notes


def build_candidate_features(candidate: CandidateProfile, brief: SearchBrief) -> FeatureBuildResult:
    notes: List[str] = []
    all_targets = unique_preserving_order([*brief.titles, *brief.title_keywords])
    title_similarity_score, matched_titles = title_similarity(candidate.current_title, all_targets)
    candidate_title_families = title_families(candidate.current_title)
    target_title_families = brief_target_title_families(brief)
    current_function_fit, off_function_blocked, function_notes, disqualifiers, family_overlap, adjacent_overlap = (
        evaluate_current_function_fit(
            candidate,
            brief,
            candidate_title_families,
            target_title_families,
            title_similarity_score,
        )
    )
    notes.extend(function_notes)

    company_match = best_company_match(
        candidate.current_company,
        extract_experience_companies(candidate),
        brief.company_aliases,
        match_mode=brief.company_match_mode,
    )
    if not company_match["current_match"] and not company_match["history_match"] and brief.sourcing_company_targets:
        sourcing_company_match = best_company_match(
            candidate.current_company,
            extract_experience_companies(candidate),
            merge_company_aliases(brief.sourcing_company_targets, {}),
            match_mode=brief.company_match_mode,
        )
        if sourcing_company_match["current_match"]:
            company_match["score"] = max(float(company_match["score"]), 0.55)
            company_match["matches"] = list(sourcing_company_match["matches"])
            notes.append("company_match: sourcing_company_signal")
        elif sourcing_company_match["history_match"]:
            company_match["score"] = max(float(company_match["score"]), 0.3)
            company_match["matches"] = list(sourcing_company_match["matches"])
            notes.append("company_match: sourcing_company_history_signal")
    text_parts = experience_text_parts(candidate)
    employment_text_parts = candidate_text_parts(candidate)
    location_match_score, location_bucket, location_aligned, location_notes = evaluate_location_match(candidate, brief)
    notes.extend(location_notes)
    skill_overlap_score, skill_notes = evaluate_skill_overlap(candidate, brief, text_parts)
    notes.extend(skill_notes)
    industry_fit_score, industry_aligned, industry_notes = evaluate_industry_fit(candidate, brief, text_parts)
    notes.extend(industry_notes)
    if brief.industry_keywords and not industry_aligned:
        if company_match["current_match"]:
            industry_fit_score = max(industry_fit_score, 0.7)
            industry_aligned = True
            notes.append("industry_fit: target_company_proxy")
        elif company_match["history_match"]:
            industry_fit_score = max(industry_fit_score, 0.45)
            industry_aligned = True
            notes.append("industry_fit: target_company_history_proxy")
    years_fit_score, years_experience, years_gap, years_notes = evaluate_years_fit(candidate, brief)
    notes.extend(years_notes)
    parser_confidence, parser_notes = evaluate_parser_confidence(candidate, brief)
    notes.extend(parser_notes)
    evidence_quality_score, evidence_notes = evaluate_evidence_quality(candidate)
    notes.extend(evidence_notes)
    employment_status_score, employment_status_match, employment_state, employment_notes = (
        evaluate_employment_status(candidate, brief, employment_text_parts)
    )
    notes.extend(employment_notes)
    semantic_similarity_score = lexical_similarity(brief, candidate, text_parts)

    exclude_hits = [
        keyword
        for keyword in brief.exclude_title_keywords
        if normalize_text(keyword) and normalize_text(keyword) in normalize_text(" ".join(text_parts))
    ]
    exclude_hits.extend(
        keyword
        for keyword in brief.exclude_company_keywords
        if normalize_text(keyword)
        and (
            company_text_matches(candidate.current_company, [keyword])
            or any(company_text_matches(company, [keyword]) for company in extract_experience_companies(candidate))
        )
    )
    low_seniority_hits = keyword_hits([candidate.current_title], LOW_SENIORITY_KEYWORDS)

    strict_title_scope = bool(not brief.allow_adjacent_titles and (brief.titles or brief.title_keywords))
    current_title_match = bool(
        title_similarity_score >= 0.8
        or ((family_overlap or current_function_fit >= 0.72) and not strict_title_scope)
        or (adjacent_overlap and brief.allow_adjacent_titles)
    )
    matched_title_family = ""
    if family_overlap:
        matched_title_family = sorted(family_overlap)[0]
    elif adjacent_overlap:
        matched_title_family = sorted(adjacent_overlap)[0]

    return FeatureBuildResult(
        feature_scores={
            "title_similarity": round(title_similarity_score, 3),
            "company_match": round(float(company_match["score"]), 3),
            "location_match": round(location_match_score, 3),
            "skill_overlap": round(skill_overlap_score, 3),
            "industry_fit": round(industry_fit_score, 3),
            "years_fit": round(years_fit_score, 3),
            "current_function_fit": round(current_function_fit, 3),
            "parser_confidence": round(parser_confidence, 3),
            "evidence_quality": round(evidence_quality_score, 3),
            "employment_status": round(employment_status_score, 3),
            "semantic_similarity": round(semantic_similarity_score, 3),
        },
        notes=unique_preserving_order(notes),
        matched_titles=matched_titles,
        matched_companies=list(company_match["matches"]),
        matched_title_family=matched_title_family,
        location_bucket=location_bucket,
        current_target_company_match=bool(company_match["current_match"]),
        target_company_history_match=bool(company_match["history_match"]),
        current_title_match=current_title_match,
        industry_aligned=industry_aligned,
        location_aligned=location_aligned,
        off_function_blocked=off_function_blocked,
        disqualifier_reasons=unique_preserving_order(disqualifiers),
        exclude_hits=unique_preserving_order(exclude_hits),
        low_seniority_hits=low_seniority_hits,
        years_experience=years_experience,
        years_experience_gap=years_gap,
        employment_status_match=employment_status_match,
        employment_state=employment_state,
    )
