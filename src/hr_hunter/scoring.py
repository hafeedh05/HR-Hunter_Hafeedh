from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

from hr_hunter.briefing import normalize_text, unique_preserving_order
from hr_hunter.geo import distance_from_center
from hr_hunter.models import CandidateProfile, SearchBrief

GLOBAL_SCOPE_KEYWORDS = [
    "global",
    "regional",
    "emea",
    "north america",
    "international",
    "multi market",
    "multiple markets",
]

TITLE_FAMILY_KEYWORDS = {
    "brand": ["brand manager", "brand lead", "brand director", "head of brand"],
    "category": ["category manager", "category lead", "category director"],
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
FMCG_SIGNAL_KEYWORDS = [
    "fmcg",
    "cpg",
    "consumer goods",
    "consumer business",
    "consumer health",
    "personal care",
    "hygiene",
    "skincare",
    "retail",
    "brand",
    "category",
]
TITLE_TOKEN_STOPWORDS = {
    "and",
    "director",
    "global",
    "head",
    "ireland",
    "lead",
    "manager",
    "of",
    "senior",
    "uk",
    "vice",
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
TITLE_PROXIMITY_STOPWORDS = {
    "a",
    "an",
    "and",
    "director",
    "for",
    "global",
    "head",
    "ireland",
    "lead",
    "manager",
    "of",
    "senior",
    "the",
    "to",
    "uk",
}
TITLE_ROLE_SIGNAL_MAP = {
    "brand": ["brand"],
    "category": ["category", "category development", "category insights"],
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
    "innovation": {"brand", "category", "portfolio", "product", "product_marketing"},
    "portfolio": {"innovation", "product", "product_marketing"},
    "product": {"innovation", "portfolio", "product_marketing"},
    "product_marketing": {"brand", "category", "portfolio", "product", "shopper_marketing"},
    "shopper_marketing": {"brand", "category", "product_marketing"},
}
IRISH_LOCALITY_TOKENS = [
    "antrim",
    "armagh",
    "athlone",
    "carlow",
    "cavan",
    "clare",
    "clonmel",
    "cork",
    "donegal",
    "drogheda",
    "dublin",
    "dundalk",
    "ennis",
    "galway",
    "kerry",
    "kildare",
    "kilkenny",
    "laois",
    "leitrim",
    "limerick",
    "longford",
    "louth",
    "mayo",
    "meath",
    "monaghan",
    "naas",
    "navan",
    "offaly",
    "portlaoise",
    "roscommon",
    "sligo",
    "tipperary",
    "waterford",
    "westmeath",
    "wexford",
    "wicklow",
]


def status_from_score(score: float) -> str:
    if score >= 70.0:
        return "verified"
    if score >= 50.0:
        return "review"
    return "reject"


def parse_year(value: object) -> Optional[int]:
    if value is None:
        return None
    text = str(value)
    match = re.search(r"(19|20)\d{2}", text)
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

    years = max(0, latest_end - min(starts))
    candidate.years_experience = float(years)
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


def title_families(value: str) -> set[str]:
    normalized = normalize_text(value)
    families = {
        family
        for family, keywords in TITLE_FAMILY_KEYWORDS.items()
        if any(normalize_text(keyword) in normalized for keyword in keywords)
    }
    return families


def title_signal_tokens(value: str) -> set[str]:
    return {
        token
        for token in normalize_text(value).split()
        if token and token not in TITLE_TOKEN_STOPWORDS
    }


def nearest_title_similarity(title: str, options: Iterable[str]) -> tuple[float, List[str]]:
    current_tokens = title_signal_tokens(title)
    if not current_tokens:
        return 0.0, []

    best_score = 0.0
    best_matches: List[str] = []
    for option in options:
        option_tokens = title_signal_tokens(option)
        if not option_tokens:
            continue
        overlap = current_tokens.intersection(option_tokens)
        if not overlap:
            continue
        score = len(overlap) / len(current_tokens.union(option_tokens))
        if len(overlap) >= 2:
            score += 0.2
        if score > best_score + 1e-9:
            best_score = score
            best_matches = [option]
        elif abs(score - best_score) <= 1e-9:
            best_matches.append(option)
    return round(best_score, 2), unique_preserving_order(best_matches)


def brief_target_title_families(brief: SearchBrief) -> set[str]:
    families: set[str] = set()
    for value in [*brief.titles, *brief.title_keywords]:
        families.update(title_families(value))
    return families


def company_text_matches(value: str, aliases: Iterable[str]) -> bool:
    normalized_value = normalize_text(value)
    if not normalized_value:
        return False
    for alias in aliases:
        normalized_alias = normalize_text(alias)
        if not normalized_alias:
            continue
        if (
            normalized_value == normalized_alias
            or normalized_alias in normalized_value
            or normalized_value in normalized_alias
        ):
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


def best_title_match(title: str, targets: Iterable[str], keywords: Iterable[str]) -> Dict[str, object]:
    normalized_title = normalize_text(title)
    result = {"score": 0.0, "matches": []}

    for target in targets:
        normalized_target = normalize_text(target)
        if normalized_title == normalized_target:
            result["score"] = max(result["score"], 35.0)
            result["matches"] = [target]
        elif normalized_target and normalized_target in normalized_title:
            result["score"] = max(result["score"], 28.0)
            result["matches"] = [target]

    for keyword in keywords:
        normalized_keyword = normalize_text(keyword)
        if normalized_keyword and normalized_keyword in normalized_title:
            result["score"] = max(result["score"], 18.0)
            if keyword not in result["matches"]:
                result["matches"].append(keyword)

    return result


def best_company_match(
    current_company: str,
    experience_companies: Iterable[str],
    aliases: Dict[str, List[str]],
) -> Dict[str, object]:
    normalized_current = normalize_text(current_company)
    result = {
        "score": 0.0,
        "matches": [],
        "current_match": False,
        "history_match": False,
        "history_matches": [],
    }

    for company, alias_values in aliases.items():
        if normalized_current and company_text_matches(current_company, alias_values):
            result["score"] = max(result["score"], 40.0)
            result["matches"] = [company]
            result["current_match"] = True
            continue

        if any(company_text_matches(experience_company, alias_values) for experience_company in experience_companies):
            result["score"] = max(result["score"], 8.0)
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


def title_similarity_score(title: str, targets: Iterable[str], keywords: Iterable[str]) -> Dict[str, object]:
    title_tokens = {
        token
        for token in normalize_text(title).split()
        if token and token not in TITLE_PROXIMITY_STOPWORDS
    }
    best_score = 0.0
    best_match = ""

    for candidate_target in [*targets, *keywords]:
        target_tokens = {
            token
            for token in normalize_text(candidate_target).split()
            if token and token not in TITLE_PROXIMITY_STOPWORDS
        }
        if not title_tokens or not target_tokens:
            continue
        overlap = len(title_tokens.intersection(target_tokens))
        if overlap == 0:
            continue
        score = overlap / len(target_tokens)
        if score > best_score:
            best_score = score
            best_match = candidate_target

    return {"score": round(best_score, 2), "match": best_match}


def current_role_signal_phrases(brief: SearchBrief) -> List[str]:
    phrases: List[str] = []
    families = brief_target_title_families(brief)
    for family in families:
        phrases.extend(TITLE_ROLE_SIGNAL_MAP.get(family, []))
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


def evaluate_location_precision(
    candidate: CandidateProfile,
    brief: SearchBrief,
) -> Tuple[str, float, bool, List[str]]:
    notes: List[str] = []
    candidate.distance_miles = distance_from_center(brief.geography, candidate.location_geo)

    if candidate.distance_miles is not None:
        if candidate.distance_miles <= brief.geography.radius_miles:
            notes.append("location_precision: highest (within target radius)")
            return "within_radius", 35.0, True, notes
        if brief.geography.radius_miles and candidate.distance_miles <= brief.geography.radius_miles * 2:
            notes.append("location_precision: moderate (within expanded radius)")
            return "within_expanded_radius", 20.0, True, notes
        notes.append("location_precision: hard_penalty (outside widened Ireland search area)")
        return "outside_ireland", -30.0, False, notes

    location_haystack = normalize_text(" ".join([candidate.location_name, candidate.summary]))
    precise_location_hits = [
        hint
        for hint in unique_preserving_order([brief.geography.location_name, *brief.geography.location_hints])
        if normalize_text(hint) and normalize_text(hint) in location_haystack
    ]
    if precise_location_hits:
        notes.append(f"location_precision: small_boost (named Irish locality: {precise_location_hits[0]})")
        return "named_ireland_location", 10.0, True, notes

    named_irish_locality = next((token for token in IRISH_LOCALITY_TOKENS if token in location_haystack), "")
    if named_irish_locality:
        notes.append(f"location_precision: small_boost (named Irish locality: {named_irish_locality})")
        return "named_irish_locality", 6.0, False, notes

    if brief.geography.country and normalize_text(brief.geography.country) in location_haystack:
        notes.append("location_precision: near_neutral (country-only Ireland)")
        return "country_only_ireland", 2.0, False, notes
    if candidate.location_name:
        notes.append("location_precision: hard_penalty (outside Ireland signal)")
        return "outside_ireland", -24.0, False, notes
    notes.append("location_precision: penalty (unknown location)")
    return "unknown_location", -12.0, False, notes


def evaluate_current_function_fit(
    candidate: CandidateProfile,
    brief: SearchBrief,
    candidate_title_families: set[str],
    target_title_families: set[str],
    family_overlap: set[str],
    title_proximity: Dict[str, object],
) -> Tuple[float, float, bool, List[str], List[str]]:
    normalized_title = normalize_text(candidate.current_title)
    target_signals = current_role_signal_phrases(brief)
    has_target_role_signal = any(signal in normalized_title for signal in target_signals if signal)
    adjacent_overlap = adjacent_family_overlap(candidate_title_families, target_title_families)
    disqualifiers = [keyword for keyword in OFF_FUNCTION_KEYWORDS if keyword in normalized_title]
    notes: List[str] = []

    if disqualifiers and not has_target_role_signal:
        notes.append(
            f"current_function_fit: blocked (off-function current role: {', '.join(disqualifiers)})"
        )
        return 0.0, -28.0, True, notes, disqualifiers

    if family_overlap.difference(GENERIC_PRODUCT_FAMILY):
        notes.append("current_function_fit: strong (target title family aligned)")
        return 1.0, 16.0, False, notes, disqualifiers
    if adjacent_overlap:
        penalty = -4.0 if disqualifiers else 8.0
        detail = ", ".join(sorted(adjacent_overlap))
        if disqualifiers:
            notes.append(
                f"current_function_fit: mixed (adjacent family with off-function modifier; adjacent={detail})"
            )
            return 0.6, penalty, False, notes, disqualifiers
        notes.append(f"current_function_fit: strong_adjacent (adjacent family: {detail})")
        return 0.8, penalty, False, notes, disqualifiers
    if title_proximity["score"] >= 0.6:
        if disqualifiers:
            notes.append("current_function_fit: mixed (adjacent title but off-function modifier present)")
            return 0.6, -4.0, False, notes, disqualifiers
        notes.append("current_function_fit: adjacent (current title overlaps target role)")
        return 0.8, 10.0, False, notes, disqualifiers
    if family_overlap:
        if disqualifiers:
            notes.append("current_function_fit: mixed (generic product family with off-function modifier)")
            return 0.5, -4.0, False, notes, disqualifiers
        notes.append("current_function_fit: generic (generic product family aligned)")
        return 0.55, 4.0, False, notes, disqualifiers
    if has_target_role_signal:
        if disqualifiers:
            notes.append("current_function_fit: mixed (target-role noun retained with off-function modifier)")
            return 0.5, -4.0, False, notes, disqualifiers
        notes.append("current_function_fit: weak (target-role noun retained)")
        return 0.45, 2.0, False, notes, disqualifiers
    if candidate.current_title:
        notes.append("current_function_fit: weak (current title not aligned)")
    return 0.15, -12.0, False, notes, disqualifiers


def evaluate_current_fmcg_fit(
    candidate: CandidateProfile,
    brief: SearchBrief,
    experience_parts: List[str],
    off_function_blocked: bool,
) -> Tuple[float, float, List[str]]:
    notes: List[str] = []
    fmcg_keywords = unique_preserving_order([*FMCG_SIGNAL_KEYWORDS, *brief.industry_keywords])
    current_hits = keyword_hits(
        [candidate.current_title, candidate.current_company, candidate.summary, candidate.industry or ""],
        fmcg_keywords,
    )
    history_hits = keyword_hits(experience_parts, fmcg_keywords)

    if candidate.current_target_company_match or current_hits >= 2:
        notes.append("current_fmcg_fit: strong (current company or role carries strong FMCG signal)")
        return 1.0, 14.0, notes
    if current_hits:
        notes.append("current_fmcg_fit: moderate (current role carries some FMCG signal)")
        return 0.7, 8.0, notes
    if history_hits:
        if off_function_blocked:
            notes.append("current_fmcg_fit: weak (historical FMCG only and current role is off-function)")
            return 0.2, 1.0, notes
        notes.append("current_fmcg_fit: weak (historical FMCG experience only)")
        return 0.35, 3.0, notes
    if brief.industry_keywords:
        notes.append("current_fmcg_fit: missing (FMCG background not evidenced)")
    return 0.0, -12.0 if brief.industry_keywords else 0.0, notes


def score_candidate(candidate: CandidateProfile, brief: SearchBrief) -> CandidateProfile:
    notes: List[str] = []
    title_match = best_title_match(candidate.current_title, brief.titles, brief.title_keywords)
    company_match = best_company_match(
        candidate.current_company,
        extract_experience_companies(candidate),
        brief.company_aliases,
    )
    candidate_title_families = title_families(candidate.current_title)
    target_title_families = brief_target_title_families(brief)
    family_overlap = candidate_title_families.intersection(target_title_families)
    adjacent_overlap = adjacent_family_overlap(candidate_title_families, target_title_families)
    title_proximity = title_similarity_score(candidate.current_title, brief.titles, brief.title_keywords)
    experience_parts = experience_text_parts(candidate)
    current_function_fit, current_function_points, off_function_blocked, function_notes, disqualifiers = (
        evaluate_current_function_fit(
            candidate,
            brief,
            candidate_title_families,
            target_title_families,
            family_overlap,
            title_proximity,
        )
    )

    candidate.matched_titles = list(title_match["matches"])
    candidate.matched_companies = list(company_match["matches"])
    candidate.current_target_company_match = bool(company_match["current_match"])
    candidate.target_company_history_match = bool(company_match["history_match"])
    candidate.current_title_match = bool(
        float(title_match["score"]) >= 28.0
        or (family_overlap and float(title_match["score"]) >= 18.0)
        or title_proximity["score"] >= 0.6
        or bool(adjacent_overlap)
        or current_function_fit >= 0.75
    )

    score = 0.0
    score += float(title_match["score"])
    score += float(company_match["score"])
    score += current_function_points
    notes.extend(function_notes)

    if candidate.current_target_company_match:
        notes.append("current employer matches target company")
    elif candidate.target_company_history_match:
        notes.append("former target company experience only")

    if family_overlap:
        if family_overlap.difference(GENERIC_PRODUCT_FAMILY):
            score += 10.0
            notes.append("title family aligned")
        else:
            score += 4.0
            notes.append("generic product title family aligned")
    elif adjacent_overlap:
        score += 6.0
        notes.append(f"adjacent title family aligned: {', '.join(sorted(adjacent_overlap))}")
    elif candidate.current_title:
        score -= 20.0
        notes.append("title family misaligned")

    if title_proximity["score"] >= 0.6 and title_proximity["match"]:
        score += 8.0
        if title_proximity["match"] not in candidate.matched_titles:
            candidate.matched_titles.append(str(title_proximity["match"]))
        notes.append("adjacent title aligned")
    elif title_proximity["score"] >= 0.34:
        score += 3.0
        notes.append("near title aligned")

    if not candidate.current_title_match and not family_overlap and candidate.current_title:
        score -= 20.0
        notes.append("current function not aligned")
    if off_function_blocked:
        score -= 10.0
        notes.append("off-function current role requires review")

    normalized_title = normalize_text(candidate.current_title)
    normalized_company = normalize_text(candidate.current_company)
    if normalized_title and normalized_title == normalized_company:
        score -= 15.0
        notes.append("current title parse unreliable")

    if candidate.linkedin_url:
        score += 5.0
        notes.append("source_quality: moderate (public profile URL present)")
    elif candidate.source_url:
        notes.append("source_quality: moderate (source URL present)")
    elif candidate.raw:
        notes.append("source_quality: moderate (structured source record present)")
    else:
        notes.append("source_quality: weak (no profile URL on source record)")

    if candidate.current_title and candidate.current_company:
        score += 5.0
        notes.append("current title and company present")

    scope_hits = keyword_hits(
        [candidate.current_title, candidate.summary],
        [*GLOBAL_SCOPE_KEYWORDS, *brief.scope_keywords],
    )
    if scope_hits:
        score += min(8.0, float(scope_hits * 2.5))
        notes.append("global or multi-market scope matched")

    location_bucket, location_points, location_aligned, location_notes = evaluate_location_precision(candidate, brief)
    candidate.location_aligned = location_aligned
    score += location_points
    notes.extend(location_notes)

    industry_hits = keyword_hits(
        [candidate.current_title, candidate.summary, candidate.industry or "", candidate.current_company],
        brief.industry_keywords,
    )
    candidate.industry_aligned = bool(
        industry_hits or candidate.current_target_company_match or candidate.target_company_history_match
    )
    if industry_hits:
        score += min(14.0, float(industry_hits * 3.5))
        notes.append("industry signal matched")
    elif brief.industry_keywords and not candidate.industry_aligned:
        score -= 14.0
        notes.append("industry signal missing")

    current_fmcg_fit, current_fmcg_points, fmcg_notes = evaluate_current_fmcg_fit(
        candidate,
        brief,
        experience_parts,
        off_function_blocked,
    )
    score += current_fmcg_points
    notes.extend(fmcg_notes)
    fmcg_hits = keyword_hits(
        [
            candidate.current_title,
            candidate.summary,
            candidate.industry or "",
            candidate.current_company,
            *experience_parts,
        ],
        unique_preserving_order([*FMCG_SIGNAL_KEYWORDS, *brief.industry_keywords]),
    )

    current_role_relevance_hits = keyword_hits(
        [candidate.current_title, candidate.summary, candidate.industry or ""],
        unique_preserving_order(
            [
                *brief.required_keywords,
                *brief.preferred_keywords,
                *brief.portfolio_keywords,
                *brief.commercial_keywords,
                *brief.industry_keywords,
                *brief.title_keywords,
            ]
        ),
    )
    if current_role_relevance_hits:
        score += min(12.0, float(current_role_relevance_hits * 2))
        notes.append("current role relevance matched")

    required_hits = keyword_hits(
        [candidate.summary, candidate.industry or "", candidate.current_title, *experience_parts],
        brief.required_keywords,
    )
    preferred_hits = keyword_hits(
        [candidate.summary, candidate.industry or "", candidate.current_title, *experience_parts],
        brief.preferred_keywords,
    )
    if required_hits:
        score += min(12.0, float(required_hits * 3))
        notes.append("required keyword hit")
    if preferred_hits:
        score += min(8.0, float(preferred_hits * 2))
        notes.append("preferred keyword hit")

    portfolio_hits = keyword_hits(
        [candidate.current_title, candidate.summary, candidate.industry or "", *experience_parts],
        brief.portfolio_keywords,
    )
    if portfolio_hits:
        score += min(8.0, float(portfolio_hits * 2))
        notes.append("portfolio signal matched")

    commercial_hits = keyword_hits(
        [candidate.current_title, candidate.summary, candidate.industry or "", *experience_parts],
        brief.commercial_keywords,
    )
    if commercial_hits:
        score += min(8.0, float(commercial_hits * 2))
        notes.append("commercial signal matched")

    leadership_hits = keyword_hits(
        [candidate.current_title, candidate.summary, *experience_parts],
        brief.leadership_keywords,
    )
    if leadership_hits:
        score += min(6.0, float(leadership_hits * 1.5))
        notes.append("leadership signal matched")

    seniority_hits = keyword_hits([candidate.current_title], brief.seniority_levels)
    if seniority_hits:
        score += min(6.0, float(seniority_hits * 3))
        notes.append("seniority aligned")

    low_seniority_hits = keyword_hits([candidate.current_title], LOW_SENIORITY_KEYWORDS)
    if low_seniority_hits and (brief.minimum_years_experience or 0) >= 6:
        score -= min(15.0, float(low_seniority_hits * 6))
        notes.append("title seniority below target")

    exclude_hits = keyword_hits(
        [
            candidate.current_title,
            candidate.summary,
            candidate.industry or "",
            candidate.current_company,
            *experience_parts,
        ],
        brief.exclude_title_keywords,
    )
    if exclude_hits:
        score -= min(45.0, float(exclude_hits * 14))
        notes.append("excluded title keyword hit")

    generic_product_manager = (
        "product manager" in normalized_title
        and not family_overlap.difference(GENERIC_PRODUCT_FAMILY)
    )
    if generic_product_manager and not candidate.current_target_company_match and not fmcg_hits:
        score -= 18.0
        notes.append("generic product manager without fmcg signal")
    if generic_product_manager and current_role_relevance_hits == 0 and not candidate.current_title_match:
        score -= 10.0
        notes.append("generic product manager without target-function evidence")

    years_experience = derive_years_experience(candidate)
    if years_experience is not None and brief.minimum_years_experience is not None:
        if years_experience >= brief.minimum_years_experience:
            score += 10.0
            notes.append("meets experience floor")
        elif years_experience >= brief.minimum_years_experience - 2:
            score += 4.0
            notes.append("near experience floor")
        else:
            score -= 6.0
            notes.append("below experience floor")

    relevant_experience_hits = keyword_hits(
        experience_parts,
        unique_preserving_order(
            [
                *brief.required_keywords,
                *brief.preferred_keywords,
                *brief.portfolio_keywords,
                *brief.commercial_keywords,
                *brief.industry_keywords,
                *brief.title_keywords,
            ]
        ),
    )
    if relevant_experience_hits:
        history_points = min(12.0, float(relevant_experience_hits * 2))
        if off_function_blocked:
            history_points = min(history_points, 2.0)
            notes.append("historical relevance capped by off-function current role")
        elif current_function_fit < 0.45:
            history_points = min(history_points, 4.0)
            notes.append("historical relevance reduced by weak current function")
        score += history_points
        notes.append("relevant experience history matched")
    elif experience_parts and not candidate.current_target_company_match:
        score -= 8.0
        notes.append("relevant experience not evidenced")

    if brief.company_targets and not candidate.current_target_company_match:
        score -= 18.0
        notes.append("current target company not yet matched")

    if (
        candidate.current_target_company_match
        and candidate.current_title_match
        and candidate.industry_aligned
    ):
        score += 8.0
        notes.append("current role strongly aligned")
    if candidate.current_target_company_match and candidate.location_aligned:
        score += 6.0
        notes.append("current target company within Ireland search area")
    if candidate.current_target_company_match and candidate.location_aligned and fmcg_hits:
        score += 10.0
        notes.append("location plus fmcg core fit")

    if location_bucket == "outside_ireland":
        score = min(score, 45.0)
        notes.append("outside Ireland candidates cannot clear review")

    candidate.score = round(min(max(score, 0.0), 100.0), 2)

    candidate.verification_status = status_from_score(candidate.score)
    if candidate.verification_status == "verified":
        missing = []
        if brief.company_targets and not candidate.current_target_company_match:
            missing.append("current target company match")
        if (brief.titles or brief.title_keywords) and not candidate.current_title_match:
            missing.append("current title match")
        if (brief.geography.location_name or brief.geography.country) and not candidate.location_aligned:
            missing.append("location match")
        if brief.industry_keywords and not candidate.industry_aligned:
            missing.append("industry match")
        if off_function_blocked:
            missing.append("current function fit")
        if location_bucket in {"country_only_ireland", "unknown_location", "outside_ireland"}:
            missing.append("precise Ireland location")
        if current_fmcg_fit < 0.7:
            missing.append("current fmcg fit")
        if not candidate.current_company or not candidate.current_title:
            missing.append("current role proof")
        if missing:
            candidate.verification_status = "review"
            notes.append(f"status capped pending {'/'.join(missing)}")

    setattr(candidate, "matched_title_family", sorted(family_overlap)[0] if family_overlap else "")
    setattr(candidate, "location_precision_bucket", location_bucket)
    setattr(candidate, "current_function_fit", round(current_function_fit, 2))
    setattr(candidate, "current_fmcg_fit", round(current_fmcg_fit, 2))
    setattr(candidate, "disqualifier_reasons", disqualifiers)

    candidate.verification_notes = unique_preserving_order(notes)
    return candidate


def sort_candidates(candidates: List[CandidateProfile]) -> List[CandidateProfile]:
    status_rank = {"verified": 0, "review": 1, "reject": 2}
    return sorted(
        candidates,
        key=lambda candidate: (
            status_rank.get(candidate.verification_status, 9),
            -candidate.score,
            candidate.full_name.lower(),
        ),
    )
