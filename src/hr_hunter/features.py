from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

from hr_hunter.briefing import normalize_text, unique_preserving_order
from hr_hunter.geo import distance_from_center
from hr_hunter.models import CandidateProfile, SearchBrief


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
INTEREST_SIGNAL_PREFIXES = (
    "interested in",
    "interested to join",
    "interested in joining",
    "keen to join",
    "looking to join",
    "open to joining",
    "wants to join",
    "would love to join",
    "would like to join",
    "follow",
    "follows",
    "following",
    "fan of",
    "admire",
    "admires",
)


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
    company_interest_signal: bool = False


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


def company_interest_text(candidate: CandidateProfile, text_parts: List[str]) -> str:
    raw_parts = _flatten_text_values(candidate.raw)
    evidence_parts: List[str] = []
    for record in candidate.evidence_records:
        evidence_parts.extend(
            [
                record.title,
                record.snippet,
                record.source_url,
                record.source_domain,
                record.location_match_text,
                record.company_match,
                *record.title_matches,
            ]
        )
    combined = [
        *text_parts,
        *raw_parts,
        *evidence_parts,
    ]
    return normalize_text(" ".join(part for part in combined if part))


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


def title_signal_tokens(value: str) -> set[str]:
    return {
        token
        for token in normalize_text(value).split()
        if token and token not in TITLE_TOKEN_STOPWORDS
    }


def title_similarity(title: str, options: Iterable[str]) -> tuple[float, List[str]]:
    current_tokens = {
        token
        for token in normalize_text(title).split()
        if token and token not in TITLE_PROXIMITY_STOPWORDS
    }
    if not current_tokens:
        return 0.0, []

    best_score = 0.0
    best_matches: List[str] = []
    for option in options:
        option_tokens = {
            token
            for token in normalize_text(option).split()
            if token and token not in TITLE_PROXIMITY_STOPWORDS
        }
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
    target_signals = current_role_signal_phrases(brief)
    has_target_role_signal = any(signal in normalized_title for signal in target_signals if signal)
    family_overlap = candidate_title_families.intersection(target_title_families)
    adjacent_overlap = adjacent_family_overlap(candidate_title_families, target_title_families)
    disqualifiers = [keyword for keyword in OFF_FUNCTION_KEYWORDS if keyword in normalized_title]
    notes: List[str] = []

    if disqualifiers and not has_target_role_signal:
        notes.append(f"current_function_fit: blocked ({', '.join(disqualifiers)})")
        return 0.0, True, notes, disqualifiers, family_overlap, adjacent_overlap
    if family_overlap.difference(GENERIC_PRODUCT_FAMILY):
        notes.append("current_function_fit: strong")
        return 1.0, False, notes, disqualifiers, family_overlap, adjacent_overlap
    if adjacent_overlap:
        notes.append("current_function_fit: adjacent_family")
        return 0.8, False, notes, disqualifiers, family_overlap, adjacent_overlap
    if title_similarity_score >= 0.6:
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
    matched_hint = next(
        (hint for hint in specific_hints if normalize_text(hint) in location_haystack),
        "",
    )
    if matched_hint:
        notes.append(f"location_match: named_target_location ({matched_hint})")
        return 0.82, "named_target_location", True, notes
    country_targets = unique_preserving_order([brief.geography.country])
    matched_country = next(
        (hint for hint in country_targets if normalize_text(hint) and normalize_text(hint) in location_haystack),
        "",
    )
    if matched_country:
        if candidate.location_name and normalize_text(candidate.location_name) != normalize_text(brief.geography.country):
            notes.append("location_match: named_profile_location")
            return 0.68, "named_profile_location", True, notes
        notes.append("location_match: country_only")
        return 0.35, "country_only", False, notes
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

    required_hits = keyword_hits(text_parts, required)
    optional_hits = keyword_hits(text_parts, optional)
    required_ratio = required_hits / len(required) if required else 0.0
    optional_ratio = optional_hits / len(optional) if optional else 0.0

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
    current_hits = keyword_hits(current_parts, brief.industry_keywords)
    history_hits = keyword_hits(text_parts, brief.industry_keywords)
    if current_hits >= 2:
        notes.append("industry_fit: strong_current")
        return 1.0, True, notes
    if current_hits == 1:
        notes.append("industry_fit: current_signal")
        return 0.75, True, notes
    if history_hits:
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


def evaluate_parser_confidence(candidate: CandidateProfile) -> tuple[float, List[str]]:
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

    normalized_title = normalize_text(candidate.current_title)
    normalized_company = normalize_text(candidate.current_company)
    if normalized_title and normalized_title == normalized_company:
        score -= 0.5
        notes.append("parser_confidence: title_equals_company")

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


def evaluate_company_interest(
    candidate: CandidateProfile,
    brief: SearchBrief,
    text_parts: List[str],
) -> tuple[float, bool, List[str]]:
    notes: List[str] = []
    aliases = unique_preserving_order([brief.hiring_company_name, *brief.hiring_company_aliases])
    if not aliases:
        return 0.0, False, notes

    haystack = company_interest_text(candidate, text_parts)
    if not haystack:
        return 0.0, False, notes

    for alias in aliases:
        normalized_alias = normalize_text(alias)
        if not normalized_alias:
            continue
        for prefix in INTEREST_SIGNAL_PREFIXES:
            normalized_phrase = normalize_text(f"{prefix} {alias}")
            if normalized_phrase and normalized_phrase in haystack:
                notes.append(f"company_interest: explicit_interest ({alias})")
                return 1.0, True, notes
        if normalized_alias in haystack:
            notes.append(f"company_interest: public_company_mention ({alias})")
            return 0.45, True, notes

    return 0.0, False, notes


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
    text_parts = experience_text_parts(candidate)
    location_match_score, location_bucket, location_aligned, location_notes = evaluate_location_match(candidate, brief)
    notes.extend(location_notes)
    skill_overlap_score, skill_notes = evaluate_skill_overlap(candidate, brief, text_parts)
    notes.extend(skill_notes)
    industry_fit_score, industry_aligned, industry_notes = evaluate_industry_fit(candidate, brief, text_parts)
    notes.extend(industry_notes)
    company_interest_score, company_interest_signal, company_interest_notes = evaluate_company_interest(
        candidate,
        brief,
        text_parts,
    )
    notes.extend(company_interest_notes)
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
    parser_confidence, parser_notes = evaluate_parser_confidence(candidate)
    notes.extend(parser_notes)
    evidence_quality_score, evidence_notes = evaluate_evidence_quality(candidate)
    notes.extend(evidence_notes)
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

    current_title_match = bool(
        title_similarity_score >= 0.8
        or family_overlap
        or adjacent_overlap
        or current_function_fit >= 0.72
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
            "company_interest": round(company_interest_score, 3),
            "location_match": round(location_match_score, 3),
            "skill_overlap": round(skill_overlap_score, 3),
            "industry_fit": round(industry_fit_score, 3),
            "years_fit": round(years_fit_score, 3),
            "current_function_fit": round(current_function_fit, 3),
            "parser_confidence": round(parser_confidence, 3),
            "evidence_quality": round(evidence_quality_score, 3),
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
        company_interest_signal=company_interest_signal,
    )
