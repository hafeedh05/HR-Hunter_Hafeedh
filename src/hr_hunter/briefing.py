from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Iterable, List

from hr_hunter.models import GeoSpec, SearchBrief
from hr_hunter.parsers.documents import extract_document_text_from_path


COMPANY_SUFFIX_PATTERN = re.compile(
    r"\b(inc|llc|ltd|limited|plc|corp|corporation|co|company|group)\b",
    re.IGNORECASE,
)
ANCHOR_PRIORITY_WEIGHTS = {
    "critical": 1.0,
    "important": 0.75,
    "preferred": 0.6,
    "nice_to_have": 0.4,
}
ANCHOR_NAME_ALIASES = {
    "title": "title_similarity",
    "titles": "title_similarity",
    "role": "title_similarity",
    "company": "company_match",
    "companies": "company_match",
    "target_company": "company_match",
    "target_companies": "company_match",
    "location": "location_match",
    "geography": "location_match",
    "years": "years_fit",
    "years_experience": "years_fit",
    "skills": "skill_overlap",
    "keywords": "skill_overlap",
    "industry": "industry_fit",
    "sector": "industry_fit",
    "parser": "parser_confidence",
    "evidence": "evidence_quality",
    "function": "current_function_fit",
    "semantic": "semantic_similarity",
}
DEFAULT_ANCHOR_WEIGHTS = {
    "title_similarity": 1.0,
    "company_match": 0.95,
    "location_match": 0.9,
    "skill_overlap": 0.85,
    "industry_fit": 0.7,
    "years_fit": 0.6,
    "current_function_fit": 0.8,
    "parser_confidence": 0.35,
    "evidence_quality": 0.35,
    "semantic_similarity": 0.0,
}
GENERIC_SINGLETON_TITLE_KEYWORDS = {
    "category",
    "portfolio",
    "product",
}


def normalize_text(value: str) -> str:
    value = value.lower().replace("&", " and ")
    value = COMPANY_SUFFIX_PATTERN.sub(" ", value)
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def unique_preserving_order(values: Iterable[str]) -> List[str]:
    seen = set()
    ordered = []
    for value in values:
        stripped = value.strip()
        if not stripped:
            continue
        lowered = stripped.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        ordered.append(stripped)
    return ordered


def default_company_aliases(company: str) -> List[str]:
    aliases = {company}
    normalized = COMPANY_SUFFIX_PATTERN.sub("", company).strip(" ,")
    if normalized and normalized.lower() != company.lower():
        aliases.add(normalized)

    lowered = company.lower()
    if lowered == "procter & gamble":
        aliases.update({"P&G", "Procter and Gamble"})
    elif lowered == "johnson & johnson":
        aliases.update({"J&J", "Johnson and Johnson"})
    elif lowered == "colgate-palmolive":
        aliases.add("Colgate Palmolive")

    return unique_preserving_order(aliases)


def merge_company_aliases(
    companies: List[str], configured_aliases: Dict[str, List[str]]
) -> Dict[str, List[str]]:
    merged: Dict[str, List[str]] = {}
    for company in companies:
        aliases = default_company_aliases(company)
        aliases.extend(configured_aliases.get(company, []))
        merged[company] = unique_preserving_order(aliases)
    return merged


def infer_title_keywords(titles: List[str]) -> List[str]:
    keywords = list(titles)
    for title in titles:
        lowered = title.lower()
        if "brand" in lowered:
            keywords.append("brand manager")
            keywords.append("global brand manager")
            keywords.append("shopper marketing manager")
            keywords.append("customer marketing manager")
            keywords.append("trade marketing manager")
        if "marketing" in lowered:
            keywords.append("marketing manager")
            keywords.append("digital marketing manager")
            keywords.append("performance marketing manager")
            keywords.append("growth marketing manager")
            keywords.append("paid media manager")
            keywords.append("acquisition marketing manager")
            keywords.append("demand generation manager")
            keywords.append("product marketing lead")
            keywords.append("shopper marketing manager")
            keywords.append("trade marketing manager")
            keywords.append("marketing innovation lead")
        if "digital marketing" in lowered:
            keywords.append("performance marketing manager")
            keywords.append("growth marketing manager")
            keywords.append("paid media manager")
        if "performance marketing" in lowered or "growth marketing" in lowered:
            keywords.append("digital marketing manager")
            keywords.append("paid media manager")
        if "product marketing" in lowered:
            keywords.append("product marketing")
            keywords.append("proposition manager")
        if "product" in lowered and "product marketing" not in lowered:
            keywords.append("product")
            if any(token in lowered for token in ("manager", "director", "lead", "head", "owner", "vp", "chief")):
                keywords.append("product development manager")
        if "portfolio" in lowered:
            keywords.append("portfolio")
            keywords.append("portfolio manager")
            keywords.append("portfolio lead")
            keywords.append("proposition manager")
            keywords.append("commercialization manager")
        if "category" in lowered:
            keywords.append("category")
            keywords.append("category lead")
            keywords.append("category and insights manager")
            keywords.append("commercial category manager")
            keywords.append("category development manager")
            keywords.append("shopper marketing manager")
        if "innovation" in lowered:
            keywords.append("innovation manager")
            keywords.append("innovation lead")
        if "global" in lowered:
            keywords.append("global product")
    return unique_preserving_order(keywords)


def sanitize_title_keywords(title_keywords: List[str], titles: List[str]) -> List[str]:
    title_contexts = [normalize_text(value) for value in titles if normalize_text(value)]
    sanitized: List[str] = []
    for keyword in title_keywords:
        normalized_keyword = normalize_text(keyword)
        if (
            normalized_keyword in GENERIC_SINGLETON_TITLE_KEYWORDS
            and any(
                len(title_context.split()) >= 2
                and normalized_keyword in title_context.split()
                for title_context in title_contexts
            )
        ):
            continue
        sanitized.append(keyword)
    return unique_preserving_order(sanitized)


def coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_company_match_mode(value: Any) -> str:
    lowered = normalize_text(str(value or "both")).replace(" ", "_")
    aliases = {
        "both": "both",
        "current": "current_only",
        "current_only": "current_only",
        "current_company_only": "current_only",
        "past": "past_only",
        "past_only": "past_only",
        "history": "past_only",
        "history_only": "past_only",
        "used_to_work": "past_only",
    }
    return aliases.get(lowered, "both")


def normalize_employment_status_mode(value: Any) -> str:
    lowered = normalize_text(str(value or "any")).replace(" ", "_")
    aliases = {
        "any": "any",
        "all": "any",
        "currently_employed": "currently_employed",
        "current": "currently_employed",
        "current_employment": "currently_employed",
        "employed": "currently_employed",
        "not_currently_employed": "not_currently_employed",
        "not_employed": "not_currently_employed",
        "jobless": "not_currently_employed",
        "unemployed": "not_currently_employed",
        "between_roles": "not_currently_employed",
        "open_to_work_signal": "open_to_work_signal",
        "open_to_work": "open_to_work_signal",
        "actively_looking": "open_to_work_signal",
    }
    return aliases.get(lowered, "any")


def normalize_years_mode(value: Any) -> str:
    lowered = normalize_text(str(value or "range")).replace(" ", "_")
    aliases = {
        "at_least": "at_least",
        "minimum": "at_least",
        "min": "at_least",
        "at_most": "at_most",
        "maximum": "at_most",
        "max": "at_most",
        "range": "range",
        "between": "range",
        "plus_minus": "plus_minus",
        "tolerance": "plus_minus",
        "exact": "plus_minus",
    }
    return aliases.get(lowered, "range")


def normalize_anchor_name(value: str) -> str:
    lowered = re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")
    return ANCHOR_NAME_ALIASES.get(lowered, lowered)


def _brief_anchor_defaults(config: Dict[str, Any]) -> Dict[str, float]:
    geography_config = config.get("geography", {})
    has_titles = bool(config.get("titles") or config.get("title_keywords"))
    has_companies = bool(config.get("company_targets"))
    has_location = bool(geography_config.get("location_name") or geography_config.get("country"))
    has_years = any(
        config.get(key) is not None for key in ("minimum_years_experience", "maximum_years_experience")
    )
    has_skills = bool(
        config.get("required_keywords")
        or config.get("preferred_keywords")
        or config.get("portfolio_keywords")
        or config.get("commercial_keywords")
        or config.get("leadership_keywords")
        or config.get("scope_keywords")
    )
    has_industry = bool(config.get("industry_keywords"))

    defaults = dict(DEFAULT_ANCHOR_WEIGHTS)
    if not has_titles:
        defaults["title_similarity"] = 0.0
        defaults["current_function_fit"] = 0.0
    if not has_companies:
        defaults["company_match"] = 0.0
    if not has_location:
        defaults["location_match"] = 0.0
    if not has_years:
        defaults["years_fit"] = 0.0
    if not has_skills:
        defaults["skill_overlap"] = 0.0
    if not has_industry:
        defaults["industry_fit"] = 0.0
    return defaults


def build_anchor_weights(config: Dict[str, Any]) -> Dict[str, float]:
    weights = _brief_anchor_defaults(config)

    configured_weights = config.get("anchor_weights", {})
    if isinstance(configured_weights, dict):
        for raw_name, raw_weight in configured_weights.items():
            try:
                weight = float(raw_weight)
            except (TypeError, ValueError):
                continue
            weights[normalize_anchor_name(str(raw_name))] = max(0.0, weight)

    configured_anchors = config.get("anchors", {})
    if isinstance(configured_anchors, dict):
        for raw_name, raw_value in configured_anchors.items():
            anchor_name = normalize_anchor_name(str(raw_name))
            if isinstance(raw_value, (int, float)):
                weights[anchor_name] = max(0.0, float(raw_value))
                continue
            priority = normalize_text(str(raw_value)).replace(" ", "_")
            if priority in ANCHOR_PRIORITY_WEIGHTS:
                weights[anchor_name] = ANCHOR_PRIORITY_WEIGHTS[priority]

    return {
        key: round(value, 3)
        for key, value in weights.items()
        if value > 0.0
    }


def build_search_brief(config: Dict[str, Any]) -> SearchBrief:
    brief_path = config.get("brief_document_path")
    inline_document_text = str(
        config.get("document_text")
        or config.get("job_description")
        or config.get("job_description_text")
        or ""
    ).strip()
    document_text = inline_document_text
    if brief_path:
        path = Path(brief_path).expanduser()
        if path.exists():
            extracted_text = extract_document_text_from_path(path)
            document_text = "\n\n".join(
                value for value in [inline_document_text, extracted_text] if value
            )

    titles = unique_preserving_order(config.get("titles", []))
    company_targets = unique_preserving_order(config.get("company_targets", []))
    peer_company_targets = unique_preserving_order(config.get("peer_company_targets", []))
    sourcing_company_targets = unique_preserving_order([*company_targets, *peer_company_targets])
    configured_title_keywords = unique_preserving_order(config.get("title_keywords", []))
    expand_title_keywords = bool(
        config.get("allow_adjacent_titles", config.get("expand_title_keywords", True))
    )
    title_keywords = sanitize_title_keywords(
        unique_preserving_order(
            configured_title_keywords + (infer_title_keywords(titles) if expand_title_keywords else [])
        ),
        [config.get("role_title", ""), *titles],
    )

    geography_config = config.get("geography", {})
    location_targets = unique_preserving_order(
        [
            *config.get("location_targets", []),
            geography_config.get("location_name", ""),
            geography_config.get("country", ""),
            *geography_config.get("location_hints", []),
        ]
    )
    geography = GeoSpec(
        location_name=geography_config.get("location_name", ""),
        country=geography_config.get("country", ""),
        center_latitude=geography_config.get("center_latitude"),
        center_longitude=geography_config.get("center_longitude"),
        radius_miles=float(geography_config.get("radius_miles", 0) or 0),
        location_hints=unique_preserving_order(geography_config.get("location_hints", [])),
    )

    summary = config.get("brief_summary", "").strip()
    if not summary and document_text:
        summary = "\n".join(document_text.splitlines()[:12])

    company_aliases = merge_company_aliases(company_targets, config.get("company_aliases", {}))
    brief_clarifications = (
        dict(config.get("brief_clarifications", {}))
        if isinstance(config.get("brief_clarifications", {}), dict)
        else {}
    )

    return SearchBrief(
        id=config.get("id", "unnamed-brief"),
        role_title=config.get("role_title", ""),
        brief_document_path=brief_path,
        brief_summary=summary,
        titles=titles,
        title_keywords=title_keywords,
        company_targets=company_targets,
        peer_company_targets=peer_company_targets,
        sourcing_company_targets=sourcing_company_targets,
        company_aliases=company_aliases,
        geography=geography,
        required_keywords=unique_preserving_order(config.get("required_keywords", [])),
        preferred_keywords=unique_preserving_order(config.get("preferred_keywords", [])),
        portfolio_keywords=unique_preserving_order(config.get("portfolio_keywords", [])),
        commercial_keywords=unique_preserving_order(config.get("commercial_keywords", [])),
        leadership_keywords=unique_preserving_order(config.get("leadership_keywords", [])),
        scope_keywords=unique_preserving_order(config.get("scope_keywords", [])),
        seniority_levels=unique_preserving_order(config.get("seniority_levels", [])),
        minimum_years_experience=config.get("minimum_years_experience"),
        maximum_years_experience=config.get("maximum_years_experience"),
        result_target_min=int(config.get("result_target_min", 100)),
        result_target_max=int(config.get("result_target_max", 200)),
        max_profiles=int(config.get("max_profiles", 200)),
        industry_keywords=unique_preserving_order(config.get("industry_keywords", [])),
        exclude_title_keywords=unique_preserving_order(config.get("exclude_title_keywords", [])),
        exclude_company_keywords=unique_preserving_order(
            config.get("exclude_company_keywords", []) + config.get("exclude_companies", [])
        ),
        location_targets=location_targets,
        company_match_mode=normalize_company_match_mode(config.get("company_match_mode", "both")),
        employment_status_mode=normalize_employment_status_mode(
            config.get("employment_status_mode", "any")
        ),
        years_mode=normalize_years_mode(config.get("years_mode", "range")),
        years_target=coerce_int(config.get("years_target")),
        years_tolerance=max(0, int(config.get("years_tolerance", 0) or 0)),
        jd_breakdown=dict(config.get("jd_breakdown", {})) if isinstance(config.get("jd_breakdown", {}), dict) else {},
        anchor_weights=build_anchor_weights(config),
        provider_settings=config.get("provider_settings", {}),
        document_text=document_text,
        allow_adjacent_titles=expand_title_keywords,
        exact_company_scope=bool(brief_clarifications.get("exact_company_scope", False)),
        strict_market_scope=bool(brief_clarifications.get("strict_market_scope", False)),
        scope_first_enabled=bool(config.get("scope_first_enabled", False)),
        in_scope_target=max(0, int(config.get("in_scope_target", 0) or 0)),
        verification_scope_target=max(0, int(config.get("verification_scope_target", 0) or 0)),
    )
