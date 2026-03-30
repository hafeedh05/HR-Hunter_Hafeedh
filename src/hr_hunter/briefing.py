from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Iterable, List

from hr_hunter.models import GeoSpec, SearchBrief
from hr_hunter.parsers.docx import extract_docx_text


COMPANY_SUFFIX_PATTERN = re.compile(
    r"\b(inc|llc|ltd|limited|plc|corp|corporation|co|company|group)\b",
    re.IGNORECASE,
)


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
        if "product marketing" in lowered:
            keywords.append("product marketing")
        if "product" in lowered:
            keywords.append("product")
        if "portfolio" in lowered:
            keywords.append("portfolio")
        if "category" in lowered:
            keywords.append("category")
        if "global" in lowered:
            keywords.append("global product")
    return unique_preserving_order(keywords)


def build_search_brief(config: Dict[str, Any]) -> SearchBrief:
    brief_path = config.get("brief_document_path")
    document_text = ""
    if brief_path:
        document_text = extract_docx_text(Path(brief_path))

    titles = unique_preserving_order(config.get("titles", []))
    company_targets = unique_preserving_order(config.get("company_targets", []))
    title_keywords = unique_preserving_order(
        config.get("title_keywords", []) + infer_title_keywords(titles)
    )

    geography_config = config.get("geography", {})
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

    return SearchBrief(
        id=config.get("id", "unnamed-brief"),
        role_title=config.get("role_title", ""),
        brief_document_path=brief_path,
        brief_summary=summary,
        titles=titles,
        title_keywords=title_keywords,
        company_targets=company_targets,
        company_aliases=company_aliases,
        geography=geography,
        required_keywords=unique_preserving_order(config.get("required_keywords", [])),
        preferred_keywords=unique_preserving_order(config.get("preferred_keywords", [])),
        seniority_levels=unique_preserving_order(config.get("seniority_levels", [])),
        minimum_years_experience=config.get("minimum_years_experience"),
        result_target_min=int(config.get("result_target_min", 100)),
        result_target_max=int(config.get("result_target_max", 200)),
        max_profiles=int(config.get("max_profiles", 200)),
        provider_settings=config.get("provider_settings", {}),
        document_text=document_text,
    )
