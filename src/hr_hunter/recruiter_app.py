from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Iterable, List

from hr_hunter.briefing import normalize_text, unique_preserving_order
from hr_hunter.config import (
    resolve_feedback_db_path,
    resolve_output_dir,
    resolve_ranker_model_dir,
)


CONTINENT_OPTIONS = [
    "Africa",
    "Asia",
    "Europe",
    "North America",
    "Oceania",
    "South America",
    "Middle East",
]
COUNTRY_TO_ALPHA2 = {
    "argentina": "ar",
    "australia": "au",
    "austria": "at",
    "bahrain": "bh",
    "belgium": "be",
    "brazil": "br",
    "canada": "ca",
    "china": "cn",
    "colombia": "co",
    "denmark": "dk",
    "egypt": "eg",
    "finland": "fi",
    "france": "fr",
    "germany": "de",
    "ghana": "gh",
    "greece": "gr",
    "hong kong": "hk",
    "india": "in",
    "indonesia": "id",
    "ireland": "ie",
    "italy": "it",
    "japan": "jp",
    "jordan": "jo",
    "kenya": "ke",
    "kuwait": "kw",
    "lebanon": "lb",
    "luxembourg": "lu",
    "malaysia": "my",
    "mexico": "mx",
    "morocco": "ma",
    "netherlands": "nl",
    "new zealand": "nz",
    "nigeria": "ng",
    "norway": "no",
    "oman": "om",
    "pakistan": "pk",
    "philippines": "ph",
    "poland": "pl",
    "portugal": "pt",
    "qatar": "qa",
    "romania": "ro",
    "saudi arabia": "sa",
    "singapore": "sg",
    "south africa": "za",
    "south korea": "kr",
    "spain": "es",
    "sweden": "se",
    "switzerland": "ch",
    "taiwan": "tw",
    "thailand": "th",
    "turkey": "tr",
    "uae": "ae",
    "uk": "gb",
    "united arab emirates": "ae",
    "united kingdom": "gb",
    "united states": "us",
    "usa": "us",
    "vietnam": "vn",
}
COUNTRY_OPTIONS = [
    "Argentina",
    "Australia",
    "Austria",
    "Bahrain",
    "Belgium",
    "Brazil",
    "Canada",
    "China",
    "Colombia",
    "Denmark",
    "Egypt",
    "Finland",
    "France",
    "Germany",
    "Ghana",
    "Greece",
    "Hong Kong",
    "India",
    "Indonesia",
    "Ireland",
    "Italy",
    "Japan",
    "Jordan",
    "Kenya",
    "Kuwait",
    "Lebanon",
    "Luxembourg",
    "Malaysia",
    "Mexico",
    "Morocco",
    "Netherlands",
    "New Zealand",
    "Nigeria",
    "Norway",
    "Oman",
    "Pakistan",
    "Philippines",
    "Poland",
    "Portugal",
    "Qatar",
    "Romania",
    "Saudi Arabia",
    "Singapore",
    "South Africa",
    "South Korea",
    "Spain",
    "Sweden",
    "Switzerland",
    "Taiwan",
    "Thailand",
    "Turkey",
    "United Arab Emirates",
    "United Kingdom",
    "United States",
    "Vietnam",
]
ANCHOR_OPTIONS = [
    {
        "id": "title",
        "label": "Title fit",
        "description": "Prioritize how closely the candidate's current role matches the target titles.",
        "default": "critical",
    },
    {
        "id": "skills",
        "label": "Skills fit",
        "description": "Prioritize must-have and nice-to-have capability overlap from the brief and JD.",
        "default": "critical",
    },
    {
        "id": "location",
        "label": "Location fit",
        "description": "Prioritize candidates already in the preferred geography or nearby.",
        "default": "important",
    },
    {
        "id": "company",
        "label": "Company fit",
        "description": "Prioritize current or historical experience at the target companies.",
        "default": "important",
    },
    {
        "id": "years",
        "label": "Years fit",
        "description": "Prioritize experience level alignment against the selected years range.",
        "default": "preferred",
    },
    {
        "id": "industry",
        "label": "Industry fit",
        "description": "Prioritize industry-domain alignment when sector experience matters.",
        "default": "preferred",
    },
    {
        "id": "function",
        "label": "Function fit",
        "description": "Prioritize adjacent role family and functional alignment.",
        "default": "important",
    },
    {
        "id": "semantic",
        "label": "JD semantic fit",
        "description": "Prioritize the overall meaning match between the JD and the candidate profile.",
        "default": "preferred",
    },
]
THEME_OPTIONS = [
    {
        "id": "bright",
        "label": "Bright",
        "description": "Clean white workspace with slate text and subtle cyan accents.",
    },
    {
        "id": "dark",
        "label": "Dark",
        "description": "Dark slate workspace with higher contrast for long sessions.",
    },
]
FEEDBACK_ACTIONS = [
    "shortlist",
    "good_fit",
    "promote_to_verified",
    "reject",
    "wrong_location",
    "wrong_function",
    "too_senior",
    "too_junior",
    "interviewed",
    "hired",
]
KEYWORD_PHRASES = [
    "a/b testing",
    "ab testing",
    "advanced excel",
    "airflow",
    "analytics engineering",
    "api integrations",
    "automation",
    "aws",
    "azure",
    "bigquery",
    "business intelligence",
    "change management",
    "commercial analytics",
    "customer analytics",
    "dashboarding",
    "data analysis",
    "data governance",
    "data modeling",
    "data quality",
    "data visualization",
    "dbt",
    "etl",
    "experimentation",
    "forecasting",
    "ga4",
    "google analytics",
    "kpis",
    "leadership",
    "looker",
    "machine learning",
    "mentoring",
    "power bi",
    "predictive modeling",
    "presentation skills",
    "pricing analytics",
    "product analytics",
    "program management",
    "project management",
    "python",
    "reporting",
    "retention analytics",
    "risk analytics",
    "roadmap ownership",
    "sales analytics",
    "segment analysis",
    "snowflake",
    "sql",
    "stakeholder management",
    "statistics",
    "storytelling",
    "tableau",
    "team management",
    "time series",
    "user research",
]
INDUSTRY_PHRASES = [
    "adtech",
    "consumer",
    "ecommerce",
    "education",
    "energy",
    "fashion",
    "fintech",
    "fmcg",
    "food delivery",
    "gaming",
    "healthcare",
    "hospitality",
    "logistics",
    "marketplace",
    "mobility",
    "proptech",
    "retail",
    "saas",
    "telecom",
    "travel",
]
SENIORITY_LEVELS = [
    "junior",
    "mid",
    "senior",
    "lead",
    "principal",
    "manager",
    "director",
    "head",
    "vp",
]
JD_SIGNAL_PATTERN = re.compile(
    r"\b(must|required|requirements|responsibilities|experience|strong|ability|preferred|bonus|nice to have)\b",
    re.IGNORECASE,
)
YEARS_RANGE_PATTERN = re.compile(r"(?P<min>\d{1,2})\s*[-to]{1,3}\s*(?P<max>\d{1,2})\s*\+?\s*years", re.IGNORECASE)
YEARS_PLUS_PATTERN = re.compile(r"(?P<value>\d{1,2})\s*\+\s*years", re.IGNORECASE)
YEARS_AT_LEAST_PATTERN = re.compile(r"(at least|minimum of|min\.)\s*(?P<value>\d{1,2})\s*years", re.IGNORECASE)
YEARS_AT_MOST_PATTERN = re.compile(r"(up to|maximum of|max\.)\s*(?P<value>\d{1,2})\s*years", re.IGNORECASE)


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-") or "brief"


def parse_multi_value(value: Any) -> List[str]:
    if isinstance(value, list):
        items = [str(item).strip() for item in value]
    else:
        text = str(value or "")
        items = re.split(r"[\n,;]+", text)
    return unique_preserving_order([item for item in items if str(item).strip()])


def _truncate(text: str, limit: int = 280) -> str:
    cleaned = re.sub(r"\s+", " ", text).strip()
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3].rstrip() + "..."


def _sentence_candidates(text: str) -> List[str]:
    lines = [
        re.sub(r"^[\-\*\u2022\d\.\)\(]+\s*", "", raw.strip())
        for raw in text.splitlines()
        if raw.strip()
    ]
    sentences = re.split(r"(?<=[\.\!\?])\s+", re.sub(r"\s+", " ", text).strip())
    merged = [*lines, *sentences]
    return unique_preserving_order(
        [
            candidate.strip(" -;:")
            for candidate in merged
            if len(candidate.strip()) >= 18
        ]
    )


def _match_phrases(text: str, phrases: Iterable[str]) -> List[str]:
    normalized = normalize_text(text)
    matches = [
        phrase
        for phrase in phrases
        if normalize_text(phrase) and normalize_text(phrase) in normalized
    ]
    return unique_preserving_order(matches)


def _extract_years_signal(text: str) -> Dict[str, Any]:
    if not text.strip():
        return {"mode": "range", "value": None, "min": None, "max": None, "tolerance": 0}

    range_match = YEARS_RANGE_PATTERN.search(text)
    if range_match:
        return {
            "mode": "range",
            "value": None,
            "min": int(range_match.group("min")),
            "max": int(range_match.group("max")),
            "tolerance": 0,
        }

    plus_match = YEARS_PLUS_PATTERN.search(text) or YEARS_AT_LEAST_PATTERN.search(text)
    if plus_match:
        return {
            "mode": "at_least",
            "value": int(plus_match.group("value")),
            "min": int(plus_match.group("value")),
            "max": None,
            "tolerance": 0,
        }

    at_most_match = YEARS_AT_MOST_PATTERN.search(text)
    if at_most_match:
        return {
            "mode": "at_most",
            "value": int(at_most_match.group("value")),
            "min": None,
            "max": int(at_most_match.group("value")),
            "tolerance": 0,
        }

    return {"mode": "range", "value": None, "min": None, "max": None, "tolerance": 0}


def extract_job_description_breakdown(job_description: str, role_title: str = "") -> Dict[str, Any]:
    text = str(job_description or "").strip()
    if not text:
        return {
            "summary": "",
            "key_experience_points": [],
            "required_keywords": [],
            "preferred_keywords": [],
            "industry_keywords": [],
            "titles": [role_title] if role_title else [],
            "seniority_levels": [],
            "years": {"mode": "range", "value": None, "min": None, "max": None, "tolerance": 0},
            "suggested_anchors": {},
        }

    sentence_candidates = _sentence_candidates(text)
    key_points = [
        sentence
        for sentence in sentence_candidates
        if JD_SIGNAL_PATTERN.search(sentence)
    ]
    if len(key_points) < 6:
        key_points = unique_preserving_order([*key_points, *sentence_candidates])
    key_points = key_points[:8]

    required_lines = [
        sentence
        for sentence in sentence_candidates
        if re.search(r"\b(must|required|requirements|strong)\b", sentence, re.IGNORECASE)
    ]
    preferred_lines = [
        sentence
        for sentence in sentence_candidates
        if re.search(r"\b(preferred|bonus|nice to have|plus)\b", sentence, re.IGNORECASE)
    ]
    required_keywords = _match_phrases(" ".join(required_lines or key_points), KEYWORD_PHRASES)
    preferred_keywords = _match_phrases(" ".join(preferred_lines), KEYWORD_PHRASES)
    industry_keywords = _match_phrases(text, INDUSTRY_PHRASES)
    seniority = _match_phrases(" ".join([role_title, text]), SENIORITY_LEVELS)
    years = _extract_years_signal(text)

    suggested_anchors: Dict[str, str] = {
        "title": "critical",
        "skills": "critical" if required_keywords else "important",
        "semantic": "important",
    }
    if years.get("min") is not None or years.get("max") is not None:
        suggested_anchors["years"] = "important"
    if industry_keywords:
        suggested_anchors["industry"] = "preferred"

    summary_source = " ".join(key_points[:4]) or text
    return {
        "summary": _truncate(summary_source, limit=420),
        "key_experience_points": key_points,
        "required_keywords": required_keywords,
        "preferred_keywords": [value for value in preferred_keywords if value not in required_keywords],
        "industry_keywords": industry_keywords,
        "titles": unique_preserving_order([role_title] if role_title else []),
        "seniority_levels": seniority,
        "years": years,
        "suggested_anchors": suggested_anchors,
    }


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _selected_country_code(countries: List[str]) -> str:
    if len(countries) != 1:
        return ""
    return COUNTRY_TO_ALPHA2.get(normalize_text(countries[0]), "")


def _resolve_year_bounds(
    *,
    mode: str,
    years_value: int | None,
    years_tolerance: int,
    min_years: int | None,
    max_years: int | None,
) -> tuple[int | None, int | None]:
    if mode == "at_least":
        return years_value if years_value is not None else min_years, max_years
    if mode == "at_most":
        return min_years, years_value if years_value is not None else max_years
    if mode == "plus_minus":
        if years_value is None:
            return min_years, max_years
        tolerance = max(0, years_tolerance)
        return max(0, years_value - tolerance), years_value + tolerance
    resolved_min = min_years
    resolved_max = max_years
    if resolved_min is None and years_value is not None:
        resolved_min = years_value
    return resolved_min, resolved_max


def build_ui_brief_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    role_title = str(payload.get("role_title", "")).strip()
    titles = parse_multi_value(payload.get("titles"))
    countries = parse_multi_value(payload.get("countries"))
    continents = parse_multi_value(payload.get("continents"))
    cities = parse_multi_value(payload.get("cities"))
    companies = parse_multi_value(payload.get("company_targets"))
    exclude_titles = parse_multi_value(payload.get("exclude_title_keywords"))
    exclude_companies = parse_multi_value(payload.get("exclude_company_keywords"))
    must_have = parse_multi_value(payload.get("must_have_keywords"))
    nice_to_have = parse_multi_value(payload.get("nice_to_have_keywords"))
    industry_keywords = parse_multi_value(payload.get("industry_keywords"))
    providers = parse_multi_value(payload.get("providers")) or ["scrapingbee_google"]
    job_description = str(payload.get("job_description", "")).strip()
    breakdown = payload.get("jd_breakdown")
    if not isinstance(breakdown, dict):
        breakdown = extract_job_description_breakdown(job_description, role_title=role_title)

    years_mode = str(payload.get("years_mode", breakdown.get("years", {}).get("mode", "range")) or "range")
    years_value = _coerce_int(payload.get("years_value"))
    years_tolerance = max(0, _coerce_int(payload.get("years_tolerance")) or 0)
    min_years = _coerce_int(payload.get("minimum_years_experience"))
    max_years = _coerce_int(payload.get("maximum_years_experience"))
    if years_value is None and isinstance(breakdown.get("years"), dict):
        years_value = _coerce_int(breakdown["years"].get("value"))
        min_years = min_years if min_years is not None else _coerce_int(breakdown["years"].get("min"))
        max_years = max_years if max_years is not None else _coerce_int(breakdown["years"].get("max"))
    minimum_years_experience, maximum_years_experience = _resolve_year_bounds(
        mode=years_mode,
        years_value=years_value,
        years_tolerance=years_tolerance,
        min_years=min_years,
        max_years=max_years,
    )

    location_targets = unique_preserving_order([*cities, *countries, *continents])
    geography_country = countries[0] if len(countries) == 1 else ""
    geography_location = cities[0] if cities else (countries[0] if len(countries) == 1 else "")
    radius_miles = float(payload.get("radius_miles", 0) or 0)
    anchors = payload.get("anchors", {})
    if not isinstance(anchors, dict):
        anchors = {}
    if breakdown.get("suggested_anchors"):
        merged_anchors = dict(breakdown["suggested_anchors"])
        merged_anchors.update({key: value for key, value in anchors.items() if value})
        anchors = merged_anchors

    titles = unique_preserving_order([*titles, *breakdown.get("titles", []), role_title])
    required_keywords = unique_preserving_order([*must_have, *breakdown.get("required_keywords", [])])
    preferred_keywords = unique_preserving_order([*nice_to_have, *breakdown.get("preferred_keywords", [])])
    industry_keywords = unique_preserving_order([*industry_keywords, *breakdown.get("industry_keywords", [])])
    seniority_levels = unique_preserving_order(
        [*parse_multi_value(payload.get("seniority_levels")), *breakdown.get("seniority_levels", [])]
    )

    output_dir = resolve_output_dir(payload.get("output_dir"))
    feedback_db = resolve_feedback_db_path(payload.get("feedback_db"))
    model_dir = resolve_ranker_model_dir(payload.get("model_dir"))
    limit = max(1, int(payload.get("limit", 20) or 20))
    csv_export_limit = max(1, int(payload.get("csv_export_limit", limit) or limit))
    reranker_enabled = bool(payload.get("reranker_enabled", True))
    learned_ranker_enabled = bool(payload.get("learned_ranker_enabled", False))
    country_code = _selected_country_code(countries)
    providers_settings = {
        "reranker": {
            "enabled": reranker_enabled,
            "model_name": str(payload.get("reranker_model_name", "BAAI/bge-reranker-v2-m3")),
            "top_n": max(limit, int(payload.get("reranker_top_n", 40) or 40)),
            "weight": float(payload.get("reranker_weight", 0.35) or 0.35),
        },
        "learned_ranker": {
            "enabled": learned_ranker_enabled,
            "model_dir": str(model_dir),
            "weight": float(payload.get("learned_ranker_weight", 0.7) or 0.7),
        },
        "scrapingbee_google": {
            "country_code": str(payload.get("scrapingbee_country_code", "") or country_code or "us"),
        },
    }

    summary_lines = []
    if role_title:
        summary_lines.append(f"Role: {role_title}")
    for point in breakdown.get("key_experience_points", [])[:6]:
        summary_lines.append(f"- {point}")

    company_match_mode = str(payload.get("company_match_mode", "both") or "both")
    brief_config = {
        "id": slugify(f"{role_title or 'search'} {countries[0] if countries else cities[0] if cities else ''}"),
        "role_title": role_title or (titles[0] if titles else "Untitled search"),
        "brief_summary": "\n".join(summary_lines).strip(),
        "document_text": job_description,
        "titles": titles,
        "company_targets": companies,
        "geography": {
            "location_name": geography_location,
            "country": geography_country,
            "radius_miles": radius_miles,
            "location_hints": location_targets,
        },
        "location_targets": location_targets,
        "required_keywords": required_keywords,
        "preferred_keywords": preferred_keywords,
        "portfolio_keywords": [],
        "commercial_keywords": [],
        "leadership_keywords": [],
        "scope_keywords": [],
        "industry_keywords": industry_keywords,
        "exclude_title_keywords": exclude_titles,
        "exclude_company_keywords": exclude_companies,
        "seniority_levels": seniority_levels,
        "minimum_years_experience": minimum_years_experience,
        "maximum_years_experience": maximum_years_experience,
        "years_mode": years_mode,
        "years_target": years_value,
        "years_tolerance": years_tolerance,
        "company_match_mode": company_match_mode,
        "jd_breakdown": breakdown,
        "anchors": anchors,
        "result_target_min": max(5, min(limit, 20)),
        "result_target_max": max(limit, 40),
        "max_profiles": max(limit, 80),
        "provider_settings": providers_settings,
    }

    return {
        "brief_config": brief_config,
        "providers": providers,
        "limit": limit,
        "csv_export_limit": csv_export_limit,
        "output_dir": str(output_dir),
        "feedback_db": str(feedback_db),
        "model_dir": str(model_dir),
        "job_description_breakdown": breakdown,
    }


def build_app_bootstrap() -> Dict[str, Any]:
    default_feedback_db = str(resolve_feedback_db_path())
    default_model_dir = str(resolve_ranker_model_dir())
    default_output_dir = str(resolve_output_dir())
    return {
        "anchors": ANCHOR_OPTIONS,
        "feedback_actions": FEEDBACK_ACTIONS,
        "providers": ["scrapingbee_google", "mock"],
        "countries": COUNTRY_OPTIONS,
        "continents": CONTINENT_OPTIONS,
        "themes": THEME_OPTIONS,
        "defaults": {
            "providers": ["scrapingbee_google"],
            "limit": 20,
            "csv_export_limit": 20,
            "radius_miles": 25,
            "company_match_mode": "both",
            "theme": "bright",
            "reranker_enabled": True,
            "learned_ranker_enabled": False,
            "feedback_db": default_feedback_db,
            "model_dir": default_model_dir,
            "output_dir": default_output_dir,
        },
        "presets": {
            "senior_data_analyst_uae": {
                "role_title": "Senior Data Analyst",
                "titles": [
                    "Senior Data Analyst",
                    "Data Analyst",
                    "Analytics Lead",
                    "Business Intelligence Analyst",
                ],
                "countries": ["United Arab Emirates"],
                "continents": ["Middle East"],
                "cities": ["Dubai", "Abu Dhabi"],
                "company_targets": ["Careem", "talabat", "noon", "Property Finder", "Emirates"],
                "company_match_mode": "both",
                "years_mode": "plus_minus",
                "years_value": 6,
                "years_tolerance": 2,
                "must_have_keywords": [
                    "SQL",
                    "Python",
                    "Power BI",
                    "Tableau",
                    "Stakeholder management",
                    "Dashboarding",
                ],
                "nice_to_have_keywords": [
                    "A/B testing",
                    "Forecasting",
                    "dbt",
                    "Snowflake",
                ],
                "industry_keywords": ["ecommerce", "marketplace", "consumer"],
                "job_description": (
                    "We are hiring a Senior Data Analyst based in the UAE to partner with product and commercial "
                    "leaders. You should bring 5-8 years of analytics experience, strong SQL and Python skills, "
                    "hands-on dashboarding in Power BI or Tableau, and the ability to turn complex data into clear "
                    "business recommendations. Experience in ecommerce, marketplaces, mobility, or consumer internet "
                    "is highly preferred. Bonus points for experimentation, forecasting, and working with modern data "
                    "stacks such as dbt and Snowflake."
                ),
                "anchors": {
                    "title": "critical",
                    "skills": "critical",
                    "location": "important",
                    "company": "important",
                    "years": "preferred",
                    "industry": "preferred",
                    "function": "important",
                    "semantic": "preferred",
                },
            }
        },
    }


def safe_artifact_path(raw_path: str, *, workspace_root: Path) -> Path:
    resolved = Path(raw_path).expanduser().resolve()
    workspace_root = workspace_root.resolve()
    output_root = (workspace_root / "output").resolve()
    try:
        resolved.relative_to(output_root)
    except ValueError:
        raise ValueError("Only files inside the workspace output directory can be downloaded.")
    else:
        return resolved
