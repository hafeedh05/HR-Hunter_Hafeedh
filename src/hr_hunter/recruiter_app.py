from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Iterable, List

from hr_hunter.briefing import normalize_text, unique_preserving_order
from hr_hunter.config import (
    env_flag,
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
EMPLOYMENT_STATUS_OPTIONS = [
    {
        "id": "any",
        "label": "Any",
        "description": "Do not filter candidates by current employment status.",
    },
    {
        "id": "currently_employed",
        "label": "Currently Employed",
        "description": "Prefer candidates with public signals showing they are in a current role.",
    },
    {
        "id": "not_currently_employed",
        "label": "Not Currently Employed",
        "description": "Prefer candidates with no current-company signal in the public profile.",
    },
    {
        "id": "open_to_work_signal",
        "label": "Open To Work Signal",
        "description": "Prefer candidates with public open-to-work language or availability signals.",
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
    "agency management",
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
    "budgeting",
    "brand strategy",
    "business intelligence",
    "campaign management",
    "change management",
    "commercial analytics",
    "consumer insights",
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
    "financial analysis",
    "financial planning",
    "fp&a",
    "fpa",
    "forecasting",
    "ga4",
    "go-to-market",
    "google analytics",
    "innovation",
    "kpis",
    "leadership",
    "looker",
    "machine learning",
    "market share",
    "mentoring",
    "p&l",
    "power bi",
    "predictive modeling",
    "pricing",
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


def compute_internal_fetch_limit(requested_limit: int) -> int:
    requested = max(1, int(requested_limit or 1))
    if requested <= 25:
        return requested

    if requested <= 50:
        scaled = requested * 3
        buffered = requested + 90
        return min(max(requested, scaled, buffered), 240)

    if requested <= 140:
        scaled = requested * 3
        buffered = requested + 120
        return min(max(requested, scaled, buffered), 420)

    if requested <= 300:
        scaled = int(round(requested * 2.8))
        buffered = requested + 220
        return min(max(requested, scaled, buffered), 900)

    if requested <= 500:
        scaled = int(round(requested * 2.6))
        buffered = requested + 300
        return min(max(requested, scaled, buffered), 1300)

    scaled = int(round(requested * 2.3))
    buffered = requested + max(360, int(requested * 0.8))
    return min(max(requested, scaled, buffered), 1800)


def compute_top_up_fetch_limit(requested_limit: int, current_fetch_limit: int) -> int:
    requested = max(1, int(requested_limit or 1))
    current = max(requested, int(current_fetch_limit or requested))
    baseline = compute_internal_fetch_limit(requested)
    stepped = max(
        current + max(80, requested),
        int(round(current * 1.75)),
        baseline,
    )
    ceiling = min(max(baseline, requested * 10), 3600)
    return min(max(current, stepped), ceiling)


def compute_provider_max_queries(requested_limit: int) -> int:
    requested = max(1, int(requested_limit or 1))
    if requested <= 50:
        return 180
    if requested <= 120:
        return 300
    if requested <= 220:
        return 460
    if requested <= 350:
        return 420
    if requested <= 500:
        return 620
    return min(1200, max(700, int(round(requested * 1.6))))
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
CLAUSE_LEAD_PATTERNS = {
    "role": re.compile(r"\b(?:the role|this role)\s+leads?\s+(?P<content>[^\.]+)", re.IGNORECASE),
    "required": re.compile(r"\brequired\s+experience\s+includes?\s+(?P<content>[^\.]+)", re.IGNORECASE),
    "ideal": re.compile(r"\bideal\s+candidates?\s+have\s+(?P<content>[^\.]+)", re.IGNORECASE),
}
ACTION_SIGNAL_PATTERN = re.compile(
    r"\b(lead|manage|own|drive|coordinate|optimiz|forecast|plan|logistics|inventory|supplier|warehouse|"
    r"s&op|distribution|service levels?|stockouts?|fulfillment|erp|sap|cross-border|multi-country)\b",
    re.IGNORECASE,
)
ROLE_HINT_PATTERNS = [
    re.compile(r"\b(?:we are hiring|hiring|seeking|looking for)\s+(?:an?\s+)?(?P<title>[A-Z][A-Za-z&/\-\s]{3,80})", re.IGNORECASE),
    re.compile(r"\b(?:role|position|job title)\s*:\s*(?P<title>[A-Z][A-Za-z&/\-\s]{3,80})", re.IGNORECASE),
]
SECTION_HEADER_MAP = {
    "key responsibilities": "responsibilities",
    "responsibilities": "responsibilities",
    "main responsibilities": "responsibilities",
    "what you will do": "responsibilities",
    "qualifications and experience": "qualifications",
    "qualifications": "qualifications",
    "requirements": "qualifications",
    "required experience": "qualifications",
    "position summary": "summary",
    "role summary": "summary",
    "job summary": "summary",
}
SECTION_BOOST = {
    "responsibilities": 3,
    "qualifications": 3,
    "summary": 1,
    "general": 0,
}
BULLET_PREFIX_PATTERN = re.compile(r"^\s*(?:[-*\u2022]|(?:\d{1,2}[\.\)]))\s*")
OVERVIEW_FLUFF_PATTERN = re.compile(
    r"\b(company overview|household name|synonymous with|aspirational value proposition|"
    r"products are sourced|cross cultural influences|luxury living|design coexistence)\b",
    re.IGNORECASE,
)
INTRO_FLUFF_PATTERN = re.compile(
    r"^\s*(we are seeking|we are hiring|we seek|this role is|the role is|the incoming)\b",
    re.IGNORECASE,
)
JD_MIN_KEY_POINTS = 8
JD_MAX_KEY_POINTS = 12


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


def _normalize_jd_point(text: str) -> str:
    cleaned = re.sub(r"[\u2022•\u2023\u2043\u2219]+", "; ", str(text or ""))
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -;:,")
    if not cleaned:
        return ""
    normalized_header = normalize_text(re.sub(r"[:\-\s]+$", "", cleaned))
    if normalized_header in SECTION_HEADER_MAP or normalized_header in {
        "company overview",
        "overview",
        "about the company",
        "key experience points",
    }:
        return ""
    cleaned = re.sub(
        r"^(key responsibilities|responsibilities|qualifications and experience|qualifications|"
        r"position summary|role summary|job summary|summary)\s*[:\-]\s*",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"^(key responsibilities|responsibilities|qualifications and experience|qualifications|position summary|summary|"
        r"the role requires|role requires|responsibilities include|responsible for|you should bring|you will|"
        r"candidates should|candidate should|ideal candidates have|ideal candidate has|must have|required|"
        r"preferred|bonus points for|experience with|experience in|strong)\s+",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"^(we are seeking|we are hiring|we seek|this role is|the role is|the incoming)\s+",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = cleaned.strip(" -;:,")
    if not cleaned:
        return ""
    if normalize_text(cleaned) in {"key responsibilities", "responsibilities", "qualifications", "summary"}:
        return ""
    if len(cleaned) > 180:
        cleaned = _truncate(cleaned, limit=180)
    return cleaned[0].upper() + cleaned[1:]


def _jd_point_fragments(text: str) -> List[str]:
    fragments: List[str] = []
    for chunk in re.split(r"(?<=[\.\!\?;])\s+", str(text or "")):
        cleaned = chunk.strip()
        if not cleaned:
            continue
        if len(cleaned) > 170 and ", and " in cleaned:
            fragments.extend(part.strip() for part in cleaned.split(", and ") if part.strip())
            continue
        fragments.append(cleaned)
    return fragments


def _split_clause_items(text: str) -> List[str]:
    if not text:
        return []
    parts = re.split(r",\s+|\s+\band\b\s+", str(text or "").strip())
    return unique_preserving_order([part.strip(" -;:.") for part in parts if part.strip(" -;:.")])


def _extract_dense_clause_points(text: str) -> List[str]:
    points: List[str] = []
    normalized_text = str(text or "")

    role_match = CLAUSE_LEAD_PATTERNS["role"].search(normalized_text)
    if role_match:
        scope = _normalize_jd_point(role_match.group("content"))
        if scope:
            points.append(f"Leads {scope}.")

    required_match = CLAUSE_LEAD_PATTERNS["required"].search(normalized_text)
    if required_match:
        for item in _split_clause_items(required_match.group("content")):
            if len(item) < 12:
                continue
            cleaned_item = _normalize_jd_point(item)
            if not cleaned_item:
                continue
            points.append(f"Required experience in {cleaned_item[0].lower() + cleaned_item[1:]}.")

    ideal_match = CLAUSE_LEAD_PATTERNS["ideal"].search(normalized_text)
    if ideal_match:
        for item in _split_clause_items(ideal_match.group("content")):
            if len(item) < 12:
                continue
            cleaned_item = _normalize_jd_point(item)
            if not cleaned_item:
                continue
            points.append(f"Ideal background includes {cleaned_item[0].lower() + cleaned_item[1:]}.")

    for sentence in _sentence_candidates(normalized_text):
        cleaned_sentence = _normalize_jd_point(sentence)
        if len(cleaned_sentence) < 24:
            continue
        if not ACTION_SIGNAL_PATTERN.search(cleaned_sentence):
            continue
        points.append(cleaned_sentence)

    return unique_preserving_order(points)


def _section_from_heading(line: str) -> str:
    normalized = normalize_text(re.sub(r"[:\-\s]+$", "", str(line or "")))
    return SECTION_HEADER_MAP.get(normalized, "")


def _section_candidates(text: str) -> List[tuple[int, str, str]]:
    rows: List[tuple[int, str, str]] = []
    current_section = "general"
    for index, raw_line in enumerate(str(text or "").splitlines()):
        raw = str(raw_line or "").strip()
        if not raw:
            continue
        cleaned = BULLET_PREFIX_PATTERN.sub("", raw).strip()
        if not cleaned:
            continue

        split = re.split(r"\s*[:\-]\s*", cleaned, maxsplit=1)
        heading = _section_from_heading(split[0])
        if heading:
            current_section = heading
            if len(split) > 1 and split[1].strip():
                rows.append((index, split[1].strip(), heading))
            continue

        is_bullet = bool(BULLET_PREFIX_PATTERN.match(raw))
        if current_section != "general":
            if is_bullet or ":" in cleaned or len(cleaned.split()) >= 6:
                rows.append((index, cleaned, current_section))
                continue
        if is_bullet:
            rows.append((index, cleaned, "general"))
    return rows


def _extract_key_experience_points(text: str, role_title: str = "") -> List[str]:
    role_tokens = [
        token
        for token in re.split(r"[^a-z0-9]+", normalize_text(role_title))
        if len(token) >= 4
    ]
    scored_points: List[tuple[int, int, str]] = []
    section_rows = _section_candidates(text)
    candidate_rows: List[tuple[int, str, str]] = [
        *section_rows,
        *[(index + 1000, sentence, "general") for index, sentence in enumerate(_sentence_candidates(text))],
    ]
    for index, sentence, section in candidate_rows:
        for fragment in _jd_point_fragments(sentence):
            cleaned = _normalize_jd_point(fragment)
            if len(cleaned) < 20:
                continue
            score = SECTION_BOOST.get(section, 0)
            if JD_SIGNAL_PATTERN.search(fragment):
                score += 3
            if any(token in normalize_text(fragment) for token in role_tokens):
                score += 2
            score += min(3, len(_match_phrases(fragment, KEYWORD_PHRASES)))
            if _match_phrases(fragment, INDUSTRY_PHRASES):
                score += 1
            if YEARS_RANGE_PATTERN.search(fragment) or YEARS_PLUS_PATTERN.search(fragment):
                score += 2
            is_overview_fluff = bool(OVERVIEW_FLUFF_PATTERN.search(fragment))
            is_intro_fluff = bool(INTRO_FLUFF_PATTERN.search(fragment))
            if is_overview_fluff:
                score -= 2
            if is_intro_fluff:
                score -= 1
            if score <= 0:
                continue
            if section == "general" and score < 3:
                continue
            if is_overview_fluff and section in {"general", "summary"}:
                continue
            if is_intro_fluff and section == "summary":
                continue
            if score < 4 and is_overview_fluff:
                continue
            scored_points.append((score, index, cleaned))

    if not scored_points:
        return []

    ordered = [item[2] for item in sorted(scored_points, key=lambda item: (-item[0], item[1]))]
    return unique_preserving_order(ordered)[:JD_MAX_KEY_POINTS]


def _build_jd_summary(
    *,
    role_title: str,
    years: Dict[str, Any],
    required_keywords: List[str],
    preferred_keywords: List[str],
    industry_keywords: List[str],
    key_points: List[str],
) -> str:
    parts: List[str] = []
    if role_title:
        parts.append(f"Target role: {role_title}.")
    if years.get("min") is not None or years.get("max") is not None:
        if years.get("min") is not None and years.get("max") is not None:
            parts.append(f"Expected experience: {years['min']}-{years['max']} years.")
        elif years.get("min") is not None:
            parts.append(f"Expected experience: {years['min']}+ years.")
        elif years.get("max") is not None:
            parts.append(f"Expected experience: up to {years['max']} years.")
    if required_keywords:
        parts.append(f"Core skills: {', '.join(required_keywords[:5])}.")
    elif key_points:
        parts.append(f"Core focus: {key_points[0]}")
    if preferred_keywords:
        parts.append(f"Preferred extras: {', '.join(preferred_keywords[:4])}.")
    if industry_keywords:
        parts.append(f"Relevant industries: {', '.join(industry_keywords[:3])}.")
    summary = " ".join(part.strip() for part in parts if part.strip())
    return _truncate(summary or "Breakdown ready.", limit=320)


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


def resolve_job_description_source(
    *,
    typed_text: str = "",
    uploaded_text: str = "",
    uploaded_file_name: str = "",
) -> Dict[str, Any]:
    typed = str(typed_text or "").strip()
    uploaded = str(uploaded_text or "").strip()
    file_name = str(uploaded_file_name or "").strip()

    if uploaded:
        combined = uploaded
        if typed:
            combined = f"{uploaded}\n\nRecruiter Notes:\n{typed}"
        return {
            "source": "uploaded_file",
            "file_name": file_name,
            "typed_text": typed,
            "uploaded_text": uploaded,
            "primary_text": uploaded,
            "combined_text": combined,
        }

    return {
        "source": "typed_text",
        "file_name": "",
        "typed_text": typed,
        "uploaded_text": "",
        "primary_text": typed,
        "combined_text": typed,
    }


def _infer_role_titles(text: str, role_title: str = "") -> List[str]:
    inferred = unique_preserving_order([role_title] if role_title else [])
    source_text = str(text or "").strip()
    if not source_text:
        return inferred

    for pattern in ROLE_HINT_PATTERNS:
        match = pattern.search(source_text)
        if not match:
            continue
        candidate = re.split(r"[\.;,\n]", match.group("title"))[0].strip(" -:")
        candidate = re.sub(
            r"\b(?:based in|located in|for|to lead|with|who|in\s+[A-Z][A-Za-z\s/&-]+)\b.*$",
            "",
            candidate,
            flags=re.IGNORECASE,
        ).strip(" -:")
        if 3 <= len(candidate) <= 80:
            inferred = unique_preserving_order([*inferred, candidate])

    if not inferred:
        first_line = next((line.strip(" -:") for line in source_text.splitlines() if line.strip()), "")
        if first_line and 4 <= len(first_line) <= 80 and re.search(r"\b(manager|analyst|lead|director|engineer|specialist|coordinator|partner|officer|executive)\b", first_line, re.IGNORECASE):
            inferred = unique_preserving_order([first_line])

    return inferred


def extract_job_description_breakdown(job_description: str, role_title: str = "") -> Dict[str, Any]:
    text = str(job_description or "").strip()
    inferred_titles = _infer_role_titles(text, role_title=role_title)
    resolved_role_title = role_title or (inferred_titles[0] if inferred_titles else "")
    if not text:
        return {
            "summary": "",
            "key_experience_points": [],
            "required_keywords": [],
            "preferred_keywords": [],
            "industry_keywords": [],
            "titles": inferred_titles,
            "seniority_levels": [],
            "years": {"mode": "range", "value": None, "min": None, "max": None, "tolerance": 0},
            "suggested_anchors": {},
        }

    sentence_candidates = _sentence_candidates(text)
    key_points = _extract_key_experience_points(text, role_title=resolved_role_title)
    if not key_points:
        key_points = unique_preserving_order(
            [_normalize_jd_point(sentence) for sentence in sentence_candidates if _normalize_jd_point(sentence)]
        )[:JD_MAX_KEY_POINTS]

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
    if len(required_keywords) < 4:
        required_keywords = unique_preserving_order(
            [
                *required_keywords,
                *_match_phrases(" ".join([*required_lines, *key_points, text]), KEYWORD_PHRASES),
            ]
        )[:10]
    preferred_keywords = _match_phrases(" ".join(preferred_lines), KEYWORD_PHRASES)
    industry_keywords = _match_phrases(text, INDUSTRY_PHRASES)
    seniority = _match_phrases(" ".join([resolved_role_title, text]), SENIORITY_LEVELS)
    years = _extract_years_signal(text)

    if len(key_points) < JD_MIN_KEY_POINTS:
        dense_points = _extract_dense_clause_points(text)
        key_points = unique_preserving_order([*key_points, *dense_points])[:JD_MAX_KEY_POINTS]

    suggested_anchors: Dict[str, str] = {
        "title": "critical",
        "skills": "critical" if required_keywords else "important",
        "semantic": "important",
    }
    if years.get("min") is not None or years.get("max") is not None:
        suggested_anchors["years"] = "important"
    if industry_keywords:
        suggested_anchors["industry"] = "preferred"

    return {
        "summary": _build_jd_summary(
            role_title=resolved_role_title,
            years=years,
            required_keywords=required_keywords,
            preferred_keywords=preferred_keywords,
            industry_keywords=industry_keywords,
            key_points=key_points,
        ),
        "key_experience_points": key_points,
        "required_keywords": required_keywords,
        "preferred_keywords": [value for value in preferred_keywords if value not in required_keywords],
        "industry_keywords": industry_keywords,
        "titles": inferred_titles,
        "seniority_levels": seniority,
        "years": years,
        "suggested_anchors": suggested_anchors,
    }


def ensure_structured_jd_breakdown(
    breakdown: Dict[str, Any] | None,
    *,
    job_description: str,
    role_title: str = "",
) -> Dict[str, Any]:
    local_breakdown = extract_job_description_breakdown(job_description, role_title=role_title)
    if not isinstance(breakdown, dict):
        return local_breakdown

    merged = dict(breakdown)

    def _missing_list(name: str) -> bool:
        value = merged.get(name)
        return not isinstance(value, list) or not [item for item in value if str(item).strip()]

    for key in [
        "titles",
        "key_experience_points",
        "required_keywords",
        "preferred_keywords",
        "industry_keywords",
        "seniority_levels",
    ]:
        if _missing_list(key):
            merged[key] = local_breakdown.get(key, [])

    existing_points = unique_preserving_order(
        [str(value).strip() for value in merged.get("key_experience_points", []) if str(value).strip()]
    )
    local_points = unique_preserving_order(
        [str(value).strip() for value in local_breakdown.get("key_experience_points", []) if str(value).strip()]
    )
    if len(existing_points) < JD_MIN_KEY_POINTS:
        merged["key_experience_points"] = unique_preserving_order([*existing_points, *local_points])[:JD_MAX_KEY_POINTS]
    else:
        merged["key_experience_points"] = existing_points[:JD_MAX_KEY_POINTS]

    years = merged.get("years")
    local_years = local_breakdown.get("years", {})
    if (
        not isinstance(years, dict)
        or all(years.get(field) in (None, "", 0) for field in ("value", "min", "max"))
        or (
            isinstance(local_years, dict)
            and (local_years.get("min") is not None or local_years.get("max") is not None)
            and years.get("min") is None
            and years.get("max") is None
        )
    ):
        merged["years"] = local_breakdown.get("years", {})

    merged["required_keywords"] = unique_preserving_order(
        [str(value).strip() for value in merged.get("required_keywords", []) if str(value).strip()]
    )
    merged["preferred_keywords"] = [
        value
        for value in unique_preserving_order(
            [str(value).strip() for value in merged.get("preferred_keywords", []) if str(value).strip()]
        )
        if value not in merged["required_keywords"]
    ]

    if not isinstance(merged.get("suggested_anchors"), dict) or not merged.get("suggested_anchors"):
        merged["suggested_anchors"] = local_breakdown.get("suggested_anchors", {})

    merged["summary"] = _build_jd_summary(
        role_title=role_title or (merged.get("titles") or [""])[0],
        years=merged.get("years", {}),
        required_keywords=merged.get("required_keywords", []),
        preferred_keywords=merged.get("preferred_keywords", []),
        industry_keywords=merged.get("industry_keywords", []),
        key_points=merged.get("key_experience_points", []),
    )

    return merged


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _selected_country_code(countries: List[str]) -> str:
    for country in countries:
        code = COUNTRY_TO_ALPHA2.get(normalize_text(country), "")
        if code:
            return code
    return ""


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


def assess_ui_brief_quality(brief_config: Dict[str, Any]) -> Dict[str, Any]:
    role_title = str(brief_config.get("role_title", "")).strip()
    geography = brief_config.get("geography", {})
    if not isinstance(geography, dict):
        geography = {}
    location_targets = [
        str(value).strip()
        for value in brief_config.get("location_targets", [])
        if str(value).strip()
    ]
    titles = [
        str(value).strip()
        for value in brief_config.get("titles", [])
        if str(value).strip()
    ]
    required_keywords = [
        str(value).strip()
        for value in brief_config.get("required_keywords", [])
        if str(value).strip()
    ]
    preferred_keywords = [
        str(value).strip()
        for value in brief_config.get("preferred_keywords", [])
        if str(value).strip()
    ]
    industry_keywords = [
        str(value).strip()
        for value in brief_config.get("industry_keywords", [])
        if str(value).strip()
    ]
    company_targets = [
        str(value).strip()
        for value in brief_config.get("company_targets", [])
        if str(value).strip()
    ]
    document_text = str(brief_config.get("document_text", "")).strip()
    min_years = _coerce_int(brief_config.get("minimum_years_experience"))
    max_years = _coerce_int(brief_config.get("maximum_years_experience"))

    score = 0
    issues: List[str] = []
    suggestions: List[str] = []
    detail_signals = 0

    if role_title:
        score += 2
    else:
        issues.append("Role title is missing.")
        suggestions.append("Add a clear role title before running search.")

    geo_present = bool(
        location_targets
        or str(geography.get("location_name", "")).strip()
        or str(geography.get("country", "")).strip()
    )
    if geo_present:
        score += 2
        detail_signals += 1
    else:
        issues.append("Target geography is missing.")
        suggestions.append("Add at least one country, city, or continent.")

    if len(required_keywords) >= 2:
        score += 3
        detail_signals += 1
    elif required_keywords:
        score += 2
        detail_signals += 1

    if len(titles) >= 2:
        score += 1
        detail_signals += 1

    if industry_keywords:
        score += 1
        detail_signals += 1

    if len(company_targets) >= 2:
        score += 1
        detail_signals += 1

    if len(document_text) >= 220:
        score += 2
        detail_signals += 1
    elif len(document_text) >= 100:
        score += 1
        detail_signals += 1

    if min_years is not None or max_years is not None:
        score += 1

    if detail_signals < 2:
        issues.append("Hunt brief is too thin for reliable ranking.")
        suggestions.append("Add at least two detail sections: JD text, must-have skills, titles, companies, or industries.")

    ok = bool(role_title and geo_present and detail_signals >= 2 and score >= 5)
    if ok:
        message = "Brief details look sufficient for search."
    else:
        message = (
            "Hunt details are not enough yet. Add role title, geography, and at least two detail sections "
            "(for example JD text + must-have skills) before running search."
        )

    return {
        "ok": ok,
        "score": int(score),
        "issues": unique_preserving_order(issues),
        "suggestions": unique_preserving_order(suggestions),
        "message": message,
    }


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
    job_description_notes = str(payload.get("job_description", "")).strip()
    uploaded_job_description_text = str(payload.get("uploaded_job_description_text", "")).strip()
    uploaded_job_description_name = str(payload.get("uploaded_job_description_name", "")).strip()
    job_description_source = resolve_job_description_source(
        typed_text=job_description_notes,
        uploaded_text=uploaded_job_description_text,
        uploaded_file_name=uploaded_job_description_name,
    )
    job_description = job_description_source["combined_text"]
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
    internal_fetch_override = _coerce_int(payload.get("internal_fetch_limit_override"))
    internal_fetch_limit = max(
        limit,
        internal_fetch_override if internal_fetch_override is not None else compute_internal_fetch_limit(limit),
    )
    csv_export_limit = max(1, int(payload.get("csv_export_limit", limit) or limit))
    reranker_enabled = bool(payload.get("reranker_enabled", True))
    learned_ranker_enabled = bool(payload.get("learned_ranker_enabled", False))
    top_up_round = max(0, _coerce_int(payload.get("top_up_round")) or 0)
    country_code = _selected_country_code(countries)
    reranker_requested_top_n = _coerce_int(payload.get("reranker_top_n"))
    if reranker_requested_top_n is None:
        if limit >= 400:
            reranker_requested_top_n = max(220, int(round(limit * 0.6)))
        elif limit >= 300:
            reranker_requested_top_n = max(180, int(round(limit * 0.6)))
        elif limit >= 180:
            reranker_requested_top_n = limit + 60
        else:
            reranker_requested_top_n = max(limit * 2, 220)
    reranker_floor = max(120, int(round(limit * 0.55))) if limit >= 220 else limit
    reranker_top_n = min(
        max(reranker_floor, int(reranker_requested_top_n or reranker_floor)),
        min(internal_fetch_limit, max(limit, 320)),
    )
    scrapingbee_parallel_requests = max(
        4,
        _coerce_int(payload.get("provider_parallel_requests"))
        or (16 if limit >= 220 else (12 if limit >= 120 else 8)),
    )
    scrapingbee_pages_per_query = max(
        1,
        min(
            5,
            (_coerce_int(payload.get("scrapingbee_pages_per_query")) or 1),
        ),
    )
    scrapingbee_max_queries = max(
        120,
        _coerce_int(payload.get("scrapingbee_max_queries")) or compute_provider_max_queries(limit),
    )
    default_geo_groups = 8 if limit >= 220 else (6 if limit >= 120 else 8)
    providers_settings = {
        "retrieval": {
            "company_chunk_size": int(payload.get("company_chunk_size", 5) or 5),
            "results_per_slice": max(internal_fetch_limit, int(payload.get("results_per_slice", 40) or 40)),
            "include_strict_slice": True,
            "include_broad_slice": True,
            "include_history_slices": bool(payload.get("include_history_slices", True)),
            "include_discovery_slices": bool(payload.get("include_discovery_slices", True)),
            "geo_fanout_enabled": bool(payload.get("geo_fanout_enabled", True)),
            "max_geo_groups": max(3, _coerce_int(payload.get("max_geo_groups")) or default_geo_groups),
            "discovery_keyword_chunk_size": int(payload.get("discovery_keyword_chunk_size", 6) or 6),
            "market_keyword_chunk_size": int(payload.get("market_keyword_chunk_size", 5) or 5),
            "history_query_terms": unique_preserving_order(
                [*breakdown.get("key_experience_points", [])[:3], "formerly", "previously", "before joining", "ex"]
            ),
        },
        "registry_memory": {
            "enabled": bool(payload.get("registry_memory_enabled", True)),
            "limit": max(internal_fetch_limit, int(payload.get("registry_memory_limit", 20) or 20)),
        },
        "reranker": {
            "enabled": reranker_enabled,
            "model_name": str(payload.get("reranker_model_name", "BAAI/bge-reranker-v2-m3")),
            "top_n": reranker_top_n,
            "weight": float(payload.get("reranker_weight", 0.35) or 0.35),
        },
        "learned_ranker": {
            "enabled": learned_ranker_enabled,
            "model_dir": str(model_dir),
            "weight": float(payload.get("learned_ranker_weight", 0.7) or 0.7),
        },
        "scrapingbee_google": {
            "country_code": str(payload.get("scrapingbee_country_code", "") or country_code or "us"),
            "parallel_requests": scrapingbee_parallel_requests,
            "pages_per_query": scrapingbee_pages_per_query,
            "max_queries": scrapingbee_max_queries,
            "max_company_terms_per_query": max(6, _coerce_int(payload.get("max_company_terms_per_query")) or 12),
            "geo_fanout_enabled": bool(payload.get("geo_fanout_enabled", True)),
            "max_geo_groups": max(3, _coerce_int(payload.get("max_geo_groups")) or default_geo_groups),
        },
    }

    summary_lines = []
    if role_title:
        summary_lines.append(f"Role: {role_title}")
    for point in breakdown.get("key_experience_points", [])[:6]:
        summary_lines.append(f"- {point}")

    company_match_mode = str(payload.get("company_match_mode", "both") or "both")
    employment_status_mode = str(payload.get("employment_status_mode", "any") or "any")
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
        "employment_status_mode": employment_status_mode,
        "jd_breakdown": breakdown,
        "anchors": anchors,
        "result_target_min": max(5, min(limit, 20)),
        "result_target_max": max(limit, 40),
        "max_profiles": max(limit, 80),
        "provider_settings": providers_settings,
        "ui_meta": {
            "titles": titles,
            "countries": countries,
            "continents": continents,
            "cities": cities,
            "company_targets": companies,
            "must_have_keywords": must_have,
            "nice_to_have_keywords": nice_to_have,
            "industry_keywords": industry_keywords,
            "exclude_title_keywords": exclude_titles,
            "exclude_company_keywords": exclude_companies,
            "years_mode": years_mode,
            "years_value": years_value,
            "years_tolerance": years_tolerance,
            "minimum_years_experience": minimum_years_experience,
            "maximum_years_experience": maximum_years_experience,
            "radius_miles": radius_miles,
            "candidate_limit": limit,
            "company_match_mode": company_match_mode,
            "employment_status_mode": employment_status_mode,
            "job_description": job_description_notes,
            "job_description_source": job_description_source["source"],
            "uploaded_job_description_name": uploaded_job_description_name,
            "uploaded_job_description_text": uploaded_job_description_text,
            "anchors": anchors,
        },
    }

    return {
        "brief_config": brief_config,
        "providers": providers,
        "limit": limit,
        "internal_fetch_limit": internal_fetch_limit,
        "csv_export_limit": csv_export_limit,
        "output_dir": str(output_dir),
        "feedback_db": str(feedback_db),
        "model_dir": str(model_dir),
        "job_description_breakdown": breakdown,
        "job_description_source": job_description_source,
    }


def build_app_bootstrap() -> Dict[str, Any]:
    default_feedback_db = str(resolve_feedback_db_path())
    default_model_dir = str(resolve_ranker_model_dir())
    default_output_dir = str(resolve_output_dir())
    code_only_login_enabled = env_flag("HR_HUNTER_CODE_ONLY_LOGIN")
    bootstrap = {
        "auth": {
            "mode": "totp",
            "issuer": "HR Hunter",
            "code_digits": 6,
            "code_label": "Authenticator Code",
            "email_required": not code_only_login_enabled,
            "code_only_login_enabled": code_only_login_enabled,
        },
        "anchors": ANCHOR_OPTIONS,
        "feedback_actions": FEEDBACK_ACTIONS,
        "providers": ["scrapingbee_google"],
        "countries": COUNTRY_OPTIONS,
        "continents": CONTINENT_OPTIONS,
        "themes": THEME_OPTIONS,
        "employment_status_options": EMPLOYMENT_STATUS_OPTIONS,
        "defaults": {
            "providers": ["scrapingbee_google"],
            "limit": 20,
            "csv_export_limit": 20,
            "radius_miles": 25,
            "company_match_mode": "both",
            "employment_status_mode": "any",
            "theme": "bright",
            "registry_memory_enabled": True,
            "include_history_slices": True,
            "include_discovery_slices": True,
            "reranker_enabled": True,
            "learned_ranker_enabled": False,
            "reranker_model_name": "BAAI/bge-reranker-v2-m3",
            "feedback_db": default_feedback_db,
            "model_dir": default_model_dir,
            "output_dir": default_output_dir,
        },
        "presets": {
            "ceo_marina_home_emea": {
                "project_name": "Marina Home - CEO Leadership Search",
                "client_name": "Marina Home Interiors",
                "role_title": "Chief Executive Officer (CEO)",
                "titles": [
                    "Chief Executive Officer",
                    "CEO",
                    "Group CEO",
                    "Managing Director",
                    "President",
                    "Chief Operating Officer",
                    "General Manager",
                    "Country Manager",
                    "Business Director",
                ],
                "countries": [
                    "United Arab Emirates",
                    "Saudi Arabia",
                    "Qatar",
                    "Oman",
                    "Kuwait",
                    "Bahrain",
                    "Egypt",
                    "United Kingdom",
                    "Germany",
                    "France",
                    "Italy",
                    "Spain",
                    "Netherlands",
                    "Poland",
                    "India",
                ],
                "continents": ["Middle East", "Africa", "Europe", "Asia"],
                "cities": [
                    "Dubai",
                    "Abu Dhabi",
                    "Riyadh",
                    "Jeddah",
                    "Doha",
                    "Muscat",
                    "Kuwait City",
                    "Manama",
                    "Cairo",
                    "Alexandria",
                    "London",
                    "Manchester",
                    "Berlin",
                    "Munich",
                    "Paris",
                    "Milan",
                    "Madrid",
                    "Amsterdam",
                    "Warsaw",
                    "Mumbai",
                    "New Delhi",
                    "Bengaluru",
                    "Chennai",
                ],
                "company_targets": [
                    "Marina Home Interiors",
                    "Home Centre",
                    "Pan Emirates",
                    "The One",
                    "Al Huzaifa",
                    "IDdesign",
                    "IKEA",
                    "Pottery Barn",
                    "West Elm",
                    "Crate & Barrel",
                    "Williams-Sonoma",
                    "RH",
                    "Ethan Allen",
                    "Roche Bobois",
                    "BoConcept",
                    "Kartell",
                    "CB2",
                    "Maisons du Monde",
                    "Zara Home",
                    "Home Box",
                    "Harrods Home",
                    "Selfridges Home",
                ],
                "company_match_mode": "both",
                "employment_status_mode": "any",
                "years_mode": "at_least",
                "years_value": 5,
                "years_tolerance": 0,
                "max_profiles": 300,
                "must_have_keywords": [
                    "General Management",
                    "P&L",
                    "Business Scaling",
                ],
                "nice_to_have_keywords": [
                    "Operational Excellence",
                    "Team Leadership",
                    "Brand Development",
                    "Market Expansion",
                    "Stakeholder Management",
                    "Family-Owned Business",
                    "Founder-Led Transition",
                    "Board Governance",
                    "Arabic",
                    "MBA",
                    "Home Interiors",
                    "Omnichannel",
                ],
                "industry_keywords": ["luxury retail", "home furnishings", "interior design", "consumer"],
                "job_description": (
                    "Marina Home Interiors is a Dubai-based premium home furnishings brand with operations across the "
                    "Arabian Gulf region, Egypt, and the Indian subcontinent. We are hiring a visionary CEO to lead "
                    "the business into its next growth phase as the founder transitions to a formal board role. The "
                    "role requires strategic leadership, operational excellence, financial stewardship, innovation, and "
                    "strong stakeholder engagement while preserving the brand's creative identity. Candidates should "
                    "bring CEO or senior executive leadership experience, ideally in luxury home interiors, premium "
                    "retail, or design-led consumer businesses, with proven capability in profitability management, "
                    "international expansion, and leading high-performing executive teams. Experience in family-owned "
                    "or founder-led environments is advantageous. Fluent English is required and Arabic is an advantage."
                ),
                "jd_breakdown": extract_job_description_breakdown(
                    (
                        "Marina Home Interiors is a Dubai-based premium home furnishings brand with operations across the "
                        "Arabian Gulf region, Egypt, and the Indian subcontinent. We are hiring a visionary CEO to lead "
                        "the business into its next growth phase as the founder transitions to a formal board role. The "
                        "role requires strategic leadership, operational excellence, financial stewardship, innovation, and "
                        "strong stakeholder engagement while preserving the brand's creative identity. Candidates should "
                        "bring CEO or senior executive leadership experience, ideally in luxury home interiors, premium "
                        "retail, or design-led consumer businesses, with proven capability in profitability management, "
                        "international expansion, and leading high-performing executive teams. Experience in family-owned "
                        "or founder-led environments is advantageous. Fluent English is required and Arabic is an advantage."
                    ),
                    role_title="Chief Executive Officer (CEO)",
                ),
                "anchors": {
                    "title": "preferred",
                    "skills": "preferred",
                    "location": "preferred",
                    "company": "preferred",
                    "years": "preferred",
                    "industry": "preferred",
                    "function": "preferred",
                    "semantic": "preferred",
                },
            },
        },
    }
    # Backward-compat alias for older UI keys.
    bootstrap["presets"]["supply_chain_manager_uae"] = dict(bootstrap["presets"]["ceo_marina_home_emea"])
    return bootstrap


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
