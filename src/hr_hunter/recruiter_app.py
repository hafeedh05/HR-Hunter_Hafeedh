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


def _coerce_bool(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
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


FOCUSED_SEARCH_PROFILE = "focused"
BALANCED_SEARCH_PROFILE = "balanced"
EXPLORATORY_SEARCH_PROFILE = "exploratory"
SUPPORTED_SEARCH_PROFILES = {
    FOCUSED_SEARCH_PROFILE,
    BALANCED_SEARCH_PROFILE,
    EXPLORATORY_SEARCH_PROFILE,
}
DEFAULT_UI_RERANKER_MODEL = "cross-encoder/ms-marco-MiniLM-L6-v2"
FOCUSED_QUERY_FAMILY_BUDGETS = {
    "org_chart_profile_pages": 8,
    "profile_like_public_pages": 8,
    "team_leadership_pages": 6,
    "appointment_news_pages": 4,
    "speaker_bio_pages": 2,
    "award_industry_pages": 1,
    "industry_association_pages": 1,
    "trade_directory_pages": 1,
}
FOCUSED_NON_EXECUTIVE_QUERY_FAMILY_BUDGETS = {
    "profile_like_public_pages": 18,
    "team_leadership_pages": 3,
    "trade_directory_pages": 8,
    "industry_association_pages": 6,
    "org_chart_profile_pages": 1,
    "appointment_news_pages": 1,
    "speaker_bio_pages": 1,
    "award_industry_pages": 0,
}
FOCUSED_NON_EXECUTIVE_TOP_UP_QUERY_FAMILY_BUDGETS = {
    "profile_like_public_pages": 14,
    "trade_directory_pages": 6,
    "industry_association_pages": 5,
    "team_leadership_pages": 2,
    "org_chart_profile_pages": 0,
    "appointment_news_pages": 1,
    "speaker_bio_pages": 0,
    "award_industry_pages": 0,
}
EXECUTIVE_ROLE_HINTS = (
    "ceo",
    "chief executive officer",
    "chief",
    "president",
    "managing director",
    "general manager",
    "vice president",
    "vp",
)
TITLE_SCOPE_EXAMPLE_MAP = (
    ("chief executive officer", ["President", "Managing Director", "General Manager"]),
    ("ceo", ["President", "Managing Director", "General Manager"]),
    ("chief marketing officer", ["President", "Managing Director", "General Manager"]),
    ("cmo", ["VP Marketing", "Marketing Director", "Head of Marketing"]),
    ("chief technology officer", ["VP Engineering", "Engineering Director", "Head of Engineering"]),
    ("cto", ["VP Engineering", "Engineering Director", "Head of Engineering"]),
    ("digital marketing manager", ["Performance Marketing Manager", "Growth Marketing Manager", "Acquisition Marketing Manager"]),
    ("marketing manager", ["Performance Marketing Manager", "Growth Marketing Manager", "Demand Generation Manager"]),
    ("data analyst", ["Business Analyst", "Commercial Analyst", "Insights Analyst"]),
)


def _is_executive_brief(role_title: str, titles: List[str]) -> bool:
    targets = [role_title, *titles]
    return any(
        hint in normalize_text(value)
        for value in targets
        if str(value).strip()
        for hint in EXECUTIVE_ROLE_HINTS
    )


def _canonical_title_scope_value(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    return normalize_text(re.sub(r"\([^)]*\)", " ", raw))


def _has_explicit_title_scope(titles: List[str]) -> bool:
    explicit_titles = unique_preserving_order(
        [_canonical_title_scope_value(str(title)) for title in titles if _canonical_title_scope_value(str(title))]
    )
    return len(explicit_titles) > 1


def _title_scope_examples(role_title: str, titles: List[str]) -> List[str]:
    normalized_scope = " ".join(
        normalize_text(value)
        for value in [role_title, *titles]
        if str(value).strip()
    )
    existing_titles = {
        _canonical_title_scope_value(value)
        for value in [role_title, *titles]
        if _canonical_title_scope_value(value)
    }
    for title_hint, examples in TITLE_SCOPE_EXAMPLE_MAP:
        if title_hint not in normalized_scope:
            continue
        return [
            example
            for example in examples
            if normalize_text(example) not in existing_titles
        ]
    return []


def _human_join(values: List[str]) -> str:
    cleaned = [str(value).strip() for value in values if str(value).strip()]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    if len(cleaned) == 2:
        return f"{cleaned[0]} or {cleaned[1]}"
    return f"{', '.join(cleaned[:-1])}, or {cleaned[-1]}"


def _preview_values(values: List[str], *, limit: int = 3) -> List[str]:
    cleaned = [str(value).strip() for value in values if str(value).strip()]
    return cleaned[:limit]


def _recommended_allow_adjacent_titles(
    *,
    search_profile: str,
    executive_brief: bool,
    common_volume_search: bool,
    explicit_title_scope: bool,
    companies: List[str],
    location_count: int,
) -> bool:
    if explicit_title_scope:
        return False
    if executive_brief:
        return bool(search_profile == EXPLORATORY_SEARCH_PROFILE and not companies and location_count >= 2)
    return bool(search_profile != FOCUSED_SEARCH_PROFILE or common_volume_search)


def _has_explicit_company_scope(company_match_mode: str) -> bool:
    return str(company_match_mode or "both").strip().lower() in {"current_only", "past_only"}


def _recommended_exact_company_scope(
    *,
    search_profile: str,
    executive_brief: bool,
    company_count: int,
) -> bool:
    if company_count <= 0:
        return False
    if executive_brief:
        return True
    if search_profile == FOCUSED_SEARCH_PROFILE and company_count <= 4:
        return True
    return False


def _recommended_strict_market_scope(
    *,
    search_profile: str,
    executive_brief: bool,
    common_volume_search: bool,
    company_count: int,
    location_targets: List[str],
    countries: List[str],
    cities: List[str],
) -> bool:
    if not location_targets:
        return False
    if executive_brief:
        return True
    if company_count > 0 and (cities or len(countries) <= 2):
        return True
    if common_volume_search:
        return False
    if cities:
        return True
    if search_profile == FOCUSED_SEARCH_PROFILE and len(countries) <= 2:
        return True
    return False


def _recommended_scope_first_enabled(
    *,
    search_profile: str,
    executive_brief: bool,
    common_volume_search: bool,
    strict_market_scope: bool,
    company_count: int,
) -> bool:
    if common_volume_search:
        return True
    if strict_market_scope:
        return True
    if executive_brief and company_count > 0:
        return True
    return search_profile == FOCUSED_SEARCH_PROFILE


def _recommended_in_scope_target(
    *,
    limit: int,
    executive_brief: bool,
    company_count: int,
    common_volume_search: bool,
    search_profile: str,
    strict_market_scope: bool,
) -> int:
    requested = max(1, int(limit or 1))
    if requested <= 20:
        return requested
    if executive_brief and company_count > 0:
        return min(requested, max(20, int(round(requested * 0.2))))
    if common_volume_search:
        return min(requested, max(50, int(round(requested * 0.6))))
    if strict_market_scope:
        return min(requested, max(25, int(round(requested * 0.5))))
    if search_profile == FOCUSED_SEARCH_PROFILE:
        return min(requested, max(20, int(round(requested * 0.45))))
    return min(requested, max(15, int(round(requested * 0.35))))


def _recommended_verification_scope_target(
    *,
    limit: int,
    verification_top_n: int,
    in_scope_target: int,
    executive_brief: bool,
) -> int:
    requested = max(1, int(limit or 1))
    verification_limit = max(0, int(verification_top_n or 0))
    scope_target = max(0, int(in_scope_target or 0))
    if verification_limit <= 0:
        return 0
    if executive_brief and requested >= 200:
        return min(verification_limit, max(scope_target, 80))
    return min(
        verification_limit,
        max(min(requested, 50), int(round(scope_target * 0.8))),
    )


def _default_query_family_budgets(
    *,
    search_profile: str,
    executive_brief: bool,
    top_up_round: int,
) -> Dict[str, int]:
    if search_profile != FOCUSED_SEARCH_PROFILE:
        return {}
    if executive_brief:
        return dict(FOCUSED_QUERY_FAMILY_BUDGETS)
    if top_up_round > 0:
        return dict(FOCUSED_NON_EXECUTIVE_TOP_UP_QUERY_FAMILY_BUDGETS)
    return dict(FOCUSED_NON_EXECUTIVE_QUERY_FAMILY_BUDGETS)


def _derive_search_profile(
    *,
    role_title: str,
    titles: List[str],
    location_targets: List[str],
    sourcing_company_targets: List[str],
    required_keywords: List[str],
    preferred_keywords: List[str],
    industry_keywords: List[str],
    document_text: str,
    limit: int,
) -> str:
    detail_signals = sum(
        1
        for signal in (
            len(required_keywords) >= 2,
            bool(preferred_keywords),
            bool(industry_keywords),
            bool(sourcing_company_targets),
            len(document_text) >= 120,
        )
        if signal
    )
    executive_brief = _is_executive_brief(role_title, titles)
    location_count = len(location_targets)
    title_count = len(titles)
    common_role_precision_search = (
        not executive_brief
        and not sourcing_company_targets
        and limit <= 120
        and location_count <= 2
        and title_count <= 3
        and detail_signals >= 1
    )

    if common_role_precision_search:
        return FOCUSED_SEARCH_PROFILE
    if (
        limit <= 60
        and location_count <= 2
        and title_count <= 3
        and detail_signals >= 1
        and not executive_brief
    ):
        return FOCUSED_SEARCH_PROFILE
    if limit >= 180 or location_count >= 4 or title_count >= 5 or executive_brief:
        return EXPLORATORY_SEARCH_PROFILE
    return BALANCED_SEARCH_PROFILE


def _build_brief_follow_up_questions(
    *,
    role_title: str,
    titles: List[str],
    countries: List[str],
    cities: List[str],
    location_targets: List[str],
    companies: List[str],
    source_companies: List[str],
    company_match_mode: str,
    required_keywords: List[str],
    industry_keywords: List[str],
    document_text: str,
    limit: int,
    search_profile: str,
) -> List[Dict[str, Any]]:
    questions: List[Dict[str, Any]] = []
    executive_brief = _is_executive_brief(role_title, titles)
    location_count = len(location_targets)
    title_count = len(titles)
    explicit_title_scope = _has_explicit_title_scope(titles)
    explicit_company_scope = _has_explicit_company_scope(company_match_mode)
    thin_detail = len(required_keywords) < 2 and not industry_keywords and len(document_text) < 160
    common_volume_search = bool(limit >= 40 and not source_companies and title_count <= 2 and not executive_brief)

    if location_count >= 2:
        questions.append(
            {
                "id": "prioritize_first_location",
                "label": "Primary Market",
                "prompt": "Should the first country or city you entered be treated as the primary market?",
                "help": "Use this when the search should lean harder on the lead market instead of splitting attention evenly.",
                "recommended_answer": bool(limit <= 120 and search_profile != EXPLORATORY_SEARCH_PROFILE),
            }
        )

    if role_title and title_count <= 2 and not explicit_title_scope:
        title_scope_examples = _title_scope_examples(role_title, titles)
        title_scope_prompt = "Should HR Hunter include adjacent role-family titles when exact matches look thin?"
        title_scope_help = (
            "Choose No to stay exact-title only. Choose Yes if closely related market titles should count."
        )
        if title_scope_examples:
            title_scope_prompt = (
                f"You mentioned {role_title}. Should HR Hunter also include "
                f"{_human_join(title_scope_examples[:3])} when the remit is genuinely equivalent?"
            )
            title_scope_help = (
                "Choose No to stay exact-title only. Choose Yes if those market-equivalent titles should count."
            )
        questions.append(
            {
                "id": "allow_adjacent_titles",
                "label": "Title Scope",
                "prompt": title_scope_prompt,
                "help": title_scope_help,
                "recommended_answer": _recommended_allow_adjacent_titles(
                    search_profile=search_profile,
                    executive_brief=executive_brief,
                    common_volume_search=common_volume_search,
                    explicit_title_scope=explicit_title_scope,
                    companies=companies,
                    location_count=location_count,
                ),
            }
        )

    if companies and not explicit_company_scope:
        company_scope_prompt = (
            "Should HR Hunter stay on the exact companies you entered, instead of widening into former employers "
            "or adjacent competitor companies?"
        )
        company_scope_help = (
            "Choose Yes to keep retrieval on current-company targets only. Choose No if former-company or peer-company "
            "signals should still count."
        )
        company_examples = _preview_values(companies, limit=3)
        if company_examples:
            company_scope_prompt = (
                f"Should HR Hunter treat {_human_join(company_examples)} as exact current-company targets, "
                "instead of widening into former employers or adjacent competitors?"
            )
        questions.append(
            {
                "id": "exact_company_scope",
                "label": "Company Scope",
                "prompt": company_scope_prompt,
                "help": company_scope_help,
                "recommended_answer": _recommended_exact_company_scope(
                    search_profile=search_profile,
                    executive_brief=executive_brief,
                    company_count=len(companies),
                ),
            }
        )

    market_scope_needed = bool(location_targets and (cities or len(countries) >= 2 or executive_brief))
    if market_scope_needed:
        location_examples = _preview_values([*cities, *countries], limit=3)
        market_scope_prompt = (
            "Should HR Hunter stay inside the exact countries and cities you entered, instead of widening into nearby "
            "markets when results look thin?"
        )
        if location_examples:
            market_scope_prompt = (
                f"Should HR Hunter stay inside {_human_join(location_examples)} only, instead of widening into nearby "
                "markets when results look thin?"
            )
        questions.append(
            {
                "id": "strict_market_scope",
                "label": "Market Scope",
                "prompt": market_scope_prompt,
                "help": "Choose Yes for strict market fidelity. Choose No if nearby cities or countries can be used as spillover.",
                "recommended_answer": _recommended_strict_market_scope(
                    search_profile=search_profile,
                    executive_brief=executive_brief,
                    common_volume_search=common_volume_search,
                    company_count=len(companies),
                    location_targets=location_targets,
                    countries=countries,
                    cities=cities,
                ),
            }
        )

    if location_targets and (limit >= 40 or thin_detail or len(companies) >= 3 or common_volume_search):
        questions.append(
            {
                "id": "expand_search_when_thin",
                "label": "Market Expansion",
                "prompt": "If exact matches are scarce, should HR Hunter widen into discovery slices and nearby public evidence?",
                "help": "This helps hard briefs reach target volume, but it trades some precision for breadth.",
                "recommended_answer": bool(
                    search_profile == EXPLORATORY_SEARCH_PROFILE or executive_brief or common_volume_search
                ),
            }
        )

    return questions


def _resolve_brief_clarifications(
    raw_clarifications: Any,
    follow_up_questions: List[Dict[str, Any]],
) -> tuple[Dict[str, bool], List[Dict[str, Any]]]:
    raw_values = dict(raw_clarifications) if isinstance(raw_clarifications, dict) else {}
    resolved_values: Dict[str, bool] = {}
    resolved_questions: List[Dict[str, Any]] = []
    for question in follow_up_questions:
        question_id = str(question.get("id", "")).strip()
        if not question_id:
            continue
        explicit_value = _coerce_bool(raw_values.get(question_id))
        recommended = bool(question.get("recommended_answer"))
        resolved = recommended if explicit_value is None else explicit_value
        resolved_values[question_id] = resolved
        resolved_questions.append(
            {
                **question,
                "resolved_answer": resolved,
                "using_recommended": explicit_value is None,
            }
        )
    for clarification_id in (
        "prioritize_first_location",
        "allow_adjacent_titles",
        "exact_company_scope",
        "strict_market_scope",
        "expand_search_when_thin",
    ):
        if clarification_id in resolved_values:
            continue
        explicit_value = _coerce_bool(raw_values.get(clarification_id))
        if explicit_value is None:
            continue
        resolved_values[clarification_id] = explicit_value
    return resolved_values, resolved_questions


def _resolve_top_up_expansion_strategy(
    *,
    top_up_round: int,
    search_profile: str,
    executive_brief: bool,
    scope_first_enabled: bool,
    has_explicit_query_family_budgets: bool,
    raw_brief_clarifications: Dict[str, Any],
    explicit_geo_fanout: bool | None,
    allow_adjacent_titles: bool,
    expand_search_when_thin: bool,
    resolved_geo_fanout: bool,
    include_country_only_queries: bool,
    max_geo_groups: int,
    scrapingbee_parallel_requests: int,
    scrapingbee_max_queries: int,
    query_family_budgets: Dict[str, int],
    limit: int,
    location_targets: List[str],
) -> Dict[str, Any]:
    strategy = {
        "round": top_up_round,
        "auto_broadened": False,
        "steps": [],
    }
    controlled_scope_first = bool(search_profile == FOCUSED_SEARCH_PROFILE or (executive_brief and scope_first_enabled))
    if top_up_round <= 0 or not controlled_scope_first:
        return {
            "allow_adjacent_titles": allow_adjacent_titles,
            "expand_search_when_thin": expand_search_when_thin,
            "resolved_geo_fanout": resolved_geo_fanout,
            "include_country_only_queries": include_country_only_queries,
            "max_geo_groups": max_geo_groups,
            "scrapingbee_parallel_requests": scrapingbee_parallel_requests,
            "scrapingbee_max_queries": scrapingbee_max_queries,
            "query_family_budgets": dict(query_family_budgets),
            "strategy": strategy,
        }

    explicit_adjacent_titles = _coerce_bool(raw_brief_clarifications.get("allow_adjacent_titles"))
    explicit_expand_search = _coerce_bool(raw_brief_clarifications.get("expand_search_when_thin"))
    explicit_prioritize_location = _coerce_bool(raw_brief_clarifications.get("prioritize_first_location"))

    budgets = dict(query_family_budgets)

    if explicit_expand_search is None and not expand_search_when_thin:
        expand_search_when_thin = True
        strategy["auto_broadened"] = True
        strategy["steps"].append("enabled discovery slices after a thin focused first pass")

    if expand_search_when_thin:
        if not include_country_only_queries:
            include_country_only_queries = True
            strategy["auto_broadened"] = True
            strategy["steps"].append("added country-level public profile queries")
        if explicit_geo_fanout is None and explicit_prioritize_location is None and not resolved_geo_fanout and location_targets:
            resolved_geo_fanout = True
            strategy["auto_broadened"] = True
            strategy["steps"].append("expanded geography fanout for top-up discovery")
        max_geo_groups = max(max_geo_groups, min(6, max(3, len(location_targets) + 2)))

    if top_up_round >= 2 and explicit_adjacent_titles is None and not allow_adjacent_titles:
        allow_adjacent_titles = True
        strategy["auto_broadened"] = True
        strategy["steps"].append("opened adjacent title families after repeated thin rounds")

    if top_up_round >= 1:
        scrapingbee_parallel_requests = max(scrapingbee_parallel_requests, 10 if top_up_round >= 2 else 8)
        scrapingbee_max_queries = max(scrapingbee_max_queries, max(120, limit * (3 if top_up_round == 1 else 4)))
        if has_explicit_query_family_budgets:
            pass
        elif executive_brief:
            budgets["org_chart_profile_pages"] = max(budgets.get("org_chart_profile_pages", 0), 10)
            budgets["profile_like_public_pages"] = max(
                budgets.get("profile_like_public_pages", 0),
                10 if top_up_round == 1 else 14,
            )
            budgets["team_leadership_pages"] = max(
                budgets.get("team_leadership_pages", 0),
                8 if top_up_round == 1 else 10,
            )
            budgets["appointment_news_pages"] = max(
                budgets.get("appointment_news_pages", 0),
                5 if top_up_round == 1 else 7,
            )
        elif expand_search_when_thin:
            budgets = _default_query_family_budgets(
                search_profile=search_profile,
                executive_brief=False,
                top_up_round=top_up_round,
            )
            strategy["auto_broadened"] = True
            strategy["steps"].append("shifted top-up queries toward profile-heavy discovery for non-executive roles")

    return {
        "allow_adjacent_titles": allow_adjacent_titles,
        "expand_search_when_thin": expand_search_when_thin,
        "resolved_geo_fanout": resolved_geo_fanout,
        "include_country_only_queries": include_country_only_queries,
        "max_geo_groups": max_geo_groups,
        "scrapingbee_parallel_requests": scrapingbee_parallel_requests,
        "scrapingbee_max_queries": scrapingbee_max_queries,
        "query_family_budgets": budgets,
        "strategy": strategy,
    }


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
    peer_company_targets = [
        str(value).strip()
        for value in brief_config.get("peer_company_targets", [])
        if str(value).strip()
    ]
    sourcing_company_targets = unique_preserving_order([*company_targets, *peer_company_targets])
    document_text = str(brief_config.get("document_text", "")).strip()
    min_years = _coerce_int(brief_config.get("minimum_years_experience"))
    max_years = _coerce_int(brief_config.get("maximum_years_experience"))
    search_profile = str(brief_config.get("brief_search_profile", BALANCED_SEARCH_PROFILE) or BALANCED_SEARCH_PROFILE)
    follow_up_questions = brief_config.get("brief_follow_up_questions", [])
    if not isinstance(follow_up_questions, list):
        follow_up_questions = []
    brief_clarifications = brief_config.get("brief_clarifications", {})
    if not isinstance(brief_clarifications, dict):
        brief_clarifications = {}

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

    if len(sourcing_company_targets) >= 2:
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
        message = (
            "Brief details look sufficient for search."
            if not follow_up_questions
            else "Brief details look sufficient for search. A couple of yes/no clarifications can make targeting tighter."
        )
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
        "search_profile": search_profile,
        "needs_clarification": bool(follow_up_questions),
        "follow_up_questions": follow_up_questions,
        "brief_clarifications": brief_clarifications,
    }


def build_ui_brief_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    role_title = str(payload.get("role_title", "")).strip()
    titles = parse_multi_value(payload.get("titles"))
    countries = parse_multi_value(payload.get("countries"))
    continents = parse_multi_value(payload.get("continents"))
    cities = parse_multi_value(payload.get("cities"))
    companies = parse_multi_value(payload.get("company_targets"))
    peer_companies = parse_multi_value(payload.get("peer_company_targets"))
    sourcing_companies = unique_preserving_order([*companies, *peer_companies])
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
    keyword_tracks = breakdown.get("keyword_tracks", {})
    if not isinstance(keyword_tracks, dict):
        keyword_tracks = {}
    breakdown_search_tuning = breakdown.get("search_tuning", {})
    if not isinstance(breakdown_search_tuning, dict):
        breakdown_search_tuning = {}
    payload_search_tuning = payload.get("search_tuning", {})
    if not isinstance(payload_search_tuning, dict):
        payload_search_tuning = {}
    search_tuning = {
        **dict(breakdown_search_tuning),
        **dict(payload_search_tuning),
    }

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
    executive_brief = _is_executive_brief(role_title, titles)
    required_keywords = unique_preserving_order([*must_have, *breakdown.get("required_keywords", [])])
    preferred_keywords = unique_preserving_order([*nice_to_have, *breakdown.get("preferred_keywords", [])])
    industry_keywords = unique_preserving_order([*industry_keywords, *breakdown.get("industry_keywords", [])])
    portfolio_keywords = unique_preserving_order(parse_multi_value(keyword_tracks.get("portfolio_keywords")))
    commercial_keywords = unique_preserving_order(parse_multi_value(keyword_tracks.get("commercial_keywords")))
    leadership_keywords = unique_preserving_order(parse_multi_value(keyword_tracks.get("leadership_keywords")))
    scope_keywords = unique_preserving_order(parse_multi_value(keyword_tracks.get("scope_keywords")))
    seniority_levels = unique_preserving_order(
        [*parse_multi_value(payload.get("seniority_levels")), *breakdown.get("seniority_levels", [])]
    )

    output_dir = resolve_output_dir(payload.get("output_dir"))
    feedback_db = resolve_feedback_db_path(payload.get("feedback_db"))
    model_dir = resolve_ranker_model_dir(payload.get("model_dir"))
    limit = max(1, int(payload.get("limit", 20) or 20))
    configured_search_profile = str(
        payload.get("search_profile")
        or payload.get("brief_search_profile")
        or search_tuning.get("search_profile")
        or ""
    ).strip().lower()
    if configured_search_profile in SUPPORTED_SEARCH_PROFILES:
        search_profile = configured_search_profile
    else:
        search_profile = _derive_search_profile(
            role_title=role_title,
            titles=titles,
            location_targets=location_targets,
            sourcing_company_targets=sourcing_companies,
            required_keywords=required_keywords,
            preferred_keywords=preferred_keywords,
            industry_keywords=industry_keywords,
            document_text=job_description,
            limit=limit,
        )
    explicit_title_scope = _has_explicit_title_scope(titles)
    common_volume_search = bool(limit >= 40 and not sourcing_companies and len(titles) <= 2 and not executive_brief)
    brief_follow_up_questions = _build_brief_follow_up_questions(
        role_title=role_title,
        titles=titles,
        countries=countries,
        cities=cities,
        location_targets=location_targets,
        companies=companies,
        source_companies=sourcing_companies,
        company_match_mode=str(payload.get("company_match_mode", "both") or "both"),
        required_keywords=required_keywords,
        industry_keywords=industry_keywords,
        document_text=job_description,
        limit=limit,
        search_profile=search_profile,
    )
    raw_brief_clarifications = dict(payload.get("brief_clarifications", {})) if isinstance(payload.get("brief_clarifications"), dict) else {}
    brief_clarifications, brief_follow_up_questions = _resolve_brief_clarifications(
        raw_brief_clarifications,
        brief_follow_up_questions,
    )
    if "allow_adjacent_titles" not in brief_clarifications:
        brief_clarifications["allow_adjacent_titles"] = _recommended_allow_adjacent_titles(
            search_profile=search_profile,
            executive_brief=executive_brief,
            common_volume_search=common_volume_search,
            explicit_title_scope=explicit_title_scope,
            companies=companies,
            location_count=len(location_targets),
        )
    if "exact_company_scope" not in brief_clarifications:
        brief_clarifications["exact_company_scope"] = _recommended_exact_company_scope(
            search_profile=search_profile,
            executive_brief=executive_brief,
            company_count=len(companies),
        )
    if "strict_market_scope" not in brief_clarifications:
        brief_clarifications["strict_market_scope"] = _recommended_strict_market_scope(
            search_profile=search_profile,
            executive_brief=executive_brief,
            common_volume_search=common_volume_search,
            company_count=len(companies),
            location_targets=location_targets,
            countries=countries,
            cities=cities,
        )
    prioritize_first_location = bool(brief_clarifications.get("prioritize_first_location"))
    allow_adjacent_titles = bool(brief_clarifications.get("allow_adjacent_titles"))
    exact_company_scope = bool(brief_clarifications.get("exact_company_scope"))
    strict_market_scope = bool(brief_clarifications.get("strict_market_scope"))
    expand_search_when_thin = bool(brief_clarifications.get("expand_search_when_thin", search_profile != FOCUSED_SEARCH_PROFILE))
    if exact_company_scope and companies:
        payload["company_match_mode"] = "current_only"
    if prioritize_first_location and location_targets:
        geography_country = countries[0] if countries else geography_country
        if cities:
            geography_location = cities[0]
        elif countries:
            geography_location = countries[0]
        else:
            geography_location = location_targets[0]
    internal_fetch_override = _coerce_int(payload.get("internal_fetch_limit_override"))
    if internal_fetch_override is None:
        internal_fetch_override = _coerce_int(search_tuning.get("internal_fetch_limit_override"))
    internal_fetch_limit = max(
        limit,
        internal_fetch_override if internal_fetch_override is not None else compute_internal_fetch_limit(limit),
    )
    if search_profile == FOCUSED_SEARCH_PROFILE and internal_fetch_override is None:
        focused_fetch_cap = max(limit * 2, limit + 50)
        if common_volume_search and limit >= 80:
            focused_fetch_cap = max(focused_fetch_cap, min(260, limit + 140))
        internal_fetch_limit = min(internal_fetch_limit, focused_fetch_cap)
    csv_export_limit = max(1, int(payload.get("csv_export_limit", limit) or limit))
    reranker_enabled = bool(payload.get("reranker_enabled", True))
    learned_ranker_enabled = bool(payload.get("learned_ranker_enabled", False))
    top_up_round = max(0, _coerce_int(payload.get("top_up_round")) or 0)
    country_code = _selected_country_code(countries)
    reranker_requested_top_n = _coerce_int(payload.get("reranker_top_n"))
    if reranker_requested_top_n is None:
        reranker_requested_top_n = _coerce_int(search_tuning.get("reranker_top_n"))
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
    if search_profile == FOCUSED_SEARCH_PROFILE:
        reranker_top_n = min(reranker_top_n, max(limit * 2, 80))
    scrapingbee_parallel_requests = max(
        4,
        _coerce_int(payload.get("provider_parallel_requests"))
        or _coerce_int(search_tuning.get("provider_parallel_requests"))
        or (10 if common_volume_search and limit >= 80 else 0)
        or (16 if limit >= 220 else (12 if limit >= 120 else 8)),
    )
    if search_profile == FOCUSED_SEARCH_PROFILE:
        focused_parallel_cap = 10 if common_volume_search and limit >= 80 else 8
        scrapingbee_parallel_requests = min(scrapingbee_parallel_requests, focused_parallel_cap)
    scrapingbee_pages_per_query = max(
        1,
        min(
            5,
            (
                _coerce_int(payload.get("scrapingbee_pages_per_query"))
                or _coerce_int(search_tuning.get("scrapingbee_pages_per_query"))
                or 1
            ),
        ),
    )
    explicit_scrapingbee_max_queries = (
        _coerce_int(payload.get("scrapingbee_max_queries"))
        or _coerce_int(search_tuning.get("scrapingbee_max_queries"))
    )
    if explicit_scrapingbee_max_queries is not None:
        scrapingbee_max_queries = max(1, explicit_scrapingbee_max_queries)
    else:
        scrapingbee_max_queries = max(120, compute_provider_max_queries(limit))
    if search_profile == FOCUSED_SEARCH_PROFILE and explicit_scrapingbee_max_queries is None:
        focused_query_cap = max(90, limit * 2)
        if common_volume_search and limit >= 80:
            focused_query_cap = max(focused_query_cap, int(round(limit * 2.4)))
        scrapingbee_max_queries = min(scrapingbee_max_queries, focused_query_cap)
    default_geo_groups = 8 if limit >= 220 else (6 if limit >= 120 else 8)
    if search_profile == FOCUSED_SEARCH_PROFILE:
        default_geo_groups = min(default_geo_groups, max(2, len(location_targets) or 2))
    elif prioritize_first_location and location_targets:
        default_geo_groups = min(default_geo_groups, max(3, min(len(location_targets) + 1, 6)))
    tuned_company_chunk_size = _coerce_int(search_tuning.get("company_chunk_size"))
    tuned_max_geo_groups = _coerce_int(search_tuning.get("max_geo_groups"))
    tuned_discovery_chunk_size = _coerce_int(search_tuning.get("discovery_keyword_chunk_size"))
    tuned_market_chunk_size = _coerce_int(search_tuning.get("market_keyword_chunk_size"))
    tuned_history_query_terms = parse_multi_value(search_tuning.get("history_query_terms"))
    tuned_company_slice_location_group_limit = _coerce_int(search_tuning.get("company_slice_location_group_limit"))
    tuned_geo_group_size = _coerce_int(search_tuning.get("geo_group_size"))
    tuned_stagnation_query_window = _coerce_int(search_tuning.get("stagnation_query_window"))
    tuned_stagnation_min_results = _coerce_int(search_tuning.get("stagnation_min_results"))
    tuned_include_history_slices = _coerce_bool(search_tuning.get("include_history_slices"))
    tuned_include_discovery_slices = _coerce_bool(search_tuning.get("include_discovery_slices"))
    tuned_verification_enabled = _coerce_bool(search_tuning.get("verification_enabled"))
    tuned_verification_top_n = _coerce_int(search_tuning.get("verification_top_n"))
    tuned_verification_parallel_candidates = _coerce_int(search_tuning.get("verification_parallel_candidates"))
    tuned_verification_queries_per_candidate = _coerce_int(search_tuning.get("verification_queries_per_candidate"))
    tuned_verification_location_probe_queries = _coerce_int(search_tuning.get("verification_location_probe_queries"))
    tuned_verification_company_location_probe_queries = _coerce_int(
        search_tuning.get("verification_company_location_probe_queries")
    )
    tuned_query_family_budgets = search_tuning.get("query_family_budgets", {})
    if not isinstance(tuned_query_family_budgets, dict):
        tuned_query_family_budgets = {}
    payload_query_family_budgets = payload.get("query_family_budgets", {})
    if not isinstance(payload_query_family_budgets, dict):
        payload_query_family_budgets = {}
    verification_enabled = _coerce_bool(payload.get("verification_enabled"))
    if verification_enabled is None:
        verification_enabled = tuned_verification_enabled
    if verification_enabled is None:
        verification_enabled = True
    verification_top_n = _coerce_int(payload.get("verification_top_n"))
    if verification_top_n is None:
        verification_top_n = tuned_verification_top_n
    if verification_top_n is None:
        if limit >= 300:
            verification_top_n = 120
        elif limit >= 180:
            verification_top_n = 90
        elif limit >= 100:
            verification_top_n = 60
        else:
            verification_top_n = limit
    verification_top_n = min(internal_fetch_limit, max(0, verification_top_n))
    if search_profile == FOCUSED_SEARCH_PROFILE:
        verification_top_n = min(verification_top_n, max(limit, 50))
    scope_first_enabled = _coerce_bool(payload.get("scope_first_enabled"))
    if scope_first_enabled is None:
        scope_first_enabled = _recommended_scope_first_enabled(
            search_profile=search_profile,
            executive_brief=executive_brief,
            common_volume_search=common_volume_search,
            strict_market_scope=strict_market_scope,
            company_count=len(companies),
        )
    in_scope_target = _coerce_int(payload.get("in_scope_target"))
    if in_scope_target is None:
        in_scope_target = _recommended_in_scope_target(
            limit=limit,
            executive_brief=executive_brief,
            company_count=len(companies),
            common_volume_search=common_volume_search,
            search_profile=search_profile,
            strict_market_scope=strict_market_scope,
        )
    in_scope_target = min(limit, max(0, int(in_scope_target or 0)))
    verification_scope_target = _coerce_int(payload.get("verification_scope_target"))
    if verification_scope_target is None:
        verification_scope_target = _recommended_verification_scope_target(
            limit=limit,
            verification_top_n=verification_top_n,
            in_scope_target=in_scope_target,
            executive_brief=executive_brief,
        )
    verification_scope_target = min(max(0, int(verification_scope_target or 0)), max(verification_top_n, 0))
    executive_scope_first = bool(executive_brief and scope_first_enabled)
    verification_parallel_candidates = max(
        1,
        _coerce_int(payload.get("verification_parallel_candidates"))
        or tuned_verification_parallel_candidates
        or 6,
    )
    verification_queries_per_candidate = max(
        1,
        _coerce_int(payload.get("verification_queries_per_candidate"))
        or tuned_verification_queries_per_candidate
        or 2,
    )
    verification_location_probe_queries_value = _coerce_int(payload.get("verification_location_probe_queries"))
    if verification_location_probe_queries_value is None:
        verification_location_probe_queries_value = tuned_verification_location_probe_queries
    verification_location_probe_queries = max(
        0,
        1 if verification_location_probe_queries_value is None else verification_location_probe_queries_value,
    )
    verification_company_location_probe_queries_value = _coerce_int(
        payload.get("verification_company_location_probe_queries")
    )
    if verification_company_location_probe_queries_value is None:
        verification_company_location_probe_queries_value = tuned_verification_company_location_probe_queries
    verification_company_location_probe_queries = max(
        0,
        0
        if verification_company_location_probe_queries_value is None
        else verification_company_location_probe_queries_value,
    )
    explicit_include_history_slices = _coerce_bool(payload.get("include_history_slices"))
    explicit_include_discovery_slices = _coerce_bool(payload.get("include_discovery_slices"))
    explicit_geo_fanout = _coerce_bool(payload.get("geo_fanout_enabled"))
    resolved_include_history_slices = (
        explicit_include_history_slices
        if explicit_include_history_slices is not None
        else (tuned_include_history_slices if tuned_include_history_slices is not None else True)
    )
    if search_profile == FOCUSED_SEARCH_PROFILE and not companies:
        resolved_include_history_slices = False
    resolved_include_discovery_slices = (
        explicit_include_discovery_slices
        if explicit_include_discovery_slices is not None
        else (tuned_include_discovery_slices if tuned_include_discovery_slices is not None else expand_search_when_thin)
    )
    if exact_company_scope and companies:
        resolved_include_history_slices = False
        resolved_include_discovery_slices = False
    resolved_geo_fanout = (
        explicit_geo_fanout
        if explicit_geo_fanout is not None
        else not (search_profile == FOCUSED_SEARCH_PROFILE and len(location_targets) <= 2)
    )
    resolved_max_geo_groups = max(3, _coerce_int(payload.get("max_geo_groups")) or tuned_max_geo_groups or default_geo_groups)
    include_country_only_queries = expand_search_when_thin or len(countries) <= 1
    if search_profile == FOCUSED_SEARCH_PROFILE and not expand_search_when_thin:
        include_country_only_queries = False
    resolved_query_family_budgets = {
        str(family).strip(): max(0, int(value))
        for family, value in dict(payload_query_family_budgets or tuned_query_family_budgets).items()
        if str(family).strip()
    }
    has_explicit_query_family_budgets = bool(payload_query_family_budgets or tuned_query_family_budgets)
    if not resolved_query_family_budgets and (search_profile == FOCUSED_SEARCH_PROFILE or executive_scope_first):
        resolved_query_family_budgets = _default_query_family_budgets(
            search_profile=FOCUSED_SEARCH_PROFILE if executive_scope_first else search_profile,
            executive_brief=executive_brief,
            top_up_round=top_up_round,
        )
    if executive_scope_first and top_up_round <= 0:
        if explicit_include_history_slices is None:
            resolved_include_history_slices = False
        if explicit_include_discovery_slices is None:
            resolved_include_discovery_slices = False
        if explicit_geo_fanout is None:
            resolved_geo_fanout = False
        include_country_only_queries = False
        if _coerce_int(payload.get("max_geo_groups")) is None and tuned_max_geo_groups is None:
            resolved_max_geo_groups = min(
                resolved_max_geo_groups,
                max(2, min(3, len(location_targets) or 2)),
            )
        if not has_explicit_query_family_budgets:
            resolved_query_family_budgets = dict(FOCUSED_QUERY_FAMILY_BUDGETS)
    top_up_strategy = _resolve_top_up_expansion_strategy(
        top_up_round=top_up_round,
        search_profile=search_profile,
        executive_brief=executive_brief,
        scope_first_enabled=bool(scope_first_enabled),
        has_explicit_query_family_budgets=has_explicit_query_family_budgets,
        raw_brief_clarifications=raw_brief_clarifications,
        explicit_geo_fanout=explicit_geo_fanout,
        allow_adjacent_titles=allow_adjacent_titles,
        expand_search_when_thin=expand_search_when_thin,
        resolved_geo_fanout=resolved_geo_fanout,
        include_country_only_queries=include_country_only_queries,
        max_geo_groups=resolved_max_geo_groups,
        scrapingbee_parallel_requests=scrapingbee_parallel_requests,
        scrapingbee_max_queries=scrapingbee_max_queries,
        query_family_budgets=resolved_query_family_budgets,
        limit=limit,
        location_targets=location_targets,
    )
    allow_adjacent_titles = bool(top_up_strategy["allow_adjacent_titles"])
    expand_search_when_thin = bool(top_up_strategy["expand_search_when_thin"])
    resolved_geo_fanout = bool(top_up_strategy["resolved_geo_fanout"])
    include_country_only_queries = bool(top_up_strategy["include_country_only_queries"])
    resolved_max_geo_groups = max(3, int(top_up_strategy["max_geo_groups"]))
    scrapingbee_parallel_requests = max(1, int(top_up_strategy["scrapingbee_parallel_requests"]))
    scrapingbee_max_queries = max(1, int(top_up_strategy["scrapingbee_max_queries"]))
    resolved_include_discovery_slices = (
        explicit_include_discovery_slices
        if explicit_include_discovery_slices is not None
        else (tuned_include_discovery_slices if tuned_include_discovery_slices is not None else expand_search_when_thin)
    )
    if executive_scope_first and top_up_round <= 0 and explicit_include_discovery_slices is None:
        resolved_include_discovery_slices = False
    if exact_company_scope and companies:
        resolved_include_history_slices = False
        resolved_include_discovery_slices = False
    if strict_market_scope and location_targets:
        resolved_geo_fanout = False
        include_country_only_queries = False
    elif executive_scope_first and top_up_round <= 0:
        if explicit_geo_fanout is None:
            resolved_geo_fanout = False
        include_country_only_queries = False
    resolved_query_family_budgets = {
        str(family).strip(): max(0, int(value))
        for family, value in dict(top_up_strategy["query_family_budgets"]).items()
        if str(family).strip()
    }
    reranker_model_name = str(
        payload.get("reranker_model_name")
        or search_tuning.get("reranker_model_name")
        or DEFAULT_UI_RERANKER_MODEL
    ).strip() or DEFAULT_UI_RERANKER_MODEL

    providers_settings = {
        "retrieval": {
            "company_chunk_size": int(payload.get("company_chunk_size", tuned_company_chunk_size or 5) or 5),
            "results_per_slice": max(internal_fetch_limit, int(payload.get("results_per_slice", 40) or 40)),
            "include_strict_slice": True,
            "include_broad_slice": allow_adjacent_titles,
            "include_history_slices": resolved_include_history_slices,
            "include_discovery_slices": resolved_include_discovery_slices,
            "geo_fanout_enabled": resolved_geo_fanout,
            "max_geo_groups": resolved_max_geo_groups,
            "discovery_keyword_chunk_size": int(payload.get("discovery_keyword_chunk_size", tuned_discovery_chunk_size or 6) or 6),
            "market_keyword_chunk_size": int(payload.get("market_keyword_chunk_size", tuned_market_chunk_size or 5) or 5),
            "history_query_terms": unique_preserving_order(
                [
                    *(tuned_history_query_terms or breakdown.get("key_experience_points", [])[:3]),
                    "formerly",
                    "previously",
                    "before joining",
                    "ex",
                ]
            ),
        },
        "registry_memory": {
            "enabled": bool(payload.get("registry_memory_enabled", True)),
            "limit": max(internal_fetch_limit, int(payload.get("registry_memory_limit", 20) or 20)),
        },
        "reranker": {
            "enabled": reranker_enabled,
            "model_name": reranker_model_name,
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
            "max_company_terms_per_query": max(
                6,
                _coerce_int(payload.get("max_company_terms_per_query"))
                or _coerce_int(search_tuning.get("max_company_terms_per_query"))
                or 12,
            ),
            "geo_fanout_enabled": resolved_geo_fanout,
            "max_geo_groups": resolved_max_geo_groups,
            "company_slice_location_group_limit": max(
                0,
                _coerce_int(payload.get("company_slice_location_group_limit"))
                or tuned_company_slice_location_group_limit
                or 0,
            ),
            "geo_group_size": max(
                1,
                _coerce_int(payload.get("geo_group_size"))
                or tuned_geo_group_size
                or (1 if prioritize_first_location or search_profile == FOCUSED_SEARCH_PROFILE else 2),
            ),
            "include_country_only_queries": include_country_only_queries,
            "stagnation_query_window": max(
                0,
                _coerce_int(payload.get("stagnation_query_window"))
                or tuned_stagnation_query_window
                or 28,
            ),
            "stagnation_min_results": max(
                0,
                _coerce_int(payload.get("stagnation_min_results"))
                or tuned_stagnation_min_results
                or 0,
            ),
            "query_family_budgets": {
                str(family).strip(): max(0, int(value))
                for family, value in resolved_query_family_budgets.items()
                if str(family).strip()
            },
        },
        "verification": {
            "enabled": bool(verification_enabled),
            "top_n": verification_top_n,
            "scope_target": verification_scope_target,
            "parallel_candidates": verification_parallel_candidates,
            "country_code": str(payload.get("scrapingbee_country_code", "") or country_code or "us"),
            "queries_per_candidate": verification_queries_per_candidate,
            "location_probe_queries": verification_location_probe_queries,
            "company_location_probe_queries": verification_company_location_probe_queries,
            "pages_per_query": scrapingbee_pages_per_query,
            "results_per_query": 10,
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
        "expand_title_keywords": allow_adjacent_titles,
        "company_targets": companies,
        "peer_company_targets": peer_companies,
        "geography": {
            "location_name": geography_location,
            "country": geography_country,
            "radius_miles": radius_miles,
            "location_hints": location_targets,
        },
        "location_targets": location_targets,
        "required_keywords": required_keywords,
        "preferred_keywords": preferred_keywords,
        "portfolio_keywords": portfolio_keywords,
        "commercial_keywords": commercial_keywords,
        "leadership_keywords": leadership_keywords,
        "scope_keywords": scope_keywords,
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
        "brief_search_profile": search_profile,
        "brief_clarifications": brief_clarifications,
        "brief_follow_up_questions": brief_follow_up_questions,
        "top_up_round": top_up_round,
        "top_up_strategy": top_up_strategy["strategy"],
        "result_target_min": max(5, min(limit, 20)),
        "result_target_max": max(limit, 40),
        "max_profiles": max(limit, 80),
        "provider_settings": providers_settings,
        "scope_first_enabled": bool(scope_first_enabled),
        "in_scope_target": in_scope_target,
        "verification_scope_target": verification_scope_target,
        "ui_meta": {
            "titles": titles,
            "countries": countries,
            "continents": continents,
            "cities": cities,
            "company_targets": companies,
            "peer_company_targets": peer_companies,
            "must_have_keywords": must_have,
            "nice_to_have_keywords": nice_to_have,
            "industry_keywords": industry_keywords,
            "portfolio_keywords": portfolio_keywords,
            "commercial_keywords": commercial_keywords,
            "leadership_keywords": leadership_keywords,
            "scope_keywords": scope_keywords,
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
            "search_tuning": search_tuning,
            "search_profile": search_profile,
            "reranker_model_name": reranker_model_name,
            "brief_clarifications": brief_clarifications,
            "top_up_round": top_up_round,
            "top_up_strategy": top_up_strategy["strategy"],
            "scope_first_enabled": bool(scope_first_enabled),
            "in_scope_target": in_scope_target,
            "verification_scope_target": verification_scope_target,
            "keyword_tracks": {
                "portfolio_keywords": portfolio_keywords,
                "commercial_keywords": commercial_keywords,
                "leadership_keywords": leadership_keywords,
                "scope_keywords": scope_keywords,
            },
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
            "reranker_model_name": DEFAULT_UI_RERANKER_MODEL,
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
                    "Managing Director",
                    "President",
                ],
                "countries": [
                    "United Arab Emirates",
                    "Saudi Arabia",
                    "Kuwait",
                    "Qatar",
                    "Bahrain",
                ],
                "continents": [],
                "cities": [
                    "Dubai",
                    "Abu Dhabi",
                    "Riyadh",
                    "Jeddah",
                    "Kuwait City",
                    "Doha",
                    "Manama",
                ],
                "company_targets": [],
                "peer_company_targets": [
                    "The One",
                    "Al Huzaifa",
                    "IDdesign",
                    "BoConcept",
                    "Crate & Barrel",
                    "West Elm",
                    "Pottery Barn",
                ],
                "company_match_mode": "both",
                "employment_status_mode": "any",
                "years_mode": "at_least",
                "years_value": 9,
                "years_tolerance": 0,
                "max_profiles": 300,
                "must_have_keywords": [
                    "P&L",
                    "Retail Operations",
                    "Store Network",
                    "Omnichannel",
                    "GCC",
                ],
                "nice_to_have_keywords": [
                    "Home Furnishings",
                    "Furniture Retail",
                    "Premium Retail",
                    "Home Decor",
                    "Regional Expansion",
                    "Brand Scaling",
                    "Founder-Led Transition",
                    "Board Governance",
                    "Arabic",
                ],
                "industry_keywords": [
                    "home furnishings",
                    "furniture retail",
                    "premium retail",
                    "home decor",
                ],
                "job_description": (
                    "Marina Home Interiors is a Dubai-headquartered premium home furnishings retailer. We are hiring "
                    "a Chief Executive Officer to drive profitable GCC growth across a store-led and omnichannel "
                    "model. The brief is for a genuine chief executive, managing director, or president from premium "
                    "home, furniture, or adjacent lifestyle retail with strong public evidence of full P&L ownership, "
                    "retail operations leadership, store-network execution, and regional expansion across Gulf "
                    "markets. Board exposure, founder-led transition experience, fluent English, and Arabic are all "
                    "valuable."
                ),
                "brief_clarifications": {
                    "prioritize_first_location": True,
                    "allow_adjacent_titles": False,
                    "strict_market_scope": True,
                    "expand_search_when_thin": False,
                },
                "jd_breakdown": {
                    **extract_job_description_breakdown(
                        (
                            "Marina Home Interiors is a Dubai-headquartered premium home furnishings retailer. We are hiring "
                            "a Chief Executive Officer to drive profitable GCC growth across a store-led and omnichannel "
                            "model. The brief is for a genuine chief executive, managing director, or president from premium "
                            "home, furniture, or adjacent lifestyle retail with strong public evidence of full P&L ownership, "
                            "retail operations leadership, store-network execution, and regional expansion across Gulf "
                            "markets. Board exposure, founder-led transition experience, fluent English, and Arabic are all "
                            "valuable."
                        ),
                        role_title="Chief Executive Officer (CEO)",
                    ),
                    "titles": [
                        "Chief Executive Officer",
                        "Managing Director",
                        "President",
                    ],
                    "required_keywords": [
                        "p&l",
                        "retail operations",
                        "store network",
                        "omnichannel",
                        "gcc",
                    ],
                    "preferred_keywords": [
                        "home furnishings",
                        "furniture retail",
                        "premium retail",
                        "board governance",
                        "founder-led transition",
                        "brand scaling",
                        "home decor",
                    ],
                    "industry_keywords": [
                        "home furnishings",
                        "furniture retail",
                        "premium retail",
                        "home decor",
                    ],
                    "years": {
                        "mode": "at_least",
                        "value": 9,
                        "min": 9,
                        "max": None,
                        "tolerance": 0,
                    },
                    "keyword_tracks": {
                        "portfolio_keywords": [
                            "home furnishings",
                            "store network",
                            "omnichannel",
                            "furniture retail",
                        ],
                        "commercial_keywords": [
                            "p&l",
                            "profitability",
                            "revenue growth",
                            "regional expansion",
                        ],
                        "leadership_keywords": [
                            "board governance",
                            "founder-led transition",
                            "executive leadership",
                            "stakeholder management",
                        ],
                        "scope_keywords": [
                            "GCC",
                            "UAE",
                            "Saudi Arabia",
                            "regional",
                        ],
                    },
                    "search_tuning": {
                        "search_profile": FOCUSED_SEARCH_PROFILE,
                        "reranker_model_name": DEFAULT_UI_RERANKER_MODEL,
                        "internal_fetch_limit_override": 360,
                        "reranker_top_n": 180,
                        "provider_parallel_requests": 24,
                        "scrapingbee_max_queries": 48,
                        "max_geo_groups": 3,
                        "geo_group_size": 1,
                        "company_chunk_size": 4,
                        "company_slice_location_group_limit": 1,
                        "max_company_terms_per_query": 6,
                        "stagnation_query_window": 10,
                        "stagnation_min_results": 260,
                        "include_history_slices": True,
                        "include_discovery_slices": True,
                        "verification_top_n": 140,
                        "verification_parallel_candidates": 28,
                        "verification_location_probe_queries": 0,
                        "query_family_budgets": {
                            "team_leadership_pages": 8,
                            "appointment_news_pages": 6,
                            "speaker_bio_pages": 4,
                            "award_industry_pages": 0,
                            "industry_association_pages": 2,
                            "trade_directory_pages": 0,
                            "org_chart_profile_pages": 8,
                            "profile_like_public_pages": 14,
                        },
                    },
                },
                "anchors": {
                    "title": "critical",
                    "skills": "preferred",
                    "location": "important",
                    "company": "important",
                    "years": "preferred",
                    "industry": "important",
                    "function": "important",
                    "semantic": "preferred",
                },
            },
            "supply_chain_manager_uae": {
                "project_name": "UAE Supply Chain Manager Search",
                "client_name": "Demo Supply Chain Search",
                "role_title": "Supply Chain Manager",
                "titles": [
                    "Supply Chain Manager",
                    "Senior Supply Chain Manager",
                    "Supply Planning Manager",
                    "Demand Planning Manager",
                ],
                "countries": [
                    "United Arab Emirates",
                ],
                "continents": [],
                "cities": [
                    "Dubai",
                    "Abu Dhabi",
                    "Sharjah",
                    "Jebel Ali",
                ],
                "company_targets": [],
                "peer_company_targets": [
                    "Amazon",
                    "noon",
                    "Majid Al Futtaim",
                    "Landmark Group",
                    "talabat",
                    "Careem",
                    "Aramex",
                    "DHL",
                    "Unilever",
                    "Nestle",
                ],
                "company_match_mode": "both",
                "employment_status_mode": "any",
                "years_mode": "plus_minus",
                "years_value": 6,
                "years_tolerance": 1,
                "max_profiles": 300,
                "must_have_keywords": [
                    "S&OP",
                    "Demand Planning",
                    "Inventory Optimization",
                    "Logistics",
                    "ERP",
                ],
                "nice_to_have_keywords": [
                    "Warehouse Operations",
                    "Fulfillment",
                    "3PL",
                    "OTIF",
                    "Procurement",
                    "IBP",
                    "Regional Distribution",
                    "SAP",
                ],
                "industry_keywords": [
                    "retail",
                    "ecommerce",
                    "consumer goods",
                    "logistics",
                    "distribution",
                ],
                "job_description": (
                    "We are hiring a UAE-based Supply Chain Manager to lead planning, inventory, logistics, and "
                    "fulfillment performance across a fast-moving retail and ecommerce network. The brief prioritizes "
                    "candidates with strong public evidence of S&OP ownership, demand and supply planning, inventory "
                    "optimization, ERP-led operations, and distribution or warehouse coordination in the UAE market. "
                    "Experience scaling service levels across omnichannel retail, consumer goods, 3PL, or regional "
                    "distribution environments is highly valuable."
                ),
                "brief_clarifications": {
                    "prioritize_first_location": True,
                    "allow_adjacent_titles": True,
                    "strict_market_scope": True,
                    "expand_search_when_thin": True,
                },
                "jd_breakdown": {
                    **extract_job_description_breakdown(
                        (
                            "We are hiring a UAE-based Supply Chain Manager to lead planning, inventory, logistics, and "
                            "fulfillment performance across a fast-moving retail and ecommerce network. The brief prioritizes "
                            "candidates with strong public evidence of S&OP ownership, demand and supply planning, inventory "
                            "optimization, ERP-led operations, and distribution or warehouse coordination in the UAE market. "
                            "Experience scaling service levels across omnichannel retail, consumer goods, 3PL, or regional "
                            "distribution environments is highly valuable."
                        ),
                        role_title="Supply Chain Manager",
                    ),
                    "titles": [
                        "Supply Chain Manager",
                        "Senior Supply Chain Manager",
                        "Supply Planning Manager",
                        "Demand Planning Manager",
                    ],
                    "required_keywords": [
                        "s&op",
                        "demand planning",
                        "inventory optimization",
                        "logistics",
                        "erp",
                    ],
                    "preferred_keywords": [
                        "warehouse operations",
                        "fulfillment",
                        "3pl",
                        "otif",
                        "procurement",
                        "sap",
                    ],
                    "industry_keywords": [
                        "retail",
                        "ecommerce",
                        "consumer goods",
                        "logistics",
                        "distribution",
                    ],
                    "years": {
                        "mode": "plus_minus",
                        "value": 6,
                        "min": 5,
                        "max": 7,
                        "tolerance": 1,
                    },
                    "keyword_tracks": {
                        "portfolio_keywords": [
                            "inventory optimization",
                            "warehouse operations",
                            "regional distribution",
                            "fulfillment",
                        ],
                        "commercial_keywords": [
                            "service levels",
                            "otif",
                            "cost to serve",
                            "stockouts",
                        ],
                        "leadership_keywords": [
                            "cross-functional leadership",
                            "supplier coordination",
                            "operations leadership",
                            "stakeholder management",
                        ],
                        "scope_keywords": [
                            "uae",
                            "dubai",
                            "jebel ali",
                            "regional distribution",
                        ],
                    },
                    "search_tuning": {
                        "search_profile": FOCUSED_SEARCH_PROFILE,
                        "reranker_model_name": DEFAULT_UI_RERANKER_MODEL,
                        "internal_fetch_limit_override": 420,
                        "reranker_top_n": 220,
                        "provider_parallel_requests": 24,
                        "scrapingbee_max_queries": 54,
                        "max_geo_groups": 2,
                        "geo_group_size": 1,
                        "company_chunk_size": 4,
                        "company_slice_location_group_limit": 1,
                        "max_company_terms_per_query": 6,
                        "stagnation_query_window": 8,
                        "stagnation_min_results": 240,
                        "include_history_slices": True,
                        "include_discovery_slices": True,
                        "verification_top_n": 160,
                        "verification_parallel_candidates": 32,
                        "verification_location_probe_queries": 1,
                        "query_family_budgets": {
                            "team_leadership_pages": 1,
                            "appointment_news_pages": 3,
                            "speaker_bio_pages": 1,
                            "award_industry_pages": 0,
                            "industry_association_pages": 2,
                            "trade_directory_pages": 2,
                            "org_chart_profile_pages": 2,
                            "profile_like_public_pages": 18,
                        },
                    },
                },
                "anchors": {
                    "title": "critical",
                    "skills": "important",
                    "location": "important",
                    "company": "preferred",
                    "years": "preferred",
                    "industry": "important",
                    "function": "important",
                    "semantic": "preferred",
                },
            },
            "data_analyst_uae": {
                "project_name": "UAE Data Analyst Search",
                "client_name": "Demo Data Search",
                "role_title": "Data Analyst",
                "titles": [
                    "Senior Data Analyst",
                    "Data Analyst",
                    "Business Intelligence Analyst",
                    "Product Analyst",
                ],
                "countries": [
                    "United Arab Emirates",
                ],
                "continents": [],
                "cities": [
                    "Dubai",
                    "Abu Dhabi",
                    "Sharjah",
                ],
                "company_targets": [],
                "peer_company_targets": [
                    "Careem",
                    "talabat",
                    "noon",
                    "dubizzle",
                    "Property Finder",
                    "Bayzat",
                    "Emirates NBD",
                    "e&",
                ],
                "company_match_mode": "both",
                "employment_status_mode": "any",
                "years_mode": "range",
                "years_value": None,
                "years_tolerance": 0,
                "max_profiles": 100,
                "must_have_keywords": [
                    "SQL",
                    "Python",
                    "Dashboarding",
                    "KPI",
                    "Stakeholder Management",
                ],
                "nice_to_have_keywords": [
                    "Tableau",
                    "Power BI",
                    "Experimentation",
                    "Forecasting",
                    "Product Analytics",
                    "Cohort Analysis",
                ],
                "industry_keywords": [
                    "marketplace",
                    "ecommerce",
                    "fintech",
                    "digital product",
                    "analytics",
                ],
                "job_description": (
                    "We are hiring a UAE-based Data Analyst to turn product, commercial, and operational data into "
                    "clear business decisions. Prioritize candidates with strong public evidence of SQL and Python, "
                    "hands-on dashboarding, KPI design, experimentation or forecasting exposure, and experience "
                    "partnering with product, growth, finance, or operations stakeholders in the UAE market."
                ),
                "brief_clarifications": {
                    "prioritize_first_location": True,
                    "allow_adjacent_titles": True,
                    "strict_market_scope": True,
                    "expand_search_when_thin": False,
                },
                "jd_breakdown": {
                    **extract_job_description_breakdown(
                        (
                            "We are hiring a UAE-based Data Analyst to turn product, commercial, and operational data "
                            "into clear business decisions. Prioritize candidates with strong public evidence of SQL "
                            "and Python, hands-on dashboarding, KPI design, experimentation or forecasting exposure, "
                            "and experience partnering with product, growth, finance, or operations stakeholders in "
                            "the UAE market."
                        ),
                        role_title="Data Analyst",
                    ),
                    "titles": [
                        "Senior Data Analyst",
                        "Data Analyst",
                        "Business Intelligence Analyst",
                        "Product Analyst",
                    ],
                    "required_keywords": [
                        "sql",
                        "python",
                        "dashboarding",
                        "kpi",
                        "stakeholder management",
                    ],
                    "preferred_keywords": [
                        "tableau",
                        "power bi",
                        "experimentation",
                        "forecasting",
                        "product analytics",
                        "cohort analysis",
                    ],
                    "industry_keywords": [
                        "marketplace",
                        "ecommerce",
                        "fintech",
                        "digital product",
                        "analytics",
                    ],
                    "years": {
                        "mode": "range",
                        "value": None,
                        "min": 4,
                        "max": 9,
                        "tolerance": 0,
                    },
                    "keyword_tracks": {
                        "portfolio_keywords": [
                            "dashboarding",
                            "kpi",
                            "reporting",
                            "business insights",
                        ],
                        "commercial_keywords": [
                            "growth",
                            "revenue",
                            "conversion",
                            "retention",
                        ],
                        "leadership_keywords": [
                            "stakeholder management",
                            "cross-functional",
                            "business partnership",
                        ],
                        "scope_keywords": [
                            "uae",
                            "dubai",
                            "abu dhabi",
                            "united arab emirates",
                        ],
                    },
                    "search_tuning": {
                        "search_profile": FOCUSED_SEARCH_PROFILE,
                        "reranker_model_name": DEFAULT_UI_RERANKER_MODEL,
                        "internal_fetch_limit_override": 240,
                        "reranker_top_n": 140,
                        "provider_parallel_requests": 18,
                        "scrapingbee_max_queries": 48,
                        "max_geo_groups": 2,
                        "geo_group_size": 1,
                        "company_chunk_size": 4,
                        "company_slice_location_group_limit": 1,
                        "max_company_terms_per_query": 6,
                        "stagnation_query_window": 10,
                        "stagnation_min_results": 140,
                        "include_history_slices": False,
                        "include_discovery_slices": True,
                        "verification_top_n": 80,
                        "verification_parallel_candidates": 20,
                        "verification_location_probe_queries": 1,
                        "query_family_budgets": {
                            "team_leadership_pages": 1,
                            "appointment_news_pages": 1,
                            "speaker_bio_pages": 1,
                            "award_industry_pages": 0,
                            "industry_association_pages": 3,
                            "trade_directory_pages": 4,
                            "org_chart_profile_pages": 1,
                            "profile_like_public_pages": 16,
                        },
                    },
                },
                "anchors": {
                    "title": "critical",
                    "skills": "critical",
                    "location": "important",
                    "company": "preferred",
                    "years": "important",
                    "industry": "important",
                    "function": "important",
                    "semantic": "preferred",
                },
            },
        },
    }
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
