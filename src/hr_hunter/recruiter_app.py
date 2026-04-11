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
    "profile_like_public_pages": 12,
    "team_leadership_pages": 5,
    "trade_directory_pages": 5,
    "industry_association_pages": 4,
    "org_chart_profile_pages": 3,
    "appointment_news_pages": 1,
    "speaker_bio_pages": 1,
    "award_industry_pages": 0,
}
FOCUSED_NON_EXECUTIVE_TOP_UP_QUERY_FAMILY_BUDGETS = {
    "profile_like_public_pages": 9,
    "trade_directory_pages": 4,
    "industry_association_pages": 3,
    "team_leadership_pages": 2,
    "org_chart_profile_pages": 1,
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


def _is_executive_brief(role_title: str, titles: List[str]) -> bool:
    targets = [role_title, *titles]
    return any(
        hint in normalize_text(value)
        for value in targets
        if str(value).strip()
        for hint in EXECUTIVE_ROLE_HINTS
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
    company_targets: List[str],
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
            bool(company_targets),
            len(document_text) >= 120,
        )
        if signal
    )
    executive_brief = _is_executive_brief(role_title, titles)
    location_count = len(location_targets)
    title_count = len(titles)

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
    location_targets: List[str],
    companies: List[str],
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
    thin_detail = len(required_keywords) < 2 and not industry_keywords and len(document_text) < 160
    common_volume_search = bool(limit >= 40 and not companies and title_count <= 2 and not executive_brief)

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

    if role_title and title_count <= 2:
        questions.append(
            {
                "id": "allow_adjacent_titles",
                "label": "Adjacent Titles",
                "prompt": "Should HR Hunter include adjacent role-family titles when exact matches look thin?",
                "help": "This broadens retrieval into near-neighbor roles like Managing Director for CEO or Growth Analyst for Data Analyst.",
                "recommended_answer": bool(
                    executive_brief or search_profile != FOCUSED_SEARCH_PROFILE or common_volume_search
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
    return resolved_values, resolved_questions


def _resolve_top_up_expansion_strategy(
    *,
    top_up_round: int,
    search_profile: str,
    executive_brief: bool,
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
    if top_up_round <= 0 or search_profile != FOCUSED_SEARCH_PROFILE:
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
    search_tuning = breakdown.get("search_tuning", {})
    if not isinstance(search_tuning, dict):
        search_tuning = {}

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
    search_profile = _derive_search_profile(
        role_title=role_title,
        titles=titles,
        location_targets=location_targets,
        company_targets=companies,
        required_keywords=required_keywords,
        preferred_keywords=preferred_keywords,
        industry_keywords=industry_keywords,
        document_text=job_description,
        limit=limit,
    )
    brief_follow_up_questions = _build_brief_follow_up_questions(
        role_title=role_title,
        titles=titles,
        location_targets=location_targets,
        companies=companies,
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
    prioritize_first_location = bool(brief_clarifications.get("prioritize_first_location"))
    allow_adjacent_titles = bool(brief_clarifications.get("allow_adjacent_titles", True))
    expand_search_when_thin = bool(brief_clarifications.get("expand_search_when_thin", search_profile != FOCUSED_SEARCH_PROFILE))
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
        internal_fetch_limit = min(internal_fetch_limit, max(limit * 2, limit + 50))
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
        or (16 if limit >= 220 else (12 if limit >= 120 else 8)),
    )
    if search_profile == FOCUSED_SEARCH_PROFILE:
        scrapingbee_parallel_requests = min(scrapingbee_parallel_requests, 8)
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
    scrapingbee_max_queries = max(
        120,
        _coerce_int(payload.get("scrapingbee_max_queries"))
        or _coerce_int(search_tuning.get("scrapingbee_max_queries"))
        or compute_provider_max_queries(limit),
    )
    if search_profile == FOCUSED_SEARCH_PROFILE:
        scrapingbee_max_queries = min(scrapingbee_max_queries, max(90, limit * 2))
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
    verification_location_probe_queries = max(
        0,
        _coerce_int(payload.get("verification_location_probe_queries"))
        or tuned_verification_location_probe_queries
        or 1,
    )
    verification_company_location_probe_queries = max(
        0,
        _coerce_int(payload.get("verification_company_location_probe_queries"))
        or tuned_verification_company_location_probe_queries
        or 0,
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
    if not resolved_query_family_budgets and search_profile == FOCUSED_SEARCH_PROFILE:
        resolved_query_family_budgets = _default_query_family_budgets(
            search_profile=search_profile,
            executive_brief=executive_brief,
            top_up_round=top_up_round,
        )
    top_up_strategy = _resolve_top_up_expansion_strategy(
        top_up_round=top_up_round,
        search_profile=search_profile,
        executive_brief=executive_brief,
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
    resolved_query_family_budgets = {
        str(family).strip(): max(0, int(value))
        for family, value in dict(top_up_strategy["query_family_budgets"]).items()
        if str(family).strip()
    }
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
        "ui_meta": {
            "titles": titles,
            "countries": countries,
            "continents": continents,
            "cities": cities,
            "company_targets": companies,
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
            "brief_clarifications": brief_clarifications,
            "top_up_round": top_up_round,
            "top_up_strategy": top_up_strategy["strategy"],
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
                    "Group CEO",
                    "Managing Director",
                    "President",
                    "Regional CEO",
                    "Business Unit CEO",
                    "CEO",
                ],
                "countries": [
                    "United Arab Emirates",
                    "Saudi Arabia",
                    "Qatar",
                    "Kuwait",
                    "Oman",
                    "Bahrain",
                    "Egypt",
                    "India",
                    "United Kingdom",
                    "France",
                    "Italy",
                ],
                "continents": [],
                "cities": [
                    "Dubai",
                    "Abu Dhabi",
                    "Riyadh",
                    "Jeddah",
                    "Doha",
                    "Kuwait City",
                    "Muscat",
                    "Manama",
                    "Cairo",
                    "Mumbai",
                    "New Delhi",
                    "Bengaluru",
                    "London",
                    "Paris",
                    "Milan",
                ],
                "company_targets": [
                    "Marina Home Interiors",
                    "The One",
                    "Al Huzaifa",
                    "Pan Emirates",
                    "Home Centre",
                    "Home Box",
                    "Landmark Group",
                    "IDdesign",
                    "Pottery Barn",
                    "West Elm",
                    "Crate & Barrel",
                    "Williams-Sonoma",
                    "RH",
                    "Roche Bobois",
                    "BoConcept",
                    "Maisons du Monde",
                    "Zara Home",
                ],
                "company_match_mode": "both",
                "employment_status_mode": "any",
                "years_mode": "at_least",
                "years_value": 12,
                "years_tolerance": 0,
                "max_profiles": 300,
                "must_have_keywords": [
                    "P&L Ownership",
                    "Multi-country Leadership",
                    "Retail Operations",
                    "Executive Team Leadership",
                    "Board / Founder Stakeholder Management",
                    "Business Scaling",
                ],
                "nice_to_have_keywords": [
                    "Home Furnishings",
                    "Premium Retail",
                    "Interior Design Retail",
                    "Omnichannel",
                    "Market Expansion",
                    "Turnaround",
                    "Arabic",
                    "MBA",
                    "Founder-Led Transition",
                    "P&L",
                ],
                "industry_keywords": [
                    "home furnishings",
                    "furniture retail",
                    "premium retail",
                    "interior design",
                    "luxury retail",
                    "home decor",
                    "consumer",
                ],
                "job_description": (
                    "Marina Home Interiors is a Dubai-headquartered premium home furnishings and design-led retail "
                    "business operating across the GCC with additional exposure in Egypt and India. We are hiring a "
                    "Chief Executive Officer to lead the next stage of profitable growth as the founder transitions "
                    "into a board-led governance model. The mandate requires a senior operator with genuine CEO, "
                    "Managing Director, President, or divisional P&L leadership experience in premium retail, home "
                    "furnishings, furniture, interiors, lifestyle, or adjacent design-led consumer businesses. The "
                    "CEO must be able to drive revenue growth, store and omnichannel performance, operating cadence, "
                    "executive team leadership, board and shareholder communication, and multi-country market "
                    "expansion while protecting brand quality and customer experience. Experience in founder-led, "
                    "family-owned, or transformation situations is valuable. Fluent English is required and Arabic is "
                    "a strong advantage."
                ),
                "jd_breakdown": {
                    **extract_job_description_breakdown(
                        (
                            "Marina Home Interiors is a Dubai-headquartered premium home furnishings and design-led retail "
                            "business operating across the GCC with additional exposure in Egypt and India. We are hiring a "
                            "Chief Executive Officer to lead the next stage of profitable growth as the founder transitions "
                            "into a board-led governance model. The mandate requires a senior operator with genuine CEO, "
                            "Managing Director, President, or divisional P&L leadership experience in premium retail, home "
                            "furnishings, furniture, interiors, lifestyle, or adjacent design-led consumer businesses. The "
                            "CEO must be able to drive revenue growth, store and omnichannel performance, operating cadence, "
                            "executive team leadership, board and shareholder communication, and multi-country market "
                            "expansion while protecting brand quality and customer experience. Experience in founder-led, "
                            "family-owned, or transformation situations is valuable. Fluent English is required and Arabic is "
                            "a strong advantage."
                        ),
                        role_title="Chief Executive Officer (CEO)",
                    ),
                    "keyword_tracks": {
                        "portfolio_keywords": [
                            "home furnishings",
                            "store network",
                            "omnichannel",
                            "brand scaling",
                        ],
                        "commercial_keywords": [
                            "P&L ownership",
                            "profitability",
                            "revenue growth",
                            "market expansion",
                        ],
                        "leadership_keywords": [
                            "board governance",
                            "executive team leadership",
                            "founder-led transition",
                            "stakeholder management",
                        ],
                        "scope_keywords": [
                            "multi-country",
                            "regional",
                            "international",
                            "GCC",
                        ],
                    },
                    "search_tuning": {
                        "internal_fetch_limit_override": 480,
                        "reranker_top_n": 240,
                        "provider_parallel_requests": 20,
                        "scrapingbee_max_queries": 72,
                        "max_geo_groups": 6,
                        "geo_group_size": 1,
                        "company_chunk_size": 3,
                        "company_slice_location_group_limit": 3,
                        "max_company_terms_per_query": 10,
                        "stagnation_query_window": 12,
                        "stagnation_min_results": 360,
                        "include_history_slices": True,
                        "include_discovery_slices": True,
                        "verification_top_n": 120,
                        "verification_parallel_candidates": 8,
                        "query_family_budgets": {
                            "team_leadership_pages": 18,
                            "appointment_news_pages": 12,
                            "speaker_bio_pages": 8,
                            "award_industry_pages": 4,
                            "industry_association_pages": 4,
                            "trade_directory_pages": 4,
                            "org_chart_profile_pages": 16,
                            "profile_like_public_pages": 16,
                        },
                    },
                },
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
