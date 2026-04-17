from __future__ import annotations

import re
from urllib.parse import urlparse

from hr_hunter_transformer.company_quality import looks_like_bad_company as _company_looks_bad
from hr_hunter_transformer.models import EvidenceRecord, RawSearchHit, SearchBrief
from hr_hunter_transformer.role_profiles import (
    PROFESSIONAL_SOURCES,
    TECHNICAL_SOURCES,
    infer_role_family,
    normalize_text,
    role_family_hints,
)


NON_PERSON_TOKENS = {
    "about",
    "blog",
    "careers",
    "company",
    "contact",
    "directory",
    "docs",
    "followers",
    "home",
    "jobs",
    "learn",
    "models",
    "news",
    "orgs",
    "people",
    "profile",
    "profiles",
    "search",
    "spaces",
    "team",
    "hiring",
    "post",
    "posts",
    "career",
    "vacancy",
}
NOISY_DOMAINS = {
    "facebook.com",
    "instagram.com",
    "indeed.com",
    "ae.indeed.com",
    "expertini.com",
    "ajman.ae.expertini.com",
    "jobsinme.wordpress.com",
    "scribd.com",
    "yumpu.com",
    "iipmr.com",
}
ROLE_WORDS = {
    "accountant",
    "analyst",
    "architect",
    "coordinator",
    "consultant",
    "designer",
    "director",
    "engineer",
    "hiring",
    "lead",
    "logistics",
    "manager",
    "marketing",
    "operations",
    "planner",
    "planning",
    "procurement",
    "product",
    "program",
    "project",
    "recruiter",
    "sales",
    "specialist",
    "strategy",
    "supply",
}
NON_NAME_WORDS = {"we", "our", "join", "team", "careers", "career", "post", "posts"}
COMPANY_NAME_HINTS = {
    "architects",
    "architecture",
    "aviation",
    "bank",
    "care",
    "consultants",
    "consulting",
    "consumer",
    "design",
    "development",
    "developments",
    "engineering",
    "foods",
    "global",
    "group",
    "healthcare",
    "holdings",
    "international",
    "lighting",
    "llc",
    "logistics",
    "ltd",
    "petroleum",
    "properties",
    "retail",
    "services",
    "solutions",
    "systems",
    "technologies",
    "technology",
    "trading",
}
LINKEDIN_HOSTS = {"linkedin.com", "ae.linkedin.com", "sa.linkedin.com", "in.linkedin.com", "pk.linkedin.com"}
LINKEDIN_COUNTRY_HOST_HINTS = {
    "ae.linkedin.com": "United Arab Emirates",
    "sa.linkedin.com": "Saudi Arabia",
    "kw.linkedin.com": "Kuwait",
    "qa.linkedin.com": "Qatar",
    "in.linkedin.com": "India",
    "pk.linkedin.com": "Pakistan",
    "de.linkedin.com": "Germany",
}
COUNTRY_EQUIVALENTS = {
    "uae": {"uae", "united arab emirates"},
    "united arab emirates": {"uae", "united arab emirates"},
    "uk": {"uk", "united kingdom", "great britain"},
    "united kingdom": {"uk", "united kingdom", "great britain"},
    "saudi arabia": {"saudi arabia", "kingdom of saudi arabia", "ksa"},
    "ksa": {"saudi arabia", "kingdom of saudi arabia", "ksa"},
    "qatar": {"qatar", "state of qatar"},
    "bahrain": {"bahrain", "kingdom of bahrain"},
    "oman": {"oman", "sultanate of oman"},
    "egypt": {"egypt", "arab republic of egypt"},
    "hong kong": {"hong kong", "hong kong sar", "hk"},
    "hk": {"hong kong", "hong kong sar", "hk"},
    "netherlands": {"netherlands", "holland", "the netherlands"},
    "switzerland": {"switzerland", "swiss confederation"},
    "singapore": {"singapore", "sg"},
    "france": {"france", "french republic"},
    "germany": {"germany", "federal republic of germany"},
    "italy": {"italy", "italian republic"},
    "india": {"india", "republic of india"},
    "kuwait": {"kuwait", "state of kuwait"},
    "sweden": {"sweden", "kingdom of sweden"},
}
BIDI_CONTROL_RE = re.compile(r"[\u200e\u200f\u202a-\u202e\u2066-\u2069]")
MONTH_YEAR_RE = re.compile(r"^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)-\d{2}$", re.IGNORECASE)
MONTH_YEAR_TEXT_RE = re.compile(r"^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+\d{2,4}$", re.IGNORECASE)
BAD_COMPANY_LITERALS = {
    "at",
    "@",
    "&",
    "+",
    "company",
    "current",
    "present",
    "educational",
    "dr",
    "mr",
    "mrs",
    "ms",
    "he",
    "she",
    "we",
    "they",
    "profile",
    "experience",
    "ceo",
    "(ceo)",
    "dubai",
    "abu dhabi",
    "riyadh",
    "jeddah",
    "saudi arabia",
    "united arab emirates",
    "uae",
    "mea",
    "mena",
    "gcc",
}


def _company_target_match(normalized_company: str, targets: set[str]) -> bool:
    if not normalized_company or not targets:
        return False
    for target in targets:
        if not target or len(target) < 3:
            continue
        if normalized_company == target:
            return True
        if target in normalized_company or normalized_company in target:
            return True
    return False


def _best_target_company_match(text: str, companies: list[str]) -> str:
    normalized_text = normalize_text(text)
    matches: list[tuple[int, int, str]] = []
    for company in companies:
        normalized_company = normalize_text(company)
        if not normalized_company:
            continue
        position = normalized_text.find(normalized_company)
        if position >= 0:
            matches.append((position, -len(normalized_company), company))
    if not matches:
        return ""
    matches.sort()
    return matches[0][2]


class ProfileExtractor:
    def _sanitize_person_name(self, value: str) -> str:
        cleaned = BIDI_CONTROL_RE.sub("", str(value or "")).strip()
        cleaned = re.sub(r"\([^)]*$", "", cleaned).strip()
        parts = [part for part in re.split(r"\s+", cleaned) if part]
        while parts and (any(char.isdigit() for char in parts[-1]) or re.fullmatch(r"[a-f0-9]{6,}", parts[-1], flags=re.IGNORECASE)):
            parts.pop()
        while parts and parts[-1].lower() in {"mba", "cscp", "cppm", "cscm", "pmp", "phd"}:
            parts.pop()
        cleaned = " ".join(parts[:5]).strip()
        return cleaned

    def _normalize_company_candidate(self, value: str, current_title: str = "") -> str:
        cleaned = BIDI_CONTROL_RE.sub("", str(value or "")).strip(" -|,.;:")
        cleaned = re.sub(r"\s+", " ", cleaned)
        if not cleaned:
            return ""
        cleaned = re.sub(r"^[@+&]+\s*", "", cleaned).strip(" -|,.;:")
        cleaned = re.sub(r"^\(([^)]+)\)$", r"\1", cleaned).strip(" -|,.;:")
        if re.search(r"\bat\s+(.+)$", cleaned, flags=re.IGNORECASE):
            cleaned = re.search(r"\bat\s+(.+)$", cleaned, flags=re.IGNORECASE).group(1).strip(" -|,.;:")
        if ". " in cleaned and not re.search(r"\b(group|company|corp|co|llc|ltd|inc)\b", cleaned, flags=re.IGNORECASE):
            tail = cleaned.split(".")[-1].strip()
            if tail and not self._looks_like_bad_company_value(tail, current_title):
                cleaned = tail
        cleaned = re.sub(r"\bView org chart\b.*$", "", cleaned, flags=re.IGNORECASE).strip(" -|,.;:")
        cleaned = re.sub(r"\bis a\b.*$", "", cleaned, flags=re.IGNORECASE).strip(" -|,.;:")
        cleaned = re.sub(r"\bfrom\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{2,4}\b.*$", "", cleaned, flags=re.IGNORECASE).strip(" -|,.;:")
        cleaned = re.sub(r"\(\s*[A-Za-z]{0,3}$", "", cleaned).strip(" -|,.;:")
        if cleaned.count("(") != cleaned.count(")"):
            cleaned = cleaned.split("(")[0].strip(" -|,.;:")
        return cleaned

    def _looks_like_bad_company_value(self, value: str, current_title: str = "") -> bool:
        cleaned = self._normalize_company_candidate(value, current_title)
        lowered = cleaned.lower()
        if _company_looks_bad(cleaned, current_title):
            return True
        role_patterns = (
            "supply chain manager",
            "procurement manager",
            "logistics manager",
            "supply planning manager",
            "demand planning manager",
            "senior supply chain manager",
            "regional senior supply chain manager",
        )
        if normalize_text(cleaned) in {normalize_text(pattern) for pattern in role_patterns}:
            return True
        if any(phrase in lowered for phrase in ("org chart", "view manager", "regional senior", "senior supply chain manager")):
            return True
        if "manager at" in lowered or "engineer at" in lowered or "planner at" in lowered:
            return True
        if re.search(r"\b(manager|engineer|planner|director|officer|executive|specialist|lead)\b", lowered):
            return True
        if "teach machines" in lowered or "how to learn" in lowered:
            return True
        return False

    def _company_from_url(self, url: str) -> str:
        parsed = urlparse(url)
        host = parsed.netloc.lower().removeprefix("www.")
        path = [segment for segment in parsed.path.strip("/").split("/") if segment]
        if host == "theorg.com" and len(path) >= 2 and path[0] == "org":
            parts = [part for part in re.split(r"[-_]+", path[1]) if part]
            return " ".join(part.capitalize() for part in parts[:6]).strip()
        return ""

    def _is_profile_url(self, url: str) -> bool:
        parsed = urlparse(url)
        host = parsed.netloc.lower().removeprefix("www.")
        path = parsed.path.strip("/")
        segments = [segment for segment in path.split("/") if segment]
        if not host or not path:
            return False
        if host in NOISY_DOMAINS or host.endswith(".expertini.com"):
            return False
        if host == "api.substack.com" or host == "gist.github.com":
            return False
        lowered_path = parsed.path.lower()
        if host in LINKEDIN_HOSTS:
            return lowered_path.startswith("/in/") or lowered_path.startswith("/pub/")
        if host == "people.bayt.com":
            return len(segments) == 1
        if host == "theorg.com":
            return len(segments) >= 4 and segments[0] == "org" and "org-chart" in segments
        if host in PROFESSIONAL_SOURCES:
            return True
        if host == "github.com":
            return len(segments) == 1 and segments[0].lower() not in NON_PERSON_TOKENS
        if host == "gitlab.com":
            return len(segments) == 1 and segments[0].lower() not in NON_PERSON_TOKENS
        if host == "huggingface.co":
            return len(segments) == 1 and segments[0].lower() not in NON_PERSON_TOKENS
        if host == "kaggle.com":
            return len(segments) == 1 and segments[0].lower() not in NON_PERSON_TOKENS
        if host == "stackoverflow.com":
            return len(segments) >= 2 and segments[0] in {"users", "story"}
        if host == "dev.to":
            return len(segments) == 1 and segments[0].lower() not in NON_PERSON_TOKENS
        return any(token in parsed.path.lower() for token in ("/in/", "/people/", "/person/", "/profile", "/bio", "/speaker/"))

    def _looks_like_person_name(self, value: str) -> bool:
        cleaned = BIDI_CONTROL_RE.sub("", value.strip())
        lowered_text = cleaned.lower()
        if any(marker in lowered_text for marker in ("we're hiring", "we are hiring", "job opening", "vacancy", "hiring now")):
            return False
        if lowered_text.endswith("'s post") or lowered_text.endswith(" post"):
            return False
        tokens = [token for token in re.split(r"[^A-Za-z'.-]+", cleaned) if token]
        if len(tokens) < 2 or len(tokens) > 5:
            return False
        if any(any(char.isdigit() for char in token) for token in tokens):
            return False
        lowered = {token.lower().strip(".") for token in tokens}
        if lowered.intersection(COMPANY_NAME_HINTS):
            return False
        if len(tokens) >= 2 and all(any(char.isalpha() for char in token) and token.upper() == token for token in tokens):
            return False
        if lowered.intersection(ROLE_WORDS):
            return False
        return not lowered.intersection(NON_PERSON_TOKENS)

    def _guess_name(self, hit: RawSearchHit) -> str:
        parsed = urlparse(hit.url)
        path = parsed.path.strip("/")
        host = parsed.netloc.lower().removeprefix("www.")
        cleaned_title = BIDI_CONTROL_RE.sub("", hit.title)
        if host in TECHNICAL_SOURCES and path:
            slug = path.split("/")[0]
            parts = [part for part in re.split(r"[-_]+", slug) if part]
            candidate = " ".join(part.capitalize() for part in parts[:4])
            if self._looks_like_person_name(candidate):
                return candidate
        if host in PROFESSIONAL_SOURCES and path:
            slug = path.split("/")[-1] if host in LINKEDIN_HOSTS else path.split("/")[0]
            if slug and slug.lower() not in NON_PERSON_TOKENS:
                original_parts = [part for part in re.split(r"[-_]+", slug) if part]
                if any(part.lower() in NON_PERSON_TOKENS or part.lower() in NON_NAME_WORDS for part in original_parts):
                    return ""
                parts = [part for part in re.split(r"[-_]+", slug) if part and part.lower() not in NON_PERSON_TOKENS]
                while parts and (any(char.isdigit() for char in parts[-1]) or re.fullmatch(r"[a-f0-9]{6,}", parts[-1], flags=re.IGNORECASE)):
                    parts.pop()
                candidate = " ".join(part.capitalize() for part in parts[:4])
                candidate = self._sanitize_person_name(candidate)
                if self._looks_like_person_name(candidate):
                    return candidate
        title = re.split(r"[|,-]", cleaned_title)[0].strip()
        title = self._sanitize_person_name(title)
        return title if self._looks_like_person_name(title) else ""

    def _guess_title(self, hit: RawSearchHit, brief: SearchBrief) -> str:
        combined = f"{hit.title} {hit.snippet}"
        normalized = normalize_text(combined)
        for family_title in role_family_hints(infer_role_family(brief.role_title, *brief.titles)):
            if normalize_text(family_title) in normalized:
                return family_title.title()
        for title in brief.titles:
            if normalize_text(title) in normalized:
                return title
        for pattern in (
            r"(ai engineer|machine learning engineer|llm engineer|senior ai engineer)",
            r"(senior accountant|accountant|finance manager)",
            r"(supply chain manager|demand planning manager|supply planning manager|logistics manager)",
            r"(digital marketing manager|marketing manager|performance marketing manager|growth manager)",
            r"(sustainability manager|esg manager|sustainability lead)",
            r"(interior designer|senior interior designer|interior design manager|architect|project architect|design manager|design director)",
        ):
            match = re.search(pattern, normalized)
            if match:
                return match.group(1).title()
        return ""

    def _strip_company_tail(self, value: str) -> str:
        cleaned = BIDI_CONTROL_RE.sub("", value).strip(" -|,.;:")
        cleaned = re.split(r"\s+[|·]\s+|\s+-\s+|;\s*", cleaned)[0].strip()
        cleaned = re.split(
            r"\.\s+(?:Dubai|Abu Dhabi|Riyadh|Jeddah|Khobar|Saudi Arabia|United Arab Emirates|UAE)\b",
            cleaned,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0].strip()
        cleaned = re.split(
            r"\s+\bin\b\s+(?:Dubai|Abu Dhabi|Riyadh|Jeddah|Khobar|Saudi Arabia|United Arab Emirates|UAE)\b",
            cleaned,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0].strip()
        cleaned = re.split(r"\b(?:since|current|currently|present)\b", cleaned, maxsplit=1, flags=re.IGNORECASE)[0].strip(" -|,.;:")
        return cleaned

    def _guess_company(self, hit: RawSearchHit, brief: SearchBrief, current_title: str) -> tuple[str, float]:
        combined_text = f"{hit.title} {hit.snippet}"
        host = urlparse(hit.url).netloc.lower().removeprefix("www.")
        title_text = BIDI_CONTROL_RE.sub("", hit.title)
        if current_title and "linkedin." in host:
            title_parts = [part.strip() for part in re.split(r"[|,\-]", title_text) if part.strip()]
            for part in title_parts[1:4]:
                lead_match = re.search(rf"{re.escape(current_title)}\s+(.+)$", part, flags=re.IGNORECASE)
                if lead_match:
                    candidate = self._normalize_company_candidate(self._strip_company_tail(lead_match.group(1)), current_title)
                    if candidate and not self._looks_like_person_name(candidate) and not self._looks_like_bad_company_value(candidate, current_title):
                        return candidate, 0.96
            at_match = re.search(rf"{re.escape(current_title)}\s+(?:at|@)\s+([^|;,.]{{2,80}})", title_text, flags=re.IGNORECASE)
            if at_match:
                candidate = self._normalize_company_candidate(self._strip_company_tail(at_match.group(1)), current_title)
                if candidate and not self._looks_like_person_name(candidate) and not self._looks_like_bad_company_value(candidate, current_title):
                    return candidate, 0.96
        exact_company = _best_target_company_match(combined_text, list(brief.company_targets))
        if exact_company:
            return exact_company, 0.92
        peer_company = _best_target_company_match(combined_text, list(brief.peer_company_targets))
        if peer_company:
            return peer_company, 0.86
        url_company = self._company_from_url(hit.url)
        if url_company and not self._looks_like_bad_company_value(url_company, current_title):
            return url_company, 0.84
        for text in (BIDI_CONTROL_RE.sub("", hit.title), BIDI_CONTROL_RE.sub("", hit.snippet)):
            at_match = re.search(r"\b(?:at|@)\s+([^|;,.]{2,80})", text, flags=re.IGNORECASE)
            if at_match:
                candidate = self._normalize_company_candidate(self._strip_company_tail(at_match.group(1)), current_title)
                if candidate and not self._looks_like_person_name(candidate) and not self._looks_like_bad_company_value(candidate, current_title):
                    return candidate, 0.82
        if current_title:
            title_parts = [part.strip() for part in re.split(r"[|,\-]", title_text) if part.strip()]
            for part in title_parts[1:3]:
                lead_match = re.search(rf"{re.escape(current_title)}\s+(.+)$", part, flags=re.IGNORECASE)
                if lead_match:
                    candidate = self._normalize_company_candidate(self._strip_company_tail(lead_match.group(1)), current_title)
                    if candidate and not self._looks_like_person_name(candidate) and not self._looks_like_bad_company_value(candidate, current_title):
                        return candidate, 0.72
        snippet_text = BIDI_CONTROL_RE.sub("", hit.snippet)
        title_dot_company = re.search(
            r"\b[A-Z][A-Za-z&+ .'-]{1,60}\.\s+([A-Z][A-Za-z0-9&+ .'-]{1,60})\.",
            snippet_text,
        )
        if title_dot_company:
            candidate = self._normalize_company_candidate(self._strip_company_tail(title_dot_company.group(1)), current_title)
            if candidate and not self._looks_like_person_name(candidate) and not self._looks_like_bad_company_value(candidate, current_title):
                return candidate, 0.76
        return "", 0.0

    def _guess_location(self, hit: RawSearchHit, brief: SearchBrief) -> str:
        normalized = normalize_text(f"{hit.title} {hit.snippet}")
        for value in [*brief.cities, *brief.countries]:
            variants = self._location_variants(value)
            if any(variant in normalized for variant in variants):
                return value
        host = urlparse(hit.url).netloc.lower().removeprefix("www.")
        hinted_country = LINKEDIN_COUNTRY_HOST_HINTS.get(host, "")
        if hinted_country:
            matched_country = self._match_brief_location(hinted_country, brief.countries)
            if matched_country:
                return matched_country
        return ""

    def _location_variants(self, value: str) -> set[str]:
        normalized_value = normalize_text(value)
        if not normalized_value:
            return set()
        return set(COUNTRY_EQUIVALENTS.get(normalized_value, {normalized_value}))

    def _match_brief_location(self, value: str, options: list[str]) -> str:
        incoming_variants = self._location_variants(value)
        if not incoming_variants:
            return ""
        for option in options:
            option_variants = self._location_variants(option)
            if option_variants.intersection(incoming_variants):
                return option
        return ""

    def _supporting_keywords(self, hit: RawSearchHit, brief: SearchBrief) -> list[str]:
        normalized = normalize_text(f"{hit.title} {hit.snippet}")
        keywords = []
        for value in [*brief.required_keywords, *brief.preferred_keywords, *brief.industry_keywords]:
            if normalize_text(value) and normalize_text(value) in normalized:
                keywords.append(value)
        return keywords

    def extract(self, hit: RawSearchHit, brief: SearchBrief) -> EvidenceRecord | None:
        if not self._is_profile_url(hit.url):
            return None
        full_name = self._guess_name(hit)
        if not full_name:
            return None
        current_title = self._guess_title(hit, brief)
        if not current_title:
            return None
        current_company, company_confidence = self._guess_company(hit, brief, current_title)
        current_location = self._guess_location(hit, brief)
        normalized_hit = normalize_text(f"{hit.title} {hit.snippet}")
        role_family = infer_role_family(current_title, brief.role_title, *brief.titles)
        requested_family = infer_role_family(brief.role_title, *brief.titles)
        title_match = bool(current_title and role_family == requested_family)
        normalized_company = normalize_text(current_company)
        exact_company_targets = {normalize_text(value) for value in brief.company_targets if normalize_text(value)}
        peer_company_targets = {normalize_text(value) for value in brief.peer_company_targets if normalize_text(value)}
        company_match = _company_target_match(normalized_company, exact_company_targets)
        peer_company_match = _company_target_match(normalized_company, peer_company_targets)
        location_match = bool(current_location and normalize_text(current_location) in {normalize_text(value) for value in [*brief.cities, *brief.countries]})
        source_domain = urlparse(hit.url).netloc.lower().removeprefix("www.")
        current_role_signal = bool(
            title_match
            and (
                company_confidence >= 0.55
                or location_match
                or source_domain in TECHNICAL_SOURCES
                or source_domain in PROFESSIONAL_SOURCES
            )
        )
        title_confidence = 0.82 if title_match else 0.22
        if company_match:
            company_confidence = max(company_confidence, 0.92)
        explicit_location_match = bool(current_location and normalize_text(current_location) in normalized_hit)
        location_confidence = 0.72 if explicit_location_match else 0.44 if location_match else 0.0
        currentness_confidence = 0.74 if current_role_signal else 0.2
        freshness_confidence = 0.62
        confidence = (
            0.34 * title_confidence
            + 0.16 * company_confidence
            + 0.14 * location_confidence
            + 0.20 * currentness_confidence
            + 0.16 * freshness_confidence
        )
        if self._supporting_keywords(hit, brief):
            confidence += 0.08
        return EvidenceRecord(
            source_url=hit.url,
            source_domain=source_domain,
            source_type=hit.source,
            page_title=hit.title,
            page_snippet=hit.snippet,
            full_name=full_name,
            current_title=current_title,
            current_company=current_company,
            current_location=current_location,
            role_family=role_family,
            title_match=title_match,
            company_match=company_match,
            peer_company_match=peer_company_match,
            location_match=location_match,
            current_role_signal=current_role_signal,
            confidence=min(1.0, round(confidence, 2)),
            title_confidence=round(title_confidence, 2),
            company_confidence=round(company_confidence, 2),
            location_confidence=round(location_confidence, 2),
            currentness_confidence=round(currentness_confidence, 2),
            freshness_confidence=round(freshness_confidence, 2),
            supporting_keywords=self._supporting_keywords(hit, brief),
        )
