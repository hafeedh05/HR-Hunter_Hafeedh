from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List
from urllib.parse import urlparse

import httpx

from hr_hunter.briefing import normalize_text, unique_preserving_order
from hr_hunter.models import CandidateProfile, EvidenceRecord, SearchBrief, SearchRunReport
from hr_hunter.providers.scrapingbee import ScrapingBeeGoogleClient
from hr_hunter.scoring import sort_candidates, status_from_score


YEAR_PATTERN = re.compile(r"\b(19|20)\d{2}\b")
CURRENT_EMPLOYMENT_BLOCKERS = (
    "alumni",
    "before joining",
    "before he joined",
    "before she joined",
    "began his career",
    "began her career",
    "career at",
    "ex ",
    "former",
    "formerly",
    "left",
    "prior to joining",
    "prior to his",
    "prior to her",
    "previous role",
    "previously",
    "started his career",
    "started her career",
    "used to",
    "worked as",
    "worked at",
    "ex-",
    "retired",
    "past ",
)
PROFILE_PATH_HINTS = (
    "/in/",
    "github.com/",
    "gitlab.com/",
    "huggingface.co/",
    "kaggle.com/",
    "stackoverflow.com/users/",
    "medium.com/@",
    "dev.to/",
    "substack.com/",
    "/bio",
    "/speaker/",
    "/speakers/",
    "/profile",
    "/people/",
    "/person/",
    "/staff/",
    "/team/",
    "/our-team/",
    "/our-people/",
    "/leadership/",
    "/management/",
    "/about/team",
    "/org-chart/",
)
TRUSTED_PROFILE_DOMAINS = {
    "linkedin.com",
    "www.linkedin.com",
    "ae.linkedin.com",
    "de.linkedin.com",
    "kw.linkedin.com",
    "qa.linkedin.com",
    "sa.linkedin.com",
    "theorg.com",
    "github.com",
    "www.github.com",
    "huggingface.co",
    "www.huggingface.co",
    "kaggle.com",
    "www.kaggle.com",
    "stackoverflow.com",
    "www.stackoverflow.com",
    "gitlab.com",
    "www.gitlab.com",
    "dev.to",
    "medium.com",
    "substack.com",
}
COUNTRY_HOST_HINTS = {
    "ae.linkedin.com": "United Arab Emirates",
    "de.linkedin.com": "Germany",
    "ie.linkedin.com": "Ireland",
    "kw.linkedin.com": "Kuwait",
    "qa.linkedin.com": "Qatar",
    "sa.linkedin.com": "Saudi Arabia",
}
LOCATION_PROBE_PHRASES = (
    '"based in"',
    '"based out of"',
    '"located in"',
    '"works in"',
    '"county"',
)
LOCATION_CONFIRMATION_PHRASES = (
    "based in",
    "based out of",
    "located in",
    "works in",
    "working in",
    "lives in",
    "living in",
    "resident in",
)
LOCATION_EVENT_BLOCKERS = (
    "award",
    "awards",
    "conference",
    "event",
    "flagship",
    "forum",
    "guest",
    "launch",
    "opening",
    "panel",
    "remarks",
    "speaker",
    "speakers",
    "summit",
    "welcomed",
)
LOCATION_SOURCE_TERMS = (
    "appointed",
    "appointment",
    "joins",
    "promoted",
    "named",
    "speaker",
    "speakers",
    "bio",
    "biography",
    "team",
    "leadership",
    "directory",
    "member",
)
COMPANY_LOCATION_SOURCE_TERMS = (
    "office",
    "contact",
    "address",
    "our presence",
    "head office",
    "ireland",
)
EXECUTIVE_TITLE_HINTS = (
    "ceo",
    "chief",
    "president",
    "managing director",
    "vice president",
    "vp",
)
EXECUTIVE_SOURCE_TERMS = (
    "leadership",
    "management",
    "team",
    "biography",
    "profile",
    "appointed",
    "appointment",
)
TECHNICAL_ROLE_HINTS = (
    "ai",
    "artificial intelligence",
    "machine learning",
    "ml",
    "llm",
    "nlp",
    "engineer",
    "developer",
    "mlops",
    "data platform",
)
TECHNICAL_SOURCE_TERMS = (
    "github",
    "hugging face",
    "kaggle",
    "stack overflow",
    "open source",
    "repositories",
    "projects",
    "model serving",
    "rag",
    "llm",
)
TECHNICAL_SITE_TERMS = (
    "site:github.com",
    "site:huggingface.co",
    "site:kaggle.com",
    "site:stackoverflow.com",
    "site:gitlab.com",
    "site:dev.to",
    "site:medium.com",
)


def parse_year(value: str) -> int | None:
    match = YEAR_PATTERN.search(value)
    if not match:
        return None
    return int(match.group(0))


def title_tokens(candidate: CandidateProfile, brief: SearchBrief) -> List[str]:
    values = [
        candidate.current_title.split(" - ")[0].strip(),
        *brief.titles,
        *brief.title_keywords,
    ]
    return unique_preserving_order([value for value in values if value])


def company_aliases(candidate: CandidateProfile, brief: SearchBrief) -> List[str]:
    if candidate.current_company in brief.company_aliases:
        return brief.company_aliases[candidate.current_company]

    aliases = [candidate.current_company] if candidate.current_company else []
    for company, values in brief.company_aliases.items():
        if company == candidate.current_company:
            aliases.extend(values)
    return unique_preserving_order([alias for alias in aliases if alias])


def _is_title_market_priority_brief(brief: SearchBrief) -> bool:
    normalized_role_scope = " ".join(
        normalize_text(str(value))
        for value in [brief.role_title, *brief.titles]
        if normalize_text(str(value))
    )
    normalized_locations = {
        normalize_text(str(value))
        for value in [*brief.location_targets, brief.geography.location_name, brief.geography.country]
        if normalize_text(str(value))
    }
    return (
        not brief.company_targets
        and bool(brief.titles)
        and len(brief.titles) <= 3
        and len(normalized_locations) <= 4
        and not any(hint in normalized_role_scope for hint in EXECUTIVE_TITLE_HINTS)
    )


def _is_executive_brief(brief: SearchBrief) -> bool:
    normalized_role_scope = " ".join(
        normalize_text(str(value))
        for value in [brief.role_title, *brief.titles]
        if normalize_text(str(value))
    )
    return any(hint in normalized_role_scope for hint in EXECUTIVE_TITLE_HINTS)


def _is_technical_engineering_brief(brief: SearchBrief) -> bool:
    normalized_role_scope = " ".join(
        normalize_text(str(value))
        for value in [
            brief.role_title,
            *brief.titles,
            *brief.title_keywords,
            *brief.required_keywords,
            *brief.preferred_keywords,
            *brief.industry_keywords,
        ]
        if normalize_text(str(value))
    )
    return any(hint in normalized_role_scope for hint in TECHNICAL_ROLE_HINTS)


class PublicEvidenceVerifier:
    def __init__(self, settings: Dict[str, Any] | None = None):
        settings = settings or {}
        self.client = ScrapingBeeGoogleClient()
        self.country_code = settings.get("country_code", "ie")
        self.language = settings.get("language", "en")
        self.light_request = bool(settings.get("light_request", True))
        self.pages_per_query = int(settings.get("pages_per_query", 1))
        self.queries_per_candidate = int(settings.get("queries_per_candidate", 2))
        self.location_probe_queries = int(settings.get("location_probe_queries", 1))
        self.company_location_probe_queries = int(settings.get("company_location_probe_queries", 0))
        self.results_per_query = int(settings.get("results_per_query", 10))
        self.parallel_candidates = max(1, int(settings.get("parallel_candidates", 6) or 6))
        self.include_site_terms = unique_preserving_order(
            [str(value).strip() for value in settings.get("include_site_terms", []) if str(value).strip()]
        )
        self.exclude_site_terms = unique_preserving_order(
            [str(value).strip() for value in settings.get("exclude_site_terms", []) if str(value).strip()]
        )
        self.location_include_site_terms = unique_preserving_order(
            [
                str(value).strip()
                for value in settings.get("location_include_site_terms", self.include_site_terms)
                if str(value).strip()
            ]
        )
        self.location_source_terms = unique_preserving_order(
            [
                str(value).strip()
                for value in settings.get("location_source_terms", list(LOCATION_SOURCE_TERMS))
                if str(value).strip()
            ]
        )
        self.company_location_source_terms = unique_preserving_order(
            [
                str(value).strip()
                for value in settings.get("company_location_source_terms", list(COMPANY_LOCATION_SOURCE_TERMS))
                if str(value).strip()
            ]
        )

    @staticmethod
    def _combine_query_parts(*parts: str) -> str:
        return " ".join(part.strip() for part in parts if part and part.strip())

    @staticmethod
    def _format_query_terms(terms: Iterable[str]) -> str:
        formatted: List[str] = []
        for value in terms:
            stripped = str(value).strip()
            if not stripped:
                continue
            if stripped.startswith(("site:", "-site:", '"', "(")):
                formatted.append(stripped)
            else:
                formatted.append(f'"{stripped}"')
        return " OR ".join(formatted)

    def _site_filters(self, include_terms: Iterable[str] | None = None) -> str:
        include_values = [value for value in (include_terms or []) if str(value).strip()]
        exclude_values = [value for value in self.exclude_site_terms if str(value).strip()]
        include_clause = ""
        if include_values:
            include_clause = f"({' OR '.join(include_values)})"
        return self._combine_query_parts(include_clause, " ".join(exclude_values))

    @staticmethod
    def _name_tokens(value: str) -> List[str]:
        return [token for token in re.split(r"[^a-z0-9]+", normalize_text(value)) if len(token) >= 3]

    @classmethod
    def _title_or_snippet_has_name_anchor(cls, candidate: CandidateProfile, *values: str) -> bool:
        normalized_name = normalize_text(candidate.full_name)
        combined = normalize_text(" ".join(str(value or "") for value in values))
        if normalized_name and normalized_name in combined:
            return True
        tokens = cls._name_tokens(candidate.full_name)
        if len(tokens) < 2:
            return False
        matches = sum(1 for token in tokens if token in combined)
        return matches >= min(2, len(tokens))

    @classmethod
    def _url_path_has_name_anchor(cls, candidate: CandidateProfile, source_url: str) -> bool:
        path = normalize_text(urlparse(source_url).path.replace("/", " "))
        tokens = cls._name_tokens(candidate.full_name)
        if not tokens:
            return False
        matches = sum(1 for token in tokens if token in path)
        required = 1 if len(tokens) == 1 else min(2, len(tokens))
        return matches >= required

    @classmethod
    def _is_profile_like_source(
        cls,
        candidate: CandidateProfile,
        source_url: str,
        source_domain: str,
        title: str,
        snippet: str,
    ) -> bool:
        lowered_url = source_url.lower()
        lowered_domain = source_domain.lower()
        if not any(hint in lowered_url for hint in PROFILE_PATH_HINTS) and lowered_domain not in TRUSTED_PROFILE_DOMAINS:
            return False
        if "/in/" in lowered_url:
            return True
        if lowered_domain == "theorg.com":
            return cls._title_or_snippet_has_name_anchor(candidate, title, snippet) or cls._url_path_has_name_anchor(candidate, source_url)
        return (
            cls._title_or_snippet_has_name_anchor(candidate, title)
            or cls._url_path_has_name_anchor(candidate, source_url)
        )

    @staticmethod
    def _looks_like_past_role(text: str) -> bool:
        lowered = normalize_text(text)
        return any(marker in lowered for marker in CURRENT_EMPLOYMENT_BLOCKERS)

    @staticmethod
    def _record_text(record: EvidenceRecord) -> str:
        return " ".join([record.title, record.snippet, record.source_url])

    @staticmethod
    def _is_country_only_location(value: str, country: str) -> bool:
        normalized_value = normalize_text(value)
        normalized_country = normalize_text(country)
        return bool(normalized_value and normalized_country and normalized_value == normalized_country)

    def _location_is_imprecise(self, candidate: CandidateProfile, brief: SearchBrief) -> bool:
        bucket = getattr(candidate, "location_precision_bucket", "")
        if bucket in {"country_only", "unknown_location", "outside_target_area"}:
            return True
        if not candidate.location_name:
            return True
        return self._is_country_only_location(candidate.location_name, brief.geography.country)

    def _country_host_hint(self, source_domain: str, brief: SearchBrief) -> str:
        normalized_domain = str(source_domain or "").lower().removeprefix("www.")
        hinted_country = COUNTRY_HOST_HINTS.get(normalized_domain, "")
        if not hinted_country:
            return ""
        normalized_targets = {
            normalize_text(str(value))
            for value in [brief.geography.country, *brief.location_targets]
            if normalize_text(str(value))
        }
        return hinted_country if normalize_text(hinted_country) in normalized_targets else ""

    def _precise_location_required(self, brief: SearchBrief) -> bool:
        precise_targets = unique_preserving_order(
            [
                value
                for value in [brief.geography.location_name, *brief.geography.location_hints, *brief.location_targets]
                if value and not self._is_country_only_location(value, brief.geography.country)
            ]
        )
        if not precise_targets:
            return False
        if brief.strict_market_scope:
            return True
        if brief.geography.radius_miles and brief.geography.radius_miles <= 150:
            return True
        return len(precise_targets) <= 1

    def _supports_current_role(self, record: EvidenceRecord) -> bool:
        if not (record.name_match and record.company_match and record.title_matches):
            return False
        if self._looks_like_past_role(self._record_text(record)):
            return False
        return bool(
            record.current_employment_signal
            or (record.profile_signal and record.confidence >= 0.75)
        )

    def _supports_technical_role_signal(self, record: EvidenceRecord, current_year: int) -> bool:
        if not (record.name_match and record.title_matches and record.profile_signal):
            return False
        if self._looks_like_past_role(self._record_text(record)):
            return False
        if self._is_stale(record, current_year):
            return False
        return record.confidence >= 0.45

    def _supports_location_confirmation(
        self,
        record: EvidenceRecord,
        *,
        current_year: int,
        precise_required: bool,
    ) -> bool:
        if not record.location_match:
            return False
        if precise_required and not record.precise_location_match:
            return False
        if not self._supports_current_role(record):
            return False
        if self._is_stale(record, current_year) or record.confidence < 0.45:
            return False
        normalized_text = normalize_text(self._record_text(record))
        explicit_location_cue = any(phrase in normalized_text for phrase in LOCATION_CONFIRMATION_PHRASES)
        event_like_context = any(term in normalized_text for term in LOCATION_EVENT_BLOCKERS)
        if record.profile_signal:
            return True
        if explicit_location_cue:
            return True
        if event_like_context:
            return False
        return False

    @staticmethod
    def _is_stale(record: EvidenceRecord, current_year: int) -> bool:
        return bool(record.recency_year and record.recency_year < current_year - 2)

    def is_configured(self) -> bool:
        return self.client.is_configured()

    def _seed_record_can_short_circuit(
        self,
        candidate: CandidateProfile,
        brief: SearchBrief,
        record: EvidenceRecord,
    ) -> bool:
        if not (
            record.name_match
            and record.company_match
            and record.title_matches
            and not self._looks_like_past_role(self._record_text(record))
        ):
            return False
        if (brief.titles or brief.title_keywords) and not candidate.current_title_match:
            return False
        if not (record.current_employment_signal or record.profile_signal):
            return False
        if record.confidence < 0.75:
            return False
        has_location_scope = bool(brief.geography.location_name or brief.geography.country or brief.location_targets)
        if not has_location_scope:
            return True
        if self._location_is_imprecise(candidate, brief):
            return bool(record.precise_location_match or record.location_match)
        return bool(candidate.location_aligned or record.location_match)

    def build_queries(self, candidate: CandidateProfile, brief: SearchBrief) -> List[str]:
        name_term = f'"{candidate.full_name}"' if candidate.full_name else ""
        company_terms = " OR ".join(f'"{alias}"' for alias in company_aliases(candidate, brief)[:3])
        title_terms = " OR ".join(f'"{token}"' for token in title_tokens(candidate, brief)[:5])
        location_terms = " OR ".join(
            f'"{hint}"'
            for hint in unique_preserving_order(
                [candidate.location_name, brief.geography.location_name, *brief.geography.location_hints[:3]]
            )
            if hint
        )
        executive_source_terms = self._format_query_terms(EXECUTIVE_SOURCE_TERMS)
        executive_brief = _is_executive_brief(brief)
        technical_brief = _is_technical_engineering_brief(brief)

        queries = [
            " ".join(
                part
                for part in [
                    name_term,
                    f"({company_terms})" if company_terms else "",
                    f"({title_terms})" if title_terms else "",
                    f"({location_terms})" if location_terms else "",
                    self._site_filters(["-site:linkedin.com", "-site:ie.linkedin.com"]),
                ]
                if part
            ),
            " ".join(
                part
                for part in [
                    name_term,
                    f"({title_terms})" if title_terms else "",
                    f"({company_terms})" if company_terms else "",
                    self._site_filters(["-site:linkedin.com", "-site:ie.linkedin.com"]),
                ]
                if part
            ),
        ]
        if executive_brief:
            queries.insert(
                1,
                " ".join(
                    part
                    for part in [
                        name_term,
                        f"({company_terms})" if company_terms else "",
                        f"({title_terms})" if title_terms else "",
                        f"({executive_source_terms})" if executive_source_terms else "",
                        self._site_filters(["-site:linkedin.com", "-site:ie.linkedin.com"]),
                    ]
                    if part
                ),
            )
        else:
            queries.insert(
                1,
                " ".join(
                    part
                    for part in [
                        name_term,
                        f"({company_terms})" if company_terms else "",
                        f'("{brief.geography.country}")' if brief.geography.country else "",
                        self._site_filters(["-site:linkedin.com", "-site:ie.linkedin.com"]),
                    ]
                    if part
                ),
            )
        if technical_brief:
            technical_source_terms = self._format_query_terms(TECHNICAL_SOURCE_TERMS)
            technical_site_filters = self._site_filters(
                [*TECHNICAL_SITE_TERMS, "-site:linkedin.com", "-site:ie.linkedin.com"]
            )
            queries.insert(
                1,
                " ".join(
                    part
                    for part in [
                        name_term,
                        f"({title_terms})" if title_terms else "",
                        f"({company_terms})" if company_terms else "",
                        f"({technical_source_terms})" if technical_source_terms else "",
                        technical_site_filters,
                    ]
                    if part
                ),
            )
        effective_query_limit = max(
            self.queries_per_candidate,
            3 if technical_brief or executive_brief else self.queries_per_candidate,
        )
        queries = unique_preserving_order([query for query in queries if query])[:effective_query_limit]

        if self.location_probe_queries > 0 and self._location_is_imprecise(candidate, brief):
            precise_hints = [
                hint
                for hint in unique_preserving_order(
                    [brief.geography.location_name, *brief.geography.location_hints[:6]]
                )
                if hint and not self._is_country_only_location(hint, brief.geography.country)
            ]
            precise_location_terms = " OR ".join(f'"{hint}"' for hint in precise_hints)
            probe_phrases = " OR ".join(LOCATION_PROBE_PHRASES)
            location_probe = " ".join(
                part
                for part in [
                    name_term,
                    f"({company_terms})" if company_terms else "",
                    f"({title_terms})" if title_terms else "",
                    f"({precise_location_terms})" if precise_location_terms else "",
                    f"({probe_phrases})" if probe_phrases else "",
                    self._site_filters(["-site:linkedin.com", "-site:ie.linkedin.com"]),
                ]
                if part
            )
            targeted_location_probe = self._combine_query_parts(
                name_term,
                f"({company_terms})" if company_terms else "",
                f"({title_terms})" if title_terms else "",
                f"({precise_location_terms})" if precise_location_terms else "",
                f"({probe_phrases})" if probe_phrases else "",
                (
                    f"({self._format_query_terms(self.location_source_terms)})"
                    if self.location_source_terms
                    else ""
                ),
                self._site_filters([*self.location_include_site_terms, "-site:linkedin.com", "-site:ie.linkedin.com"]),
            )
            probes = [query for query in [location_probe, targeted_location_probe] if query]
            if probes:
                queries.extend(unique_preserving_order(probes)[: self.location_probe_queries])

        return unique_preserving_order([query for query in queries if query])

    def build_company_location_queries(self, candidate: CandidateProfile, brief: SearchBrief) -> List[str]:
        if self.company_location_probe_queries <= 0 or not candidate.current_company:
            return []
        company_terms = " OR ".join(f'"{alias}"' for alias in company_aliases(candidate, brief)[:3])
        precise_hints = [
            hint
            for hint in unique_preserving_order([brief.geography.location_name, *brief.geography.location_hints[:6]])
            if hint and not self._is_country_only_location(hint, brief.geography.country)
        ]
        precise_location_terms = " OR ".join(f'"{hint}"' for hint in precise_hints)
        source_terms = self._format_query_terms(self.company_location_source_terms)
        queries = [
            self._combine_query_parts(
                f"({company_terms})" if company_terms else "",
                f"({precise_location_terms})" if precise_location_terms else "",
                f"({source_terms})" if source_terms else "",
                self._site_filters(self.location_include_site_terms),
            ),
            self._combine_query_parts(
                f"({company_terms})" if company_terms else "",
                f'"{brief.geography.country}"' if brief.geography.country else "",
                f"({source_terms})" if source_terms else "",
                self._site_filters(self.location_include_site_terms),
            ),
        ]
        return unique_preserving_order([query for query in queries if query])[: self.company_location_probe_queries]

    def build_record(
        self,
        candidate: CandidateProfile,
        brief: SearchBrief,
        query: str,
        result: Dict[str, Any],
    ) -> EvidenceRecord:
        title = result.get("title", "") or ""
        snippet = result.get("description") or result.get("snippet") or ""
        source_url = result.get("url") or result.get("link") or ""
        source_domain = (urlparse(source_url).netloc or result.get("domain") or "").lower()
        combined = " ".join([title, snippet, source_url])
        normalized = normalize_text(combined)
        normalized_name = normalize_text(candidate.full_name)
        profile_signal = self._is_profile_like_source(candidate, source_url, source_domain, title, snippet)

        matched_company = ""
        for alias in company_aliases(candidate, brief):
            normalized_alias = normalize_text(alias)
            if normalized_alias and normalized_alias in normalized:
                matched_company = candidate.current_company or alias
                break

        matched_titles: List[str] = []
        for token in title_tokens(candidate, brief):
            normalized_token = normalize_text(token)
            if normalized_token and normalized_token in normalized:
                matched_titles.append(token)

        location_match = False
        location_match_text = ""
        precise_location_match = False
        precise_hints = [
            hint
            for hint in unique_preserving_order(
                [brief.geography.location_name, *brief.geography.location_hints]
            )
            if hint and not self._is_country_only_location(hint, brief.geography.country)
        ]
        fallback_hints = [
            hint
            for hint in unique_preserving_order([candidate.location_name, brief.geography.country])
            if hint and self._is_country_only_location(hint, brief.geography.country)
        ]
        for hint in [*precise_hints, *fallback_hints]:
            normalized_hint = normalize_text(hint)
            if normalized_hint and normalized_hint in normalized:
                location_match = True
                location_match_text = hint
                precise_location_match = not self._is_country_only_location(hint, brief.geography.country)
                break

        if not location_match and profile_signal:
            host_country_hint = self._country_host_hint(source_domain, brief)
            if host_country_hint and not self._looks_like_past_role(combined):
                location_match = True
                location_match_text = host_country_hint
                precise_location_match = False
        current_employment_signal = bool(
            matched_company
            and matched_titles
            and profile_signal
            and not self._looks_like_past_role(combined)
        )
        recency_year = parse_year(combined)
        confidence = 0.0
        if normalized_name and normalized_name in normalized:
            confidence += 0.35
        if matched_company:
            confidence += 0.25
        if matched_titles:
            confidence += min(0.2, 0.1 * len(matched_titles))
        if location_match:
            confidence += 0.1
        if source_domain and "linkedin.com" not in source_domain:
            confidence += 0.1
        if current_employment_signal:
            confidence += 0.1

        return EvidenceRecord(
            query=query,
            source_url=source_url,
            source_domain=source_domain,
            title=title,
            snippet=snippet,
            name_match=bool(normalized_name and normalized_name in normalized),
            company_match=matched_company,
            title_matches=unique_preserving_order(matched_titles),
            location_match=location_match,
            location_match_text=location_match_text,
            precise_location_match=precise_location_match,
            profile_signal=profile_signal,
            current_employment_signal=current_employment_signal,
            recency_year=recency_year,
            confidence=round(min(confidence, 1.0), 2),
            raw=result,
        )

    def build_company_location_record(
        self,
        candidate: CandidateProfile,
        brief: SearchBrief,
        query: str,
        result: Dict[str, Any],
    ) -> EvidenceRecord:
        title = result.get("title", "") or ""
        snippet = result.get("description") or result.get("snippet") or ""
        source_url = result.get("url") or result.get("link") or ""
        source_domain = (urlparse(source_url).netloc or result.get("domain") or "").lower()
        combined = " ".join([title, snippet, source_url])
        normalized = normalize_text(combined)

        matched_company = ""
        for alias in company_aliases(candidate, brief):
            normalized_alias = normalize_text(alias)
            if normalized_alias and normalized_alias in normalized:
                matched_company = candidate.current_company or alias
                break

        location_match = False
        location_match_text = ""
        precise_location_match = False
        precise_hints = [
            hint
            for hint in unique_preserving_order([brief.geography.location_name, *brief.geography.location_hints])
            if hint and not self._is_country_only_location(hint, brief.geography.country)
        ]
        fallback_hints = [
            hint
            for hint in unique_preserving_order([brief.geography.country])
            if hint and self._is_country_only_location(hint, brief.geography.country)
        ]
        for hint in [*precise_hints, *fallback_hints]:
            normalized_hint = normalize_text(hint)
            if normalized_hint and normalized_hint in normalized:
                location_match = True
                location_match_text = hint
                precise_location_match = not self._is_country_only_location(hint, brief.geography.country)
                break

        confidence = 0.0
        if matched_company:
            confidence += 0.35
        if precise_location_match:
            confidence += 0.35
        elif location_match:
            confidence += 0.15
        if source_domain and "linkedin.com" not in source_domain:
            confidence += 0.1
        if any(term in normalized for term in ("office", "contact", "address", "our presence", "head office")):
            confidence += 0.1

        return EvidenceRecord(
            query=query,
            source_url=source_url,
            source_domain=source_domain,
            title=title,
            snippet=snippet,
            source_type="company_location",
            name_match=False,
            company_match=matched_company,
            title_matches=[],
            location_match=location_match,
            location_match_text=location_match_text,
            precise_location_match=precise_location_match,
            profile_signal=False,
            current_employment_signal=False,
            recency_year=parse_year(combined),
            confidence=round(min(confidence, 1.0), 2),
            raw=result,
        )

    async def collect_evidence(
        self,
        candidate: CandidateProfile,
        brief: SearchBrief,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> tuple[List[EvidenceRecord], int]:
        if not self.is_configured():
            return [], 0

        evidence: Dict[str, EvidenceRecord] = {}
        request_count = 0

        if candidate.raw:
            seed_record = self.build_record(candidate, brief, "source_profile", candidate.raw)
            if seed_record.name_match or seed_record.company_match or seed_record.title_matches:
                seed_key = seed_record.source_url or f"{seed_record.source_domain}:{seed_record.title}"
                evidence[seed_key] = seed_record
                if self._seed_record_can_short_circuit(candidate, brief, seed_record):
                    return [seed_record], 0

        queries = self.build_queries(candidate, brief)
        location_queries = self.build_company_location_queries(candidate, brief) if self._location_is_imprecise(candidate, brief) else []
        search_specs = [
            ("profile", query, page)
            for query in queries
            for page in range(1, self.pages_per_query + 1)
        ]
        search_specs.extend(
            ("company_location", query, page)
            for query in location_queries
            for page in range(1, self.pages_per_query + 1)
        )

        async def run_search(kind: str, query: str, page: int) -> tuple[str, str, httpx.Response | None]:
            try:
                response = await active_client.search(
                    active_http_client,
                    query,
                    page=page,
                    country_code=self.country_code,
                    language=self.language,
                    light_request=self.light_request,
                )
            except httpx.HTTPError:
                return kind, query, None
            return kind, query, response

        active_client = self.client
        active_http_client = client
        owns_client = active_http_client is None
        if owns_client:
            limits = httpx.Limits(
                max_keepalive_connections=max(10, self.parallel_candidates * 2),
                max_connections=max(20, self.parallel_candidates * 4),
            )
            active_http_client = httpx.AsyncClient(timeout=30.0, limits=limits)

        try:
            results = await asyncio.gather(*(run_search(kind, query, page) for kind, query, page in search_specs))
            for kind, query, response in results:
                if response is None:
                    continue
                request_count += 1
                if response.status_code >= 400:
                    continue
                payload = response.json()
                for result in payload.get("organic_results", [])[: self.results_per_query]:
                    if kind == "company_location":
                        record = self.build_company_location_record(candidate, brief, query, result)
                        if not (record.company_match and record.location_match):
                            continue
                    else:
                        record = self.build_record(candidate, brief, query, result)
                        if not (record.name_match or record.company_match or record.title_matches):
                            continue
                    key = record.source_url or f"{record.source_domain}:{record.title}"
                    existing = evidence.get(key)
                    if existing is None or record.confidence > existing.confidence:
                        evidence[key] = record
        finally:
            if owns_client and active_http_client is not None:
                await active_http_client.aclose()
        ordered = sorted(
            evidence.values(),
            key=lambda record: (
                -record.confidence,
                record.source_domain,
                record.source_url,
            ),
        )
        return ordered, request_count

    def apply_evidence(
        self,
        candidate: CandidateProfile,
        brief: SearchBrief,
        evidence_records: List[EvidenceRecord],
    ) -> CandidateProfile:
        candidate.evidence_records = evidence_records
        candidate.last_verified_at = datetime.now(timezone.utc).isoformat()
        if not evidence_records:
            candidate.evidence_confidence = 0.0
            candidate.evidence_verdict = "missing"
            candidate.current_company_confirmed = False
            candidate.current_title_confirmed = False
            candidate.current_location_confirmed = candidate.location_aligned
            candidate.current_employment_confirmed = False
            candidate.verification_notes.append("no public corroboration found")
            candidate.verification_notes.append("source_quality: missing (no public evidence)")
            candidate.verification_notes.append("evidence_freshness: missing (no current-role evidence)")
            candidate.verification_status = "review" if candidate.score >= 50.0 else "reject"
            setattr(candidate, "source_quality_score", 0.0)
            setattr(candidate, "evidence_freshness_year", None)
            setattr(candidate, "current_role_proof_count", 0)
            setattr(candidate, "cap_reasons", ["missing_public_evidence"])
            return candidate

        current_year = datetime.now(timezone.utc).year
        technical_brief = _is_technical_engineering_brief(brief)
        strong_records = [record for record in evidence_records if record.confidence >= 0.65]
        non_linkedin_domains = {
            record.source_domain for record in evidence_records if record.source_domain and "linkedin.com" not in record.source_domain
        }
        corroborated_records = [
            record
            for record in evidence_records
            if record.name_match and record.company_match and (record.title_matches or record.location_match)
        ]
        current_role_records = [
            record
            for record in evidence_records
            if self._supports_current_role(record)
        ]
        technical_soft_role_records = [
            record
            for record in evidence_records
            if technical_brief and self._supports_technical_role_signal(record, current_year)
        ]
        fresh_current_role_records = [
            record for record in current_role_records if not self._is_stale(record, current_year)
        ]
        stale_current_role_records = [
            record for record in current_role_records if self._is_stale(record, current_year)
        ]
        historical_only_records = [
            record
            for record in evidence_records
            if record.name_match
            and record.company_match
            and record.title_matches
            and not self._supports_current_role(record)
        ]
        current_role_years = [record.recency_year for record in current_role_records if record.recency_year]
        latest_year = max(current_role_years, default=0)
        stale_data_risk = bool(stale_current_role_records and not fresh_current_role_records)

        top_confidences = [record.confidence for record in evidence_records[:3]]
        evidence_confidence = sum(top_confidences) / max(len(top_confidences), 1)
        if corroborated_records:
            evidence_confidence += 0.1
        if non_linkedin_domains:
            evidence_confidence += 0.1
        if stale_data_risk:
            evidence_confidence -= 0.1
        evidence_confidence = round(min(max(evidence_confidence, 0.0), 1.0), 2)

        source_quality_score = 0.0
        if fresh_current_role_records:
            source_quality_score += 0.5
        elif strong_records:
            source_quality_score += 0.3
        if non_linkedin_domains:
            source_quality_score += 0.25
        if any(record.profile_signal for record in evidence_records):
            source_quality_score += 0.15
        if stale_data_risk:
            source_quality_score -= 0.1
        source_quality_score = round(min(max(source_quality_score, 0.0), 1.0), 2)

        candidate.evidence_confidence = evidence_confidence
        candidate.stale_data_risk = stale_data_risk
        candidate.current_company_confirmed = any(
            record.name_match
            and record.company_match
            and self._supports_current_role(record)
            and not self._is_stale(record, current_year)
            and record.confidence >= 0.55
            for record in evidence_records
        )
        candidate.current_title_confirmed = any(
            record.name_match
            and record.title_matches
            and self._supports_current_role(record)
            and not self._is_stale(record, current_year)
            and record.confidence >= 0.55
            for record in evidence_records
        )
        if technical_soft_role_records:
            candidate.current_title_confirmed = True
        candidate.current_location_confirmed = candidate.location_aligned or any(
            self._supports_location_confirmation(
                record,
                current_year=current_year,
                precise_required=False,
            )
            for record in evidence_records
        )
        candidate.precise_location_confirmed = (
            candidate.location_precision_bucket
            not in {"country_only", "unknown_location", "outside_target_area", ""}
            or any(
                self._supports_location_confirmation(
                    record,
                    current_year=current_year,
                    precise_required=True,
                )
                for record in evidence_records
            )
        )
        candidate.current_employment_confirmed = bool(fresh_current_role_records)
        setattr(candidate, "source_quality_score", source_quality_score)
        setattr(candidate, "evidence_freshness_year", latest_year or None)
        setattr(candidate, "current_role_proof_count", len(fresh_current_role_records))
        setattr(candidate, "technical_role_signal_count", len(technical_soft_role_records))

        precise_location_records = [
            record
            for record in evidence_records
            if self._supports_location_confirmation(
                record,
                current_year=current_year,
                precise_required=True,
            )
            and record.source_type != "company_location"
        ]
        company_location_records = [
            record
            for record in evidence_records
            if record.source_type == "company_location"
            and record.company_match
            and record.location_match
            and record.confidence >= 0.45
        ]
        if precise_location_records:
            precise_location_text = precise_location_records[0].location_match_text
            if precise_location_text:
                if self._is_country_only_location(candidate.location_name, brief.geography.country) or not candidate.location_name:
                    if (
                        brief.geography.country
                        and normalize_text(brief.geography.country) not in normalize_text(precise_location_text)
                    ):
                        candidate.location_name = f"{precise_location_text}, {brief.geography.country}"
                    else:
                        candidate.location_name = precise_location_text
                if candidate.location_precision_bucket in {"country_only", "unknown_location", "outside_target_area", ""}:
                    candidate.location_precision_bucket = "named_target_location"
                candidate.location_aligned = True
        elif (
            company_location_records
            and candidate.current_company_confirmed
            and candidate.location_precision_bucket != "outside_target_area"
            and (
                candidate.current_location_confirmed
                or getattr(candidate, "location_precision_bucket", "") == "country_only"
            )
        ):
            precise_company_location = next(
                (record for record in company_location_records if record.precise_location_match),
                None,
            )
            if precise_company_location and precise_company_location.location_match_text:
                precise_location_text = precise_company_location.location_match_text
                if self._is_country_only_location(candidate.location_name, brief.geography.country) or not candidate.location_name:
                    if (
                        brief.geography.country
                        and normalize_text(brief.geography.country) not in normalize_text(precise_location_text)
                    ):
                        candidate.location_name = f"{precise_location_text}, {brief.geography.country}"
                    else:
                        candidate.location_name = precise_location_text
                if candidate.location_precision_bucket in {"country_only", "unknown_location", ""}:
                    candidate.location_precision_bucket = "named_target_location"
                candidate.location_aligned = True
                candidate.current_location_confirmed = True
                candidate.precise_location_confirmed = True
                candidate.verification_notes.append(
                    "precise location inferred from company office/contact evidence"
                )

        if len(fresh_current_role_records) >= 2:
            candidate.evidence_verdict = "corroborated"
            candidate.verification_notes.append(
                f"{len(fresh_current_role_records)} public sources corroborate the current role"
            )
        elif fresh_current_role_records or strong_records:
            candidate.evidence_verdict = "supported"
            candidate.verification_notes.append(
                f"{max(len(fresh_current_role_records), len(strong_records))} public sources materially support the match"
            )
        else:
            candidate.evidence_verdict = "weak"
            candidate.verification_notes.append("public evidence exists but remains weak")

        if fresh_current_role_records and len(fresh_current_role_records) >= 2:
            candidate.verification_notes.append("source_quality: strong (multiple fresh current-role sources)")
        elif fresh_current_role_records:
            candidate.verification_notes.append("source_quality: moderate (single fresh current-role source)")
        elif strong_records:
            candidate.verification_notes.append("source_quality: weak (supportive sources but no fresh current-role proof)")
        else:
            candidate.verification_notes.append("source_quality: weak (no reliable current-role source)")

        if non_linkedin_domains:
            candidate.verification_notes.append(
                f"{len(non_linkedin_domains)} non-LinkedIn domains corroborate the profile"
            )
        if company_location_records:
            candidate.verification_notes.append(
                f"{len(company_location_records)} company office/contact sources support target-market locality"
            )
        if technical_soft_role_records and not fresh_current_role_records:
            candidate.verification_notes.append(
                f"{len(technical_soft_role_records)} technical profile sources corroborate the current title even though current-company proof is still incomplete"
            )

        if stale_data_risk:
            candidate.verification_notes.append(
                f"evidence_freshness: stale (latest current-role mention {latest_year})"
            )
            candidate.verification_notes.append("public evidence looks stale")
            candidate.score = round(max(0.0, candidate.score - 6.0), 2)
        elif latest_year:
            candidate.verification_notes.append(
                f"evidence_freshness: fresh (latest current-role mention {latest_year})"
            )
        elif fresh_current_role_records:
            candidate.verification_notes.append("evidence_freshness: current (no stale year markers found)")
        else:
            candidate.verification_notes.append("evidence_freshness: unknown (no fresh current-role proof)")

        if candidate.evidence_verdict == "corroborated":
            candidate.score = round(min(100.0, candidate.score + 8.0), 2)
        elif candidate.evidence_verdict == "supported":
            candidate.score = round(min(100.0, candidate.score + 4.0), 2)

        if candidate.current_company_confirmed:
            candidate.verification_notes.append("current company publicly corroborated")
        if candidate.current_title_confirmed:
            candidate.verification_notes.append("current title publicly corroborated")
        if candidate.current_location_confirmed:
            candidate.verification_notes.append("location signal corroborated")
        if candidate.precise_location_confirmed:
            candidate.verification_notes.append("precise location corroborated")
        technical_review_ready = (
            technical_brief
            and candidate.current_title_match
            and candidate.current_location_confirmed
            and candidate.current_function_fit >= 0.85
            and candidate.parser_confidence >= 0.65
            and (
                candidate.current_employment_confirmed
                or bool(technical_soft_role_records)
            )
        )
        if not candidate.current_employment_confirmed:
            if stale_data_risk:
                candidate.verification_notes.append("current role proof is stale and cannot verify current employment")
            elif historical_only_records:
                candidate.verification_notes.append("current role proof appears historical rather than current")
            elif technical_review_ready:
                candidate.verification_notes.append(
                    "current title is well supported, but current-company proof is still incomplete"
                )
            else:
                candidate.verification_notes.append("current role not yet publicly confirmed")
            penalty = 3.0 if technical_review_ready else 10.0
            candidate.score = round(max(0.0, candidate.score - penalty), 2)
        if historical_only_records and not fresh_current_role_records:
            candidate.verification_notes.append("public evidence appears historical rather than current")

        relaxed_industry_verification = (
            _is_title_market_priority_brief(brief)
            and candidate.current_employment_confirmed
            and candidate.current_company_confirmed
            and candidate.current_title_confirmed
            and candidate.current_title_match
            and candidate.current_location_confirmed
            and (candidate.precise_location_confirmed or not self._precise_location_required(brief))
        )
        precise_location_required = self._precise_location_required(brief)

        hard_verified = (
            candidate.current_employment_confirmed
            and candidate.current_company_confirmed
            and candidate.current_title_confirmed
            and candidate.current_location_confirmed
            and (candidate.precise_location_confirmed or not precise_location_required)
            and (not brief.company_targets or candidate.current_target_company_match)
            and (not (brief.titles or brief.title_keywords) or candidate.current_title_match)
            and (
                not brief.industry_keywords
                or candidate.industry_aligned
                or relaxed_industry_verification
            )
        )
        if hard_verified:
            candidate.score = round(max(candidate.score, 72.0), 2)
            candidate.verification_notes.append("hard verification gate satisfied")
            if relaxed_industry_verification and brief.industry_keywords and not candidate.industry_aligned:
                candidate.verification_notes.append(
                    "industry fit treated as supportive rather than mandatory because title, market, and current-role evidence are strong"
                )
        elif technical_review_ready:
            technical_review_floor = 55.0 if (
                candidate.current_company_confirmed
                or getattr(candidate, "current_role_proof_count", 0) >= 1
            ) else 50.0
            if candidate.score < technical_review_floor:
                candidate.score = round(technical_review_floor, 2)
                candidate.verification_notes.append(
                    "technical role evidence is strong enough for review despite incomplete employer confirmation"
                )

        cap_reasons = []
        if stale_data_risk:
            cap_reasons.append("stale_public_evidence")
        if historical_only_records and not fresh_current_role_records:
            cap_reasons.append("historical_only_public_evidence")
        if not candidate.current_employment_confirmed:
            cap_reasons.append(
                "missing_current_company_confirmation" if technical_review_ready else "missing_current_role_proof"
            )

        candidate.verification_status = status_from_score(candidate.score)
        if candidate.verification_status == "verified":
            missing = []
            if brief.company_targets and not (
                candidate.current_target_company_match and candidate.current_company_confirmed
            ):
                missing.append("current target company")
            if (brief.titles or brief.title_keywords) and not (
                candidate.current_title_match and candidate.current_title_confirmed
            ):
                missing.append("current title")
            if (brief.geography.location_name or brief.geography.country) and not candidate.current_location_confirmed:
                missing.append("current location")
            if precise_location_required and (
                not candidate.precise_location_confirmed
                or getattr(candidate, "location_precision_bucket", "") in {
                    "country_only",
                    "unknown_location",
                    "outside_target_area",
                }
            ):
                missing.append("precise location")
            if brief.industry_keywords and not candidate.industry_aligned:
                missing.append("industry fit")
            if not candidate.current_employment_confirmed:
                missing.append("current role proof")
            if missing:
                candidate.verification_status = "review"
                candidate.verification_notes.append(
                    f"status capped pending {'/'.join(missing)}"
                )
                cap_reasons.extend(missing)
        elif (
            candidate.verification_status == "review"
            and not candidate.current_employment_confirmed
            and candidate.score < 60.0
            and not technical_review_ready
        ):
            candidate.verification_status = "reject"

        setattr(candidate, "cap_reasons", unique_preserving_order([str(reason) for reason in cap_reasons]))
        candidate.verification_notes = unique_preserving_order(candidate.verification_notes)
        return candidate

    async def verify_candidates(
        self,
        candidates: List[CandidateProfile],
        brief: SearchBrief,
        limit: int,
        progress_callback: Callable[[Dict[str, Any]], None] | None = None,
    ) -> Dict[str, Any]:
        request_count = 0
        verified_count = 0
        reviewed_count = 0
        rejected_count = 0
        checked_verified_count = 0
        checked_review_count = 0
        checked_reject_count = 0
        total = min(max(0, int(limit or 0)), len(candidates))
        if total <= 0:
            return {
                "candidates_checked": 0,
                "requests_used": 0,
                "promoted_to_verified": 0,
                "promoted_to_review": 0,
                "verified_count": 0,
                "review_count": 0,
                "reject_count": 0,
                "verifying_count": 0,
            }

        checked_count = 0
        semaphore = asyncio.Semaphore(self.parallel_candidates)
        progress_lock = asyncio.Lock()
        limits = httpx.Limits(
            max_keepalive_connections=max(20, self.parallel_candidates * 4),
            max_connections=max(40, self.parallel_candidates * max(4, self.queries_per_candidate * 4)),
        )

        async def emit_progress() -> None:
            if not progress_callback:
                return
            try:
                progress_callback(
                    {
                        "candidates_checked": checked_count,
                        "candidates_total": total,
                        "requests_used": request_count,
                        "promoted_to_verified": verified_count,
                        "promoted_to_review": reviewed_count,
                        "verified_count": checked_verified_count,
                        "review_count": checked_review_count,
                        "reject_count": checked_reject_count,
                        "verifying_count": max(0, total - checked_count),
                    }
                )
            except Exception:
                return

        async def verify_one(candidate: CandidateProfile) -> None:
            nonlocal checked_count, request_count, verified_count, reviewed_count, rejected_count
            nonlocal checked_verified_count, checked_review_count, checked_reject_count
            before_status = candidate.verification_status
            evidence_records: List[EvidenceRecord] = []
            used_requests = 0
            async with semaphore:
                try:
                    evidence_records, used_requests = await self.collect_evidence(
                        candidate,
                        brief,
                        client=shared_client,
                    )
                except Exception:
                    evidence_records, used_requests = [], 0
                self.apply_evidence(candidate, brief, evidence_records)

            async with progress_lock:
                checked_count += 1
                request_count += used_requests
                if candidate.verification_status == "verified" and before_status != "verified":
                    verified_count += 1
                if candidate.verification_status == "review" and before_status == "reject":
                    reviewed_count += 1
                if candidate.verification_status == "reject":
                    rejected_count += 1
                if candidate.verification_status == "verified":
                    checked_verified_count += 1
                elif candidate.verification_status == "review":
                    checked_review_count += 1
                else:
                    checked_reject_count += 1
                await emit_progress()

        await emit_progress()
        async with httpx.AsyncClient(timeout=30.0, limits=limits) as shared_client:
            await asyncio.gather(*(verify_one(candidate) for candidate in candidates[:total]))
        return {
            "candidates_checked": total,
            "requests_used": request_count,
            "promoted_to_verified": verified_count,
            "promoted_to_review": reviewed_count,
            "verified_count": len([candidate for candidate in candidates[:total] if candidate.verification_status == "verified"]),
            "review_count": len([candidate for candidate in candidates[:total] if candidate.verification_status == "review"]),
            "reject_count": len([candidate for candidate in candidates[:total] if candidate.verification_status == "reject"]),
            "verifying_count": 0,
        }


def refresh_report_summary(
    report: SearchRunReport,
    verification_stats: Dict[str, Any] | None = None,
    *,
    brief: SearchBrief | None = None,
) -> None:
    verified = len([candidate for candidate in report.candidates if candidate.verification_status == "verified"])
    review = len([candidate for candidate in report.candidates if candidate.verification_status == "review"])
    rejected = len([candidate for candidate in report.candidates if candidate.verification_status == "reject"])
    corroborated = len(
        [candidate for candidate in report.candidates if candidate.evidence_verdict == "corroborated"]
    )
    supported = len(
        [candidate for candidate in report.candidates if candidate.evidence_verdict == "supported"]
    )

    report.candidates = sort_candidates(report.candidates, brief)
    report.summary.update(
        {
            "candidate_count": len(report.candidates),
            "verified_count": verified,
            "review_count": review,
            "reject_count": rejected,
            "evidence_corroborated_count": corroborated,
            "evidence_supported_count": supported,
        }
    )
    if verification_stats:
        report.summary["verification_stats"] = verification_stats
