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
LOCATION_PROBE_PHRASES = (
    '"based in"',
    '"based out of"',
    '"located in"',
    '"works in"',
    '"county"',
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
    def _is_profile_like_source(source_url: str, source_domain: str) -> bool:
        lowered_url = source_url.lower()
        lowered_domain = source_domain.lower()
        if any(hint in lowered_url for hint in PROFILE_PATH_HINTS):
            return True
        return lowered_domain in {"theorg.com"}

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

    def _supports_current_role(self, record: EvidenceRecord) -> bool:
        if not (record.name_match and record.company_match and record.title_matches):
            return False
        if self._looks_like_past_role(self._record_text(record)):
            return False
        return bool(
            record.current_employment_signal
            or (record.profile_signal and record.confidence >= 0.75)
        )

    @staticmethod
    def _is_stale(record: EvidenceRecord, current_year: int) -> bool:
        return bool(record.recency_year and record.recency_year < current_year - 2)

    def is_configured(self) -> bool:
        return self.client.is_configured()

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
                    f"({company_terms})" if company_terms else "",
                    f'("{brief.geography.country}")' if brief.geography.country else "",
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

        queries = unique_preserving_order([query for query in queries if query])[: self.queries_per_candidate]

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
                [brief.geography.location_name, *brief.geography.location_hints, candidate.location_name]
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

        profile_signal = self._is_profile_like_source(source_url, source_domain)
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

        queries = self.build_queries(candidate, brief)
        async with httpx.AsyncClient(timeout=30.0) as client:
            for query in queries:
                for page in range(1, self.pages_per_query + 1):
                    try:
                        response = await self.client.search(
                            client,
                            query,
                            page=page,
                            country_code=self.country_code,
                            language=self.language,
                            light_request=self.light_request,
                        )
                        request_count += 1
                    except httpx.HTTPError:
                        continue
                    if response.status_code >= 400:
                        continue
                    payload = response.json()
                    for result in payload.get("organic_results", [])[: self.results_per_query]:
                        record = self.build_record(candidate, brief, query, result)
                        if not (record.name_match or record.company_match or record.title_matches):
                            continue
                        key = record.source_url or f"{record.source_domain}:{record.title}"
                        existing = evidence.get(key)
                        if existing is None or record.confidence > existing.confidence:
                            evidence[key] = record
            if self._location_is_imprecise(candidate, brief):
                for query in self.build_company_location_queries(candidate, brief):
                    for page in range(1, self.pages_per_query + 1):
                        try:
                            response = await self.client.search(
                                client,
                                query,
                                page=page,
                                country_code=self.country_code,
                                language=self.language,
                                light_request=self.light_request,
                            )
                            request_count += 1
                        except httpx.HTTPError:
                            continue
                        if response.status_code >= 400:
                            continue
                        payload = response.json()
                        for result in payload.get("organic_results", [])[: self.results_per_query]:
                            record = self.build_company_location_record(candidate, brief, query, result)
                            if not (record.company_match and record.location_match):
                                continue
                            key = record.source_url or f"{record.source_domain}:{record.title}"
                            existing = evidence.get(key)
                            if existing is None or record.confidence > existing.confidence:
                                evidence[key] = record
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
        candidate.current_location_confirmed = candidate.location_aligned or any(
            record.location_match
            and self._supports_current_role(record)
            and not self._is_stale(record, current_year)
            and record.confidence >= 0.45
            for record in evidence_records
        )
        candidate.precise_location_confirmed = (
            candidate.location_precision_bucket
            not in {"country_only", "unknown_location", "outside_target_area", ""}
            or any(
                record.precise_location_match
                and self._supports_current_role(record)
                and not self._is_stale(record, current_year)
                and record.confidence >= 0.45
                for record in evidence_records
            )
        )
        candidate.current_employment_confirmed = bool(fresh_current_role_records)
        setattr(candidate, "source_quality_score", source_quality_score)
        setattr(candidate, "evidence_freshness_year", latest_year or None)
        setattr(candidate, "current_role_proof_count", len(fresh_current_role_records))

        precise_location_records = [
            record
            for record in evidence_records
            if record.precise_location_match
            and record.source_type != "company_location"
            and self._supports_current_role(record)
            and not self._is_stale(record, current_year)
            and record.confidence >= 0.45
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
                f"{len(company_location_records)} company office/contact sources support Ireland locality"
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
        if not candidate.current_employment_confirmed:
            if stale_data_risk:
                candidate.verification_notes.append("current role proof is stale and cannot verify current employment")
            elif historical_only_records:
                candidate.verification_notes.append("current role proof appears historical rather than current")
            else:
                candidate.verification_notes.append("current role not yet publicly confirmed")
            candidate.score = round(max(0.0, candidate.score - 10.0), 2)
        if historical_only_records and not fresh_current_role_records:
            candidate.verification_notes.append("public evidence appears historical rather than current")

        cap_reasons = []
        if stale_data_risk:
            cap_reasons.append("stale_public_evidence")
        if historical_only_records and not fresh_current_role_records:
            cap_reasons.append("historical_only_public_evidence")
        if not candidate.current_employment_confirmed:
            cap_reasons.append("missing_current_role_proof")

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
            if not candidate.precise_location_confirmed or getattr(candidate, "location_precision_bucket", "") in {
                "country_only",
                "unknown_location",
                "outside_target_area",
            }:
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
        elif candidate.verification_status == "review" and not candidate.current_employment_confirmed and candidate.score < 60.0:
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
        total = min(max(0, int(limit or 0)), len(candidates))
        if total <= 0:
            return {
                "candidates_checked": 0,
                "requests_used": 0,
                "promoted_to_verified": 0,
                "promoted_to_review": 0,
            }

        checked_count = 0
        semaphore = asyncio.Semaphore(self.parallel_candidates)
        progress_lock = asyncio.Lock()

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
                    }
                )
            except Exception:
                return

        async def verify_one(candidate: CandidateProfile) -> None:
            nonlocal checked_count, request_count, verified_count, reviewed_count
            before_status = candidate.verification_status
            evidence_records: List[EvidenceRecord] = []
            used_requests = 0
            async with semaphore:
                try:
                    evidence_records, used_requests = await self.collect_evidence(candidate, brief)
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
                await emit_progress()

        await emit_progress()
        await asyncio.gather(*(verify_one(candidate) for candidate in candidates[:total]))
        return {
            "candidates_checked": total,
            "requests_used": request_count,
            "promoted_to_verified": verified_count,
            "promoted_to_review": reviewed_count,
        }


def refresh_report_summary(report: SearchRunReport, verification_stats: Dict[str, Any] | None = None) -> None:
    verified = len([candidate for candidate in report.candidates if candidate.verification_status == "verified"])
    review = len([candidate for candidate in report.candidates if candidate.verification_status == "review"])
    rejected = len([candidate for candidate in report.candidates if candidate.verification_status == "reject"])
    corroborated = len(
        [candidate for candidate in report.candidates if candidate.evidence_verdict == "corroborated"]
    )
    supported = len(
        [candidate for candidate in report.candidates if candidate.evidence_verdict == "supported"]
    )

    report.candidates = sort_candidates(report.candidates)
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
