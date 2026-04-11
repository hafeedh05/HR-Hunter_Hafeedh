from __future__ import annotations

import asyncio
import os
import re
import time
from typing import Any, Callable, Dict, List
from urllib.parse import urlparse

import httpx

from hr_hunter.briefing import normalize_text, unique_preserving_order
from hr_hunter.config import resolve_secret
from hr_hunter.identity import canonical_query_fingerprint
from hr_hunter.models import CandidateProfile, ProviderRunResult, SearchBrief, SearchSlice
from hr_hunter.providers.base import SearchProvider


NON_PERSON_NAME_TOKENS = {
    "about",
    "blog",
    "brand",
    "careers",
    "company",
    "contact",
    "email",
    "free",
    "info",
    "leadership",
    "management",
    "our",
    "people",
    "phone",
    "press",
    "profile",
    "reveal",
    "speaker",
    "speakers",
    "staff",
    "team",
    "works",
}
BIDI_CONTROL_RE = re.compile(r"[\u200e\u200f\u202a-\u202e\u2066-\u2069]")
NOISY_PROFILE_PATTERNS = (
    "email & phone",
    "contact info",
    "contact details",
    "reveal for free",
    "email address",
    "phone number",
)
DEFAULT_BLOCKED_HOSTNAMES = {
    "contactout.com",
    "datanyze.com",
    "lusha.com",
    "rocketreach.co",
    "signalhire.com",
}
ROLE_LIKE_TOKENS = {
    "analyst",
    "analytics",
    "associate",
    "business",
    "consultant",
    "data",
    "designer",
    "developer",
    "director",
    "engineer",
    "founder",
    "head",
    "lead",
    "manager",
    "marketing",
    "officer",
    "operations",
    "owner",
    "partner",
    "president",
    "principal",
    "product",
    "program",
    "project",
    "sales",
    "scientist",
    "senior",
    "specialist",
    "strategy",
    "supervisor",
    "vice",
}

DEFAULT_PROFILE_URL_SUBSTRINGS = [
    "linkedin.com/in/",
    "theorg.com/org/",
    "/people/",
    "/person/",
    "/profile",
    "/profiles/",
    "/bio",
    "/biography",
    "/speaker",
    "/speakers/",
    "/staff/",
    "/team/",
    "/leadership/",
    "/management/",
    "/our-team",
    "/our-people",
    "/our-leadership",
    "/executive-team",
    "/leadership-team",
    "/news/",
    "/press/",
    "/articles/",
    "/awards/",
    "/directory/",
    "/member/",
    "/members/",
    "/committee/",
    "/board/",
]

PUBLIC_QUERY_FAMILY_TERMS = {
    "team_leadership_pages": ['"team"', '"leadership"', '"management"', '"people"'],
    "appointment_news_pages": ['"appointed"', '"appointment"', '"joins"', '"promoted"', '"named"'],
    "speaker_bio_pages": ['"speaker"', '"speakers"', '"bio"', '"biography"', '"panel"'],
    "award_industry_pages": ['"award"', '"awards"', '"finalist"', '"judge"', '"conference"'],
    "industry_association_pages": ['"steering group"', '"committee"', '"member"', '"members"', '"forum"', '"board"'],
    "trade_directory_pages": ['"directory"', '"supplier"', '"buyers guide"', '"contact"', '"members"'],
    "org_chart_profile_pages": ['"org chart"', '"org"', '"profile"', '"leadership"'],
    "profile_like_public_pages": ['"profile"', '"profiles"', '"bio"', '"biography"', '"people"'],
}
HISTORICAL_ROLE_MARKERS = (
    "before joining",
    "before he joined",
    "before she joined",
    "began his career",
    "began her career",
    "career at",
    "former",
    "formerly",
    "prior to joining",
    "prior to his",
    "prior to her",
    "previously",
    "started his career",
    "started her career",
    "used to",
    "worked as",
    "worked at",
)


class ScrapingBeeGoogleClient:
    endpoint = "https://app.scrapingbee.com/api/v1/store/google"

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or resolve_secret("SCRAPINGBEE_API_KEY")

    def is_configured(self) -> bool:
        return bool(self.api_key)

    async def search(
        self,
        client: httpx.AsyncClient,
        search_query: str,
        *,
        page: int,
        country_code: str,
        language: str,
        light_request: bool,
    ) -> httpx.Response:
        return await client.get(
            self.endpoint,
            params={
                "api_key": self.api_key or "",
                "search": search_query,
                "page": page,
                "country_code": country_code,
                "language": language,
                "light_request": str(light_request).lower(),
            },
        )


class ScrapingBeeGoogleProvider(SearchProvider):
    name = "scrapingbee_google"

    def __init__(self, settings: Dict[str, Any]):
        super().__init__(settings)
        self.client = ScrapingBeeGoogleClient(os.getenv("SCRAPINGBEE_API_KEY"))
        self.pages_per_query = int(settings.get("pages_per_query", 1))
        self.country_code = settings.get("country_code", "ie")
        self.language = settings.get("language", "en")
        self.light_request = bool(settings.get("light_request", True))
        self.request_timeout_seconds = float(settings.get("request_timeout_seconds", 30.0))
        self.max_retries = int(settings.get("max_retries", 2))
        self.retry_backoff_seconds = float(settings.get("retry_backoff_seconds", 2.0))
        self.stop_grace_period_seconds = max(0.0, float(settings.get("stop_grace_period_seconds", 1.0) or 1.0))
        self.parallel_requests = max(1, int(settings.get("parallel_requests", 8) or 8))
        self.include_country_only_queries = bool(settings.get("include_country_only_queries", True))
        self.include_relaxed_queries = bool(settings.get("include_relaxed_queries", True))
        self.max_company_aliases_per_query = int(settings.get("max_company_aliases_per_query", 3))
        self.max_company_terms_per_query = max(1, int(settings.get("max_company_terms_per_query", 12) or 12))
        self.company_slice_location_group_limit = int(settings.get("company_slice_location_group_limit", 0))
        self.geo_fanout_enabled = bool(settings.get("geo_fanout_enabled", True))
        self.max_geo_groups = max(1, int(settings.get("max_geo_groups", 10) or 10))
        self.geo_group_size = max(1, int(settings.get("geo_group_size", 2) or 2))
        self.max_queries = int(settings.get("max_queries", 0))
        self.stagnation_query_window = max(0, int(settings.get("stagnation_query_window", 28) or 28))
        self.stagnation_min_results = max(0, int(settings.get("stagnation_min_results", 0) or 0))
        self.include_query_terms = list(settings.get("include_query_terms", []))
        self.exclude_query_terms = list(settings.get("exclude_query_terms", []))
        self.include_site_terms = list(
            settings.get(
                "include_site_terms",
                [],
            )
        )
        self.exclude_site_terms = list(
            settings.get(
                "exclude_site_terms",
                [
                    "-site:linkedin.com/jobs",
                    "-site:ie.linkedin.com/jobs",
                    "-site:linkedin.com/company",
                    "-site:ie.linkedin.com/company",
                    "-site:linkedin.com/posts",
                    "-site:ie.linkedin.com/posts",
                ],
            )
        )
        configured_allowed_url_substrings = list(settings.get("allowed_url_substrings", []))
        self.allowed_url_substrings = self._unique(
            [
                *(str(value).lower().strip() for value in configured_allowed_url_substrings if str(value).strip()),
                *DEFAULT_PROFILE_URL_SUBSTRINGS,
            ]
        )
        self.blocked_url_substrings = [
            value.lower()
            for value in settings.get(
                "blocked_url_substrings",
                ["/jobs", "/company", "/posts", "/careers"],
            )
            if str(value).strip()
        ]
        self.blocked_hostnames = {
            str(value).strip().lower().removeprefix("www.")
            for value in settings.get("blocked_hostnames", sorted(DEFAULT_BLOCKED_HOSTNAMES))
            if str(value).strip()
        }
        raw_query_family_budgets = settings.get(
            "query_family_budgets",
            settings.get("max_queries_per_family", {}),
        )
        self.query_family_budgets = {
            str(family).strip(): max(0, int(limit))
            for family, limit in dict(raw_query_family_budgets or {}).items()
            if str(family).strip()
        }

    def is_configured(self) -> bool:
        return self.client.is_configured()

    @staticmethod
    def _unique(values: List[str]) -> List[str]:
        seen = set()
        ordered: List[str] = []
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

    @staticmethod
    def _chunked(values: List[str], size: int) -> List[List[str]]:
        if size <= 0:
            return [values]
        return [values[index : index + size] for index in range(0, len(values), size)]

    def _title_groups(self, brief: SearchBrief, slice_config: SearchSlice) -> List[List[str]]:
        if slice_config.search_mode == "strict":
            title_values = self._unique(slice_config.titles)
            return self._chunked(title_values, 4)

        title_values = self._unique([*slice_config.title_keywords, *slice_config.titles])
        chunk_size = 4 if slice_config.search_mode in {"discovery", "market"} else 5
        return self._chunked(title_values, chunk_size)

    def _location_groups(self, brief: SearchBrief) -> List[List[str]]:
        canonical_targets = self._unique(
            [
                brief.geography.location_name,
                brief.geography.country,
                *brief.location_targets,
                *brief.geography.location_hints,
            ]
        )
        canonical_targets = [value for value in canonical_targets if value]
        if not canonical_targets:
            return [[brief.geography.location_name, brief.geography.country]]

        if not self.geo_fanout_enabled:
            local_terms = self._unique(
                [
                    brief.geography.location_name,
                    *canonical_targets[:5],
                ]
            )
            regional_terms = self._unique(
                [
                    brief.geography.country,
                    *canonical_targets[5:],
                ]
            )
            groups = [group for group in [local_terms, regional_terms] if group]
            return groups or [canonical_targets]

        fanout_groups: List[List[str]] = [[target] for target in canonical_targets[: self.max_geo_groups]]
        combined_groups = self._chunked(canonical_targets, self.geo_group_size)
        for group in combined_groups:
            if len(fanout_groups) >= self.max_geo_groups:
                break
            if group and group not in fanout_groups:
                fanout_groups.append(group)
        return fanout_groups[: self.max_geo_groups]

    def _site_filters(self) -> str:
        include_terms = [term.strip() for term in self.include_site_terms if str(term).strip()]
        exclude_terms = [term.strip() for term in self.exclude_site_terms if str(term).strip()]
        include_clause = ""
        if include_terms:
            include_clause = f"({' OR '.join(include_terms)})"
        parts = [part for part in [include_clause, *exclude_terms] if part]
        return " ".join(parts)

    def _query_filters(self) -> str:
        include_terms = self._unique([str(term).strip() for term in self.include_query_terms if str(term).strip()])
        exclude_terms = self._unique([str(term).strip() for term in self.exclude_query_terms if str(term).strip()])

        parts: List[str] = []
        if include_terms:
            joined = " OR ".join(f'"{term}"' for term in include_terms)
            parts.append(f"({joined})")
        for term in exclude_terms:
            if " " in term:
                parts.append(f'-"{term}"')
            else:
                parts.append(f"-{term}")
        return " ".join(parts)

    @staticmethod
    def _combine_query_parts(*parts: str) -> str:
        return " ".join(part.strip() for part in parts if part and part.strip())

    @staticmethod
    def _family_budget_remaining(
        family: str,
        family_query_counts: Dict[str, int],
        family_query_budgets: Dict[str, int],
    ) -> bool:
        if family not in family_query_budgets:
            return True
        budget = family_query_budgets.get(family, 0)
        if budget <= 0:
            return False
        return family_query_counts.get(family, 0) < budget

    def _adjacent_query_terms(self, slice_config: SearchSlice) -> str:
        keywords = self._unique(slice_config.query_keywords)[:8]
        return " OR ".join(f'"{keyword}"' for keyword in keywords if keyword)

    def _append_query_plan(
        self,
        plans: List[Dict[str, str]],
        seen_fingerprints: set[str],
        slice_config: SearchSlice,
        family: str,
        variant: str,
        search_query: str,
    ) -> None:
        cleaned_query = self._combine_query_parts(search_query)
        if not cleaned_query:
            return
        fingerprint = canonical_query_fingerprint(cleaned_query)
        if not fingerprint or fingerprint in seen_fingerprints:
            return
        seen_fingerprints.add(fingerprint)
        plans.append(
            {
                "slice_id": slice_config.id,
                "family": family,
                "variant": variant,
                "tag": f"{slice_config.id}:{family}:{variant}",
                "search": cleaned_query,
                "fingerprint": fingerprint,
            }
        )

    def _is_profile_url(self, url: str) -> bool:
        lowered = url.lower().strip()
        if not lowered:
            return False
        parsed = urlparse(lowered if "://" in lowered else f"https://{lowered}")
        host = parsed.netloc.lower().removeprefix("www.")
        path = parsed.path.lower()
        if host in self.blocked_hostnames:
            return False
        looks_profile_like = any(token in lowered for token in self.allowed_url_substrings) or any(
            token in path
            for token in (
                "/people/",
                "/person/",
                "/profile",
                "/profiles/",
                "/bio",
                "/biography",
                "/speaker",
                "/speakers/",
                "/staff/",
                "/team/",
                "/leadership/",
                "/management/",
                "/news/",
                "/press/",
                "/articles/",
                "/awards/",
                "/directory/",
                "/member/",
                "/members/",
                "/committee/",
                "/board/",
            )
        )
        if not looks_profile_like:
            return False
        if any(token in lowered for token in self.blocked_url_substrings):
            return False
        return True

    @staticmethod
    def _sanitize_public_text(value: str) -> str:
        cleaned = BIDI_CONTROL_RE.sub("", str(value or ""))
        cleaned = cleaned.replace("…", "...")
        cleaned = re.sub(r"\s+", " ", cleaned)
        cleaned = re.sub(r"\s+([|,;:])", r"\1", cleaned)
        return cleaned.strip().strip("|-,.;:")

    @classmethod
    def _looks_like_contact_directory(cls, url: str, title: str, description: str) -> bool:
        combined = " ".join(
            cls._sanitize_public_text(value).lower() for value in (url, title, description) if value
        )
        if not combined:
            return False
        if any(pattern in combined for pattern in NOISY_PROFILE_PATTERNS):
            return True
        return bool(re.search(r"\bworks\s+as\b", combined) and re.search(r"\b(reveal|contact)\b", combined))

    @staticmethod
    def _looks_like_person_name(value: str) -> bool:
        tokens = [token for token in re.split(r"[^A-Za-z'’.-]+", value.strip()) if token]
        if len(tokens) < 2 or len(tokens) > 5:
            return False
        lowered = {token.lower().strip(".") for token in tokens}
        if lowered.intersection(NON_PERSON_NAME_TOKENS):
            return False
        return len([token for token in tokens if any(char.isalpha() for char in token)]) >= 2

    @staticmethod
    def _clean_title_fragment(value: str) -> str:
        cleaned = value.strip().strip("|-,.;:")
        return re.sub(r"\s+", " ", cleaned)

    def _extract_location_name(self, brief: SearchBrief, combined_text: str) -> str:
        normalized = normalize_text(combined_text)
        for hint in unique_preserving_order(
            [brief.geography.location_name, *brief.geography.location_hints, brief.geography.country]
        ):
            normalized_hint = normalize_text(hint)
            if not normalized_hint or normalized_hint not in normalized:
                continue
            if brief.geography.country and normalize_text(brief.geography.country) not in normalized_hint:
                return f"{hint}, {brief.geography.country}"
            return hint
        return ""

    @staticmethod
    def _looks_historical_role_text(text: str) -> bool:
        normalized = normalize_text(text)
        return any(marker in normalized for marker in HISTORICAL_ROLE_MARKERS)

    def _infer_title_from_description(
        self,
        description: str,
        brief: SearchBrief,
        current_company: str,
    ) -> str:
        if not description or self._looks_historical_role_text(description):
            return ""
        company_candidates = [current_company] if current_company else []
        if current_company in brief.company_aliases:
            company_candidates.extend(brief.company_aliases[current_company])
        for company in unique_preserving_order(company_candidates):
            escaped_company = re.escape(company)
            for pattern in (
                rf"(?P<title>[^|,.;:()]+?)\s+(?:at|@)\s+{escaped_company}\b",
                rf"{escaped_company}\s+[|,-]\s+(?P<title>[^|,.;:()]+)",
            ):
                match = re.search(pattern, description, flags=re.IGNORECASE)
                if not match:
                    continue
                title = self._clean_title_fragment(match.group("title"))
                if title and not self._looks_like_person_name(title):
                    return title
        return ""

    def _build_query_plans(self, brief: SearchBrief, slice_config: SearchSlice) -> List[Dict[str, str]]:
        title_groups = self._title_groups(brief, slice_config)
        location_groups = self._location_groups(brief)
        if (
            slice_config.search_mode in {"strict", "broad"}
            and self.company_slice_location_group_limit > 0
        ):
            location_groups = location_groups[: self.company_slice_location_group_limit]
        company_targets = slice_config.companies or [""]
        plans: List[Dict[str, str]] = []
        seen_fingerprints: set[str] = set()
        site_filters = self._site_filters()
        query_filters = self._query_filters()
        country_terms = f'"{brief.geography.country}"' if brief.geography.country else ""
        query_terms = self._adjacent_query_terms(slice_config)
        combined_company_terms = self._build_company_terms(brief, company_targets)

        for title_group in title_groups:
            title_terms = " OR ".join(f'"{title}"' for title in title_group if title)
            if not title_terms:
                continue

            for location_group in location_groups:
                location_terms = " OR ".join(f'"{hint}"' for hint in location_group if hint)
                if not location_terms:
                    continue

                family_location_terms = [
                    ("local", f"({location_terms})"),
                ]
                if self.include_country_only_queries and country_terms:
                    family_location_terms.append(("country", f"({country_terms})"))
                if self.include_relaxed_queries:
                    family_location_terms.append(("relaxed", country_terms or f"({location_terms})"))

                if slice_config.search_mode in {"discovery", "market"} and not query_terms:
                    continue

                for family, family_terms in PUBLIC_QUERY_FAMILY_TERMS.items():
                    family_clause = f"({' OR '.join(family_terms)})"
                    if slice_config.search_mode == "history":
                        history_terms = self._adjacent_query_terms(slice_config)
                        if not history_terms or not combined_company_terms:
                            continue
                        for variant, location_clause in family_location_terms:
                            self._append_query_plan(
                                plans,
                                seen_fingerprints,
                                slice_config,
                                family,
                                variant,
                                self._combine_query_parts(
                                    f"({title_terms})",
                                    f"({combined_company_terms})",
                                    f"({history_terms})",
                                    location_clause,
                                    family_clause,
                                    query_filters,
                                    site_filters,
                                ),
                            )
                        continue
                    for variant, location_clause in family_location_terms:
                        if slice_config.search_mode in {"discovery", "market"}:
                            self._append_query_plan(
                                plans,
                                seen_fingerprints,
                                slice_config,
                                family,
                                variant,
                                self._combine_query_parts(
                                    f"({title_terms})",
                                    f"({query_terms})",
                                    location_clause,
                                    family_clause,
                                    query_filters,
                                    site_filters,
                                ),
                            )
                            continue

                        if not combined_company_terms:
                            self._append_query_plan(
                                plans,
                                seen_fingerprints,
                                slice_config,
                                family,
                                variant,
                                self._combine_query_parts(
                                    f"({title_terms})",
                                    f"({query_terms})" if query_terms else "",
                                    location_clause,
                                    family_clause,
                                    query_filters,
                                    site_filters,
                                ),
                            )
                            continue
                        self._append_query_plan(
                            plans,
                            seen_fingerprints,
                            slice_config,
                            family,
                            variant,
                            self._combine_query_parts(
                                f"({title_terms})",
                                f"({combined_company_terms})",
                                f"({query_terms})" if query_terms else "",
                                location_clause,
                                family_clause,
                                query_filters,
                                site_filters,
                            ),
                        )
        return plans

    def _build_company_terms(self, brief: SearchBrief, company_targets: List[str]) -> str:
        company_terms: List[str] = []
        seen_terms: set[str] = set()
        max_terms = max(1, self.max_company_terms_per_query)
        aliases_per_company = max(1, self.max_company_aliases_per_query)

        for company in company_targets:
            aliases = brief.company_aliases.get(company, [company])[:aliases_per_company]
            for alias in aliases:
                cleaned = str(alias or "").strip()
                if not cleaned:
                    continue
                normalized = normalize_text(cleaned)
                if not normalized or normalized in seen_terms:
                    continue
                seen_terms.add(normalized)
                company_terms.append(f'"{cleaned}"')
                if len(company_terms) >= max_terms:
                    return " OR ".join(company_terms)
        return " OR ".join(company_terms)

    def build_search_queries(self, brief: SearchBrief, slice_config: SearchSlice) -> List[str]:
        return [plan["search"] for plan in self._build_query_plans(brief, slice_config)]

    def _find_company_match(self, text: str, brief: SearchBrief) -> str:
        lowered = normalize_text(text)
        for company, aliases in brief.company_aliases.items():
            for alias in aliases:
                if normalize_text(alias) and normalize_text(alias) in lowered:
                    return company
        return ""

    @staticmethod
    def _humanize_org_slug(slug: str) -> str:
        parts = [part for part in slug.split("-") if part]
        if not parts:
            return ""
        return " ".join(part.upper() if len(part) <= 3 and part.isalpha() else part.capitalize() for part in parts)

    def _extract_org_from_url(self, url: str) -> str:
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        segments = [segment for segment in parsed.path.split("/") if segment]

        if "theorg.com" in host and len(segments) >= 2 and segments[0] == "org":
            return self._humanize_org_slug(segments[1])

        return ""

    def _parse_title_fields(self, raw_title: str, url: str) -> tuple[str, str, str]:
        title = raw_title.strip()
        name = title.split(" - ")[0].split(" | ")[0].strip()
        current_title = title
        current_company = ""

        pipe_segments = [segment.strip() for segment in title.split("|") if segment.strip()]
        if len(pipe_segments) >= 3:
            name = pipe_segments[0]
            current_title = pipe_segments[1]
            current_company = pipe_segments[2]

        dash_segments = [segment.strip() for segment in title.split(" - ") if segment.strip()]
        if len(dash_segments) >= 3:
            name = dash_segments[0]
            current_title = " - ".join(dash_segments[1:-1]).strip() or dash_segments[1]
            current_company = dash_segments[-1]
        elif " - " in title:
            _, remainder = title.split(" - ", 1)
            current_title = remainder.strip()

        for delimiter in (" at ", " @ "):
            if delimiter in current_title:
                role, company = current_title.rsplit(delimiter, 1)
                current_title = role.strip()
                current_company = company.strip().strip(".")
                break

        if normalize_text(current_company) in {"linkedin", "the org"}:
            current_company = ""

        if not current_company:
            current_company = self._extract_org_from_url(url)

        return name, current_title.strip(), current_company

    def _candidate_from_result(
        self,
        result: Dict[str, Any],
        brief: SearchBrief,
    ) -> CandidateProfile | None:
        title = result.get("title", "")
        description = result.get("description") or result.get("snippet") or ""
        url = result.get("url") or result.get("link")
        name_guess, current_title, current_company = self._parse_title_fields(title, url or "")
        location_name = self._extract_location_name(brief, f"{title} {description}")

        if not current_company and not self._looks_historical_role_text(description):
            current_company = self._find_company_match(f"{title} {description} {url or ''}", brief)
        if not current_title or normalize_text(current_title) == normalize_text(title):
            inferred_title = self._infer_title_from_description(description, brief, current_company)
            if inferred_title:
                current_title = inferred_title
        if not self._looks_like_person_name(name_guess):
            return None

        return CandidateProfile(
            full_name=name_guess,
            current_title=current_title or title,
            current_company=current_company,
            location_name=location_name,
            linkedin_url=url if url and "linkedin.com" in url else None,
            source=self.name,
            source_url=url,
            summary=description,
            raw=result,
        )

    def _is_profile_url(self, url: str) -> bool:
        lowered = url.lower().strip()
        if not lowered:
            return False
        parsed = urlparse(lowered if "://" in lowered else f"https://{lowered}")
        host = parsed.netloc.lower().removeprefix("www.")
        path = parsed.path.lower()
        if host in self.blocked_hostnames:
            return False
        looks_profile_like = any(token in lowered for token in self.allowed_url_substrings) or any(
            token in path
            for token in (
                "/people/",
                "/person/",
                "/profile",
                "/profiles/",
                "/bio",
                "/biography",
                "/speaker",
                "/speakers/",
                "/staff/",
                "/team/",
                "/leadership/",
                "/management/",
                "/news/",
                "/press/",
                "/articles/",
                "/awards/",
                "/directory/",
                "/member/",
                "/members/",
                "/committee/",
                "/board/",
            )
        )
        if not looks_profile_like:
            return False
        if any(token in lowered for token in self.blocked_url_substrings):
            return False
        return True

    @staticmethod
    def _looks_like_person_name(value: str) -> bool:
        value = ScrapingBeeGoogleProvider._sanitize_public_text(value)
        lowered_value = value.lower()
        if any(pattern in lowered_value for pattern in NOISY_PROFILE_PATTERNS):
            return False
        if re.search(r"\bworks\s+as\b", lowered_value):
            return False
        tokens = [token for token in re.split(r"[^A-Za-z'.-]+", value.strip()) if token]
        if len(tokens) < 2 or len(tokens) > 5:
            return False
        lowered = {token.lower().strip(".") for token in tokens}
        if lowered.intersection(NON_PERSON_NAME_TOKENS):
            return False
        if lowered.intersection(ROLE_LIKE_TOKENS):
            return False
        return len([token for token in tokens if any(char.isalpha() for char in token)]) >= 2

    @staticmethod
    def _clean_title_fragment(value: str) -> str:
        cleaned = ScrapingBeeGoogleProvider._sanitize_public_text(value)
        return re.sub(r"\s+", " ", cleaned)

    def _parse_title_fields(self, raw_title: str, url: str) -> tuple[str, str, str]:
        title = self._sanitize_public_text(raw_title)
        name = title.split(" - ")[0].split(" | ")[0].strip()
        current_title = title
        current_company = ""

        pipe_segments = [self._clean_title_fragment(segment) for segment in title.split("|") if self._clean_title_fragment(segment)]
        if len(pipe_segments) >= 3:
            name = pipe_segments[0]
            current_title = pipe_segments[1]
            current_company = pipe_segments[2]

        dash_segments = [self._clean_title_fragment(segment) for segment in title.split(" - ") if self._clean_title_fragment(segment)]
        if len(dash_segments) >= 3:
            name = dash_segments[0]
            current_title = " - ".join(dash_segments[1:-1]).strip() or dash_segments[1]
            current_company = dash_segments[-1]
        elif " - " in title:
            _, remainder = title.split(" - ", 1)
            current_title = self._clean_title_fragment(remainder)

        trailing_pipe_segments = [
            self._clean_title_fragment(segment)
            for segment in current_title.split("|")
            if self._clean_title_fragment(segment)
        ]
        if len(trailing_pipe_segments) >= 2:
            current_title = trailing_pipe_segments[0]
            current_company = current_company or trailing_pipe_segments[1]

        for delimiter in (" at ", " @ "):
            if delimiter in current_title:
                role, company = current_title.rsplit(delimiter, 1)
                current_title = self._clean_title_fragment(role)
                current_company = self._clean_title_fragment(company)
                break

        if normalize_text(current_company) in {"linkedin", "the org"}:
            current_company = ""

        if not current_company:
            current_company = self._extract_org_from_url(url)

        return (
            self._clean_title_fragment(name),
            self._clean_title_fragment(current_title),
            self._clean_title_fragment(current_company),
        )

    def _should_infer_current_title(
        self,
        raw_title: str,
        current_title: str,
        current_company: str,
        brief: SearchBrief,
    ) -> bool:
        normalized_current_title = normalize_text(current_title)
        if not normalized_current_title:
            return True
        if normalized_current_title == normalize_text(raw_title):
            return True

        company_like_values = unique_preserving_order(
            [
                current_company,
                *brief.company_targets,
                *[alias for aliases in brief.company_aliases.values() for alias in aliases],
            ]
        )
        for value in company_like_values:
            normalized_company = normalize_text(value)
            if not normalized_company:
                continue
            if normalized_current_title == normalized_company:
                return True
            if len(normalized_current_title.split()) <= 3 and (
                normalized_current_title in normalized_company or normalized_company in normalized_current_title
            ):
                return True
        return False

    def _candidate_from_result(
        self,
        result: Dict[str, Any],
        brief: SearchBrief,
    ) -> CandidateProfile | None:
        title = self._sanitize_public_text(result.get("title", ""))
        description = self._sanitize_public_text(result.get("description") or result.get("snippet") or "")
        url = result.get("url") or result.get("link")
        if self._looks_like_contact_directory(url or "", title, description):
            return None

        name_guess, current_title, current_company = self._parse_title_fields(title, url or "")
        location_name = self._extract_location_name(brief, f"{title} {description}")

        if not current_company and not self._looks_historical_role_text(description):
            current_company = self._find_company_match(f"{title} {description} {url or ''}", brief)
        if self._should_infer_current_title(title, current_title, current_company, brief):
            inferred_title = self._infer_title_from_description(description, brief, current_company)
            if inferred_title:
                current_title = inferred_title
        if not self._looks_like_person_name(name_guess):
            return None

        return CandidateProfile(
            full_name=name_guess,
            current_title=current_title or title,
            current_company=current_company,
            location_name=location_name,
            linkedin_url=url if url and "linkedin.com" in url else None,
            source=self.name,
            source_url=url,
            summary=description,
            raw=result,
        )

    async def run(
        self,
        brief: SearchBrief,
        slices: List[SearchSlice],
        limit: int,
        dry_run: bool,
        exclude_queries: set[str] | None = None,
        progress_callback: Callable[[Dict[str, Any]], None] | None = None,
    ) -> ProviderRunResult:
        excluded_queries = {value.strip() for value in (exclude_queries or set()) if value and value.strip()}
        excluded_fingerprints = {
            canonical_query_fingerprint(value) for value in excluded_queries if canonical_query_fingerprint(value)
        }
        diagnostics = {
            "queries": [],
            "skipped_query_count": 0,
            "query_budget_exhausted": False,
            "query_budget": {
                "max_queries_per_run": self.max_queries,
                "max_queries_per_family": dict(self.query_family_budgets),
                "executed_per_family": {},
                "skipped_per_family": {},
                "planned_per_family": {},
                "family_budget_exhausted": [],
            },
        }
        family_query_counts: Dict[str, int] = {}
        skipped_per_family: Dict[str, int] = {}

        def record_query_entry(
            plan: Dict[str, str],
            *,
            skipped: bool,
            skip_reason: str = "",
        ) -> None:
            family = plan["family"]
            diagnostics["query_budget"]["planned_per_family"][family] = (
                diagnostics["query_budget"]["planned_per_family"].get(family, 0) + 1
            )
            if skipped:
                skipped_per_family[family] = skipped_per_family.get(family, 0) + 1
                diagnostics["skipped_query_count"] += 1
            diagnostics["queries"].append(
                {
                    "slice_id": plan["slice_id"],
                    "family": family,
                    "variant": plan["variant"],
                    "tag": plan["tag"],
                    "fingerprint": plan["fingerprint"],
                    "search": plan["search"],
                    "skipped": skipped,
                    "skip_reason": skip_reason,
                }
            )

        if dry_run:
            for slice_config in slices:
                for plan in self._build_query_plans(brief, slice_config):
                    family = plan["family"]
                    if self.max_queries > 0 and sum(family_query_counts.values()) >= self.max_queries:
                        diagnostics["query_budget_exhausted"] = True
                        record_query_entry(plan, skipped=True, skip_reason="run_budget")
                        continue
                    if not self._family_budget_remaining(family, family_query_counts, self.query_family_budgets):
                        if family not in diagnostics["query_budget"]["family_budget_exhausted"]:
                            diagnostics["query_budget"]["family_budget_exhausted"].append(family)
                        record_query_entry(plan, skipped=True, skip_reason="family_budget")
                        continue
                    if plan["search"] in excluded_queries or plan["fingerprint"] in excluded_fingerprints:
                        record_query_entry(plan, skipped=True, skip_reason="exclude_query")
                        continue
                    family_query_counts[family] = family_query_counts.get(family, 0) + 1
                    record_query_entry(plan, skipped=False)
            diagnostics["query_budget"]["executed_per_family"] = family_query_counts
            diagnostics["query_budget"]["skipped_per_family"] = skipped_per_family
            return ProviderRunResult(
                provider_name=self.name,
                executed=False,
                dry_run=True,
                diagnostics=diagnostics,
            )

        if not self.is_configured():
            return ProviderRunResult(
                provider_name=self.name,
                executed=False,
                dry_run=False,
                diagnostics=diagnostics,
                errors=["Missing SCRAPINGBEE_API_KEY."],
            )

        executable_plans: List[Dict[str, str]] = []
        executed_queries = 0
        for slice_config in slices:
            for plan in self._build_query_plans(brief, slice_config):
                search_query = plan["search"]
                family = plan["family"]
                if self.max_queries > 0 and executed_queries >= self.max_queries:
                    diagnostics["query_budget_exhausted"] = True
                    record_query_entry(plan, skipped=True, skip_reason="run_budget")
                    continue
                if not self._family_budget_remaining(family, family_query_counts, self.query_family_budgets):
                    if family not in diagnostics["query_budget"]["family_budget_exhausted"]:
                        diagnostics["query_budget"]["family_budget_exhausted"].append(family)
                    record_query_entry(plan, skipped=True, skip_reason="family_budget")
                    continue
                if search_query in excluded_queries or plan["fingerprint"] in excluded_fingerprints:
                    record_query_entry(plan, skipped=True, skip_reason="exclude_query")
                    continue
                family_query_counts[family] = family_query_counts.get(family, 0) + 1
                record_query_entry(plan, skipped=False)
                executable_plans.append(plan)
                executed_queries += 1

        candidates: List[CandidateProfile] = []
        request_count = 0
        errors: List[str] = []
        queries_completed = 0
        raw_found = 0
        queries_in_flight = 0
        last_completion_monotonic = time.monotonic()
        last_growth_completed = 0
        last_growth_raw_found = 0
        stagnation_stop_reason = ""
        total_query_pages = len(executable_plans) * max(1, self.pages_per_query)
        lock = asyncio.Lock()
        stop_event = asyncio.Event()
        telemetry_done = asyncio.Event()

        if progress_callback:
            progress_callback(
                {
                    "provider": self.name,
                    "stage": "retrieval",
                    "queries_total": total_query_pages,
                    "queries_completed": 0,
                    "raw_found": 0,
                    "queries_in_flight": 0,
                    "message": f"Running {len(executable_plans)} query slices across geographies.",
                }
            )

        async def run_plan(plan: Dict[str, str]) -> None:
            nonlocal request_count, queries_completed, raw_found, queries_in_flight
            nonlocal last_completion_monotonic, last_growth_completed, last_growth_raw_found, stagnation_stop_reason
            if stop_event.is_set():
                return
            search_query = plan["search"]
            family = plan["family"]
            for page in range(1, self.pages_per_query + 1):
                if stop_event.is_set():
                    return
                async with lock:
                    queries_in_flight += 1
                response = None
                try:
                    for attempt in range(self.max_retries + 1):
                        try:
                            response = await self.client.search(
                                client,
                                search_query,
                                page=page,
                                country_code=self.country_code,
                                language=self.language,
                                light_request=self.light_request,
                            )
                            async with lock:
                                request_count += 1
                        except httpx.HTTPError as exc:
                            if attempt < self.max_retries:
                                await asyncio.sleep(self.retry_backoff_seconds * (attempt + 1))
                                continue
                            async with lock:
                                errors.append(f"{plan['slice_id']}: {type(exc).__name__} {exc}")
                            response = None
                            break

                        if response.status_code < 400:
                            break

                        if response.status_code in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                            await asyncio.sleep(self.retry_backoff_seconds * (attempt + 1))
                            continue

                        async with lock:
                            errors.append(f"{plan['slice_id']}: HTTP {response.status_code} {response.text[:240]}")
                        response = None
                        break

                    if response is not None:
                        payload = response.json()
                        fresh_candidates: List[CandidateProfile] = []
                        for result in payload.get("organic_results", []):
                            url = result.get("url") or result.get("link") or ""
                            if not self._is_profile_url(url):
                                continue
                            candidate = self._candidate_from_result(result, brief)
                            if candidate is None:
                                continue
                            candidate.raw = {
                                **candidate.raw,
                                "query_family": family,
                                "query_variant": plan["variant"],
                                "query_tag": plan["tag"],
                                "query_fingerprint": plan["fingerprint"],
                                "search_query": search_query,
                            }
                            fresh_candidates.append(candidate)

                        async with lock:
                            if fresh_candidates:
                                candidates.extend(fresh_candidates)
                                raw_found = len(candidates)
                                if raw_found > last_growth_raw_found:
                                    last_growth_raw_found = raw_found
                                    last_growth_completed = queries_completed
                                if len(candidates) >= limit:
                                    stop_event.set()

                    async with lock:
                        queries_completed += 1
                        last_completion_monotonic = time.monotonic()
                        if raw_found > last_growth_raw_found:
                            last_growth_raw_found = raw_found
                            last_growth_completed = queries_completed
                        stagnation_window = max(0, int(self.stagnation_query_window))
                        stagnation_min_results = max(
                            self.stagnation_min_results,
                            max(180, int(max(brief.max_profiles, brief.result_target_max) * 0.6)),
                        )
                        stale_queries = max(0, queries_completed - last_growth_completed)
                        if (
                            not stop_event.is_set()
                            and stagnation_window > 0
                            and raw_found >= stagnation_min_results
                            and stale_queries >= stagnation_window
                        ):
                            stagnation_stop_reason = (
                                f"Retrieval plateaued after {stale_queries} queries without new candidates; "
                                f"stopping early at {raw_found} raw results."
                            )
                            stop_event.set()
                finally:
                    async with lock:
                        queries_in_flight = max(0, queries_in_flight - 1)
                        telemetry = {
                            "provider": self.name,
                            "stage": "retrieval",
                            "queries_total": total_query_pages,
                            "queries_completed": queries_completed,
                            "raw_found": raw_found,
                            "queries_in_flight": queries_in_flight,
                            "message": (
                                stagnation_stop_reason
                                if stagnation_stop_reason
                                else f"Query {queries_completed}/{max(1, total_query_pages)} completed."
                            ),
                        }
                    if progress_callback:
                        progress_callback(telemetry)

                if response is None and stop_event.is_set():
                    return

        async def emit_progress_heartbeat() -> None:
            while not telemetry_done.is_set():
                await asyncio.sleep(3.0)
                if telemetry_done.is_set():
                    break
                async with lock:
                    completed = int(queries_completed)
                    total = int(total_query_pages)
                    found = int(raw_found)
                    in_flight = int(queries_in_flight)
                    stale_queries = max(0, completed - last_growth_completed)
                    plateau_message = str(stagnation_stop_reason)
                stalled_for = max(0, int(time.monotonic() - last_completion_monotonic))
                pending = max(0, total - completed)
                if progress_callback:
                    progress_callback(
                        {
                            "provider": self.name,
                            "stage": "retrieval",
                            "queries_total": total,
                            "queries_completed": completed,
                            "raw_found": found,
                            "queries_in_flight": in_flight,
                            "queries_pending": pending,
                            "stalled_for_seconds": stalled_for,
                            "heartbeat": True,
                            "message": plateau_message
                            or (
                                f"Waiting on {in_flight} in-flight query pages; "
                                f"{completed}/{max(1, total)} completed."
                                if in_flight > 0
                                else (
                                    f"Retrieval plateau watch: {stale_queries} stale queries, "
                                    f"{completed}/{max(1, total)} completed."
                                    if stale_queries >= max(8, self.parallel_requests)
                                    else f"Retrieval active: {completed}/{max(1, total)} completed."
                                )
                            ),
                        }
                    )

        async with httpx.AsyncClient(timeout=self.request_timeout_seconds) as client:
            semaphore = asyncio.Semaphore(self.parallel_requests)
            heartbeat_task: asyncio.Task | None = None

            async def run_plan_with_limit(plan: Dict[str, str]) -> None:
                async with semaphore:
                    await run_plan(plan)

            tasks = [asyncio.create_task(run_plan_with_limit(plan)) for plan in executable_plans]
            stop_waiter: asyncio.Task | None = None
            if progress_callback and tasks:
                heartbeat_task = asyncio.create_task(emit_progress_heartbeat())
            if tasks:
                try:
                    stop_waiter = asyncio.create_task(stop_event.wait())
                    pending_tasks: set[asyncio.Task[None]] = set(tasks)
                    while pending_tasks:
                        watch_set = set(pending_tasks)
                        if stop_waiter and not stop_waiter.done():
                            watch_set.add(stop_waiter)
                        done, pending = await asyncio.wait(
                            watch_set,
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        if stop_waiter and stop_waiter in done:
                            done.remove(stop_waiter)
                            completed_after_stop = set(done)
                            remaining_tasks = {task for task in pending_tasks if task not in completed_after_stop}
                            if remaining_tasks:
                                # Give active page fetches a short grace window to finish so we keep
                                # monotonic query telemetry, but do not let a hung request stall the run.
                                grace_done, still_pending = await asyncio.wait(
                                    remaining_tasks,
                                    timeout=self.stop_grace_period_seconds,
                                )
                                completed_after_stop.update(grace_done)
                                if still_pending:
                                    for task in still_pending:
                                        task.cancel()
                                    cancelled_results = await asyncio.gather(
                                        *still_pending,
                                        return_exceptions=True,
                                    )
                                    for result in cancelled_results:
                                        if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
                                            raise result
                            for task in completed_after_stop:
                                if task.cancelled():
                                    continue
                                exception = task.exception()
                                if exception:
                                    raise exception
                            pending_tasks.clear()
                            break
                        for task in done:
                            if task is stop_waiter:
                                continue
                            exception = task.exception()
                            if exception:
                                raise exception
                        pending_tasks = {task for task in pending if task is not stop_waiter}
                finally:
                    if stop_waiter and not stop_waiter.done():
                        stop_waiter.cancel()
                    telemetry_done.set()
                    if heartbeat_task:
                        try:
                            await heartbeat_task
                        except asyncio.CancelledError:
                            pass
            if progress_callback:
                progress_callback(
                    {
                        "provider": self.name,
                        "stage": "retrieval",
                        "queries_total": int(total_query_pages),
                        "queries_completed": int(queries_completed),
                        "raw_found": int(raw_found),
                        "queries_in_flight": 0,
                        "message": f"Retrieval complete: {queries_completed}/{max(1, total_query_pages)} query pages finished.",
                    }
                )

        diagnostics["query_budget"]["executed_per_family"] = family_query_counts
        diagnostics["query_budget"]["skipped_per_family"] = skipped_per_family
        diagnostics["query_budget"]["executed_query_count"] = len(executable_plans)
        diagnostics["query_budget"]["query_page_total"] = total_query_pages
        diagnostics["query_budget"]["query_page_completed"] = queries_completed
        diagnostics["query_budget"]["stagnation_query_window"] = self.stagnation_query_window
        diagnostics["query_budget"]["stagnation_min_results"] = max(
            self.stagnation_min_results,
            max(180, int(max(brief.max_profiles, brief.result_target_max) * 0.6)),
        )
        diagnostics["query_budget"]["stagnation_stop_triggered"] = bool(stagnation_stop_reason)
        if stagnation_stop_reason:
            diagnostics["query_budget"]["stagnation_stop_reason"] = stagnation_stop_reason

        return ProviderRunResult(
            provider_name=self.name,
            executed=True,
            dry_run=False,
            request_count=request_count,
            candidate_count=len(candidates),
            candidates=candidates[:limit],
            diagnostics=diagnostics,
            errors=errors,
        )
