from __future__ import annotations

import asyncio
import os
import re
from typing import Any, Dict, List
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
    "leadership",
    "management",
    "our",
    "people",
    "press",
    "profile",
    "speaker",
    "speakers",
    "staff",
    "team",
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
]

PUBLIC_QUERY_FAMILY_TERMS = {
    "team_leadership_pages": ['"team"', '"leadership"', '"management"', '"people"'],
    "appointment_news_pages": ['"appointed"', '"appointment"', '"joins"', '"promoted"', '"named"'],
    "speaker_bio_pages": ['"speaker"', '"speakers"', '"bio"', '"biography"', '"panel"'],
    "award_industry_pages": ['"award"', '"awards"', '"finalist"', '"judge"', '"conference"'],
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
        self.include_country_only_queries = bool(settings.get("include_country_only_queries", True))
        self.include_relaxed_queries = bool(settings.get("include_relaxed_queries", True))
        self.max_company_aliases_per_query = int(settings.get("max_company_aliases_per_query", 3))
        self.company_slice_location_group_limit = int(settings.get("company_slice_location_group_limit", 0))
        self.max_queries = int(settings.get("max_queries", 0))
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
        local_terms = self._unique(
            [
                brief.geography.location_name,
                *brief.geography.location_hints[:5],
            ]
        )
        regional_terms = self._unique(
            [
                brief.geography.country,
                *brief.geography.location_hints[5:],
            ]
        )
        groups = [group for group in [local_terms, regional_terms] if group]
        return groups or [[brief.geography.location_name, brief.geography.country]]

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
        budget = family_query_budgets.get(family, 0)
        if budget <= 0:
            return True
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
        path = parsed.path.lower()
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
            )
        )
        if not looks_profile_like:
            return False
        if any(token in lowered for token in self.blocked_url_substrings):
            return False
        return True

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

                        for company in company_targets:
                            company_aliases = brief.company_aliases.get(company, [company])
                            company_terms = " OR ".join(
                                f'"{alias}"'
                                for alias in company_aliases[: self.max_company_aliases_per_query]
                                if alias
                            )
                            if not company_terms:
                                continue
                            self._append_query_plan(
                                plans,
                                seen_fingerprints,
                                slice_config,
                                family,
                                variant,
                                self._combine_query_parts(
                                    f"({title_terms})",
                                    f"({company_terms})",
                                    f"({query_terms})" if query_terms else "",
                                    location_clause,
                                    family_clause,
                                    query_filters,
                                    site_filters,
                                ),
                            )
        return plans

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

    async def run(
        self,
        brief: SearchBrief,
        slices: List[SearchSlice],
        limit: int,
        dry_run: bool,
        exclude_queries: set[str] | None = None,
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

        candidates: List[CandidateProfile] = []
        request_count = 0
        errors: List[str] = []
        executed_queries = 0

        async with httpx.AsyncClient(timeout=self.request_timeout_seconds) as client:
            for slice_config in slices:
                if len(candidates) >= limit:
                    break

                for plan in self._build_query_plans(brief, slice_config):
                    search_query = plan["search"]
                    family = plan["family"]
                    if self.max_queries > 0 and executed_queries >= self.max_queries:
                        diagnostics["query_budget_exhausted"] = True
                        break
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
                    executed_queries += 1
                    for page in range(1, self.pages_per_query + 1):
                        response = None
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
                                request_count += 1
                            except httpx.HTTPError as exc:
                                if attempt < self.max_retries:
                                    await asyncio.sleep(self.retry_backoff_seconds * (attempt + 1))
                                    continue
                                errors.append(f"{slice_config.id}: {type(exc).__name__} {exc}")
                                response = None
                                break

                            if response.status_code < 400:
                                break

                            if (
                                response.status_code in {429, 500, 502, 503, 504}
                                and attempt < self.max_retries
                            ):
                                await asyncio.sleep(self.retry_backoff_seconds * (attempt + 1))
                                continue

                            errors.append(
                                f"{slice_config.id}: HTTP {response.status_code} {response.text[:240]}"
                            )
                            response = None
                            break

                        if response is None:
                            break

                        payload = response.json()
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
                            candidates.append(candidate)
                            if len(candidates) >= limit:
                                break
                        if len(candidates) >= limit:
                            break
                    if len(candidates) >= limit:
                        break
                if diagnostics.get("query_budget_exhausted"):
                    break

        diagnostics["query_budget"]["executed_per_family"] = family_query_counts
        diagnostics["query_budget"]["skipped_per_family"] = skipped_per_family

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
