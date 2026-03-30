from __future__ import annotations

import os
from typing import Any, Dict, List

import httpx

from hr_hunter.briefing import normalize_text
from hr_hunter.config import resolve_secret
from hr_hunter.models import CandidateProfile, ProviderRunResult, SearchBrief, SearchSlice
from hr_hunter.providers.base import SearchProvider


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

    def is_configured(self) -> bool:
        return self.client.is_configured()

    def build_search_queries(self, brief: SearchBrief, slice_config: SearchSlice) -> List[str]:
        title_values = slice_config.titles if slice_config.search_mode == "strict" else slice_config.title_keywords
        title_terms = " OR ".join(f'"{title}"' for title in title_values[:8] if title)
        location_terms = " OR ".join(
            f'"{hint}"' for hint in ([brief.geography.location_name] + brief.geography.location_hints[:3])
        )
        queries = []
        for company in slice_config.companies:
            company_aliases = brief.company_aliases.get(company, [company])
            company_terms = " OR ".join(f'"{alias}"' for alias in company_aliases[:3] if alias)
            queries.append(
                f"({title_terms}) ({company_terms}) ({location_terms}) "
                "(site:linkedin.com/in/ OR site:ie.linkedin.com/in/) "
                "-site:linkedin.com/jobs -site:ie.linkedin.com/jobs "
                "-site:linkedin.com/company -site:ie.linkedin.com/company "
                "-site:linkedin.com/posts -site:ie.linkedin.com/posts"
            )
        return queries

    def _find_company_match(self, text: str, brief: SearchBrief) -> str:
        lowered = normalize_text(text)
        for company, aliases in brief.company_aliases.items():
            for alias in aliases:
                if normalize_text(alias) and normalize_text(alias) in lowered:
                    return company
        return ""

    def _candidate_from_result(
        self,
        result: Dict[str, Any],
        brief: SearchBrief,
    ) -> CandidateProfile:
        title = result.get("title", "")
        description = result.get("description") or result.get("snippet") or ""
        url = result.get("url") or result.get("link")
        location_name = brief.geography.location_name if "ireland" in normalize_text(description) else ""

        name_guess = title.split(" - ")[0].split(" | ")[0].strip()
        current_company = self._find_company_match(f"{title} {description}", brief)

        return CandidateProfile(
            full_name=name_guess,
            current_title=title,
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
    ) -> ProviderRunResult:
        diagnostics = {"queries": []}
        if dry_run:
            for slice_config in slices:
                for search_query in self.build_search_queries(brief, slice_config):
                    diagnostics["queries"].append(
                        {"slice_id": slice_config.id, "search": search_query}
                    )
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

        async with httpx.AsyncClient(timeout=30.0) as client:
            for slice_config in slices:
                if len(candidates) >= limit:
                    break

                for search_query in self.build_search_queries(brief, slice_config):
                    diagnostics["queries"].append({"slice_id": slice_config.id, "search": search_query})
                    for page in range(1, self.pages_per_query + 1):
                        response = await self.client.search(
                            client,
                            search_query,
                            page=page,
                            country_code=self.country_code,
                            language=self.language,
                            light_request=self.light_request,
                        )
                        request_count += 1
                        if response.status_code >= 400:
                            errors.append(
                                f"{slice_config.id}: HTTP {response.status_code} {response.text[:240]}"
                            )
                            break

                        payload = response.json()
                        for result in payload.get("organic_results", []):
                            url = result.get("url") or result.get("link") or ""
                            if "linkedin.com" not in url:
                                continue
                            if "/in/" not in url:
                                continue
                            candidates.append(self._candidate_from_result(result, brief))
                            if len(candidates) >= limit:
                                break
                        if len(candidates) >= limit:
                            break
                    if len(candidates) >= limit:
                        break

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
