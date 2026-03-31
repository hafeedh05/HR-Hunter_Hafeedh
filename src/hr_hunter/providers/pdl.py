from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any, Dict, List, Optional

import httpx

from hr_hunter.briefing import normalize_text
from hr_hunter.models import CandidateProfile, ProviderRunResult, SearchBrief, SearchSlice
from hr_hunter.providers.base import SearchProvider


class PDLProvider(SearchProvider):
    name = "pdl"
    endpoint = "https://api.peopledatalabs.com/v5/person/search"

    def __init__(self, settings: Dict[str, Any]):
        super().__init__(settings)
        self.api_key = os.getenv("PDL_API_KEY") or os.getenv("PEOPLEDATALABS_API_KEY")
        self.rate_limit_per_minute = int(settings.get("rate_limit_per_minute", 10))
        self.dataset = settings.get("dataset", "all")
        self.batch_size = int(settings.get("batch_size", 50))
        self._last_request_at = 0.0

    @staticmethod
    def _literal(value: str) -> str:
        return str(value).strip().lower()

    def is_configured(self) -> bool:
        return bool(self.api_key)

    async def _respect_rate_limit(self) -> None:
        if self.rate_limit_per_minute <= 0:
            return
        minimum_interval = 60.0 / self.rate_limit_per_minute
        now = time.monotonic()
        remaining = minimum_interval - (now - self._last_request_at)
        if remaining > 0:
            await asyncio.sleep(remaining)
        self._last_request_at = time.monotonic()

    def _company_clauses(self, brief: SearchBrief, slice_config: SearchSlice) -> List[Dict[str, Any]]:
        clauses: List[Dict[str, Any]] = []
        for company in slice_config.companies:
            aliases = brief.company_aliases.get(company, [company])
            for alias in aliases:
                lowered = self._literal(alias)
                if not lowered:
                    continue
                clauses.append({"term": {"job_company_name": lowered}})
                clauses.append({"match_phrase": {"job_company_name": lowered}})
                if slice_config.search_mode == "broad":
                    clauses.append({"match_phrase": {"experience.company.name": lowered}})
        return clauses

    def _title_clauses(self, slice_config: SearchSlice) -> List[Dict[str, Any]]:
        clauses: List[Dict[str, Any]] = []
        title_values = list(slice_config.titles)
        if slice_config.search_mode == "broad":
            title_values.extend(slice_config.title_keywords)

        for title in title_values:
            lowered = self._literal(title)
            if not lowered:
                continue
            clauses.append({"term": {"job_title": lowered}})
            clauses.append({"match_phrase": {"job_title": lowered}})

        for keyword in slice_config.title_keywords:
            lowered = self._literal(keyword)
            if lowered:
                clauses.append({"match_phrase": {"headline": lowered}})

        return clauses

    def build_query(self, brief: SearchBrief, slice_config: SearchSlice) -> Dict[str, Any]:
        filters: List[Dict[str, Any]] = []
        if brief.geography.country:
            filters.append({"term": {"location_country": self._literal(brief.geography.country)}})

        must_clauses = []
        company_clauses = self._company_clauses(brief, slice_config)
        if company_clauses:
            must_clauses.append(
                {
                    "bool": {
                        "should": company_clauses,
                        "minimum_should_match": 1,
                    }
                }
            )
        must_clauses.append(
            {
                "bool": {
                    "should": self._title_clauses(slice_config),
                    "minimum_should_match": 1,
                }
            }
        )

        should_clauses: List[Dict[str, Any]] = []
        for level in brief.seniority_levels:
            lowered = self._literal(level)
            if lowered:
                should_clauses.append({"match_phrase": {"job_title": lowered}})
        for keyword in brief.required_keywords + brief.preferred_keywords:
            lowered = self._literal(keyword)
            if lowered:
                should_clauses.append({"match_phrase": {"summary": lowered}})
                should_clauses.append({"match_phrase": {"headline": lowered}})
        for keyword in slice_config.query_keywords:
            lowered = self._literal(keyword)
            if lowered:
                should_clauses.append({"match_phrase": {"summary": lowered}})
                should_clauses.append({"match_phrase": {"headline": lowered}})
                should_clauses.append({"match_phrase": {"industry": lowered}})

        return {
            "query": {
                "bool": {
                    "filter": filters,
                    "must": must_clauses,
                    "should": should_clauses,
                    "minimum_should_match": 0,
                }
            }
        }

    def _build_params(
        self,
        query: Dict[str, Any],
        size: int,
        scroll_token: Optional[str] = None,
    ) -> Dict[str, str]:
        params = {
            "query": json.dumps(query),
            "size": str(size),
            "dataset": self.dataset,
            "titlecase": "true",
        }
        if scroll_token:
            params["scroll_token"] = scroll_token
        return params

    def _record_to_candidate(self, record: Dict[str, Any]) -> CandidateProfile:
        profiles = record.get("profiles") or []
        linkedin_url = record.get("linkedin_url")
        if not linkedin_url:
            for profile in profiles:
                url = profile.get("url")
                if url and "linkedin.com" in url:
                    linkedin_url = url
                    break

        summary_parts = [
            record.get("summary", ""),
            record.get("headline", ""),
            record.get("job_summary", ""),
        ]
        company = record.get("job_company_name", "")
        title = record.get("job_title", "")
        experience = record.get("experience") or []
        industry = record.get("job_company_industry") or record.get("industry")

        return CandidateProfile(
            full_name=record.get("full_name", ""),
            current_title=title or "",
            current_company=company or "",
            location_name=record.get("location_name", ""),
            location_geo=record.get("location_geo"),
            linkedin_url=linkedin_url,
            source=self.name,
            source_url=linkedin_url,
            summary=" ".join(part for part in summary_parts if part).strip(),
            industry=industry,
            years_experience=record.get("inferred_years_experience"),
            experience=experience,
            raw=record,
        )

    async def _execute_slice(
        self,
        client: httpx.AsyncClient,
        brief: SearchBrief,
        slice_config: SearchSlice,
        limit: int,
        query: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        query = query or self.build_query(brief, slice_config)
        candidates: List[CandidateProfile] = []
        request_count = 0
        errors: List[str] = []
        scroll_token: Optional[str] = None
        total = None

        while len(candidates) < limit:
            batch_size = min(self.batch_size, limit - len(candidates))
            await self._respect_rate_limit()
            response = await client.get(
                self.endpoint,
                params=self._build_params(query, batch_size, scroll_token=scroll_token),
                headers={"X-Api-Key": self.api_key or ""},
            )
            request_count += 1

            if response.status_code >= 400:
                errors.append(
                    f"{slice_config.id}: HTTP {response.status_code} {response.text[:240]}"
                )
                break

            payload = response.json()
            total = payload.get("total", total)
            records = payload.get("data") or []
            if not records:
                break

            candidates.extend(self._record_to_candidate(record) for record in records)
            scroll_token = payload.get("scroll_token")
            if not scroll_token:
                break

        return {
            "slice_id": slice_config.id,
            "query": query,
            "request_count": request_count,
            "candidate_count": len(candidates),
            "total": total,
            "errors": errors,
            "candidates": candidates,
        }

    async def run(
        self,
        brief: SearchBrief,
        slices: List[SearchSlice],
        limit: int,
        dry_run: bool,
        exclude_queries: set[str] | None = None,
    ) -> ProviderRunResult:
        excluded_queries = {value for value in (exclude_queries or set()) if value}
        diagnostics = {
            "queries": [],
            "rate_limit_per_minute": self.rate_limit_per_minute,
            "skipped_query_count": 0,
        }
        if dry_run:
            dry_run_queries = []
            for slice_config in slices:
                query = self.build_query(brief, slice_config)
                dry_run_queries.append(
                    {
                        "slice_id": slice_config.id,
                        "query": query,
                        "skipped": json.dumps(query, sort_keys=True) in excluded_queries,
                    }
                )
            diagnostics["queries"] = dry_run_queries
            diagnostics["skipped_query_count"] = len(
                [item for item in dry_run_queries if item.get("skipped")]
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
                errors=["Missing PDL_API_KEY or PEOPLEDATALABS_API_KEY."],
            )

        candidates: List[CandidateProfile] = []
        request_count = 0
        errors: List[str] = []

        async with httpx.AsyncClient(timeout=30.0) as client:
            for slice_config in slices:
                remaining = max(limit - len(candidates), 0)
                if remaining == 0:
                    break
                query = self.build_query(brief, slice_config)
                query_key = json.dumps(query, sort_keys=True)
                if query_key in excluded_queries:
                    diagnostics["skipped_query_count"] += 1
                    diagnostics["queries"].append(
                        {
                            "slice_id": slice_config.id,
                            "query": query,
                            "skipped": True,
                        }
                    )
                    continue

                slice_result = await self._execute_slice(
                    client,
                    brief,
                    slice_config,
                    min(slice_config.limit, remaining),
                    query=query,
                )
                diagnostics["queries"].append(
                    {
                        "slice_id": slice_result["slice_id"],
                        "query": slice_result["query"],
                        "total": slice_result["total"],
                        "candidate_count": slice_result["candidate_count"],
                    }
                )
                request_count += slice_result["request_count"]
                errors.extend(slice_result["errors"])
                candidates.extend(slice_result["candidates"])

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
