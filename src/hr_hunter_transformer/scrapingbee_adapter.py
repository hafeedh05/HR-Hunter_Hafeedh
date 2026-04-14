from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable
from typing import Iterable
from urllib.parse import urlparse

import httpx

from hr_hunter_transformer.config import resolve_secret
from hr_hunter_transformer.models import QueryPlan, QueryTask, RawSearchHit, SearchBrief
from hr_hunter_transformer.query_planner import build_query_plan
from hr_hunter_transformer.role_profiles import PROFESSIONAL_SOURCES, infer_role_family, normalize_text, role_family_hints


TECHNICAL_SITE_QUERIES = (
    "site:github.com",
    "site:huggingface.co",
    "site:kaggle.com",
    "site:stackoverflow.com/users",
    "site:gitlab.com",
    "site:dev.to",
)
PROFESSIONAL_SITE_QUERIES = (
    "site:linkedin.com/in",
    "site:ae.linkedin.com/in",
    "site:sa.linkedin.com/in",
    "site:theorg.com",
    "site:people.bayt.com",
)
SUPPLY_CHAIN_FAMILY_TERMS = (
    '"demand planning"',
    '"inventory"',
    '"s&op"',
    '"logistics"',
    '"procurement"',
)
FINANCE_FAMILY_TERMS = (
    '"accounting"',
    '"finance"',
    '"controller"',
    '"erp"',
)
DESIGN_ARCH_FAMILY_TERMS = (
    '"interior design"',
    '"architecture"',
    '"design manager"',
    '"fit-out"',
    '"space planning"',
)
ROLE_FAMILY_TERMS: dict[str, tuple[str, ...]] = {
    "operations_process": ('"operations"', '"process improvement"', '"service delivery"', '"operational excellence"'),
    "sales_business_development": ('"sales"', '"business development"', '"account management"', '"partnerships"'),
    "customer_service_success": ('"customer success"', '"client relations"', '"support"', '"service delivery"'),
    "hr_talent": ('"talent acquisition"', '"recruitment"', '"human resources"', '"people operations"'),
    "product_management": ('"product strategy"', '"product roadmap"', '"product owner"', '"platform product"'),
    "project_program_management": ('"project delivery"', '"program management"', '"pmo"', '"agile delivery"'),
    "procurement_sourcing": ('"procurement"', '"sourcing"', '"vendor management"', '"category management"'),
    "manufacturing_production": ('"manufacturing"', '"production"', '"plant operations"', '"quality"'),
    "engineering_non_it": ('"engineering"', '"mechanical"', '"electrical"', '"civil"'),
    "construction_facilities": ('"construction"', '"facilities"', '"maintenance"', '"site management"'),
    "healthcare_medical": ('"clinical"', '"healthcare"', '"medical"', '"patient care"'),
    "education_training": ('"teaching"', '"training"', '"instructional design"', '"learning"'),
    "legal_compliance": ('"legal"', '"compliance"', '"contracts"', '"regulatory"'),
    "risk_audit_security": ('"internal audit"', '"risk"', '"cybersecurity"', '"information security"'),
    "research_development": ('"research"', '"innovation"', '"r&d"', '"laboratory"'),
    "design_creative": ('"design"', '"creative"', '"ux"', '"branding"'),
    "media_communications": ('"communications"', '"public relations"', '"content"', '"journalism"'),
    "admin_office_support": ('"administration"', '"office support"', '"executive support"', '"coordination"'),
    "hospitality_tourism": ('"hospitality"', '"guest experience"', '"travel"', '"hotel operations"'),
    "retail_merchandising": ('"retail"', '"merchandising"', '"store operations"', '"category"'),
    "real_estate_property": ('"real estate"', '"property"', '"leasing"', '"brokerage"'),
    "public_sector_government": ('"public sector"', '"government"', '"policy"', '"municipal"'),
    "agriculture_environment": ('"sustainability"', '"environment"', '"agriculture"', '"hse"'),
    "transportation_mobility": ('"transport"', '"mobility"', '"fleet"', '"aviation"'),
}

PROFILE_QUERY_TERMS = (
    '"profile"',
    '"speaker"',
    '"team"',
    '"leadership"',
)

GENERAL_PROFILE_QUERY_TERMS = (
    '"profile"',
    '"bio"',
    '"resume"',
    '"cv"',
    '"team"',
    '"people"',
)


@dataclass(slots=True)
class ScrapingBeeSearchConfig:
    api_key: str | None = None
    endpoint: str = "https://app.scrapingbee.com/api/v1/store/google"
    pages_per_query: int = 1
    parallel_requests: int = 8
    country_code: str = "ae"
    language: str = "en"
    light_request: bool = True
    timeout_seconds: float = 30.0
    max_queries: int = 60
    max_retries: int = 2
    retry_backoff_seconds: float = 1.5

    def __post_init__(self) -> None:
        if not self.api_key:
            self.api_key = resolve_secret("SCRAPINGBEE_API_KEY")


class ScrapingBeeTransformerRetriever:
    def __init__(self, config: ScrapingBeeSearchConfig | None = None) -> None:
        self.config = config or ScrapingBeeSearchConfig(api_key=resolve_secret("SCRAPINGBEE_API_KEY"))
        self._usage: dict[str, int] = {
            "queries_total": 0,
            "queries_completed": 0,
            "request_pages_attempted": 0,
            "request_pages_succeeded": 0,
            "retry_count": 0,
            "raw_found": 0,
        }

    def is_configured(self) -> bool:
        return bool(self.config.api_key)

    def build_queries(self, brief: SearchBrief) -> list[str]:
        return [task.query_text for task in build_query_plan(brief).queries]

    def _resolve_plan(self, brief: SearchBrief, query_plan: QueryPlan | None = None) -> QueryPlan:
        if query_plan is not None:
            return query_plan
        return build_query_plan(brief)

    def _page_budget_for_query(self, query_plan: QueryPlan, query_text: str) -> int:
        normalized = normalize_text(query_text)
        for task in query_plan.queries:
            if normalize_text(task.query_text) == normalized:
                return max(1, int(task.page_budget or query_plan.pages_per_query))
        return max(1, int(query_plan.pages_per_query))

    def _legacy_build_queries(self, brief: SearchBrief) -> list[str]:
        titles = brief.titles or [brief.role_title]
        countries = brief.countries or [""]
        cities = brief.cities or [""]
        keywords = [*brief.required_keywords[:4], *brief.preferred_keywords[:2]]
        requested_family = infer_role_family(brief.role_title, *brief.titles)
        family_hints = list(role_family_hints(requested_family)) or titles
        queries: list[str] = []
        geography_values = [value for value in [*cities[:6], *countries[:6]] if value]
        primary_geographies = geography_values[:3] or [""]
        primary_keywords = keywords[:4]
        primary_industries = brief.industry_keywords[:4]
        profile_terms = PROFILE_QUERY_TERMS if requested_family != "other" else GENERAL_PROFILE_QUERY_TERMS

        professional_sites = list(PROFESSIONAL_SITE_QUERIES)
        if requested_family == "technical_ai":
            professional_sites.extend(TECHNICAL_SITE_QUERIES)

        family_terms: list[str] = []
        if requested_family == "supply_chain":
            family_terms.extend(SUPPLY_CHAIN_FAMILY_TERMS)
        elif requested_family == "finance":
            family_terms.extend(FINANCE_FAMILY_TERMS)
        elif requested_family == "design_architecture":
            family_terms.extend(DESIGN_ARCH_FAMILY_TERMS)
        elif requested_family in ROLE_FAMILY_TERMS:
            family_terms.extend(ROLE_FAMILY_TERMS[requested_family])
        else:
            family_terms.extend(f'"{keyword}"' for keyword in keywords[:4] if keyword)
        family_query_terms = family_terms[:2]
        if not family_query_terms and primary_keywords:
            family_query_terms = [f'"{primary_keywords[0]}"']
        if not family_query_terms:
            family_query_terms = [""]

        for title in titles[:6]:
            for geography in geography_values[:8] or [""]:
                parts = [f'"{title}"']
                if geography:
                    parts.append(f'"{geography}"')
                parts.extend(profile_terms[:1])
                if family_terms:
                    parts.append(family_terms[0])
                queries.append(" ".join(part for part in parts if part))

        for site_query in professional_sites:
            for title in titles[:6]:
                for geography in primary_geographies:
                    for family_term in family_query_terms:
                        base = [site_query, f'"{title}"']
                        if geography:
                            base.append(f'"{geography}"')
                        if family_term:
                            base.append(family_term)
                        queries.append(" ".join(base))

        for title in family_hints[:6]:
            for company in brief.company_targets[:8]:
                queries.append(f'site:linkedin.com/in "{title}" "{company}"')
                for geography in primary_geographies:
                    if geography:
                        queries.append(f'site:linkedin.com/in "{title}" "{company}" "{geography}"')
                queries.append(f'site:people.bayt.com "{title}" "{company}"')
                for family_term in family_terms[:2]:
                    queries.append(f'site:linkedin.com/in "{title}" "{company}" {family_term}')
                    queries.append(f'site:people.bayt.com "{title}" "{company}" {family_term}')

        if requested_family == "technical_ai":
            for title in titles[:4]:
                for site_query in TECHNICAL_SITE_QUERIES:
                    parts = [site_query, f'"{title}"']
                    if keywords:
                        parts.append(f'"{keywords[0]}"')
                    if brief.countries:
                        parts.append(f'"{brief.countries[0]}"')
                    queries.append(" ".join(parts))

        if brief.industry_keywords:
            for title in titles[:5]:
                for industry in brief.industry_keywords[:4]:
                    queries.append(f'site:linkedin.com/in "{title}" "{industry}"')
                    queries.append(f'"{title}" "{industry}" {" ".join(profile_terms[:1])}')

        for title in titles[:5]:
            for geography in geography_values[:6]:
                for family_term in family_terms[:4]:
                    queries.append(f'site:linkedin.com/in "{title}" "{geography}" {family_term}')
                    queries.append(f'site:theorg.com "{title}" "{geography}" {family_term}')
                    queries.append(f'site:people.bayt.com "{title}" "{geography}" {family_term}')
                    if requested_family == "design_architecture":
                        queries.append(f'site:behance.net "{title}" "{geography}" {family_term}')

        if requested_family == "other":
            for title in titles[:6]:
                for geography in primary_geographies:
                    for keyword in primary_keywords[:3]:
                        queries.append(f'site:linkedin.com/in "{title}" "{geography}" "{keyword}"')
                        queries.append(f'site:people.bayt.com "{title}" "{geography}" "{keyword}"')
                        queries.append(f'"{title}" "{geography}" "{keyword}" {" ".join(profile_terms[:1])}')
                    for industry in primary_industries[:3]:
                        queries.append(f'site:linkedin.com/in "{title}" "{geography}" "{industry}"')
                        queries.append(f'"{title}" "{geography}" "{industry}" {" ".join(profile_terms[:1])}')
                for company in brief.company_targets[:8]:
                    queries.append(f'site:linkedin.com/in "{title}" "{company}"')
                    queries.append(f'site:people.bayt.com "{title}" "{company}"')
                    for geography in primary_geographies:
                        if geography:
                            queries.append(f'site:linkedin.com/in "{title}" "{company}" "{geography}"')
            if not brief.company_targets:
                for geography in primary_geographies:
                    for keyword in primary_keywords[:3]:
                        queries.append(f'"{brief.role_title}" "{geography}" "{keyword}" {" ".join(GENERAL_PROFILE_QUERY_TERMS[:2])}')
                    for industry in primary_industries[:3]:
                        queries.append(f'"{brief.role_title}" "{geography}" "{industry}" {" ".join(GENERAL_PROFILE_QUERY_TERMS[:2])}')

        deduped: list[str] = []
        seen: set[str] = set()
        for query in queries:
            normalized = normalize_text(query)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(query)
        return deduped[: self.config.max_queries]

    def parse_payload(self, payload: dict, *, query: str) -> list[RawSearchHit]:
        hits: list[RawSearchHit] = []
        for index, result in enumerate(payload.get("organic_results", [])):
            url = str(result.get("url") or result.get("link") or "").strip()
            if not url:
                continue
            domain = urlparse(url).netloc.lower().removeprefix("www.")
            if domain and not (
                domain in PROFESSIONAL_SOURCES
                or domain in {"github.com", "gitlab.com", "huggingface.co", "kaggle.com", "stackoverflow.com", "dev.to", "medium.com"}
                or "/in/" in urlparse(url).path.lower()
                or "/people/" in urlparse(url).path.lower()
                or "/consultant/" in urlparse(url).path.lower()
            ):
                continue
            hits.append(
                RawSearchHit(
                    title=str(result.get("title") or "").strip(),
                    snippet=str(result.get("description") or result.get("snippet") or "").strip(),
                    url=url,
                    source="scrapingbee_google",
                    metadata={
                        "query": query,
                        "position": index + 1,
                        "displayed_link": result.get("displayed_link") or result.get("displayedLink") or "",
                        "domain": domain,
                    },
                )
            )
        return hits

    async def _fetch_query(self, client: httpx.AsyncClient, query: str, *, page_budget: int) -> list[RawSearchHit]:
        hits: list[RawSearchHit] = []
        for page in range(1, page_budget + 1):
            attempt = 0
            while True:
                self._usage["request_pages_attempted"] += 1
                try:
                    response = await client.get(
                        self.config.endpoint,
                        params={
                            "api_key": self.config.api_key or "",
                            "search": query,
                            "page": page,
                            "country_code": self.config.country_code,
                            "language": self.config.language,
                            "light_request": str(self.config.light_request).lower(),
                        },
                    )
                    response.raise_for_status()
                    page_hits = self.parse_payload(response.json(), query=query)
                    hits.extend(page_hits)
                    self._usage["request_pages_succeeded"] += 1
                    self._usage["raw_found"] += len(page_hits)
                    break
                except (httpx.TimeoutException, httpx.HTTPError):
                    attempt += 1
                    if attempt > self.config.max_retries:
                        break
                    self._usage["retry_count"] += 1
                    await asyncio.sleep(self.config.retry_backoff_seconds * attempt)
        return hits

    async def search_async(
        self,
        brief: SearchBrief,
        *,
        query_plan: QueryPlan | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> tuple[list[str], list[RawSearchHit]]:
        if not self.is_configured():
            raise RuntimeError("Missing SCRAPINGBEE_API_KEY for transformer retriever.")
        plan = self._resolve_plan(brief, query_plan)
        queries = [task.query_text for task in plan.queries]
        self._usage = {
            "queries_total": len(queries),
            "queries_completed": 0,
            "request_pages_attempted": 0,
            "request_pages_succeeded": 0,
            "retry_count": 0,
            "raw_found": 0,
        }
        timeout = httpx.Timeout(self.config.timeout_seconds)
        semaphore = asyncio.Semaphore(plan.parallel_requests)
        hits: list[RawSearchHit] = []

        async with httpx.AsyncClient(timeout=timeout) as client:
            async def run_query(task: QueryTask) -> tuple[str, list[RawSearchHit]]:
                async with semaphore:
                    return task.query_text, await self._fetch_query(
                        client,
                        task.query_text,
                        page_budget=max(1, int(task.page_budget or plan.pages_per_query)),
                    )

            tasks = [asyncio.create_task(run_query(task)) for task in plan.queries]
            for completed_task in asyncio.as_completed(tasks):
                try:
                    query, query_hits = await completed_task
                except Exception:
                    query = ""
                    query_hits = []
                self._usage["queries_completed"] += 1
                hits.extend(query_hits)
                if progress_callback:
                    progress_callback(
                        {
                            "stage": "retrieval_running",
                            "message": (
                                f"Running transformer retrieval. "
                                f"{self._usage['queries_completed']}/{self._usage['queries_total']} queries complete."
                            ),
                            "queries_total": self._usage["queries_total"],
                            "queries_completed": self._usage["queries_completed"],
                            "queries_in_flight": max(0, self._usage["queries_total"] - self._usage["queries_completed"]),
                            "raw_found": self._usage["raw_found"],
                            "percent": max(
                                8,
                                min(
                                    72,
                                    8 + int(round((self._usage["queries_completed"] / max(1, self._usage["queries_total"])) * 64)),
                                ),
                            ),
                            "current_query": query,
                        }
                    )
        return queries, hits

    def usage_summary(self) -> dict[str, int]:
        return dict(self._usage)

    def search(self, brief: SearchBrief) -> tuple[list[str], list[RawSearchHit]]:
        return asyncio.run(self.search_async(brief))
