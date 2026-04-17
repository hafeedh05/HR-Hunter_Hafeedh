from __future__ import annotations

from hr_hunter_transformer.models import QueryPlan, QueryTask, RoleUnderstanding, SearchBrief
from hr_hunter_transformer.query_profiles import resolve_query_profile
from hr_hunter_transformer.role_profiles import (
    infer_role_family_with_confidence,
    normalize_text,
    role_subfamily,
    title_variants,
)


SOURCE_PACK_QUERIES: dict[str, tuple[str, ...]] = {
    "professional": ("site:linkedin.com/in", "site:ae.linkedin.com/in", "site:people.bayt.com", "site:theorg.com"),
    "technical": ("site:github.com", "site:huggingface.co", "site:kaggle.com", "site:stackoverflow.com/users"),
    "leadership": ("site:linkedin.com/in", "site:theorg.com", "site:zoominfo.com"),
    "news": ("site:forbes.com", "site:arabianbusiness.com", "site:gulfnews.com"),
    "operations": ("site:linkedin.com/in", "site:people.bayt.com"),
    "finance": ("site:linkedin.com/in", "site:people.bayt.com"),
    "marketing": ("site:linkedin.com/in", "site:people.bayt.com"),
    "design": ("site:linkedin.com/in", "site:behance.net", "site:archinect.com", "site:architizer.com"),
    "hr": ("site:linkedin.com/in", "site:people.bayt.com"),
    "sales": ("site:linkedin.com/in", "site:people.bayt.com", "site:apollo.io"),
    "customer": ("site:linkedin.com/in", "site:people.bayt.com", "site:theorg.com"),
    "healthcare": ("site:linkedin.com/in", "site:doximity.com", "site:healthgrades.com", "site:apollo.io"),
    "legal": ("site:linkedin.com/in", "site:martindale.com", "site:theorg.com"),
    "education": ("site:linkedin.com/in", "site:scholar.google.com", "site:researchgate.net"),
    "research": ("site:researchgate.net", "site:scholar.google.com", "site:linkedin.com/in"),
    "general": ("site:linkedin.com/in", "site:people.bayt.com", "site:theorg.com"),
}


def understand_role(brief: SearchBrief) -> RoleUnderstanding:
    family, family_confidence = infer_role_family_with_confidence(
        brief.role_title,
        *brief.titles,
        *brief.required_keywords,
        *brief.preferred_keywords,
        *brief.industry_keywords,
    )
    subfamily = role_subfamily(family, brief.role_title)
    variants = title_variants(family, brief.role_title, brief.titles)
    adjacent = variants[1:6]
    inferred_skills = list(dict.fromkeys([*brief.required_keywords[:6], *brief.preferred_keywords[:6], *brief.industry_keywords[:4]]))
    normalized_title = variants[0] if variants else brief.role_title
    complexity = "hard" if family in {"executive", "technical_ai"} else "dense" if family in {"supply_chain", "finance", "marketing", "data", "design_architecture"} else "balanced"
    return RoleUnderstanding(
        normalized_title=normalized_title,
        role_family=family,
        role_subfamily=subfamily,
        family_confidence=family_confidence,
        title_variants=variants[:12],
        adjacent_titles=adjacent,
        inferred_skills=inferred_skills,
        seniority_hint="executive" if family == "executive" else "manager",
        search_complexity=complexity,
    )


def build_query_plan(brief: SearchBrief) -> QueryPlan:
    understanding = understand_role(brief)
    profile = resolve_query_profile(
        understanding.role_family,
        brief.target_count,
        family_confidence=understanding.family_confidence,
    )
    geographies = [value for value in [*brief.cities[:6], *brief.countries[:6]] if str(value).strip()] or [""]
    executive_search = understanding.role_family == "executive"
    large_executive_search = executive_search and brief.target_count >= 500
    strict_company_scope = bool(brief.exact_company_scope and brief.company_targets)
    strict_market_scope = bool(brief.strict_market_scope and any(geo for geo in geographies if str(geo).strip()))
    must_company_limit = 24 if large_executive_search else 24 if executive_search else 8
    peer_company_limit = 16 if large_executive_search else 18 if executive_search else 8
    must_companies = []
    for value in brief.company_targets[:must_company_limit]:
        text = str(value or "").strip()
        if text and text not in must_companies:
            must_companies.append(text)
    peer_companies = []
    if not strict_company_scope:
        for value in brief.peer_company_targets[:peer_company_limit]:
            text = str(value or "").strip()
            if text and text not in must_companies and text not in peer_companies:
                peer_companies.append(text)
    companies = [*must_companies, *peer_companies]
    skills = [value for value in understanding.inferred_skills[:6] if str(value).strip()]
    queries: list[QueryTask] = []
    seen: set[str] = set()

    def add(query_text: str, query_type: str, source_pack: str, *, page_budget: int | None = None) -> None:
        normalized = normalize_text(query_text)
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        queries.append(
            QueryTask(
                query_text=query_text,
                query_type=query_type,
                source_pack=source_pack,
                page_budget=page_budget if page_budget is not None else profile.pages_per_query,
            )
        )

    source_sites: list[str] = []
    for pack in profile.source_packs:
        source_sites.extend(SOURCE_PACK_QUERIES.get(pack, ()))
    if not source_sites:
        source_sites = list(SOURCE_PACK_QUERIES["general"])
    if executive_search and companies:
        source_sites = [
            site
            for site in source_sites
            if site in {"site:linkedin.com/in", "site:ae.linkedin.com/in", "site:theorg.com"}
        ] or ("site:linkedin.com/in", "site:theorg.com")

    explicit_titles = []
    for value in [brief.role_title, *brief.titles]:
        text = str(value or "").strip()
        if text and normalize_text(text) not in {normalize_text(existing) for existing in explicit_titles}:
            explicit_titles.append(text)
    exact_titles = (
        explicit_titles[:8]
        if (executive_search or not brief.allow_adjacent_titles)
        else understanding.title_variants[:8]
    ) or [brief.role_title]
    adjacent_titles = []
    if brief.allow_adjacent_titles and profile.adjacent_titles_enabled:
        if executive_search and companies:
            adjacent_titles = understanding.adjacent_titles[:2 if large_executive_search else 1]
        else:
            adjacent_titles = understanding.adjacent_titles[:4]

    if must_companies:
        priority_title_count = 3 if large_executive_search else 4 if executive_search else 3
        priority_geo_count = 2 if large_executive_search else 2
        executive_title_clause = " OR ".join(f'"{title}"' for title in exact_titles[:3 if large_executive_search else 4]) if executive_search else ""
        for company in must_companies:
            if executive_title_clause and not strict_market_scope:
                add(
                    f'site:linkedin.com/in ({executive_title_clause}) "{company}"',
                    "company_leadership_priority",
                    "professional",
                    page_budget=2 if large_executive_search else None,
                )
                for geography in geographies[:priority_geo_count]:
                    if geography:
                        add(
                            f'site:linkedin.com/in ({executive_title_clause}) "{company}" "{geography}"',
                            "company_leadership_geo_priority",
                            "professional",
                            page_budget=2 if large_executive_search else None,
                        )
                if large_executive_search:
                    add(
                        f'site:theorg.com ({executive_title_clause}) "{company}"',
                        "company_org_priority",
                        "leadership",
                    )
            for title in exact_titles[:priority_title_count]:
                if not strict_market_scope:
                    add(
                        f'site:linkedin.com/in "{title}" "{company}"',
                        "company_exact_priority",
                        "professional",
                        page_budget=2 if large_executive_search and title == exact_titles[0] else None,
                    )
                    if large_executive_search:
                        add(
                            f'site:theorg.com "{title}" "{company}"',
                            "company_org_title_priority",
                            "leadership",
                        )
                for geography in geographies[:priority_geo_count]:
                    if geography:
                        add(
                            f'site:linkedin.com/in "{title}" "{company}" "{geography}"',
                            "company_geo_priority",
                            "professional",
                            page_budget=2 if large_executive_search and title == exact_titles[0] else None,
                        )

    if peer_companies:
        peer_title_count = 2 if large_executive_search else 2
        peer_geo_count = 2 if large_executive_search else 2
        executive_title_clause = " OR ".join(f'"{title}"' for title in exact_titles[:3 if large_executive_search else 3]) if executive_search else ""
        for company in peer_companies:
            if executive_title_clause and not strict_market_scope:
                add(
                    f'site:linkedin.com/in ({executive_title_clause}) "{company}"',
                    "peer_leadership_priority",
                    "professional",
                    page_budget=2 if large_executive_search else None,
                )
                for geography in geographies[:peer_geo_count]:
                    if geography:
                        add(
                            f'site:linkedin.com/in ({executive_title_clause}) "{company}" "{geography}"',
                            "peer_leadership_geo_priority",
                            "professional",
                            page_budget=2 if large_executive_search else None,
                        )
                if large_executive_search:
                    add(
                        f'site:theorg.com ({executive_title_clause}) "{company}"',
                        "peer_org_priority",
                        "leadership",
                    )
            for title in exact_titles[:peer_title_count]:
                if not strict_market_scope:
                    add(
                        f'site:linkedin.com/in "{title}" "{company}"',
                        "peer_company_exact_priority",
                        "professional",
                        page_budget=2 if large_executive_search and title == exact_titles[0] else None,
                    )
                    if large_executive_search:
                        add(
                            f'site:theorg.com "{title}" "{company}"',
                            "peer_org_title_priority",
                            "leadership",
                        )
                for geography in geographies[:peer_geo_count]:
                    if geography:
                        add(
                            f'site:linkedin.com/in "{title}" "{company}" "{geography}"',
                            "peer_company_geo_priority",
                            "professional",
                            page_budget=2 if large_executive_search and title == exact_titles[0] else None,
                        )

    generic_title_limit = 4 if large_executive_search else 3 if executive_search else len(exact_titles)
    generic_geo_limit = 2 if large_executive_search and companies else 2 if executive_search and companies else 4
    generic_site_budget = min(profile.source_site_budget, 2 if large_executive_search and companies else 2 if executive_search and companies else profile.source_site_budget)
    if not strict_company_scope:
        for title in exact_titles[:generic_title_limit]:
            for geography in geographies[:generic_geo_limit]:
                query = " ".join(
                    part
                    for part in [f'"{title}"', f'"{geography}"' if geography else "", *profile.family_terms[: profile.family_term_budget]]
                    if part
                )
                add(query, "exact_title_geo", "general")
                for site in source_sites[: generic_site_budget]:
                    if strict_market_scope and not geography:
                        continue
                    add(f'{site} "{title}" "{geography}"' if geography else f'{site} "{title}"', "exact_title_source", site)

    for title in adjacent_titles:
        for geography in geographies[:2 if executive_search else 3]:
            add(
                " ".join(
                    part
                    for part in [f'"{title}"', f'"{geography}"' if geography else "", *profile.family_terms[: profile.family_term_budget]]
                    if part
                ),
                "adjacent_title_geo",
                "general",
            )

    if not executive_search:
        for title in exact_titles[:4]:
            for company in companies:
                if not strict_market_scope:
                    add(f'site:linkedin.com/in "{title}" "{company}"', "company_exact", "professional")
                for geography in geographies[:3]:
                    if geography:
                        add(f'site:linkedin.com/in "{title}" "{company}" "{geography}"', "company_geo", "professional")

    for title in exact_titles[:4]:
        for skill in skills[:4]:
            for geography in geographies[:3]:
                add(
                    " ".join(part for part in [f'"{title}"', f'"{skill}"', f'"{geography}"' if geography else ""] if part),
                    "skill_geo",
                    "general",
                )

    for title in exact_titles[: (3 if executive_search else 4)]:
        for industry in brief.industry_keywords[: (2 if executive_search else 3)]:
            add(f'site:linkedin.com/in "{title}" "{industry}"', "industry_exact", "professional")

    return QueryPlan(
        role_understanding=understanding,
        queries=queries[: profile.max_queries],
        max_queries=profile.max_queries,
        pages_per_query=profile.pages_per_query,
        parallel_requests=profile.parallel_requests,
    )
