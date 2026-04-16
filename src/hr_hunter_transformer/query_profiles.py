from __future__ import annotations

from dataclasses import dataclass, field

from hr_hunter_transformer.family_learning import family_learning_stats


@dataclass(frozen=True)
class QueryProfile:
    max_queries: int
    pages_per_query: int
    parallel_requests: int
    source_packs: tuple[str, ...] = field(default_factory=tuple)
    family_terms: tuple[str, ...] = field(default_factory=tuple)
    adjacent_titles_enabled: bool = True
    source_site_budget: int = 4
    family_term_budget: int = 1
    adaptive_mode: str = "base"


QUERY_PROFILES: dict[str, QueryProfile] = {
    "executive": QueryProfile(140, 1, 6, ("professional", "leadership"), ('"executive"', '"board"'), True, 4, 1, "hard"),
    "technical_ai": QueryProfile(140, 2, 8, ("professional", "technical"), ('"machine learning"', '"artificial intelligence"', '"llm"'), True, 6, 2, "hard"),
    "supply_chain": QueryProfile(54, 1, 10, ("professional", "operations"), ('"s&op"', '"inventory"', '"logistics"'), True, 5, 2, "dense"),
    "finance": QueryProfile(54, 1, 10, ("professional", "finance"), ('"accounting"', '"finance"', '"controller"'), True, 5, 2, "dense"),
    "marketing": QueryProfile(60, 1, 10, ("professional", "marketing"), ('"growth"', '"campaign"', '"brand"'), True, 5, 2, "dense"),
    "data": QueryProfile(84, 2, 10, ("professional", "technical"), ('"data analyst"', '"business intelligence"', '"sql"', '"power bi"'), True, 6, 3, "dense"),
    "design_architecture": QueryProfile(100, 2, 8, ("professional", "design"), ('"interior design"', '"architecture"', '"fit-out"'), True, 5, 2, "strong"),
    "hr_talent": QueryProfile(70, 1, 10, ("professional", "hr"), ('"talent acquisition"', '"recruitment"', '"people partner"'), True, 5, 2, "balanced"),
    "operations_process": QueryProfile(72, 1, 9, ("professional", "operations"), ('"operations"', '"process"', '"service delivery"'), True, 5, 2, "balanced"),
    "sales_business_development": QueryProfile(72, 1, 10, ("professional", "sales"), ('"sales"', '"business development"', '"account management"'), True, 5, 2, "balanced"),
    "customer_service_success": QueryProfile(60, 1, 10, ("professional", "customer"), ('"customer success"', '"client service"', '"support"'), True, 5, 2, "balanced"),
    "product_management": QueryProfile(110, 2, 8, ("professional", "technical", "customer"), ('"product manager"', '"roadmap"', '"go-to-market"', '"user stories"'), True, 6, 3, "balanced"),
    "project_program_management": QueryProfile(110, 2, 8, ("professional", "operations"), ('"program manager"', '"project manager"', '"delivery"', '"pmo"'), True, 6, 3, "balanced"),
    "procurement_sourcing": QueryProfile(60, 1, 10, ("professional", "operations"), ('"procurement"', '"sourcing"', '"category"'), True, 5, 2, "dense"),
    "manufacturing_production": QueryProfile(72, 1, 8, ("professional", "operations"), ('"manufacturing"', '"production"', '"plant"'), True, 5, 2, "balanced"),
    "engineering_non_it": QueryProfile(120, 2, 8, ("professional", "technical", "operations"), ('"mechanical engineer"', '"electrical engineer"', '"mep"', '"maintenance"'), True, 6, 3, "balanced"),
    "construction_facilities": QueryProfile(96, 2, 8, ("professional", "operations"), ('"construction"', '"site"', '"facilities"'), True, 5, 2, "balanced"),
    "healthcare_medical": QueryProfile(110, 2, 8, ("professional", "healthcare"), ('"doctor"', '"physician"', '"hospital"'), True, 5, 2, "hard"),
    "education_training": QueryProfile(96, 2, 8, ("professional", "education", "research"), ('"instructional designer"', '"learning"', '"curriculum"', '"training"'), True, 6, 3, "balanced"),
    "legal_compliance": QueryProfile(96, 2, 8, ("professional", "legal"), ('"legal counsel"', '"lawyer"', '"contracts"', '"regulatory"'), True, 6, 3, "balanced"),
    "risk_audit_security": QueryProfile(96, 2, 8, ("professional", "technical"), ('"risk"', '"audit"', '"security"'), True, 5, 2, "balanced"),
    "research_development": QueryProfile(110, 2, 8, ("professional", "research", "technical"), ('"research scientist"', '"innovation"', '"lab"', '"r&d"'), True, 6, 3, "balanced"),
    "design_creative": QueryProfile(84, 2, 8, ("professional", "design"), ('"ux"', '"creative"', '"graphic design"'), True, 5, 2, "balanced"),
    "media_communications": QueryProfile(72, 1, 8, ("professional", "marketing"), ('"communications"', '"pr"', '"content"'), True, 5, 2, "balanced"),
    "admin_office_support": QueryProfile(84, 2, 10, ("professional", "operations", "general"), ('"office manager"', '"executive assistant"', '"administration"', '"office operations"'), False, 5, 2, "balanced"),
    "hospitality_tourism": QueryProfile(72, 1, 10, ("professional", "general"), ('"hotel"', '"restaurant"', '"guest relations"'), True, 5, 2, "balanced"),
    "retail_merchandising": QueryProfile(72, 1, 10, ("professional", "general"), ('"retail"', '"merchandising"', '"store operations"'), True, 5, 2, "dense"),
    "real_estate_property": QueryProfile(80, 1, 8, ("professional", "general"), ('"property"', '"real estate"', '"leasing"'), True, 5, 2, "balanced"),
    "public_sector_government": QueryProfile(120, 2, 6, ("professional", "education", "general"), ('"policy analyst"', '"public sector"', '"government"', '"regulation"'), True, 6, 3, "hard"),
    "agriculture_environment": QueryProfile(84, 2, 8, ("professional", "general"), ('"sustainability"', '"environment"', '"esg"'), True, 5, 2, "balanced"),
    "transportation_mobility": QueryProfile(84, 2, 8, ("professional", "operations"), ('"transport"', '"aviation"', '"fleet"'), True, 5, 2, "balanced"),
    "other": QueryProfile(120, 2, 8, ("professional", "general"), ('"profile"', '"bio"', '"team"'), True, 5, 2, "fallback"),
}


def resolve_query_profile(role_family: str, requested_limit: int, *, family_confidence: float = 1.0) -> QueryProfile:
    profile = QUERY_PROFILES.get(role_family, QUERY_PROFILES["other"])
    scale = max(0.75, min(2.0, max(1, int(requested_limit or 1)) / 300))
    max_queries = max(24, int(round(profile.max_queries * scale)))
    pages_per_query = profile.pages_per_query
    parallel_requests = profile.parallel_requests
    source_site_budget = profile.source_site_budget
    family_term_budget = profile.family_term_budget
    adjacent_titles_enabled = profile.adjacent_titles_enabled
    adaptive_mode = profile.adaptive_mode

    stats = family_learning_stats(role_family)
    if stats and stats.run_count >= 2:
        if stats.average_fill_rate < 0.82:
            max_queries = int(round(max_queries * 1.2))
            source_site_budget = min(8, source_site_budget + 1)
            family_term_budget = min(3, family_term_budget + 1)
        if stats.average_verified_rate < 0.12 and stats.average_review_rate > 0.65:
            max_queries = int(round(max_queries * 1.12))
            adjacent_titles_enabled = True
            pages_per_query = max(pages_per_query, 2 if profile.adaptive_mode in {"hard", "fallback"} else pages_per_query)
        if stats.average_reject_rate > 0.22:
            family_term_budget = min(3, family_term_budget + 1)
        if (
            0.18 <= stats.average_verified_rate < 0.45
            and stats.average_review_rate >= 0.45
            and stats.average_reject_rate < 0.08
            and stats.average_fill_rate >= 0.88
            and profile.adaptive_mode in {"dense", "balanced", "strong"}
        ):
            max_queries = int(round(max_queries * 1.15))
            source_site_budget = min(8, source_site_budget + 1)
            if profile.adaptive_mode in {"dense", "strong"}:
                pages_per_query = max(pages_per_query, 2)
            adaptive_mode = f"{adaptive_mode}_verified_recall"
        if stats.positive_feedback_rate > 0.55 and stats.average_fill_rate > 0.92:
            max_queries = int(round(max_queries * 0.92))

    if family_confidence < 0.7:
        max_queries = int(round(max_queries * 1.15))
        pages_per_query = max(pages_per_query, 2)
        family_term_budget = min(3, family_term_budget + 1)
        adjacent_titles_enabled = True
        adaptive_mode = f"{adaptive_mode}_low_confidence"

    if int(requested_limit or 0) >= 250:
        if role_family == "executive":
            boost = 1.18 if int(requested_limit or 0) >= 600 else 1.12 if int(requested_limit or 0) >= 500 else 1.08
        else:
            boost = 1.35 if profile.adaptive_mode in {"dense", "balanced", "strong"} else 1.18
        max_queries = int(round(max_queries * boost))
        source_site_budget = min(8, source_site_budget + 1)
        family_term_budget = min(3, family_term_budget + 1)
        if role_family != "executive" and profile.adaptive_mode in {"dense", "balanced"}:
            pages_per_query = max(pages_per_query, 2)
        adaptive_mode = f"{adaptive_mode}_high_target"

    return QueryProfile(
        max_queries=max_queries,
        pages_per_query=pages_per_query,
        parallel_requests=parallel_requests,
        source_packs=profile.source_packs,
        family_terms=profile.family_terms,
        adjacent_titles_enabled=adjacent_titles_enabled,
        source_site_budget=source_site_budget,
        family_term_budget=family_term_budget,
        adaptive_mode=adaptive_mode,
    )
