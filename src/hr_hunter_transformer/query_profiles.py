from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class QueryProfile:
    max_queries: int
    pages_per_query: int
    parallel_requests: int
    source_packs: tuple[str, ...] = field(default_factory=tuple)
    family_terms: tuple[str, ...] = field(default_factory=tuple)
    adjacent_titles_enabled: bool = True


QUERY_PROFILES: dict[str, QueryProfile] = {
    "executive": QueryProfile(180, 2, 6, ("professional", "leadership", "news"), ('"leadership"', '"executive"', '"board"')),
    "technical_ai": QueryProfile(140, 2, 8, ("professional", "technical"), ('"machine learning"', '"artificial intelligence"', '"llm"')),
    "supply_chain": QueryProfile(54, 1, 10, ("professional", "operations"), ('"s&op"', '"inventory"', '"logistics"')),
    "finance": QueryProfile(54, 1, 10, ("professional", "finance"), ('"accounting"', '"finance"', '"controller"')),
    "marketing": QueryProfile(60, 1, 10, ("professional", "marketing"), ('"growth"', '"campaign"', '"brand"')),
    "data": QueryProfile(60, 1, 10, ("professional", "technical"), ('"analytics"', '"sql"', '"dashboard"')),
    "design_architecture": QueryProfile(100, 2, 8, ("professional", "design"), ('"interior design"', '"architecture"', '"fit-out"')),
    "hr_talent": QueryProfile(70, 1, 10, ("professional", "hr"), ('"talent acquisition"', '"recruitment"', '"people partner"')),
    "other": QueryProfile(120, 2, 8, ("professional", "general"), ('"profile"', '"bio"', '"team"')),
}


def resolve_query_profile(role_family: str, requested_limit: int) -> QueryProfile:
    profile = QUERY_PROFILES.get(role_family, QUERY_PROFILES["other"])
    scale = max(0.75, min(2.0, max(1, int(requested_limit or 1)) / 300))
    return QueryProfile(
        max_queries=max(24, int(round(profile.max_queries * scale))),
        pages_per_query=profile.pages_per_query,
        parallel_requests=profile.parallel_requests,
        source_packs=profile.source_packs,
        family_terms=profile.family_terms,
        adjacent_titles_enabled=profile.adjacent_titles_enabled,
    )

