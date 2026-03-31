from __future__ import annotations

from typing import Iterable, List

from hr_hunter.briefing import unique_preserving_order
from hr_hunter.models import SearchBrief, SearchSlice


def chunked(values: Iterable[str], size: int) -> List[List[str]]:
    items = list(values)
    if size <= 0:
        return [items]
    return [items[index : index + size] for index in range(0, len(items), size)]


def build_search_slices(brief: SearchBrief) -> List[SearchSlice]:
    pdl_settings = brief.provider_settings.get("pdl", {})
    chunk_size = int(pdl_settings.get("company_chunk_size", 5))
    slice_limit = int(pdl_settings.get("results_per_slice", 40))
    include_strict_slice = bool(pdl_settings.get("include_strict_slice", True))
    include_broad_slice = bool(pdl_settings.get("include_broad_slice", True))
    include_discovery_slices = bool(pdl_settings.get("include_discovery_slices", True))
    precision_keywords = unique_preserving_order(
        brief.industry_keywords
        + brief.required_keywords
        + brief.portfolio_keywords
        + brief.commercial_keywords
        + brief.preferred_keywords
    )[:8]
    discovery_keyword_chunk_size = int(pdl_settings.get("discovery_keyword_chunk_size", 6))
    market_keyword_chunk_size = int(pdl_settings.get("market_keyword_chunk_size", 5))

    company_chunks = chunked(brief.company_targets, chunk_size)
    slices: List[SearchSlice] = []

    for index, companies in enumerate(company_chunks, start=1):
        if include_strict_slice:
            slices.append(
                SearchSlice(
                    id=f"strict-{index}",
                    description="Current company + exact title slice",
                    companies=companies,
                    titles=brief.titles,
                    title_keywords=[],
                    query_keywords=[],
                    search_mode="strict",
                    limit=slice_limit,
                )
            )
        if include_broad_slice:
            slices.append(
                SearchSlice(
                    id=f"broad-{index}",
                    description="Current company + title family slice",
                    companies=companies,
                    titles=brief.titles,
                    title_keywords=brief.title_keywords,
                    query_keywords=precision_keywords,
                    search_mode="broad",
                    limit=slice_limit,
                )
            )

    discovery_keywords = unique_preserving_order(
        brief.industry_keywords
        + brief.required_keywords
        + brief.preferred_keywords
        + brief.portfolio_keywords
        + brief.commercial_keywords
    )
    if discovery_keywords and include_discovery_slices:
        for index, keyword_group in enumerate(
            chunked(discovery_keywords, max(1, discovery_keyword_chunk_size)),
            start=1,
        ):
            slices.append(
                SearchSlice(
                    id=f"discovery-{index}",
                    description="Title family + FMCG-adjacent discovery slice",
                    companies=[],
                    titles=brief.titles,
                    title_keywords=brief.title_keywords,
                    query_keywords=keyword_group,
                    search_mode="discovery",
                    limit=slice_limit,
                )
            )

        for index, keyword_group in enumerate(
            chunked(discovery_keywords, max(1, market_keyword_chunk_size)),
            start=1,
        ):
            slices.append(
                SearchSlice(
                    id=f"market-{index}",
                    description="Exact title + FMCG-adjacent sector discovery slice",
                    companies=[],
                    titles=brief.titles[:8],
                    title_keywords=[],
                    query_keywords=keyword_group,
                    search_mode="market",
                    limit=slice_limit,
                )
            )

    return slices
