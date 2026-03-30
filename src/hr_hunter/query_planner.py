from __future__ import annotations

from typing import Iterable, List

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

    company_chunks = chunked(brief.company_targets, chunk_size)
    slices: List[SearchSlice] = []

    for index, companies in enumerate(company_chunks, start=1):
        slices.append(
            SearchSlice(
                id=f"strict-{index}",
                description="Current company + exact title slice",
                companies=companies,
                titles=brief.titles,
                title_keywords=[],
                search_mode="strict",
                limit=slice_limit,
            )
        )
        slices.append(
            SearchSlice(
                id=f"broad-{index}",
                description="Current company + title family slice",
                companies=companies,
                titles=brief.titles,
                title_keywords=brief.title_keywords,
                search_mode="broad",
                limit=slice_limit,
            )
        )

    return slices
