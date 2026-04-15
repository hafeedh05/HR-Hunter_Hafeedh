from __future__ import annotations

import asyncio

from hr_hunter_transformer.models import QueryPlan, QueryTask, RawSearchHit, RoleUnderstanding, SearchBrief
from hr_hunter_transformer.scrapingbee_adapter import ScrapingBeeSearchConfig, ScrapingBeeTransformerRetriever


def _role_understanding(*, family: str, complexity: str) -> RoleUnderstanding:
    return RoleUnderstanding(
        normalized_title="Role",
        role_family=family,
        role_subfamily=family,
        family_confidence=0.9,
        title_variants=["Role"],
        adjacent_titles=["Adjacent Role"],
        inferred_skills=["Skill"],
        seniority_hint="manager",
        search_complexity=complexity,
    )


def test_retriever_prioritizes_exact_profile_queries_over_adjacent_noise() -> None:
    retriever = ScrapingBeeTransformerRetriever(ScrapingBeeSearchConfig(api_key="test-key", parallel_requests=2))
    plan = QueryPlan(
        role_understanding=_role_understanding(family="supply_chain", complexity="dense"),
        queries=[
            QueryTask(query_text='"Adjacent Role" "Dubai"', query_type="adjacent_title_geo", source_pack="general"),
            QueryTask(query_text='site:linkedin.com/in "Supply Chain Manager" "Dubai"', query_type="exact_title_source", source_pack="professional"),
            QueryTask(query_text='"Supply Chain Manager" "S&OP" "Dubai"', query_type="skill_geo", source_pack="general"),
        ],
        max_queries=3,
        pages_per_query=1,
        parallel_requests=2,
    )

    ordered = retriever._prioritize_tasks(plan)

    assert ordered[0].query_type == "exact_title_source"
    assert ordered[-1].query_type == "adjacent_title_geo"


def test_retriever_stops_early_once_dense_search_hits_target_and_plateaus(monkeypatch) -> None:
    retriever = ScrapingBeeTransformerRetriever(ScrapingBeeSearchConfig(api_key="test-key", parallel_requests=2))
    brief = SearchBrief(role_title="Supply Chain Manager", titles=["Supply Chain Manager"], target_count=50)
    plan = QueryPlan(
        role_understanding=_role_understanding(family="supply_chain", complexity="dense"),
        queries=[
            QueryTask(query_text=f'site:linkedin.com/in "Supply Chain Manager" "Dubai" {index:02d}', query_type="exact_title_source", source_pack="professional")
            for index in range(14)
        ],
        max_queries=14,
        pages_per_query=1,
        parallel_requests=2,
    )

    async def _fake_fetch(_client, query: str, *, page_budget: int):  # type: ignore[no-untyped-def]
        index = int(query.rsplit(" ", 1)[-1])
        if index < 4:
            return [
                RawSearchHit(
                    title=f"Candidate {index}-{offset}",
                    snippet="profile",
                    url=f"https://www.linkedin.com/in/candidate-{index}-{offset}",
                )
                for offset in range(50)
            ]
        return []

    monkeypatch.setattr(retriever, "_fetch_query", _fake_fetch)

    executed_queries, hits = asyncio.run(retriever.search_async(brief, query_plan=plan))
    usage = retriever.usage_summary()

    assert len(executed_queries) < len(plan.queries)
    assert len(hits) == 200
    assert usage["early_stop_triggered"] is True
    assert str(usage["stop_reason"]).startswith("plateau_after_")
    assert usage["unique_hits"] == 200


def test_retriever_boosts_page_budget_for_hard_roles_when_early_yield_is_weak(monkeypatch) -> None:
    retriever = ScrapingBeeTransformerRetriever(ScrapingBeeSearchConfig(api_key="test-key", parallel_requests=2))
    brief = SearchBrief(role_title="AI Engineer", titles=["AI Engineer"], target_count=100)
    plan = QueryPlan(
        role_understanding=_role_understanding(family="technical_ai", complexity="hard"),
        queries=[
            QueryTask(query_text='site:linkedin.com/in "AI Engineer" "Dubai" 1', query_type="exact_title_source", source_pack="professional", page_budget=2),
            QueryTask(query_text='site:github.com "AI Engineer" "Dubai" 2', query_type="exact_title_source", source_pack="technical", page_budget=2),
            QueryTask(query_text='site:linkedin.com/in "AI Engineer" "Abu Dhabi" 3', query_type="exact_title_source", source_pack="professional", page_budget=2),
            QueryTask(query_text='site:huggingface.co "AI Engineer" "UAE" 4', query_type="exact_title_source", source_pack="technical", page_budget=2),
            QueryTask(query_text='site:kaggle.com "LLM Engineer" "Dubai" 5', query_type="skill_geo", source_pack="technical", page_budget=2),
            QueryTask(query_text='site:linkedin.com/in "Machine Learning Engineer" "Dubai" 6', query_type="skill_geo", source_pack="professional", page_budget=2),
        ],
        max_queries=6,
        pages_per_query=2,
        parallel_requests=2,
    )

    seen_page_budgets: dict[str, int] = {}

    async def _fake_fetch(_client, query: str, *, page_budget: int):  # type: ignore[no-untyped-def]
        seen_page_budgets[query] = page_budget
        return []

    monkeypatch.setattr(retriever, "_fetch_query", _fake_fetch)

    asyncio.run(retriever.search_async(brief, query_plan=plan))

    assert seen_page_budgets['site:kaggle.com "LLM Engineer" "Dubai" 5'] == 3
    assert seen_page_budgets['site:linkedin.com/in "Machine Learning Engineer" "Dubai" 6'] == 3
