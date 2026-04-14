from hr_hunter_transformer.models import SearchBrief
from hr_hunter_transformer.query_planner import build_query_plan


def test_query_plan_uses_family_specific_profile_for_supply_chain() -> None:
    brief = SearchBrief(
        role_title="Supply Chain Manager",
        titles=["Supply Chain Manager"],
        countries=["United Arab Emirates"],
        cities=["Dubai"],
        company_targets=["Aramex"],
        required_keywords=["S&OP", "Inventory"],
        target_count=300,
    )
    plan = build_query_plan(brief)
    assert plan.role_understanding.role_family == "supply_chain"
    assert plan.max_queries >= 40
    assert plan.pages_per_query == 1
    assert any("linkedin.com/in" in task.query_text for task in plan.queries)

