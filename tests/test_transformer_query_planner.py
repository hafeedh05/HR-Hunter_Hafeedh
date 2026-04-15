from hr_hunter_transformer.models import SearchBrief
from hr_hunter_transformer.query_planner import build_query_plan
from hr_hunter_transformer.role_profiles import infer_role_family_with_confidence


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
    assert plan.max_queries > 54
    assert plan.pages_per_query >= 2
    assert any("linkedin.com/in" in task.query_text for task in plan.queries)


def test_unknown_but_related_ai_title_maps_to_technical_family() -> None:
    family, confidence = infer_role_family_with_confidence(
        "AI Governance Lead",
        "Responsible AI",
        "Model Risk",
    )
    assert family == "technical_ai"
    assert confidence >= 0.34


def test_query_plan_uses_wider_profile_for_low_confidence_role() -> None:
    brief = SearchBrief(
        role_title="Quantum Logistics Planner",
        titles=["Quantum Logistics Planner"],
        countries=["United Arab Emirates"],
        required_keywords=["optimization", "planning"],
        target_count=300,
    )
    plan = build_query_plan(brief)
    assert plan.role_understanding.role_family == "supply_chain"
    assert plan.role_understanding.family_confidence < 1.0
    assert plan.pages_per_query >= 2
    assert plan.max_queries > 54

