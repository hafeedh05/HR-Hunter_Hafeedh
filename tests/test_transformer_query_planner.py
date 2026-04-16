from hr_hunter_transformer.models import SearchBrief
from hr_hunter_transformer.family_learning import FamilyLearningStats
from hr_hunter_transformer.query_planner import build_query_plan
from hr_hunter_transformer.query_profiles import resolve_query_profile
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


def test_executive_query_plan_prioritizes_peer_company_queries_before_generic_titles() -> None:
    brief = SearchBrief(
        role_title="Chief Executive Officer",
        titles=["Chief Executive Officer", "CEO", "Managing Director"],
        countries=["United Arab Emirates", "Saudi Arabia"],
        cities=["Dubai", "Riyadh"],
        peer_company_targets=["Home Centre", "Chalhoub Group", "Al-Futtaim"],
        industry_keywords=["premium retail", "home furnishings"],
        target_count=300,
    )

    plan = build_query_plan(brief)
    first_queries = plan.queries[:12]

    assert plan.role_understanding.role_family == "executive"
    assert all(task.query_type in {"company_exact_priority", "company_geo_priority"} for task in first_queries)
    assert any('"Home Centre"' in task.query_text for task in first_queries)
    assert any('"Dubai"' in task.query_text for task in first_queries)


def test_dense_family_learning_does_not_overexpand_high_fill_supply_chain(monkeypatch) -> None:
    monkeypatch.setattr(
        "hr_hunter_transformer.query_profiles.family_learning_stats",
        lambda family: FamilyLearningStats(
            family=family,
            run_count=4,
            average_fill_rate=1.0,
            average_verified_rate=0.08,
            average_review_rate=0.82,
            average_reject_rate=0.02,
        ),
    )

    profile = resolve_query_profile("supply_chain", 300)

    assert profile.max_queries == 81


def test_dense_family_learning_widens_promising_low_verified_supply_chain(monkeypatch) -> None:
    monkeypatch.setattr(
        "hr_hunter_transformer.query_profiles.family_learning_stats",
        lambda family: FamilyLearningStats(
            family=family,
            run_count=6,
            average_fill_rate=1.0,
            average_verified_rate=0.4,
            average_review_rate=0.57,
            average_reject_rate=0.02,
        ),
    )

    profile = resolve_query_profile("supply_chain", 300)

    assert profile.max_queries == 84
    assert profile.pages_per_query >= 2
