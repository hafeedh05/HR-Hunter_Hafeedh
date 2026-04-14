from types import SimpleNamespace

from hr_hunter.briefing import build_search_brief
from hr_hunter.transformer_bridge import _candidate_from_transformer_entity


def test_candidate_from_transformer_entity_matches_current_candidate_profile_shape() -> None:
    brief = build_search_brief(
        {
            "role_title": "Supply Chain Manager",
            "titles": ["Supply Chain Manager"],
            "geography": {"country": "United Arab Emirates", "location_name": "Dubai"},
        }
    )
    evidence = SimpleNamespace(
        source_url="https://www.linkedin.com/in/jane-doe",
        source_domain="linkedin.com",
        page_title="Jane Doe | Supply Chain Manager",
        page_snippet="Supply Chain Manager at ExampleCo in Dubai",
        source_type="profile",
        current_company="ExampleCo",
        company_match=True,
        current_title="Supply Chain Manager",
        location_match=True,
        current_location="Dubai, United Arab Emirates",
        current_role_signal=True,
        confidence=0.91,
        supporting_keywords=["s&op", "erp"],
    )
    entity = SimpleNamespace(
        evidence=[evidence],
        full_name="Jane Doe",
        current_title="Supply Chain Manager",
        current_company="ExampleCo",
        current_location="Dubai, United Arab Emirates",
        notes=[],
        diagnostics=[],
        verification_status="verified",
        current_company_confirmed=True,
        current_location_confirmed=True,
        current_title_confirmed=True,
        current_role_proof_count=2,
        company_match=True,
        title_match=True,
        location_match=True,
        role_family="operations",
        source_domains=["linkedin.com"],
        semantic_similarity=0.84,
        score=0.88,
        semantic_fit=0.82,
        title_match_score=0.94,
        skill_match_score=0.76,
        company_match_score=0.71,
        location_match_score=0.92,
        seniority_match_score=0.67,
        currentness_score=0.9,
        source_trust_score=0.95,
        verification_confidence=0.89,
    )

    candidate = _candidate_from_transformer_entity(entity, brief)

    assert candidate.full_name == "Jane Doe"
    assert candidate.current_company == "ExampleCo"
    assert candidate.location_precision_bucket == "named_target_location"
    assert candidate.verification_status == "verified"
