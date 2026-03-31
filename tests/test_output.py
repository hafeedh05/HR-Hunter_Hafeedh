from pathlib import Path

from hr_hunter.briefing import build_search_brief
from hr_hunter.models import CandidateProfile, EvidenceRecord, SearchRunReport
from hr_hunter.output import build_reporting_summary, hydrate_candidate_reporting, load_report, write_report
from hr_hunter.scoring import score_candidate


def test_report_roundtrip_preserves_reporting_fields(tmp_path: Path) -> None:
    candidate = CandidateProfile(
        full_name="Jane Search",
        current_title="Senior Brand Manager",
        current_company="Unilever",
        location_name="Dublin, Ireland",
        location_aligned=True,
        current_target_company_match=True,
        current_title_match=True,
        current_company_confirmed=True,
        current_title_confirmed=True,
        current_location_confirmed=True,
        precise_location_confirmed=True,
        current_employment_confirmed=True,
        verification_status="verified",
        qualification_tier="strict_verified",
        matched_title_family="brand",
        location_precision_bucket="within_expanded_radius",
        current_role_proof_count=2,
        source_quality_score=0.9,
        evidence_freshness_year=2026,
        current_function_fit=1.0,
        current_fmcg_fit=1.0,
        cap_reasons=["missing_none"],
        disqualifier_reasons=[],
        score=88.0,
        evidence_records=[
            EvidenceRecord(
                source_url="https://example.com/people/jane-search",
                source_domain="example.com",
                profile_signal=True,
                current_employment_signal=True,
                recency_year=2026,
                confidence=0.84,
            )
        ],
    )
    report = SearchRunReport(
        run_id="roundtrip-reporting",
        brief_id="brief",
        dry_run=False,
        generated_at="2026-03-31T00:00:00+00:00",
        provider_results=[],
        candidates=[candidate],
        summary={},
    )

    json_path, _ = write_report(report, tmp_path)
    loaded = load_report(json_path)
    restored = loaded.candidates[0]

    assert restored.qualification_tier == "strict_verified"
    assert restored.matched_title_family == "brand"
    assert restored.location_precision_bucket == "within_expanded_radius"
    assert restored.precise_location_confirmed is True
    assert restored.current_role_proof_count == 2
    assert restored.source_quality_score == 0.9
    assert restored.evidence_freshness_year == 2026
    assert restored.current_function_fit == 1.0
    assert restored.current_fmcg_fit == 1.0


def test_build_reporting_summary_counts_strict_and_search_qualified() -> None:
    strict_candidate = CandidateProfile(
        full_name="Strict Match",
        current_title="Senior Brand Manager",
        current_company="Unilever",
        location_aligned=True,
        current_target_company_match=True,
        current_title_match=True,
        current_company_confirmed=True,
        current_title_confirmed=True,
        current_location_confirmed=True,
        current_employment_confirmed=True,
        verification_status="verified",
        qualification_tier="strict_verified",
    )
    search_qualified_candidate = CandidateProfile(
        full_name="Search Qualified",
        current_title="Shopper Marketing Manager",
        current_company="Unilever",
        location_aligned=True,
        current_target_company_match=True,
        current_title_match=False,
        current_company_confirmed=True,
        current_title_confirmed=False,
        current_location_confirmed=True,
        current_employment_confirmed=False,
        verification_status="review",
        qualification_tier="search_qualified",
        cap_reasons=["missing_current_title_confirmation", "missing_current_role_proof"],
    )
    weak_candidate = CandidateProfile(
        full_name="Weak Match",
        current_title="Marketing Specialist",
        current_company="Other Co",
        location_aligned=False,
        verification_status="reject",
        qualification_tier="weak",
    )

    summary = build_reporting_summary(
        [strict_candidate, search_qualified_candidate, weak_candidate],
        {"role_title": "Brand Manager"},
    )

    assert summary["candidate_count"] == 3
    assert summary["verified_count"] == 1
    assert summary["review_count"] == 1
    assert summary["reject_count"] == 1
    assert summary["strict_verified_count"] == 1
    assert summary["search_qualified_count"] == 1
    assert summary["weak_count"] == 1


def test_hydrate_candidate_reporting_recomputes_tier_after_verified_candidate_is_downgraded() -> None:
    candidate = CandidateProfile(
        full_name="Downgraded Match",
        current_title="Brand Manager",
        current_company="Procter & Gamble",
        location_name="Ireland",
        location_aligned=True,
        current_target_company_match=True,
        current_title_match=True,
        current_company_confirmed=False,
        current_title_confirmed=False,
        current_location_confirmed=True,
        current_employment_confirmed=False,
        verification_status="review",
        qualification_tier="strict_verified",
        score=90.0,
    )

    hydrated = hydrate_candidate_reporting(candidate)

    assert hydrated.qualification_tier == "search_qualified"


def test_score_candidate_treats_country_only_ireland_as_imprecise_location() -> None:
    brief = build_search_brief(
        {
            "id": "score-country-only-ireland-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "company_targets": ["Unilever"],
            "geography": {
                "location_name": "Drogheda",
                "country": "Ireland",
                "location_hints": ["Ireland", "Dublin", "Galway"],
                "center_latitude": 53.7179,
                "center_longitude": -6.3561,
                "radius_miles": 60,
            },
            "industry_keywords": ["FMCG"],
        }
    )

    candidate = CandidateProfile(
        full_name="Country Only Ireland",
        current_title="Senior Brand Manager",
        current_company="Unilever",
        location_name="Ireland",
        summary="Brand leadership across Ireland FMCG markets.",
    )

    scored = score_candidate(candidate, brief)

    assert scored.location_precision_bucket == "country_only_ireland"
    assert scored.verification_status != "verified"
