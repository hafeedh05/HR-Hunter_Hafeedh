from hr_hunter.briefing import build_search_brief
from hr_hunter.models import CandidateProfile
from hr_hunter.output import (
    build_progress_counts,
    build_reporting_summary,
    hydrate_candidate_reporting,
    prepare_verification_shortlist,
    sanitize_brief_payload,
    sanitize_report_summary,
)


def _candidate(
    *,
    name: str,
    status: str,
    score: float,
    current_title_match: bool,
    location_aligned: bool,
    location_bucket: str,
    parser_confidence: float,
    evidence_quality_score: float,
    skill_overlap_score: float,
    current_function_fit: float,
    years_fit_score: float,
    industry_fit_score: float,
    cap_reasons: list[str],
) -> CandidateProfile:
    return CandidateProfile(
        full_name=name,
        current_title="Chief Executive Officer" if current_title_match else "Regional Director",
        current_company="Marina Home Interiors" if status == "verified" else "Adjacent Retail Co",
        location_name="Dubai" if location_aligned else "London",
        current_title_match=current_title_match,
        location_aligned=location_aligned,
        location_precision_bucket=location_bucket,
        parser_confidence=parser_confidence,
        evidence_quality_score=evidence_quality_score,
        skill_overlap_score=skill_overlap_score,
        current_function_fit=current_function_fit,
        years_fit_score=years_fit_score,
        industry_fit_score=industry_fit_score,
        current_target_company_match=status == "verified",
        target_company_history_match=False,
        verification_status=status,
        score=score,
        cap_reasons=cap_reasons,
    )


def test_build_reporting_summary_adds_low_yield_quality_diagnostics() -> None:
    candidates = [
        _candidate(
            name="Verified One",
            status="verified",
            score=78.0,
            current_title_match=True,
            location_aligned=True,
            location_bucket="named_target_location",
            parser_confidence=0.82,
            evidence_quality_score=0.76,
            skill_overlap_score=0.78,
            current_function_fit=0.86,
            years_fit_score=0.74,
            industry_fit_score=0.8,
            cap_reasons=[],
        ),
        _candidate(
            name="Review Title Gap",
            status="review",
            score=58.0,
            current_title_match=False,
            location_aligned=False,
            location_bucket="outside_target_area",
            parser_confidence=0.22,
            evidence_quality_score=0.2,
            skill_overlap_score=0.12,
            current_function_fit=0.2,
            years_fit_score=0.32,
            industry_fit_score=0.08,
            cap_reasons=["title_alignment_required", "outside_target_area", "parser_confidence_too_low"],
        ),
        _candidate(
            name="Review Geo Gap",
            status="review",
            score=55.0,
            current_title_match=False,
            location_aligned=False,
            location_bucket="outside_target_area",
            parser_confidence=0.24,
            evidence_quality_score=0.24,
            skill_overlap_score=0.15,
            current_function_fit=0.24,
            years_fit_score=0.35,
            industry_fit_score=0.1,
            cap_reasons=["precise_location_required", "title_alignment_required"],
        ),
        _candidate(
            name="Reject Weak Anchors",
            status="reject",
            score=44.0,
            current_title_match=False,
            location_aligned=True,
            location_bucket="country_only",
            parser_confidence=0.31,
            evidence_quality_score=0.21,
            skill_overlap_score=0.1,
            current_function_fit=0.28,
            years_fit_score=0.2,
            industry_fit_score=0.12,
            cap_reasons=["title_alignment_required"],
        ),
        _candidate(
            name="Reject Sparse Evidence",
            status="reject",
            score=41.0,
            current_title_match=False,
            location_aligned=False,
            location_bucket="unknown_location",
            parser_confidence=0.18,
            evidence_quality_score=0.1,
            skill_overlap_score=0.08,
            current_function_fit=0.12,
            years_fit_score=0.18,
            industry_fit_score=0.05,
            cap_reasons=["parser_confidence_too_low", "outside_target_area"],
        ),
        _candidate(
            name="Reject Adjacent Retail",
            status="reject",
            score=39.0,
            current_title_match=False,
            location_aligned=False,
            location_bucket="outside_target_area",
            parser_confidence=0.28,
            evidence_quality_score=0.22,
            skill_overlap_score=0.12,
            current_function_fit=0.18,
            years_fit_score=0.24,
            industry_fit_score=0.09,
            cap_reasons=["current_function_review", "outside_target_area"],
        ),
    ]

    summary = build_reporting_summary(
        candidates,
        {
            "target_range": [300, 300],
            "pipeline_metrics": {
                "raw_found": 96,
                "unique_after_dedupe": 18,
                "finalized_count": len(candidates),
            },
        },
    )

    diagnostics = summary["quality_diagnostics"]

    assert diagnostics["enabled"] is True
    assert diagnostics["yield_status"] == "low"
    assert diagnostics["verified_count"] == 1
    assert diagnostics["verification_ready_count"] == 1
    assert diagnostics["unique_after_dedupe"] == 18
    issue_keys = [issue["key"] for issue in diagnostics["issues"]]
    assert "title_mismatch" in issue_keys
    assert "geo_mismatch" in issue_keys
    assert "parser_confidence" in issue_keys
    assert "market_scarcity" in issue_keys


def test_build_reporting_summary_keeps_non_executive_diagnostics_copy_generic() -> None:
    candidates = [
        _candidate(
            name="Reject One",
            status="reject",
            score=32.0,
            current_title_match=False,
            location_aligned=True,
            location_bucket="country_only",
            parser_confidence=0.28,
            evidence_quality_score=0.22,
            skill_overlap_score=0.1,
            current_function_fit=0.18,
            years_fit_score=0.2,
            industry_fit_score=0.08,
            cap_reasons=["title_alignment_required"],
        ),
        _candidate(
            name="Reject Two",
            status="reject",
            score=29.0,
            current_title_match=False,
            location_aligned=False,
            location_bucket="outside_target_area",
            parser_confidence=0.22,
            evidence_quality_score=0.18,
            skill_overlap_score=0.08,
            current_function_fit=0.16,
            years_fit_score=0.18,
            industry_fit_score=0.05,
            cap_reasons=["outside_target_area"],
        ),
    ]

    summary = build_reporting_summary(
        candidates,
        {
            "role_title": "Digital Marketing Manager",
            "target_range": [50, 50],
            "pipeline_metrics": {
                "raw_found": 18,
                "unique_after_dedupe": 8,
                "finalized_count": len(candidates),
            },
        },
    )

    diagnostics = summary["quality_diagnostics"]

    assert diagnostics["enabled"] is True
    assert all("executive" not in issue["message"].lower() for issue in diagnostics["issues"])
    assert all("executive" not in issue["action"].lower() for issue in diagnostics["issues"])


def test_sanitize_scope_legacy_fields_from_brief_and_summary() -> None:
    brief = sanitize_brief_payload(
        {
            "role_title": "Supply Chain Manager",
            "in_scope_target": 150,
            "verification_scope_target": 120,
            "scope_first_enabled": True,
            "ui_meta": {
                "in_scope_target": 150,
                "verification_scope_target": 120,
                "scope_first_enabled": True,
            },
            "provider_settings": {"verification": {"scope_target": 120}},
        }
    )
    summary = sanitize_report_summary(
        {
            "verified_count": 8,
            "in_scope_count": 13,
            "precise_in_scope_count": 13,
            "scope_counts": {"in_scope": 13},
            "verification_scope_target": 120,
            "verification_shortlist_scope_count": 25,
            "verification_shortlist_precise_scope_count": 21,
            "scope_first_enabled": True,
            "scope_first_in_scope_target": 150,
            "scope_first_in_scope_achieved": 42,
            "verification": {"scope_target": 120},
        }
    )

    assert brief == {
        "role_title": "Supply Chain Manager",
        "ui_meta": {},
        "provider_settings": {"verification": {}},
    }
    assert summary == {"verified_count": 8, "verification": {}}


def test_build_reporting_summary_drops_legacy_scope_counts() -> None:
    summary = build_reporting_summary(
        [
            _candidate(
                name="Verified",
                status="verified",
                score=82.0,
                current_title_match=True,
                location_aligned=True,
                location_bucket="named_target_location",
                parser_confidence=0.9,
                evidence_quality_score=0.8,
                skill_overlap_score=0.75,
                current_function_fit=0.78,
                years_fit_score=0.72,
                industry_fit_score=0.7,
                cap_reasons=[],
            )
        ],
        {
            "verified_count": 1,
            "in_scope_count": 1,
            "precise_in_scope_count": 1,
            "scope_counts": {"in_scope": 1},
            "verification_scope_target": 10,
            "verification_shortlist_scope_count": 1,
        },
    )

    assert "in_scope_count" not in summary
    assert "precise_in_scope_count" not in summary
    assert "scope_counts" not in summary
    assert "verification_scope_target" not in summary
    assert "verification_shortlist_scope_count" not in summary


def test_build_progress_counts_only_reports_match_metrics() -> None:
    candidates = [
        _candidate(
            name="Verified Scope Match",
            status="verified",
            score=78.0,
            current_title_match=True,
            location_aligned=True,
            location_bucket="named_target_location",
            parser_confidence=0.82,
            evidence_quality_score=0.76,
            skill_overlap_score=0.78,
            current_function_fit=0.86,
            years_fit_score=0.74,
            industry_fit_score=0.8,
            cap_reasons=[],
        ),
        _candidate(
            name="Rejected Geo Gap",
            status="reject",
            score=41.0,
            current_title_match=False,
            location_aligned=False,
            location_bucket="outside_target_area",
            parser_confidence=0.18,
            evidence_quality_score=0.1,
            skill_overlap_score=0.08,
            current_function_fit=0.12,
            years_fit_score=0.18,
            industry_fit_score=0.05,
            cap_reasons=["outside_target_area"],
        ),
    ]

    counts = build_progress_counts(candidates)

    assert counts["title_match_count"] == 1
    assert counts["market_match_count"] == 1
    assert "verified_count" not in counts


def test_hydrate_candidate_reporting_preserves_explicit_blocked_function_fit() -> None:
    candidate = CandidateProfile(
        full_name="Blocked Product Analyst",
        current_title="Senior Digital Product Analyst",
        current_company="Sephora",
        location_name="Dubai, United Arab Emirates",
        current_title_match=True,
        verification_status="reject",
        verification_notes=["current_function_fit: blocked (digital)"],
        feature_scores={"current_function_fit": 0.0},
    )

    hydrated = hydrate_candidate_reporting(candidate)

    assert hydrated.current_function_fit == 0.0


def test_prepare_verification_shortlist_prioritizes_core_fit_candidates_first() -> None:
    candidates = [
        _candidate(
            name="Out Of Scope High Score",
            status="review",
            score=92.0,
            current_title_match=False,
            location_aligned=False,
            location_bucket="outside_target_area",
            parser_confidence=0.82,
            evidence_quality_score=0.72,
            skill_overlap_score=0.76,
            current_function_fit=0.8,
            years_fit_score=0.7,
            industry_fit_score=0.68,
            cap_reasons=[],
        ),
        _candidate(
            name="In Scope Exact One",
            status="reject",
            score=58.0,
            current_title_match=True,
            location_aligned=True,
            location_bucket="named_target_location",
            parser_confidence=0.65,
            evidence_quality_score=0.44,
            skill_overlap_score=0.5,
            current_function_fit=0.72,
            years_fit_score=0.52,
            industry_fit_score=0.4,
            cap_reasons=[],
        ),
        _candidate(
            name="In Scope Exact Two",
            status="reject",
            score=56.0,
            current_title_match=True,
            location_aligned=True,
            location_bucket="country_only",
            parser_confidence=0.62,
            evidence_quality_score=0.42,
            skill_overlap_score=0.46,
            current_function_fit=0.7,
            years_fit_score=0.5,
            industry_fit_score=0.38,
            cap_reasons=[],
        ),
    ]

    ordered = prepare_verification_shortlist(
        candidates,
        company_required=False,
        verification_limit=2,
    )

    assert [candidate.full_name for candidate in ordered[:2]] == [
        "In Scope Exact One",
        "In Scope Exact Two",
    ]


def test_prepare_verification_shortlist_prefers_precise_market_before_country_only() -> None:
    candidates = [
        _candidate(
            name="Country Only Scope",
            status="review",
            score=62.0,
            current_title_match=True,
            location_aligned=True,
            location_bucket="country_only",
            parser_confidence=0.6,
            evidence_quality_score=0.4,
            skill_overlap_score=0.48,
            current_function_fit=0.7,
            years_fit_score=0.5,
            industry_fit_score=0.4,
            cap_reasons=[],
        ),
        _candidate(
            name="Precise Scope",
            status="reject",
            score=57.0,
            current_title_match=True,
            location_aligned=True,
            location_bucket="named_target_location",
            parser_confidence=0.58,
            evidence_quality_score=0.38,
            skill_overlap_score=0.46,
            current_function_fit=0.68,
            years_fit_score=0.48,
            industry_fit_score=0.39,
            cap_reasons=[],
        ),
        _candidate(
            name="Out Of Scope",
            status="review",
            score=91.0,
            current_title_match=False,
            location_aligned=False,
            location_bucket="outside_target_area",
            parser_confidence=0.82,
            evidence_quality_score=0.74,
            skill_overlap_score=0.76,
            current_function_fit=0.8,
            years_fit_score=0.7,
            industry_fit_score=0.68,
            cap_reasons=[],
        ),
    ]

    ordered = prepare_verification_shortlist(
        candidates,
        company_required=False,
        verification_limit=2,
    )

    assert [candidate.full_name for candidate in ordered[:2]] == [
        "Precise Scope",
        "Country Only Scope",
    ]


def test_prepare_verification_shortlist_prefers_verification_ready_candidates() -> None:
    candidates = [
        _candidate(
            name="Weak High Score",
            status="review",
            score=90.0,
            current_title_match=False,
            location_aligned=False,
            location_bucket="outside_target_area",
            parser_confidence=0.18,
            evidence_quality_score=0.12,
            skill_overlap_score=0.16,
            current_function_fit=0.28,
            years_fit_score=0.4,
            industry_fit_score=0.18,
            cap_reasons=["outside_target_area", "parser_confidence_too_low"],
        ),
        _candidate(
            name="Ready Exact One",
            status="reject",
            score=57.0,
            current_title_match=True,
            location_aligned=True,
            location_bucket="named_target_location",
            parser_confidence=0.66,
            evidence_quality_score=0.52,
            skill_overlap_score=0.5,
            current_function_fit=0.72,
            years_fit_score=0.56,
            industry_fit_score=0.42,
            cap_reasons=[],
        ),
        _candidate(
            name="Ready Exact Two",
            status="review",
            score=55.0,
            current_title_match=True,
            location_aligned=True,
            location_bucket="country_only",
            parser_confidence=0.62,
            evidence_quality_score=0.47,
            skill_overlap_score=0.46,
            current_function_fit=0.69,
            years_fit_score=0.54,
            industry_fit_score=0.38,
            cap_reasons=[],
        ),
    ]
    candidates[1].current_employment_confirmed = True
    candidates[2].current_role_proof_count = 1

    ordered = prepare_verification_shortlist(
        candidates,
        company_required=False,
        verification_limit=2,
    )

    assert [candidate.full_name for candidate in ordered[:2]] == [
        "Ready Exact One",
        "Ready Exact Two",
    ]


def test_prepare_verification_shortlist_honors_title_market_priority_brief() -> None:
    brief = build_search_brief(
        {
            "id": "verification-priority-test",
            "role_title": "Supply Chain Manager",
            "titles": [
                "Supply Chain Manager",
                "Senior Supply Chain Manager",
            ],
            "geography": {
                "location_name": "Dubai",
                "country": "United Arab Emirates",
                "center_latitude": 25.2048,
                "center_longitude": 55.2708,
                "radius_miles": 25,
            },
        }
    )

    candidates = [
        _candidate(
            name="Title Match Weak Market",
            status="review",
            score=49.0,
            current_title_match=True,
            location_aligned=False,
            location_bucket="outside_target_area",
            parser_confidence=0.3,
            evidence_quality_score=0.22,
            skill_overlap_score=0.18,
            current_function_fit=0.28,
            years_fit_score=0.42,
            industry_fit_score=0.32,
            cap_reasons=["outside_target_area"],
        ),
        _candidate(
            name="Strong Function Precise Market",
            status="review",
            score=61.0,
            current_title_match=False,
            location_aligned=True,
            location_bucket="named_target_location",
            parser_confidence=0.72,
            evidence_quality_score=0.58,
            skill_overlap_score=0.52,
            current_function_fit=0.78,
            years_fit_score=0.54,
            industry_fit_score=0.41,
            cap_reasons=[],
        ),
    ]

    ordered = prepare_verification_shortlist(
        candidates,
        brief=brief,
        company_required=False,
        verification_limit=2,
    )

    assert [candidate.full_name for candidate in ordered[:2]] == [
        "Title Match Weak Market",
        "Strong Function Precise Market",
    ]


def test_prepare_verification_shortlist_prefers_anchor_rich_precise_profiles() -> None:
    candidates = [
        CandidateProfile(
            full_name="Generic Precise Match",
            current_title="Supply Chain Manager",
            current_company="Generic Retailer",
            location_name="Dubai",
            current_title_match=True,
            location_aligned=True,
            location_precision_bucket="named_target_location",
            parser_confidence=0.74,
            evidence_quality_score=0.44,
            source_quality_score=0.22,
            current_function_fit=0.56,
            skill_overlap_score=0.2,
            industry_fit_score=0.12,
            company_match_score=0.08,
            verification_status="review",
            score=67.0,
            source_url="https://example.com/company/news",
        ),
        CandidateProfile(
            full_name="Anchor Rich Precise Match",
            current_title="Supply Chain Manager",
            current_company="Amazon",
            location_name="Dubai",
            current_title_match=True,
            location_aligned=True,
            location_precision_bucket="named_target_location",
            parser_confidence=0.66,
            evidence_quality_score=0.38,
            source_quality_score=0.55,
            current_function_fit=0.84,
            skill_overlap_score=0.64,
            industry_fit_score=0.62,
            company_match_score=0.52,
            verification_status="review",
            score=61.0,
            linkedin_url="https://www.linkedin.com/in/anchor-rich-precise-match",
        ),
    ]

    ordered = prepare_verification_shortlist(
        candidates,
        company_required=False,
        verification_limit=2,
    )

    assert [candidate.full_name for candidate in ordered[:2]] == [
        "Anchor Rich Precise Match",
        "Generic Precise Match",
    ]
