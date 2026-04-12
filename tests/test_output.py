from hr_hunter.models import CandidateProfile
from hr_hunter.output import build_reporting_summary, build_scope_progress_counts


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


def test_build_scope_progress_counts_only_reports_scope_metrics() -> None:
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

    counts = build_scope_progress_counts(candidates)

    assert counts["in_scope_count"] == 1
    assert counts["precise_in_scope_count"] == 1
    assert counts["title_match_count"] == 1
    assert "verified_count" not in counts
    assert "review_count" not in counts
    assert "reject_count" not in counts
