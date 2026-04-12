from hr_hunter.api import (
    _apply_strict_scope_shortlist,
    _finalize_report_for_limit,
    _job_actor_from_payload,
    _resolve_pipeline_progress_percent,
    _should_stop_after_stagnant_top_up,
)
from hr_hunter.models import CandidateProfile, GeoSpec, SearchBrief, SearchRunReport


def test_job_actor_from_payload_preserves_admin_flag():
    actor = _job_actor_from_payload(
        {
            "recruiter_id": "user_admin_hrhunter",
            "recruiter_name": "HR Hunter Admin",
            "team_id": "leadership",
            "recruiter_is_admin": True,
        }
    )

    assert actor == {
        "id": "user_admin_hrhunter",
        "full_name": "HR Hunter Admin",
        "team_id": "leadership",
        "is_admin": True,
    }


def test_resolve_pipeline_progress_percent_stays_monotonic_across_stages():
    assert _resolve_pipeline_progress_percent(
        stage="verifying",
        explicit_percent=92,
        previous_percent=95,
        queries_completed=13,
        queries_total=13,
    ) == 95
    assert _resolve_pipeline_progress_percent(
        stage="finalizing",
        explicit_percent=97,
        previous_percent=98,
        queries_completed=13,
        queries_total=13,
    ) == 98


def test_finalize_report_for_limit_keeps_raw_found_above_unique_pool():
    report = SearchRunReport(
        run_id="run-test",
        brief_id="brief-test",
        dry_run=False,
        generated_at="2026-04-11T00:00:00+00:00",
        provider_results=[],
        candidates=[
            CandidateProfile(full_name=f"Candidate {index}", verification_status="reject")
            for index in range(60)
        ],
        summary={
            "pipeline_metrics": {
                "queries_completed": 13,
                "queries_total": 13,
                "raw_found": 28,
                "unique_after_dedupe": 117,
                "rerank_target": 100,
                "reranked_count": 100,
                "finalized_count": 60,
            }
        },
    )

    finalized = _finalize_report_for_limit(report, requested_limit=50, internal_fetch_limit=100)
    pipeline_metrics = finalized.summary["pipeline_metrics"]

    assert len(finalized.candidates) == 50
    assert pipeline_metrics["unique_after_dedupe"] == 117
    assert pipeline_metrics["raw_found"] == 117
    assert pipeline_metrics["finalized_count"] == 50


def test_should_stop_after_stagnant_top_up_when_near_target():
    assert _should_stop_after_stagnant_top_up(
        requested_limit=50,
        updated_unique_count=40,
        top_up_rounds=1,
        stagnant_rounds=1,
    ) is True


def test_should_not_stop_after_single_stagnant_top_up_when_gap_is_still_large():
    assert _should_stop_after_stagnant_top_up(
        requested_limit=50,
        updated_unique_count=28,
        top_up_rounds=1,
        stagnant_rounds=1,
    ) is False


def test_apply_strict_scope_shortlist_keeps_company_market_matches_only():
    brief = SearchBrief(
        id="brief-test",
        role_title="Chief Executive Officer (CEO)",
        brief_document_path=None,
        brief_summary="",
        titles=["Chief Executive Officer", "Managing Director", "President"],
        title_keywords=["Chief Executive Officer", "Managing Director", "President"],
        company_targets=["The One", "Marina Home Interiors"],
        company_aliases={},
        geography=GeoSpec(location_name="Dubai", country="United Arab Emirates", location_hints=["Dubai"]),
        required_keywords=[],
        preferred_keywords=[],
        portfolio_keywords=[],
        commercial_keywords=[],
        leadership_keywords=[],
        scope_keywords=[],
        seniority_levels=[],
        minimum_years_experience=None,
        maximum_years_experience=None,
        result_target_min=5,
        result_target_max=50,
        max_profiles=50,
        location_targets=["Dubai", "United Arab Emirates"],
        company_match_mode="current_only",
        allow_adjacent_titles=False,
        exact_company_scope=True,
        strict_market_scope=True,
    )
    report = SearchRunReport(
        run_id="run-test",
        brief_id="brief-test",
        dry_run=False,
        generated_at="2026-04-12T00:00:00+00:00",
        provider_results=[],
        candidates=[
            CandidateProfile(
                full_name="Exact Match",
                current_target_company_match=True,
                current_title_match=True,
                location_aligned=True,
                verification_status="verified",
                score=72.0,
            ),
            CandidateProfile(
                full_name="Adjacent Title Same Company Market",
                current_target_company_match=True,
                current_title_match=False,
                location_aligned=True,
                verification_status="review",
                score=61.0,
            ),
            CandidateProfile(
                full_name="Same Company Wrong Market",
                current_target_company_match=True,
                current_title_match=True,
                location_aligned=False,
                verification_status="review",
                score=59.0,
            ),
            CandidateProfile(
                full_name="Wrong Company Same Market",
                current_target_company_match=False,
                current_title_match=True,
                location_aligned=True,
                verification_status="review",
                score=58.0,
            ),
        ],
        summary={},
    )

    shortlisted = _apply_strict_scope_shortlist(report, brief=brief)

    assert [candidate.full_name for candidate in shortlisted.candidates] == [
        "Exact Match",
        "Adjacent Title Same Company Market",
    ]
    assert shortlisted.summary["strict_scope_shortlist"] == {
        "enabled": True,
        "scope_candidate_count": 2,
        "scope_filtered_out_count": 2,
        "exact_title_scope_count": 1,
        "company_market_scope_count": 2,
    }


def test_finalize_report_for_limit_applies_strict_scope_shortlist_when_brief_present():
    brief = SearchBrief(
        id="brief-test",
        role_title="Chief Executive Officer (CEO)",
        brief_document_path=None,
        brief_summary="",
        titles=["Chief Executive Officer", "Managing Director", "President"],
        title_keywords=["Chief Executive Officer", "Managing Director", "President"],
        company_targets=["The One", "Marina Home Interiors"],
        company_aliases={},
        geography=GeoSpec(location_name="Dubai", country="United Arab Emirates", location_hints=["Dubai"]),
        required_keywords=[],
        preferred_keywords=[],
        portfolio_keywords=[],
        commercial_keywords=[],
        leadership_keywords=[],
        scope_keywords=[],
        seniority_levels=[],
        minimum_years_experience=None,
        maximum_years_experience=None,
        result_target_min=5,
        result_target_max=50,
        max_profiles=50,
        location_targets=["Dubai", "United Arab Emirates"],
        company_match_mode="current_only",
        allow_adjacent_titles=False,
        exact_company_scope=True,
        strict_market_scope=True,
    )
    report = SearchRunReport(
        run_id="run-test",
        brief_id="brief-test",
        dry_run=False,
        generated_at="2026-04-12T00:00:00+00:00",
        provider_results=[],
        candidates=[
            CandidateProfile(
                full_name="Exact Match",
                current_target_company_match=True,
                current_title_match=True,
                location_aligned=True,
                verification_status="verified",
                score=72.0,
            ),
            CandidateProfile(
                full_name="Adjacent Title Same Company Market",
                current_target_company_match=True,
                current_title_match=False,
                location_aligned=True,
                verification_status="review",
                score=61.0,
            ),
            CandidateProfile(
                full_name="Wrong Company Same Market",
                current_target_company_match=False,
                current_title_match=True,
                location_aligned=True,
                verification_status="review",
                score=58.0,
            ),
        ],
        summary={"pipeline_metrics": {"queries_completed": 1, "queries_total": 1, "raw_found": 3, "unique_after_dedupe": 3}},
    )

    finalized = _finalize_report_for_limit(
        report,
        requested_limit=10,
        internal_fetch_limit=25,
        brief=brief,
    )

    assert [candidate.full_name for candidate in finalized.candidates] == [
        "Exact Match",
        "Adjacent Title Same Company Market",
    ]
    assert finalized.summary["strict_scope_shortlist"]["scope_candidate_count"] == 2
    assert finalized.summary["pipeline_metrics"]["finalized_count"] == 2
