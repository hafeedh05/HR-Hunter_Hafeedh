from hr_hunter.briefing import build_search_brief
from hr_hunter.api import (
    _apply_strict_shortlist,
    _collect_candidate_keys_from_report,
    _collect_provider_query_exclusions_from_report,
    _finalize_report_for_limit,
    _job_actor_from_payload,
    _quality_recovery_gap,
    _quality_recovery_settings,
    _quality_recovery_verification_candidates,
    _resolve_effective_verification_target,
    _resolve_pipeline_progress_percent,
    _runtime_storage_snapshot,
    _should_stop_after_stagnant_top_up,
    _verification_progress_base,
)
from hr_hunter.models import CandidateProfile, GeoSpec, ProviderRunResult, SearchBrief, SearchRunReport


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


def test_finalize_report_for_limit_tracks_title_and_market_counts():
    report = SearchRunReport(
        run_id="run-scope-test",
        brief_id="brief-scope-test",
        dry_run=False,
        generated_at="2026-04-12T00:00:00+00:00",
        provider_results=[],
        candidates=[
            CandidateProfile(
                full_name="Exact Dubai Match",
                current_title_match=True,
                location_aligned=True,
                location_precision_bucket="named_target_location",
                industry_fit_score=0.42,
                parser_confidence=0.72,
                verification_status="review",
            ),
            CandidateProfile(
                full_name="Country Match",
                current_title_match=True,
                location_aligned=True,
                location_precision_bucket="country_only",
                skill_overlap_score=0.26,
                parser_confidence=0.68,
                verification_status="reject",
            ),
            CandidateProfile(
                full_name="Wrong Market",
                current_title_match=True,
                location_aligned=False,
                location_precision_bucket="outside_target_area",
                parser_confidence=0.84,
                verification_status="reject",
            ),
        ],
        summary={"pipeline_metrics": {"queries_completed": 5, "queries_total": 5, "raw_found": 3, "unique_after_dedupe": 3}},
    )

    finalized = _finalize_report_for_limit(report, requested_limit=3, internal_fetch_limit=10)

    assert finalized.summary["title_match_count"] == 3
    assert finalized.summary["market_match_count"] == 2
    assert "in_scope_count" not in finalized.summary
    assert "precise_in_scope_count" not in finalized.summary
    assert "scope_counts" not in finalized.summary


def test_finalize_report_for_limit_does_not_emit_scope_counts_for_title_geo_noise():
    report = SearchRunReport(
        run_id="run-scope-noise-test",
        brief_id="brief-scope-noise-test",
        dry_run=False,
        generated_at="2026-04-13T00:00:00+00:00",
        provider_results=[],
        candidates=[
            CandidateProfile(
                full_name="Title And Geo Only",
                current_title_match=True,
                location_aligned=True,
                location_precision_bucket="named_target_location",
                parser_confidence=0.78,
                verification_status="reject",
            )
        ],
        summary={"pipeline_metrics": {"queries_completed": 1, "queries_total": 1, "raw_found": 1, "unique_after_dedupe": 1}},
    )

    finalized = _finalize_report_for_limit(report, requested_limit=1, internal_fetch_limit=5)

    assert "in_scope_count" not in finalized.summary
    assert "precise_in_scope_count" not in finalized.summary


def test_finalize_report_for_limit_honors_title_market_priority_brief() -> None:
    brief = build_search_brief(
        {
            "id": "scope-priority-test",
            "role_title": "Supply Chain Manager",
            "titles": ["Supply Chain Manager", "Senior Supply Chain Manager"],
            "geography": {
                "location_name": "Dubai",
                "country": "United Arab Emirates",
                "location_hints": ["Abu Dhabi"],
            },
        }
    )
    report = SearchRunReport(
        run_id="run-scope-priority-test",
        brief_id="brief-scope-priority-test",
        dry_run=False,
        generated_at="2026-04-13T00:00:00+00:00",
        provider_results=[],
        candidates=[
            CandidateProfile(
                full_name="Strong Function Precise Market",
                current_title="Operations Director",
                current_title_match=False,
                location_aligned=True,
                location_precision_bucket="named_target_location",
                current_function_fit=0.82,
                skill_overlap_score=0.58,
                parser_confidence=0.72,
                evidence_quality_score=0.61,
                verification_status="review",
                score=61.0,
            ),
            CandidateProfile(
                full_name="Title Match Weak Market",
                current_title="Supply Chain Manager",
                current_title_match=True,
                location_aligned=False,
                location_precision_bucket="outside_target_area",
                current_function_fit=0.28,
                skill_overlap_score=0.2,
                parser_confidence=0.31,
                evidence_quality_score=0.18,
                verification_status="review",
                score=49.0,
            ),
        ],
        summary={"pipeline_metrics": {"queries_completed": 4, "queries_total": 4, "raw_found": 2, "unique_after_dedupe": 2}},
    )

    finalized = _finalize_report_for_limit(report, requested_limit=2, internal_fetch_limit=10, brief=brief)

    assert [candidate.full_name for candidate in finalized.candidates] == [
        "Title Match Weak Market",
        "Strong Function Precise Market",
    ]


def test_quality_recovery_helpers_capture_thresholds_and_fresh_candidates() -> None:
    brief = build_search_brief(
        {
            "id": "quality-recovery-test",
            "role_title": "Supply Chain Manager",
            "titles": ["Supply Chain Manager"],
            "provider_settings": {
                "quality_recovery": {
                    "enabled": True,
                    "min_verified_count": 50,
                    "max_reject_count": 50,
                    "max_rounds": 3,
                    "fetch_limit_increment": 120,
                    "parallel_requests": 32,
                    "max_queries": 96,
                    "reranker_top_n": 300,
                    "verification_top_n": 220,
                    "verification_parallel_candidates": 48,
                    "disable_history_slices": True,
                    "disable_registry_memory": True,
                }
            },
        }
    )
    settings = _quality_recovery_settings(brief, requested_limit=300, current_fetch_limit=420)

    assert settings["enabled"] is True
    assert settings["min_verified_count"] == 50
    assert settings["max_reject_count"] == 50
    assert settings["fetch_limit_increment"] == 120

    gap = _quality_recovery_gap({"verified_count": 8, "reject_count": 287}, settings)

    assert gap["should_retry"] is True
    assert gap["verified_gap"] == 42
    assert gap["reject_gap"] == 237

    verified_candidate = CandidateProfile(full_name="Already Checked", verification_status="verified", last_verified_at="2026-04-13T00:00:00+00:00")
    fresh_candidate = CandidateProfile(full_name="Fresh Candidate", verification_status="reject")
    retry_candidate = CandidateProfile(full_name="Retry Candidate", verification_status="review")
    selected = _quality_recovery_verification_candidates(
        [verified_candidate, fresh_candidate, retry_candidate],
        limit=2,
    )

    assert [candidate.full_name for candidate in selected] == ["Fresh Candidate", "Retry Candidate"]


def test_collect_provider_query_exclusions_from_report_skips_skipped_queries() -> None:
    report = SearchRunReport(
        run_id="query-exclusion-test",
        brief_id="brief-query-exclusion-test",
        dry_run=False,
        generated_at="2026-04-13T00:00:00+00:00",
        provider_results=[
            ProviderRunResult(
                provider_name="scrapingbee_google",
                executed=True,
                dry_run=False,
                diagnostics={
                    "queries": [
                        {"search": "supply chain manager dubai", "fingerprint": "fp-1", "skipped": False},
                        {"search": "supply chain manager uae", "fingerprint": "fp-2", "skipped": True},
                    ]
                },
            )
        ],
        candidates=[
            CandidateProfile(full_name="Candidate One", linkedin_url="https://linkedin.com/in/candidate-one"),
        ],
    )

    assert "url:linkedin.com/in/candidate-one" in _collect_candidate_keys_from_report(report)
    assert _collect_provider_query_exclusions_from_report(report) == {
        "scrapingbee_google": {"fp-1", "supply chain manager dubai"}
    }


def test_verification_progress_base_keeps_monotonic_retrieval_counts():
    progress_base = _verification_progress_base(
        {
            "queries_completed": 42,
            "queries_total": 42,
            "raw_found": 81,
            "unique_after_dedupe": 81,
            "rerank_target": 180,
            "reranked_count": 180,
        },
        {
            "queries_completed": 42,
            "queries_total": 42,
            "raw_found": 378,
            "unique_after_dedupe": 378,
            "rerank_target": 180,
            "reranked_count": 180,
        },
    )

    assert progress_base == {
        "queries_completed": 42,
        "queries_total": 42,
        "raw_found": 378,
        "unique_after_dedupe": 378,
        "rerank_target": 180,
        "reranked_count": 180,
    }


def test_finalize_report_for_limit_uses_final_priority_order_after_verification() -> None:
    brief = build_search_brief(
        {
            "id": "scope-final-order-test",
            "role_title": "Supply Chain Manager",
            "titles": ["Supply Chain Manager"],
            "geography": {
                "location_name": "Dubai",
                "country": "United Arab Emirates",
            },
        }
    )
    report = SearchRunReport(
        run_id="run-final-order-test",
        brief_id="brief-final-order-test",
        dry_run=False,
        generated_at="2026-04-13T00:00:00+00:00",
        provider_results=[],
        candidates=[
            CandidateProfile(
                full_name="Rejected But Proof Heavy",
                current_title="Supply Chain Manager",
                current_title_match=True,
                location_aligned=True,
                location_precision_bucket="named_target_location",
                current_employment_confirmed=True,
                current_location_confirmed=True,
                precise_location_confirmed=True,
                current_company_confirmed=True,
                current_title_confirmed=True,
                current_role_proof_count=4,
                current_function_fit=0.86,
                skill_overlap_score=0.74,
                industry_fit_score=0.62,
                parser_confidence=0.8,
                evidence_quality_score=0.78,
                verification_status="reject",
                score=78.0,
            ),
            CandidateProfile(
                full_name="Verified Exact Match",
                current_title="Supply Chain Manager",
                current_title_match=True,
                location_aligned=True,
                location_precision_bucket="named_target_location",
                current_employment_confirmed=True,
                current_location_confirmed=True,
                precise_location_confirmed=True,
                current_company_confirmed=True,
                current_title_confirmed=True,
                current_role_proof_count=1,
                current_function_fit=0.82,
                skill_overlap_score=0.7,
                industry_fit_score=0.64,
                parser_confidence=0.78,
                evidence_quality_score=0.76,
                verification_status="verified",
                score=74.0,
            ),
        ],
        summary={"pipeline_metrics": {"queries_completed": 2, "queries_total": 2, "raw_found": 2, "unique_after_dedupe": 2}},
    )

    finalized = _finalize_report_for_limit(report, requested_limit=2, internal_fetch_limit=10, brief=brief)

    assert [candidate.full_name for candidate in finalized.candidates] == [
        "Verified Exact Match",
        "Rejected But Proof Heavy",
    ]


def test_resolve_effective_verification_target_trims_weak_tail() -> None:
    candidates = [
            CandidateProfile(
                full_name=f"In Scope {index}",
                current_title="Supply Chain Manager",
                current_title_match=True,
                location_aligned=True,
                location_precision_bucket="named_target_location",
                industry_fit_score=0.41,
                parser_confidence=0.72,
                verification_status="review",
                score=68.0,
            )
        for index in range(30)
    ] + [
        CandidateProfile(
            full_name=f"Adjacent {index}",
            current_title="Operations Director",
            current_title_match=False,
            location_aligned=True,
            location_precision_bucket="country_only",
            parser_confidence=0.28,
            verification_status="reject",
            score=44.0,
        )
        for index in range(110)
    ]

    plan = _resolve_effective_verification_target(
        candidates,
        requested_limit=100,
        verification_target=80,
        company_required=False,
    )

    assert plan["requested_target"] == 80
    assert plan["effective_target"] == 80


def test_resolve_effective_verification_target_does_not_blindly_follow_oversized_scope_goal() -> None:
    candidates = [
            CandidateProfile(
                full_name=f"Precise In Scope {index}",
                current_title="Chief Executive Officer",
                current_title_match=True,
                location_aligned=True,
                location_precision_bucket="named_target_location",
                industry_fit_score=0.4,
                parser_confidence=0.76,
                verification_status="review",
                score=72.0,
            )
        for index in range(41)
    ] + [
        CandidateProfile(
            full_name=f"Adjacent Executive {index}",
            current_title="Managing Director",
            current_title_match=False,
            location_aligned=True,
            location_precision_bucket="country_only",
            parser_confidence=0.3,
            verification_status="reject",
            score=45.0,
        )
        for index in range(99)
    ]

    plan = _resolve_effective_verification_target(
        candidates,
        requested_limit=300,
        verification_target=140,
        company_required=False,
    )

    assert plan["requested_target"] == 140
    assert plan["effective_target"] == 140


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


def test_runtime_storage_snapshot_prefers_shared_database_url(monkeypatch):
    monkeypatch.setenv("HR_HUNTER_DATABASE_URL", "postgresql://user:secret@db.internal/hr_hunter")
    monkeypatch.delenv("HR_HUNTER_STATE_DB", raising=False)

    snapshot = _runtime_storage_snapshot()

    assert snapshot["state"]["backend"] == "postgres"
    assert snapshot["state"]["display_locator"] == "postgresql://<redacted>/hr_hunter"
    assert snapshot["workspace"]["display_locator"] == "postgresql://<redacted>/hr_hunter"


def test_apply_strict_shortlist_keeps_company_market_matches_only():
    brief = SearchBrief(
        id="brief-test",
        role_title="Chief Executive Officer (CEO)",
        brief_document_path=None,
        brief_summary="",
        titles=["Chief Executive Officer", "Managing Director", "President"],
        title_keywords=["Chief Executive Officer", "Managing Director", "President"],
        company_targets=["The One", "Marina Home Interiors"],
        peer_company_targets=[],
        sourcing_company_targets=["The One", "Marina Home Interiors"],
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

    shortlisted = _apply_strict_shortlist(report, brief=brief)

    assert [candidate.full_name for candidate in shortlisted.candidates] == [
        "Exact Match",
        "Adjacent Title Same Company Market",
    ]
    assert shortlisted.summary["strict_shortlist"] == {
        "enabled": True,
        "candidate_count": 2,
        "filtered_out_count": 2,
        "exact_title_count": 1,
        "company_market_count": 2,
    }


def test_finalize_report_for_limit_applies_strict_shortlist_when_brief_present():
    brief = SearchBrief(
        id="brief-test",
        role_title="Chief Executive Officer (CEO)",
        brief_document_path=None,
        brief_summary="",
        titles=["Chief Executive Officer", "Managing Director", "President"],
        title_keywords=["Chief Executive Officer", "Managing Director", "President"],
        company_targets=["The One", "Marina Home Interiors"],
        peer_company_targets=[],
        sourcing_company_targets=["The One", "Marina Home Interiors"],
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
    assert finalized.summary["strict_shortlist"]["candidate_count"] == 2
    assert finalized.summary["pipeline_metrics"]["finalized_count"] == 2
