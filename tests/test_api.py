from hr_hunter.api import (
    _finalize_report_for_limit,
    _job_actor_from_payload,
    _resolve_pipeline_progress_percent,
    _should_stop_after_stagnant_top_up,
)
from hr_hunter.models import CandidateProfile, SearchRunReport


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
