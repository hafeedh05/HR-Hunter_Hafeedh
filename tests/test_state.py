import asyncio
from pathlib import Path

import httpx
import pytest

from hr_hunter.briefing import build_search_brief
from hr_hunter.db import connect_database, resolve_database_target
from hr_hunter.models import CandidateProfile, SearchRunReport
from hr_hunter.remote import RemoteSourcingClient, RemoteSourcingError
from hr_hunter.scoring import score_candidate
from hr_hunter.state import (
    attach_registry_metadata,
    enqueue_job,
    expire_stale_jobs,
    find_similar_candidates,
    latest_project_job,
    list_review_history,
    list_run_history,
    load_job,
    persist_search_run,
    review_candidate,
    search_registry_memory,
    start_job,
    stop_job,
    summarize_system_state,
)
from hr_hunter.output import write_report


def _build_brief():
    return build_search_brief(
        {
            "id": "state-test-brief",
            "role_title": "Senior Data Analyst",
            "titles": ["Senior Data Analyst", "Data Analyst"],
            "company_targets": ["noon", "Careem"],
            "geography": {"location_name": "Dubai", "country": "United Arab Emirates"},
            "required_keywords": ["sql", "python"],
            "provider_settings": {"registry_memory": {"enabled": True, "limit": 10}},
        }
    )


def test_persist_search_run_populates_history_and_registry(tmp_path: Path) -> None:
    brief = _build_brief()
    candidate = score_candidate(
        CandidateProfile(
            full_name="Registry Candidate",
            current_title="Senior Data Analyst",
            current_company="noon",
            location_name="Dubai, United Arab Emirates",
            linkedin_url="https://www.linkedin.com/in/registry-candidate",
            summary="SQL and Python analytics leader.",
        ),
        brief,
    )
    report = SearchRunReport(
        run_id="registry-run",
        brief_id=brief.id,
        dry_run=False,
        generated_at="2026-04-06T00:00:00+00:00",
        provider_results=[],
        candidates=[candidate],
        summary={"role_title": brief.role_title},
    )
    db_path = tmp_path / "state.db"

    state_record = persist_search_run(
        brief,
        report,
        provider_names=["scrapingbee_google"],
        limit_requested=20,
        db_path=db_path,
        json_report_path=tmp_path / "report.json",
        csv_report_path=tmp_path / "report.csv",
    )

    assert state_record["candidate_count"] == 1
    history = list_run_history(db_path=db_path)
    assert history[0]["run_id"] == "registry-run"
    assert history[0]["candidate_count"] == 1

    attached = attach_registry_metadata([candidate], db_path=db_path)[0]
    assert attached.raw["registry"]["search_count"] == 1

    memory_results = search_registry_memory(brief, db_path=db_path, limit=5)
    assert memory_results
    assert memory_results[0].full_name == "Registry Candidate"


def test_review_and_similar_candidates_are_persisted(tmp_path: Path) -> None:
    brief = _build_brief()
    candidate_one = score_candidate(
        CandidateProfile(
            full_name="Candidate One",
            current_title="Senior Data Analyst",
            current_company="noon",
            location_name="Dubai, United Arab Emirates",
            linkedin_url="https://www.linkedin.com/in/candidate-one",
            summary="SQL and Python analytics leader.",
        ),
        brief,
    )
    candidate_two = score_candidate(
        CandidateProfile(
            full_name="Candidate Two",
            current_title="Data Analyst",
            current_company="Careem",
            location_name="Dubai, United Arab Emirates",
            linkedin_url="https://www.linkedin.com/in/candidate-two",
            summary="Business intelligence and reporting specialist.",
        ),
        brief,
    )
    report = SearchRunReport(
        run_id="review-run",
        brief_id=brief.id,
        dry_run=False,
        generated_at="2026-04-06T00:00:00+00:00",
        provider_results=[],
        candidates=[candidate_one, candidate_two],
        summary={"role_title": brief.role_title},
    )
    db_path = tmp_path / "state.db"
    persist_search_run(
        brief,
        report,
        provider_names=["scrapingbee_google"],
        limit_requested=20,
        db_path=db_path,
    )

    review = review_candidate(
        mandate_id=f"mandate:local:{brief.id}",
        run_id=report.run_id,
        candidate_id="url:linkedin.com/in/candidate-one",
        reviewer_id="rec-1",
        reviewer_name="Recruiter",
        action="shortlist",
        note="Looks strong.",
        db_path=db_path,
    )
    assert review["action"] == "shortlist"
    review_history = list_review_history(db_path=db_path, limit=5)
    assert review_history
    assert review_history[0]["candidate_id"] == "url:linkedin.com/in/candidate-one"
    assert review_history[0]["full_name"] == "Candidate One"

    similar = find_similar_candidates(candidate_one, db_path=db_path, limit=5)
    assert similar
    assert similar[0]["full_name"] == "Candidate Two"

    ops = summarize_system_state(db_path=db_path)
    assert ops["counts"]["review_actions"] == 1


def test_summarize_system_state_supports_mapping_rows(monkeypatch) -> None:
    class _FakeCursor:
        def __init__(self, row):
            self._row = row

        def fetchone(self):
            return self._row

    class _FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=None):
            if "ORDER BY created_at DESC" in sql:
                return _FakeCursor(
                    {
                        "id": "run-postgres",
                        "created_at": "2026-04-08T00:00:00+00:00",
                        "execution_backend": "local_engine",
                        "candidate_count": 4,
                    }
                )
            return _FakeCursor({"count": 2})

    monkeypatch.setattr("hr_hunter.state.init_state_db", lambda db_path=None: "postgresql://fake/hr_hunter")
    monkeypatch.setattr("hr_hunter.state._connect", lambda db_path=None: _FakeConnection())

    ops = summarize_system_state()

    assert ops["db_path"] == "postgresql://fake/hr_hunter"
    assert ops["counts"]["mandates"] == 2
    assert ops["counts"]["jobs"] == 2
    assert ops["latest_run"]["id"] == "run-postgres"


def test_project_run_persistence_allows_same_brief_id_across_project_and_local_mandates(tmp_path: Path) -> None:
    brief = _build_brief()
    candidate = score_candidate(
        CandidateProfile(
            full_name="Project Candidate",
            current_title="Senior Data Analyst",
            current_company="noon",
            location_name="Dubai, United Arab Emirates",
            linkedin_url="https://www.linkedin.com/in/project-candidate",
            summary="SQL and Python analytics leader.",
        ),
        brief,
    )
    local_report = SearchRunReport(
        run_id="local-run",
        brief_id=brief.id,
        dry_run=False,
        generated_at="2026-04-06T00:00:00+00:00",
        provider_results=[],
        candidates=[candidate],
        summary={"role_title": brief.role_title},
    )
    project_report = SearchRunReport(
        run_id="project-run",
        brief_id=brief.id,
        dry_run=False,
        generated_at="2026-04-06T00:05:00+00:00",
        provider_results=[],
        candidates=[candidate],
        summary={"role_title": brief.role_title},
    )
    db_path = tmp_path / "state.db"

    persist_search_run(
        brief,
        local_report,
        provider_names=["scrapingbee_google"],
        limit_requested=20,
        db_path=db_path,
    )
    state_record = persist_search_run(
        brief,
        project_report,
        provider_names=["scrapingbee_google"],
        limit_requested=20,
        db_path=db_path,
        mandate_id_override="project_abc123",
    )

    assert state_record["mandate_id"] == "project_abc123"
    project_history = list_run_history(db_path=db_path, mandate_id="project_abc123")
    assert project_history
    assert project_history[0]["run_id"] == "project-run"


def test_list_run_history_recovers_summary_from_saved_report(tmp_path: Path) -> None:
    brief = _build_brief()
    candidate = score_candidate(
        CandidateProfile(
            full_name="History Candidate",
            current_title="Senior Data Analyst",
            current_company="noon",
            location_name="Dubai, United Arab Emirates",
            linkedin_url="https://www.linkedin.com/in/history-candidate",
            summary="SQL and Python analytics leader.",
        ),
        brief,
    )
    report = SearchRunReport(
        run_id="history-summary-run",
        brief_id=brief.id,
        dry_run=False,
        generated_at="2026-04-07T00:00:00+00:00",
        provider_results=[],
        candidates=[candidate],
        summary={},
    )
    db_path = tmp_path / "state.db"
    json_path, csv_path = write_report(report, tmp_path)
    persist_search_run(
        brief,
        report,
        provider_names=["scrapingbee_google"],
        limit_requested=20,
        db_path=db_path,
        json_report_path=json_path,
        csv_report_path=csv_path,
    )

    import sqlite3, json as _json

    with sqlite3.connect(str(db_path)) as connection:
        connection.execute("UPDATE search_runs SET summary_json = ? WHERE id = ?", (_json.dumps({}), report.run_id))

    history = list_run_history(db_path=db_path)

    assert history[0]["summary"]["verified_count"] == 1
    assert history[0]["summary"]["candidate_count"] == 1


def test_remote_sourcing_client_uses_default_endpoint_when_required(monkeypatch) -> None:
    monkeypatch.delenv("REMOTE_SOURCING_API_URL", raising=False)
    monkeypatch.setenv("REMOTE_SOURCING_API_KEY", "test-key")
    monkeypatch.setenv("REMOTE_SOURCING_REQUIRED", "true")

    client = RemoteSourcingClient()

    assert client.is_configured() is True
    assert client.base_url == "https://openclaw.hyvelabs.tech/api/hr-hunter"


def test_remote_sourcing_client_marks_gateway_timeout_as_recoverable(monkeypatch) -> None:
    class _FakeAsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, headers=None, json=None):
            request = httpx.Request("POST", url)
            return httpx.Response(504, request=request)

    monkeypatch.setenv("REMOTE_SOURCING_API_URL", "https://example.com/api/hr-hunter")
    monkeypatch.setenv("REMOTE_SOURCING_REQUIRED", "true")
    monkeypatch.setattr("hr_hunter.remote.httpx.AsyncClient", _FakeAsyncClient)

    client = RemoteSourcingClient()

    with pytest.raises(RemoteSourcingError) as exc_info:
        asyncio.run(client.request("/search", {"test": True}))

    assert exc_info.value.recoverable is True
    assert exc_info.value.status_code == 504


def test_remote_sourcing_client_uses_internal_fetch_limit_when_present(monkeypatch) -> None:
    captured = {}

    async def _fake_request(self, path, payload):
        captured["path"] = path
        captured["payload"] = payload
        return {"runId": "remote-run", "provider_results": [], "candidates": [], "summary": {}}

    monkeypatch.setenv("REMOTE_SOURCING_API_URL", "https://example.com/api/hr-hunter")
    monkeypatch.setattr(RemoteSourcingClient, "request", _fake_request)

    client = RemoteSourcingClient()
    ui_payload = {
        "limit": 100,
        "internal_fetch_limit": 220,
        "brief_config": {
            "id": "search-1",
            "role_title": "Senior Data Analyst",
            "titles": ["Senior Data Analyst"],
            "company_targets": ["noon"],
            "required_keywords": ["sql"],
            "preferred_keywords": [],
            "industry_keywords": [],
            "exclude_title_keywords": [],
            "exclude_company_keywords": [],
            "seniority_levels": [],
            "years_target": 6,
            "years_tolerance": 2,
            "anchors": {},
            "document_text": "sql",
            "employment_status_mode": "any",
            "jd_breakdown": {"key_experience_points": []},
        },
        "job_description_breakdown": {"key_experience_points": []},
    }
    payload = {"org_id": "local", "countries": ["United Arab Emirates"]}

    report = asyncio.run(client.run_search(payload, ui_payload))

    assert report.run_id == "remote-run"
    assert captured["path"] == "/search"
    assert captured["payload"]["limit"] == 220
    assert captured["payload"]["requestedLimit"] == 100


def test_stop_job_marks_running_job_failed_and_latest_project_job_returns_it(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    queued = enqueue_job("search", {"project_id": "project_123", "role_title": "Supply Chain Manager"}, db_path=db_path)
    start_job(queued["job_id"], db_path=db_path)

    stopped = stop_job(queued["job_id"], reason="Stopped by admin. Retry when ready.", db_path=db_path)

    assert stopped is not None
    assert stopped["status"] == "failed"
    assert "Retry" in stopped["error"]

    latest = latest_project_job("project_123", db_path=db_path)
    assert latest is not None
    assert latest["job_id"] == queued["job_id"]
    assert latest["status"] == "failed"


def test_expire_stale_jobs_marks_old_running_jobs_failed(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    queued = enqueue_job("search", {"project_id": "project_456", "role_title": "Senior Brand Manager"}, db_path=db_path)
    start_job(queued["job_id"], db_path=db_path)

    target = db_path
    with connect_database(resolve_database_target(target, env_var="HR_HUNTER_STATE_DB", default_path="output/state/hr_hunter_state.db")) as connection:
        connection.execute(
            "UPDATE jobs SET created_at = ?, started_at = ? WHERE id = ?",
            ("2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00", queued["job_id"]),
        )

    expired = expire_stale_jobs(db_path=db_path, max_age_seconds=60)
    job = load_job(queued["job_id"], db_path=db_path)

    assert queued["job_id"] in expired
    assert job is not None
    assert job["status"] == "failed"
    assert "Please retry" in job["error"]
