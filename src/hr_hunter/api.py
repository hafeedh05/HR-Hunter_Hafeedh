from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Set

from hr_hunter.briefing import build_search_brief
from hr_hunter.config import (
    load_env_file,
    load_yaml_file,
    resolve_feedback_db_path,
    resolve_output_dir,
    resolve_ranker_model_dir,
    resolve_state_db_path,
)
from hr_hunter.db import describe_database_target, resolve_database_target
from hr_hunter.engine import SearchEngine, dedupe_candidates
from hr_hunter.feedback import export_training_rows, init_feedback_db, load_ranker_training_rows, log_feedback
from hr_hunter.identity import candidate_identity_keys
from hr_hunter.candidate_order import candidate_is_verification_ready
from hr_hunter.output import (
    build_progress_counts,
    build_reporting_summary,
    collect_seen_candidate_keys,
    collect_seen_provider_queries,
    prioritize_final_candidates,
    prioritize_verification_candidates,
    write_report,
)
from hr_hunter.parsers.documents import extract_document_text_from_bytes
from hr_hunter.recruiter_app import (
    assess_ui_brief_quality,
    build_app_bootstrap,
    build_ui_brief_payload,
    compute_top_up_fetch_limit,
    ensure_structured_jd_breakdown,
    extract_job_description_breakdown,
    resolve_job_description_source,
    safe_artifact_path,
)
from hr_hunter.remote import RemoteSourcingClient, RemoteSourcingError, candidate_from_remote
from hr_hunter.ranker import train_learned_ranker
from hr_hunter.reranker import parse_reranker_settings, rerank_candidates
from hr_hunter.scoring import sort_candidates
from hr_hunter.state import (
    complete_job,
    enqueue_job,
    expire_stale_jobs,
    fail_job,
    find_similar_candidates,
    latest_project_job,
    list_jobs,
    list_review_history,
    list_run_history,
    load_job,
    persist_search_run,
    review_candidate,
    stop_job,
    start_job,
    summarize_system_state,
    update_job_progress,
)
from hr_hunter.verifier import PublicEvidenceVerifier, refresh_report_summary
from hr_hunter.workspace import (
    PROJECT_STATUS_OPTIONS,
    SESSION_TTL_DAYS,
    authenticate_user,
    attach_project_run,
    create_project,
    create_user_account,
    delete_project,
    delete_project_run,
    get_project,
    get_project_run_report,
    get_user_totp_setup,
    init_workspace_db,
    list_project_runs,
    list_projects,
    list_users,
    revoke_session,
    resolve_session_user,
    save_project_brief,
    seed_default_admin_account,
    update_project,
)

try:
    from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, Request, Response, UploadFile
    from fastapi.responses import FileResponse, RedirectResponse
    from fastapi.staticfiles import StaticFiles
except ImportError as exc:  # pragma: no cover - optional dependency
    FastAPI = None
    BackgroundTasks = None
    File = None
    Form = None
    HTTPException = RuntimeError
    Request = object
    Response = object
    UploadFile = None
    FileResponse = None
    RedirectResponse = None
    StaticFiles = None
    FASTAPI_IMPORT_ERROR = exc
else:
    FASTAPI_IMPORT_ERROR = None


def _strict_shortlist_enabled(brief) -> bool:
    current_only_scope = str(getattr(brief, "company_match_mode", "both") or "both").strip().lower() == "current_only"
    strict_title_scope = not getattr(brief, "allow_adjacent_titles", True)
    has_company_scope = bool(getattr(brief, "company_targets", []))
    has_market_scope = bool(getattr(brief, "location_targets", []) or getattr(brief.geography, "country", ""))
    return bool(
        current_only_scope
        and strict_title_scope
        and has_company_scope
        and has_market_scope
    )


def _strict_shortlist_bucket(candidate, brief) -> int:
    current_company = bool(getattr(candidate, "current_target_company_match", False))
    market_match = bool(getattr(candidate, "location_aligned", False))
    exact_title = bool(getattr(candidate, "current_title_match", False))
    if current_company and market_match and exact_title:
        return 0
    if current_company and market_match:
        return 1
    if current_company:
        return 2
    if market_match:
        return 3
    return 9


def _apply_strict_shortlist(report, *, brief):
    if not _strict_shortlist_enabled(brief):
        return report
    original_candidates = list(report.candidates)
    shortlisted = [
        candidate
        for candidate in original_candidates
        if _strict_shortlist_bucket(candidate, brief) <= 1
    ]
    status_rank = {"verified": 0, "review": 1, "reject": 2}
    shortlisted = sorted(
        shortlisted,
        key=lambda candidate: (
            _strict_shortlist_bucket(candidate, brief),
            status_rank.get(getattr(candidate, "verification_status", "reject"), 9),
            -float(getattr(candidate, "score", 0.0) or 0.0),
            str(getattr(candidate, "full_name", "") or "").lower(),
        ),
    )
    summary = dict(report.summary or {})
    summary["strict_shortlist"] = {
        "enabled": True,
        "candidate_count": len(shortlisted),
        "filtered_out_count": max(0, len(original_candidates) - len(shortlisted)),
        "exact_title_count": len(
            [candidate for candidate in shortlisted if _strict_shortlist_bucket(candidate, brief) == 0]
        ),
        "company_market_count": len(shortlisted),
    }
    report.summary = summary
    report.candidates = shortlisted
    return report


def _resolve_effective_verification_target(
    candidates: List[Any],
    *,
    requested_limit: int,
    verification_target: int,
    company_required: bool = False,
) -> Dict[str, int]:
    shortlist_limit = max(0, min(int(verification_target or 0), len(candidates)))
    if shortlist_limit <= 0:
        return {
            "requested_target": 0,
            "effective_target": 0,
        }

    requested = max(1, int(requested_limit or 1))
    verification_floor = min(
        shortlist_limit,
        max(16, min(48, int(round(requested * 0.2)))),
    )
    effective_target = shortlist_limit
    if company_required:
        effective_target = max(effective_target, verification_floor)
    return {
        "requested_target": shortlist_limit,
        "effective_target": min(shortlist_limit, max(verification_floor, effective_target)),
    }


def _verification_progress_base(
    pipeline_metrics: Dict[str, Any] | None,
    latest_telemetry: Dict[str, Any] | None,
) -> Dict[str, int]:
    pipeline_metrics = dict(pipeline_metrics or {})
    latest_telemetry = dict(latest_telemetry or {})
    return {
        "queries_completed": max(
            int(pipeline_metrics.get("queries_completed", 0) or 0),
            int(latest_telemetry.get("queries_completed", 0) or 0),
        ),
        "queries_total": max(
            int(pipeline_metrics.get("queries_total", 0) or 0),
            int(latest_telemetry.get("queries_total", 0) or 0),
        ),
        "raw_found": max(
            int(pipeline_metrics.get("raw_found", 0) or 0),
            int(latest_telemetry.get("raw_found", 0) or 0),
        ),
        "unique_after_dedupe": max(
            int(pipeline_metrics.get("unique_after_dedupe", 0) or 0),
            int(latest_telemetry.get("unique_after_dedupe", 0) or 0),
        ),
        "rerank_target": max(
            int(pipeline_metrics.get("rerank_target", 0) or 0),
            int(latest_telemetry.get("rerank_target", 0) or 0),
        ),
        "reranked_count": max(
            int(pipeline_metrics.get("reranked_count", 0) or 0),
            int(latest_telemetry.get("reranked_count", 0) or 0),
        ),
    }


def _write_app_request(kind: str, payload: Dict[str, Any]) -> Dict[str, str]:
    output_root = resolve_output_dir() / "inbox"
    output_root.mkdir(parents=True, exist_ok=True)
    output_path = output_root / f"{kind}.jsonl"
    record = {
        "kind": kind,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }
    with output_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    return {"saved_to": str(output_path)}


def _private_api_credentials() -> Dict[str, str]:
    return {
        "api_key": str(os.getenv("HR_HUNTER_API_KEY", "")).strip(),
        "bearer_token": str(os.getenv("HR_HUNTER_API_BEARER_TOKEN", "")).strip(),
    }


def _private_api_auth_configured() -> bool:
    credentials = _private_api_credentials()
    return bool(credentials["api_key"] or credentials["bearer_token"])


def _runtime_storage_snapshot() -> Dict[str, Dict[str, Any]]:
    state_target = resolve_database_target(
        None,
        env_var="HR_HUNTER_STATE_DB",
        default_path="output/state/hr_hunter_state.db",
    )
    state_storage = describe_database_target(state_target)
    return {
        "state": dict(state_storage),
        "workspace": dict(state_storage),
    }


def _finalize_report_for_limit(report, *, requested_limit: int, internal_fetch_limit: int, brief=None):
    requested = max(1, int(requested_limit or 1))
    retrieval = max(requested, int(internal_fetch_limit or requested))
    if brief is not None:
        report = _apply_strict_shortlist(report, brief=brief)
        report.candidates = prioritize_final_candidates(
            report.candidates,
            brief=brief,
            company_required=bool(getattr(brief, "company_targets", [])),
        )
    report.candidates = list(report.candidates[:requested])
    summary = dict(report.summary or {})
    summary["requested_candidate_limit"] = requested
    summary["retrieval_candidate_limit"] = retrieval
    summary["returned_candidate_count"] = len(report.candidates)
    rebuilt_summary = build_reporting_summary(report.candidates, summary)
    pipeline_metrics = dict(rebuilt_summary.get("pipeline_metrics") or {})
    reranked_count = max(0, int(pipeline_metrics.get("reranked_count", 0) or 0))
    rerank_target = max(0, int(pipeline_metrics.get("rerank_target", 0) or 0))
    unique_after_dedupe = max(
        len(report.candidates),
        int(pipeline_metrics.get("unique_after_dedupe", len(report.candidates)) or len(report.candidates)),
    )
    rerank_target = max(rerank_target, reranked_count)
    rerank_target = min(rerank_target, unique_after_dedupe)
    reranked_count = min(reranked_count, rerank_target) if rerank_target > 0 else reranked_count
    raw_found = max(
        unique_after_dedupe,
        max(0, int(pipeline_metrics.get("raw_found", 0) or 0)),
    )
    pipeline_metrics.update(
        {
            "queries_completed": max(0, int(pipeline_metrics.get("queries_completed", 0) or 0)),
            "queries_total": max(0, int(pipeline_metrics.get("queries_total", 0) or 0)),
            "raw_found": raw_found,
            "unique_after_dedupe": unique_after_dedupe,
            "rerank_target": rerank_target,
            "reranked_count": reranked_count,
            "finalized_count": len(report.candidates),
        }
    )
    rebuilt_summary["pipeline_metrics"] = pipeline_metrics
    report.summary = rebuilt_summary
    return report


def _should_stop_after_stagnant_top_up(
    *,
    requested_limit: int,
    updated_unique_count: int,
    top_up_rounds: int,
    stagnant_rounds: int,
) -> bool:
    if stagnant_rounds >= 2:
        return True
    if top_up_rounds <= 0:
        return False
    requested = max(1, int(requested_limit or 1))
    remaining_needed = max(0, requested - max(0, int(updated_unique_count or 0)))
    near_target_remaining = max(10, int(round(requested * 0.05)))
    return remaining_needed <= near_target_remaining


def _merge_ranked_report_candidates(report, supplemental_report, *, brief=None):
    report.provider_results = [*report.provider_results, *supplemental_report.provider_results]
    report.candidates = sort_candidates(
        dedupe_candidates([*report.candidates, *supplemental_report.candidates]),
        brief,
    )
    return report


def _rerank_merged_report_candidates(
    report,
    *,
    brief=None,
    rerank_top_n: int | None = None,
):
    if brief is None or not list(getattr(report, "candidates", []) or []):
        return report, {"enabled": False, "rerank_target": 0, "reranked_count": 0}

    provider_settings = getattr(brief, "provider_settings", None)
    if not isinstance(provider_settings, dict):
        provider_settings = {}
        brief.provider_settings = provider_settings
    reranker_settings = provider_settings.get("reranker")
    if not isinstance(reranker_settings, dict):
        reranker_settings = {}
        provider_settings["reranker"] = reranker_settings
    if rerank_top_n is not None:
        reranker_settings["top_n"] = max(1, int(rerank_top_n or 1))

    parsed_settings = parse_reranker_settings(brief)
    if not parsed_settings.enabled:
        report.candidates = sort_candidates(report.candidates, brief)
        return report, {"enabled": False, "rerank_target": 0, "reranked_count": 0}

    rerank_target = min(len(report.candidates), max(1, int(parsed_settings.top_n or 1)))
    report.candidates = sort_candidates(rerank_candidates(brief, report.candidates), brief)
    return report, {
        "enabled": True,
        "rerank_target": rerank_target,
        "reranked_count": rerank_target,
    }


def _collect_candidate_keys_from_report(report: Any) -> Set[str]:
    seen: Set[str] = set()
    for candidate in list(getattr(report, "candidates", []) or []):
        seen.update(candidate_identity_keys(candidate))
    return seen


def _collect_provider_query_exclusions_from_report(report: Any) -> Dict[str, Set[str]]:
    seen: Dict[str, Set[str]] = {}
    for provider_result in list(getattr(report, "provider_results", []) or []):
        provider_name = str(getattr(provider_result, "provider_name", "") or "").strip()
        if not provider_name:
            continue
        diagnostics = dict(getattr(provider_result, "diagnostics", {}) or {})
        for item in list(diagnostics.get("queries", []) or []):
            if not isinstance(item, dict) or bool(item.get("skipped")):
                continue
            search_value = str(item.get("search", "") or "").strip()
            fingerprint = str(item.get("fingerprint", "") or "").strip()
            if not search_value and not fingerprint:
                continue
            bucket = seen.setdefault(provider_name, set())
            if search_value:
                bucket.add(search_value)
            if fingerprint:
                bucket.add(fingerprint)
    return {provider_name: values for provider_name, values in seen.items() if values}


def _quality_recovery_settings(
    brief: Any,
    *,
    requested_limit: int,
    current_fetch_limit: int,
) -> Dict[str, Any]:
    raw_settings = dict(getattr(brief, "provider_settings", {}).get("quality_recovery", {}) or {})
    requested = max(1, int(requested_limit or 1))
    fetch_limit = max(requested, int(current_fetch_limit or requested))
    return {
        "enabled": bool(raw_settings.get("enabled", False)),
        "min_verified_count": max(0, min(requested, int(raw_settings.get("min_verified_count", 0) or 0))),
        "max_reject_count": max(0, min(requested, int(raw_settings.get("max_reject_count", 0) or 0))),
        "max_rounds": max(1, int(raw_settings.get("max_rounds", 2) or 2)),
        "fetch_limit_increment": max(
            40,
            int(raw_settings.get("fetch_limit_increment", max(80, int(round(requested * 0.35)))) or 40),
        ),
        "parallel_requests": max(1, int(raw_settings.get("parallel_requests", 0) or 1)),
        "max_queries": max(1, int(raw_settings.get("max_queries", 0) or fetch_limit)),
        "max_geo_groups": max(1, int(raw_settings.get("max_geo_groups", 0) or 1)),
        "reranker_top_n": max(requested, int(raw_settings.get("reranker_top_n", requested) or requested)),
        "verification_top_n": max(0, int(raw_settings.get("verification_top_n", fetch_limit) or 0)),
        "verification_parallel_candidates": max(
            1,
            int(raw_settings.get("verification_parallel_candidates", 1) or 1),
        ),
        "disable_history_slices": bool(raw_settings.get("disable_history_slices", False)),
        "disable_registry_memory": bool(raw_settings.get("disable_registry_memory", False)),
        "force_discovery_slices": bool(raw_settings.get("force_discovery_slices", True)),
        "force_geo_fanout": bool(raw_settings.get("force_geo_fanout", True)),
        "force_country_only_queries": bool(raw_settings.get("force_country_only_queries", True)),
        "force_adjacent_titles": bool(raw_settings.get("force_adjacent_titles", True)),
    }


def _quality_recovery_gap(summary: Dict[str, Any] | None, settings: Dict[str, Any]) -> Dict[str, Any]:
    resolved_summary = dict(summary or {})
    verified_count = max(0, int(resolved_summary.get("verified_count", 0) or 0))
    reject_count = max(0, int(resolved_summary.get("reject_count", 0) or 0))
    needs_verified = settings["min_verified_count"] > 0 and verified_count < settings["min_verified_count"]
    too_many_rejects = settings["max_reject_count"] > 0 and reject_count > settings["max_reject_count"]
    return {
        "verified_count": verified_count,
        "reject_count": reject_count,
        "needs_verified": needs_verified,
        "too_many_rejects": too_many_rejects,
        "verified_gap": max(0, settings["min_verified_count"] - verified_count),
        "reject_gap": max(0, reject_count - settings["max_reject_count"]),
        "should_retry": bool(needs_verified or too_many_rejects),
    }


def _quality_recovery_verification_candidates(
    candidates: List[Any],
    *,
    limit: int,
    brief: Any | None = None,
) -> List[Any]:
    shortlist_limit = max(0, min(int(limit or 0), len(candidates)))
    if shortlist_limit <= 0:
        return []
    unverified_candidates = [
        candidate
        for candidate in candidates
        if not str(getattr(candidate, "last_verified_at", "") or "").strip()
    ]
    verification_ready_candidates = [
        candidate
        for candidate in unverified_candidates
        if candidate_is_verification_ready(candidate, brief)
    ]
    if verification_ready_candidates:
        shortlisted_ready = verification_ready_candidates[:shortlist_limit]
        if len(shortlisted_ready) >= min(shortlist_limit, max(20, shortlist_limit // 3 or 1)):
            if len(shortlisted_ready) < shortlist_limit:
                fallback_ready = [
                    candidate
                    for candidate in unverified_candidates
                    if candidate not in shortlisted_ready
                ]
                shortlisted_ready.extend(fallback_ready[: max(0, shortlist_limit - len(shortlisted_ready))])
            return shortlisted_ready
    if len(unverified_candidates) >= shortlist_limit:
        return unverified_candidates[:shortlist_limit]
    fallback_candidates = [
        candidate
        for candidate in candidates
        if str(getattr(candidate, "verification_status", "") or "").strip().lower() != "verified"
    ]
    return fallback_candidates[:shortlist_limit]


def _resolve_top_up_max_rounds(payload: Dict[str, Any] | None) -> int:
    raw_value: Any = None
    if isinstance(payload, dict):
        raw_value = payload.get("top_up_max_rounds")
        if raw_value is None:
            search_tuning = payload.get("search_tuning")
            if isinstance(search_tuning, dict):
                raw_value = search_tuning.get("top_up_max_rounds")
    try:
        resolved = int(raw_value) if raw_value is not None else 8
    except (TypeError, ValueError):
        resolved = 8
    return max(0, resolved)


def _resolve_pipeline_progress_percent(
    *,
    stage: str,
    explicit_percent: Any,
    previous_percent: int,
    queries_completed: int,
    queries_total: int,
) -> int:
    resolved_percent = explicit_percent
    if resolved_percent is None:
        if stage == "retrieval" and queries_total > 0:
            resolved_percent = max(5, min(70, int(round((queries_completed / max(1, queries_total)) * 65 + 5))))
        elif stage == "dedupe":
            resolved_percent = 72
        elif stage == "rerank":
            resolved_percent = 84
        elif stage == "verifying":
            resolved_percent = 92
        elif stage == "finalizing":
            resolved_percent = 95
        else:
            resolved_percent = previous_percent or 5
    percent = int(max(0, min(99, int(resolved_percent))))
    if stage not in {"completed", "failed"}:
        percent = max(max(0, int(previous_percent or 0)), percent)
    return percent


def _enforce_private_api_auth(request: "Request") -> None:
    credentials = _private_api_credentials()
    if not (credentials["api_key"] or credentials["bearer_token"]):
        return
    api_key = request.headers.get("X-API-Key", "").strip()
    auth_header = request.headers.get("Authorization", "").strip()
    bearer_token = auth_header.removeprefix("Bearer").strip() if auth_header.lower().startswith("bearer") else ""
    if credentials["api_key"] and api_key == credentials["api_key"]:
        return
    if credentials["bearer_token"] and bearer_token == credentials["bearer_token"]:
        return
    raise HTTPException(status_code=401, detail="Private HR Hunter API credentials are required.")


def _coerce_company_match_mode(company_targets: List[Dict[str, Any]]) -> str:
    modes = {
        str(target.get("employmentMode", "both")).strip().lower()
        for target in company_targets
        if str(target.get("name", "")).strip()
    }
    if modes == {"current"}:
        return "current_only"
    if modes == {"past"}:
        return "past_only"
    return "both"


def _brief_config_from_remote_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    criteria = dict(payload.get("criteria", {}) or {})
    job_description = dict(criteria.get("jobDescription", {}) or {})
    experience = dict(criteria.get("experience", {}) or {})
    location_scopes = list(criteria.get("locationScopes", []) or [])
    company_targets = list(criteria.get("companyTargets", []) or [])
    titles = [str(value).strip() for value in criteria.get("jobTitles", criteria.get("titles", [])) if str(value).strip()]
    countries = [str(scope.get("country", "")).strip() for scope in location_scopes if str(scope.get("country", "")).strip()]
    cities = [str(scope.get("city", scope.get("value", ""))).strip() for scope in location_scopes if str(scope.get("type", "")).strip() == "city_radius" and str(scope.get("city", scope.get("value", ""))).strip()]
    location_targets = [
        str(scope.get("value", scope.get("city", scope.get("country", "")))).strip()
        for scope in location_scopes
        if str(scope.get("value", scope.get("city", scope.get("country", "")))).strip()
    ]
    radius_miles = 0.0
    if location_scopes:
        radius_miles = float(location_scopes[0].get("radiusMiles", 0) or 0)
    target_years = experience.get("targetYears")
    tolerance_years = int(experience.get("toleranceYears", 0) or 0)
    minimum_years = None
    maximum_years = None
    if target_years is not None:
        minimum_years = max(0, int(target_years) - tolerance_years)
        maximum_years = int(target_years) + tolerance_years if tolerance_years else int(target_years)
    return {
        "id": str(payload.get("searchId", payload.get("runId", "remote-search"))).strip() or "remote-search",
        "role_title": str(payload.get("title", criteria.get("roleTitle", ""))).strip(),
        "brief_summary": str(payload.get("title", criteria.get("roleTitle", ""))).strip(),
        "document_text": str(job_description.get("text", "")).strip(),
        "titles": titles,
        "company_targets": [
            str(target.get("name", "")).strip()
            for target in company_targets
            if str(target.get("name", "")).strip()
        ],
        "geography": {
            "location_name": cities[0] if cities else (countries[0] if countries else ""),
            "country": countries[0] if countries else "",
            "radius_miles": radius_miles,
            "location_hints": location_targets,
        },
        "location_targets": location_targets,
        "required_keywords": list(criteria.get("mustHaveKeywords", []) or []),
        "preferred_keywords": list(criteria.get("niceToHaveKeywords", []) or []),
        "industry_keywords": list(criteria.get("industries", []) or []),
        "exclude_title_keywords": list(criteria.get("excludeTitles", []) or []),
        "exclude_company_keywords": list(criteria.get("excludeCompanies", []) or []),
        "minimum_years_experience": minimum_years,
        "maximum_years_experience": maximum_years,
        "years_mode": "plus_minus" if target_years is not None and tolerance_years else "range",
        "years_target": target_years,
        "years_tolerance": tolerance_years,
        "company_match_mode": _coerce_company_match_mode(company_targets),
        "employment_status_mode": str(criteria.get("employmentStatus", "any") or "any"),
        "anchors": dict(criteria.get("anchors", {})),
        "jd_breakdown": {
            "summary": str(job_description.get("text", "")).strip()[:420],
            "key_experience_points": list(job_description.get("extractedPoints", []) or []),
            "required_keywords": list(criteria.get("mustHaveKeywords", []) or []),
            "preferred_keywords": list(criteria.get("niceToHaveKeywords", []) or []),
            "industry_keywords": list(criteria.get("industries", []) or []),
            "titles": titles,
            "seniority_levels": [],
            "years": {
                "mode": "plus_minus" if target_years is not None and tolerance_years else "range",
                "value": target_years,
                "min": minimum_years,
                "max": maximum_years,
                "tolerance": tolerance_years,
            },
            "suggested_anchors": dict(criteria.get("anchors", {})),
        },
        "provider_settings": {},
}


def _remote_error_allows_local_fallback(exc: Exception) -> bool:
    return isinstance(exc, RemoteSourcingError) and exc.recoverable


def _session_token_from_request(request: "Request") -> str:
    header_token = request.headers.get("X-Session-Token", "").strip()
    if header_token:
        return header_token
    cookie_token = str(getattr(request, "cookies", {}).get("hr_hunter_session", "")).strip()
    if cookie_token:
        return cookie_token
    query_token = str(getattr(request, "query_params", {}).get("session_token", "")).strip()
    if query_token:
        return query_token
    auth_header = request.headers.get("Authorization", "").strip()
    if auth_header.lower().startswith("session "):
        return auth_header[8:].strip()
    return ""


def _require_app_user(request: "Request") -> Dict[str, Any]:
    token = _session_token_from_request(request)
    if not token:
        raise HTTPException(status_code=401, detail="Please sign in to continue.")
    try:
        return resolve_session_user(token)
    except Exception as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc


def _require_admin_user(request: "Request") -> Dict[str, Any]:
    user = _require_app_user(request)
    if not user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Admin access is required for this action.")
    return user


def _job_actor_from_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": str(payload.get("recruiter_id", "")).strip(),
        "full_name": str(payload.get("recruiter_name", "")).strip(),
        "team_id": str(payload.get("team_id", "")).strip(),
        "is_admin": bool(payload.get("recruiter_is_admin")),
    }


def _summarize_target_geography(ui_payload: Dict[str, Any], brief: Any) -> str:
    brief_config = ui_payload.get("brief_config", {}) if isinstance(ui_payload, dict) else {}
    ui_meta = brief_config.get("ui_meta", {}) if isinstance(brief_config, dict) else {}
    countries = [str(value).strip() for value in ui_meta.get("countries", []) if str(value).strip()]
    continents = [str(value).strip() for value in ui_meta.get("continents", []) if str(value).strip()]
    cities = [str(value).strip() for value in ui_meta.get("cities", []) if str(value).strip()]

    def summarize(values: List[str], *, limit: int = 3) -> str:
        picked = values[:limit]
        remainder = max(0, len(values) - len(picked))
        label = ", ".join(picked)
        if remainder > 0:
            label = f"{label} (+{remainder} more)"
        return label

    if len(countries) > 1:
        return summarize(countries, limit=3)
    if len(countries) == 1 and cities:
        city_summary = summarize(cities, limit=2)
        return ", ".join([value for value in [city_summary, countries[0]] if value]).strip(", ")
    if countries:
        return countries[0]
    if continents:
        return summarize(continents, limit=3)
    if cities:
        return summarize(cities, limit=3)

    fallback_values = [
        str(getattr(getattr(brief, "geography", None), "location_name", "") or "").strip(),
        str(getattr(getattr(brief, "geography", None), "country", "") or "").strip(),
        ", ".join(list(getattr(brief, "location_targets", []) or [])[:2]).strip(),
    ]
    return ", ".join([value for value in fallback_values if value][:2])


def create_app() -> "FastAPI":
    if FastAPI is None:
        raise RuntimeError(
            "FastAPI is not installed. Run `uv sync --extra api` to enable the API surface."
        ) from FASTAPI_IMPORT_ERROR

    load_env_file(Path(".env"))
    init_workspace_db()
    seed_default_admin_account()
    app = FastAPI(title="HR Hunter", version="0.1.0")
    engine = SearchEngine()
    remote_client = RemoteSourcingClient()
    raw_job_stale_seconds = str(os.getenv("HR_HUNTER_JOB_STALE_SECONDS", "0") or "0").strip()
    try:
        job_stale_seconds = max(0, int(raw_job_stale_seconds))
    except ValueError:
        job_stale_seconds = 0
    workspace_root = Path(__file__).resolve().parents[2]
    ui_dir = workspace_root / "UI"
    active_job_threads: Dict[str, threading.Thread] = {}
    active_job_threads_lock = threading.Lock()
    if ui_dir.exists():
        app.mount("/assets", StaticFiles(directory=ui_dir), name="assets")

    def _spawn_background_job(job_id: str, runner: Any) -> None:
        with active_job_threads_lock:
            existing = active_job_threads.get(job_id)
            if existing and existing.is_alive():
                return

            def _thread_target() -> None:
                try:
                    asyncio.run(runner())
                finally:
                    with active_job_threads_lock:
                        active_job_threads.pop(job_id, None)

            thread = threading.Thread(
                target=_thread_target,
                name=f"hr-hunter-job-{job_id[:12]}",
                daemon=True,
            )
            active_job_threads[job_id] = thread
            thread.start()

    async def _run_local_search(
        brief: Any,
        *,
        providers: List[str],
        limit: int,
        dry_run: bool,
        exclude_report_paths: List[Path] | None = None,
        exclude_history_dirs: List[Path] | None = None,
        extra_exclude_candidate_keys: Set[str] | None = None,
        extra_exclude_provider_queries: Dict[str, Set[str]] | None = None,
        progress_callback: Any = None,
    ):
        exclusion_sources = [*(exclude_report_paths or []), *(exclude_history_dirs or [])]
        exclude_candidate_keys = collect_seen_candidate_keys(exclusion_sources)
        if extra_exclude_candidate_keys:
            exclude_candidate_keys.update(
                {
                    str(value).strip()
                    for value in set(extra_exclude_candidate_keys)
                    if str(value or "").strip()
                }
            )
        exclude_provider_queries = collect_seen_provider_queries(exclusion_sources)
        for provider_name, queries in dict(extra_exclude_provider_queries or {}).items():
            normalized_provider_name = str(provider_name or "").strip()
            if not normalized_provider_name:
                continue
            bucket = exclude_provider_queries.setdefault(normalized_provider_name, set())
            bucket.update(
                {
                    str(value).strip()
                    for value in set(queries or set())
                    if str(value or "").strip()
                }
            )
        return await engine.run(
            brief,
            list(providers),
            limit=limit,
            dry_run=dry_run,
            exclude_candidate_keys=exclude_candidate_keys,
            exclude_provider_queries=exclude_provider_queries,
            progress_callback=progress_callback,
        )

    async def _expand_report_to_requested_limit(
        report: Any,
        *,
        payload: Dict[str, Any],
        ui_payload: Dict[str, Any],
        requested_limit: int,
        internal_fetch_limit: int,
        execution_backend: str,
        exclude_report_paths: List[Path],
        exclude_history_dirs: List[Path],
        progress_callback: Any = None,
    ) -> tuple[Any, int]:
        requested = max(1, int(requested_limit or 1))
        current_fetch_limit = max(requested, int(internal_fetch_limit or requested))
        current_unique_count = len(report.candidates)
        top_up_rounds = 0
        top_up_notes: List[str] = []
        max_rounds = _resolve_top_up_max_rounds(payload)
        stagnant_rounds = 0
        source_exhausted = False

        while top_up_rounds < max_rounds:
            need_more_candidates = current_unique_count < requested
            if not need_more_candidates:
                break
            next_fetch_limit = compute_top_up_fetch_limit(requested, current_fetch_limit)
            if next_fetch_limit <= current_fetch_limit and stagnant_rounds > 0:
                break

            top_up_rounds += 1
            round_growth = 0
            payload_override = dict(payload)
            payload_override["internal_fetch_limit_override"] = next_fetch_limit
            payload_override["top_up_round"] = top_up_rounds
            payload_override["jd_breakdown"] = dict(ui_payload.get("job_description_breakdown", {}))
            top_up_ui_payload = build_ui_brief_payload(payload_override)
            top_up_brief = build_search_brief(top_up_ui_payload["brief_config"])

            if progress_callback:
                progress_callback(
                    {
                        "stage": "retrieval",
                        "stage_label": "Retrieval",
                        "message": f"Top-up round {top_up_rounds} started.",
                        "round": top_up_rounds,
                        "target": requested,
                        "unique_after_dedupe": current_unique_count,
                        "finalized_count": 0,
                    }
                )

            if execution_backend == "remote_api":
                try:
                    supplemental_report = await remote_client.run_search(payload_override, top_up_ui_payload)
                    before_merge = len(report.candidates)
                    report = _merge_ranked_report_candidates(report, supplemental_report, brief=top_up_brief)
                    round_growth += max(0, len(report.candidates) - before_merge)
                except Exception as exc:
                    top_up_notes.append(f"Remote top-up failed: {exc}")

            if len(report.candidates) < requested:
                supplemental_report = await _run_local_search(
                    top_up_brief,
                    providers=list(top_up_ui_payload["providers"]),
                    limit=next_fetch_limit,
                    dry_run=bool(payload.get("dry_run", False)),
                    exclude_report_paths=exclude_report_paths,
                    exclude_history_dirs=exclude_history_dirs,
                    progress_callback=progress_callback,
                )
                before_merge = len(report.candidates)
                report = _merge_ranked_report_candidates(report, supplemental_report, brief=top_up_brief)
                round_growth += max(0, len(report.candidates) - before_merge)

            current_fetch_limit = next_fetch_limit
            updated_unique_count = len(report.candidates)
            if round_growth <= 0 or updated_unique_count <= current_unique_count:
                stagnant_rounds += 1
                if _should_stop_after_stagnant_top_up(
                    requested_limit=requested,
                    updated_unique_count=updated_unique_count,
                    top_up_rounds=top_up_rounds,
                    stagnant_rounds=stagnant_rounds,
                ):
                    source_exhausted = True
                    remaining_needed = max(0, requested - updated_unique_count)
                    if remaining_needed <= max(10, int(round(requested * 0.05))):
                        top_up_notes.append(
                            "Stopped after a stagnant top-up round because the remaining gap was small and no new unique candidates appeared."
                        )
                    break
            else:
                stagnant_rounds = 0
            current_unique_count = updated_unique_count

        if top_up_rounds:
            summary = dict(report.summary or {})
            summary["top_up_rounds"] = top_up_rounds
            summary["top_up_fetch_limit"] = current_fetch_limit
            summary["top_up_source_exhausted"] = bool(source_exhausted)
            summary["top_up_max_rounds"] = max_rounds
            if top_up_notes:
                summary["top_up_notes"] = top_up_notes[-3:]
            report.summary = summary

        return report, current_fetch_limit

    async def _search_job_runner(job_id: str, payload: Dict[str, Any]) -> None:
        try:
            start_job(job_id)
            job_started_monotonic = time.monotonic()
            ui_payload = build_ui_brief_payload(payload)
            brief = build_search_brief(ui_payload["brief_config"])
            requested_limit = int(ui_payload["limit"])
            internal_fetch_limit = int(ui_payload.get("internal_fetch_limit", requested_limit))
            verification_settings = dict(brief.provider_settings.get("verification", {}) or {})
            verification_enabled = bool(verification_settings.get("enabled", True)) and not bool(payload.get("dry_run", False))
            verification_target = min(
                internal_fetch_limit,
                max(0, int(verification_settings.get("top_n", 0) or 0)),
            )
            project_id = str(payload.get("project_id", "")).strip()
            target_geography = _summarize_target_geography(ui_payload, brief)
            actor = _job_actor_from_payload(payload)
            last_progress_write = 0.0
            runtime_target_seconds = max(60, int(round(max(1, requested_limit) * 3)))
            latest_telemetry: Dict[str, Any] = {
                "stage": "running",
                "stage_label": "Running",
                "queries_completed": 0,
                "queries_total": 0,
                "queries_in_flight": 0,
                "raw_found": 0,
                "unique_after_dedupe": 0,
                "reranked_count": 0,
                "rerank_target": 0,
                "finalized_count": 0,
                "verified_candidates_checked": 0,
                "verification_target": verification_target,
                "verification_requests_used": 0,
                "verifying_count": 0,
                "verified_count": 0,
                "review_count": 0,
                "reject_count": 0,
                "stage_elapsed_seconds": 0,
                "estimated_total_seconds": 0,
                "target_runtime_seconds": runtime_target_seconds,
                "target": requested_limit,
                "round": 0,
                "percent": 2,
                "message": "Search job started.",
            }
            live_stage_name = "running"
            live_stage_started_monotonic = job_started_monotonic

            def _project_total_runtime_seconds() -> int:
                elapsed_seconds = int(latest_telemetry.get("elapsed_seconds", 0) or 0)
                stage = str(latest_telemetry.get("stage", "")).strip().lower() or "running"
                target_count = max(1, int(latest_telemetry.get("target", requested_limit) or requested_limit or 1))
                stage_elapsed_seconds = max(0, int(latest_telemetry.get("stage_elapsed_seconds", 0) or 0))
                queries_completed = max(0, int(latest_telemetry.get("queries_completed", 0) or 0))
                queries_total = max(0, int(latest_telemetry.get("queries_total", 0) or 0))
                queries_in_flight = max(0, int(latest_telemetry.get("queries_in_flight", 0) or 0))
                unique_after_dedupe = max(0, int(latest_telemetry.get("unique_after_dedupe", 0) or 0))
                reranked_count = max(0, int(latest_telemetry.get("reranked_count", 0) or 0))
                rerank_target = max(reranked_count, int(latest_telemetry.get("rerank_target", 0) or 0))
                finalized_count = max(0, int(latest_telemetry.get("finalized_count", 0) or 0))
                verified_candidates_checked = max(
                    0,
                    int(latest_telemetry.get("verified_candidates_checked", 0) or 0),
                )
                verification_target_count = max(
                    0,
                    int(latest_telemetry.get("verification_target", verification_target) or verification_target or 0),
                )
                completed_before_stage = max(0, elapsed_seconds - stage_elapsed_seconds)
                finalizing_budget = max(8, min(150, int(max(1, target_count) * 0.07) + 8))
                predicted_rerank_pool = max(rerank_target, min(max(unique_after_dedupe, target_count), max(target_count, 120)))
                rerank_budget = max(40, min(900, int(predicted_rerank_pool * 0.38) + 24))
                verification_budget = (
                    max(25, min(900, int(max(1, verification_target_count) * 2.2) + 20))
                    if verification_target_count > 0
                    else 0
                )
                retrieval_tail = max(20, min(240, int(max(queries_total, target_count) * 0.12) + (queries_in_flight * 3)))

                if stage in {"completed", "failed"}:
                    return elapsed_seconds
                if stage in {"retrieval", "dedupe", "running"}:
                    if queries_total > 0 and queries_completed > 0 and stage_elapsed_seconds > 0:
                        coverage = min(0.995, max(0.02, queries_completed / max(1, queries_total)))
                        stage_total = int(round(stage_elapsed_seconds / coverage))
                    else:
                        stage_total = stage_elapsed_seconds + retrieval_tail
                    return (
                        completed_before_stage
                        + max(stage_elapsed_seconds, stage_total)
                        + rerank_budget
                        + verification_budget
                        + finalizing_budget
                    )
                if stage == "rerank":
                    stable_rerank_samples = (
                        min(rerank_target, max(24, int(round(rerank_target * 0.18))))
                        if rerank_target > 0
                        else 0
                    )
                    rerank_projection_ready = (
                        rerank_target > 0
                        and reranked_count >= stable_rerank_samples
                        and stage_elapsed_seconds >= 20
                    )
                    if rerank_projection_ready:
                        coverage = min(0.995, max(0.12, reranked_count / max(1, rerank_target)))
                        stage_total = int(round(stage_elapsed_seconds / coverage))
                    else:
                        stage_total = stage_elapsed_seconds + max(
                            45,
                            min(240, int(max(1, rerank_target or predicted_rerank_pool) * 0.9) + 30),
                        )
                    return completed_before_stage + max(stage_elapsed_seconds, stage_total) + verification_budget + finalizing_budget
                if stage == "verifying":
                    if verification_target_count > 0 and verified_candidates_checked > 0 and stage_elapsed_seconds > 0:
                        coverage = min(0.995, max(0.02, verified_candidates_checked / max(1, verification_target_count)))
                        stage_total = int(round(stage_elapsed_seconds / coverage))
                    else:
                        stage_total = stage_elapsed_seconds + verification_budget
                    return completed_before_stage + max(stage_elapsed_seconds, stage_total) + finalizing_budget
                if stage == "finalizing":
                    if target_count > 0 and finalized_count > 0 and stage_elapsed_seconds > 0:
                        coverage = min(0.995, max(0.05, finalized_count / max(1, target_count)))
                        stage_total = int(round(stage_elapsed_seconds / coverage))
                    else:
                        stage_total = stage_elapsed_seconds + max(8, min(90, int(max(1, target_count) * 0.05) + 8))
                    return completed_before_stage + max(stage_elapsed_seconds, stage_total)
                return max(elapsed_seconds + 2, completed_before_stage + stage_elapsed_seconds + finalizing_budget)

            def _push_progress(
                patch: Dict[str, Any],
                *,
                checkpoint_patch: Dict[str, Any] | None = None,
                force: bool = False,
            ) -> None:
                nonlocal last_progress_write, live_stage_name, live_stage_started_monotonic
                now = time.monotonic()
                if not force and (now - last_progress_write) < 0.6:
                    return
                patch_payload = {key: value for key, value in dict(patch or {}).items() if value is not None}
                previous_stage = str(latest_telemetry.get("stage", "")).strip().lower() or "running"
                previous_estimated_total = int(latest_telemetry.get("estimated_total_seconds", 0) or 0)
                stage_name = str(patch_payload.get("stage", latest_telemetry.get("stage", ""))).strip().lower() or "running"
                if stage_name != live_stage_name:
                    live_stage_name = stage_name
                    live_stage_started_monotonic = now
                provided_stage_elapsed = patch_payload.get("stage_elapsed_seconds")
                if provided_stage_elapsed is not None:
                    stage_elapsed_seconds = max(0, int(provided_stage_elapsed or 0))
                    live_stage_started_monotonic = now - float(stage_elapsed_seconds)
                else:
                    stage_elapsed_seconds = max(0, int(now - live_stage_started_monotonic))
                patch_payload["stage_elapsed_seconds"] = stage_elapsed_seconds
                latest_telemetry.update(patch_payload)
                latest_telemetry["target"] = requested_limit
                latest_telemetry["target_runtime_seconds"] = runtime_target_seconds
                elapsed_seconds = max(0, int(now - job_started_monotonic))
                latest_telemetry["elapsed_seconds"] = elapsed_seconds
                stage = str(latest_telemetry.get("stage", "")).strip().lower() or "running"
                projected_total_seconds = _project_total_runtime_seconds()
                computed_estimated_total = max(
                    elapsed_seconds + (0 if stage in {"completed", "failed"} else 2),
                    min(4 * 3600, max(0, int(projected_total_seconds or 0))),
                )
                if (
                    previous_estimated_total > 0
                    and stage == previous_stage
                    and stage not in {"completed", "failed"}
                ):
                    smoothed_estimate = int(round((previous_estimated_total * 0.7) + (computed_estimated_total * 0.3)))
                    computed_estimated_total = max(
                        elapsed_seconds + 2,
                        min(4 * 3600, smoothed_estimate),
                    )
                latest_telemetry["estimated_total_seconds"] = computed_estimated_total
                latest_telemetry["eta_seconds"] = max(
                    0,
                    int(latest_telemetry["estimated_total_seconds"]) - elapsed_seconds,
                ) if stage not in {"completed", "failed"} else 0
                update_job_progress(
                    job_id,
                    latest_telemetry,
                    checkpoint={
                        "project_id": project_id,
                        "stage": latest_telemetry.get("stage", ""),
                        "queries_completed": int(latest_telemetry.get("queries_completed", 0) or 0),
                        "queries_total": int(latest_telemetry.get("queries_total", 0) or 0),
                        "queries_in_flight": int(latest_telemetry.get("queries_in_flight", 0) or 0),
                        "raw_found": int(latest_telemetry.get("raw_found", 0) or 0),
                        "unique_after_dedupe": int(latest_telemetry.get("unique_after_dedupe", 0) or 0),
                        "reranked_count": int(latest_telemetry.get("reranked_count", 0) or 0),
                        "rerank_target": int(latest_telemetry.get("rerank_target", 0) or 0),
                        "finalized_count": int(latest_telemetry.get("finalized_count", 0) or 0),
                        "verified_candidates_checked": int(latest_telemetry.get("verified_candidates_checked", 0) or 0),
                        "verification_target": int(latest_telemetry.get("verification_target", verification_target) or verification_target or 0),
                        "verification_requests_used": int(latest_telemetry.get("verification_requests_used", 0) or 0),
                        "verifying_count": int(latest_telemetry.get("verifying_count", 0) or 0),
                        "verified_count": int(latest_telemetry.get("verified_count", 0) or 0),
                        "review_count": int(latest_telemetry.get("review_count", 0) or 0),
                        "reject_count": int(latest_telemetry.get("reject_count", 0) or 0),
                        "stage_elapsed_seconds": int(latest_telemetry.get("stage_elapsed_seconds", 0) or 0),
                        "estimated_total_seconds": int(latest_telemetry.get("estimated_total_seconds", 0) or 0),
                        "eta_seconds": int(latest_telemetry.get("eta_seconds", 0) or 0),
                        "target_runtime_seconds": runtime_target_seconds,
                        "requested_limit": requested_limit,
                        "internal_fetch_limit": internal_fetch_limit,
                        "round": int(latest_telemetry.get("round", 0) or 0),
                        **(checkpoint_patch or {}),
                    },
                )
                last_progress_write = now

            def _on_pipeline_progress(event: Dict[str, Any]) -> None:
                stage = str(event.get("stage", "")).strip().lower() or "running"
                previous_stage = str(latest_telemetry.get("stage", "")).strip().lower() or "running"
                previous_round = int(latest_telemetry.get("round", 0) or 0)
                round_number = int(event.get("round", previous_round) or previous_round)
                round_reset = round_number > previous_round and stage == "retrieval"
                stage_label_map = {
                    "retrieval": "Retrieval",
                    "dedupe": "Dedupe",
                    "rerank": "Rerank",
                    "verifying": "Verifying",
                    "finalizing": "Finalizing",
                    "running": "Running",
                }
                previous_queries_total = 0 if round_reset else int(latest_telemetry.get("queries_total", 0) or 0)
                previous_queries_completed = 0 if round_reset else int(latest_telemetry.get("queries_completed", 0) or 0)
                previous_raw_found = 0 if round_reset else int(latest_telemetry.get("raw_found", 0) or 0)
                previous_unique = int(latest_telemetry.get("unique_after_dedupe", 0) or 0)
                previous_reranked = int(latest_telemetry.get("reranked_count", 0) or 0)
                previous_rerank_target = int(latest_telemetry.get("rerank_target", 0) or 0)
                previous_finalized = int(latest_telemetry.get("finalized_count", 0) or 0)
                previous_verified = int(latest_telemetry.get("verified_count", 0) or 0)
                previous_review = int(latest_telemetry.get("review_count", 0) or 0)
                previous_reject = int(latest_telemetry.get("reject_count", 0) or 0)
                previous_verifying = int(latest_telemetry.get("verifying_count", 0) or 0)

                queries_total = max(previous_queries_total, int(event.get("queries_total", previous_queries_total) or 0))
                queries_completed = max(
                    previous_queries_completed,
                    int(event.get("queries_completed", previous_queries_completed) or 0),
                )
                if "queries_in_flight" in event:
                    queries_in_flight = int(event.get("queries_in_flight", latest_telemetry.get("queries_in_flight", 0)) or 0)
                elif stage in {"dedupe", "rerank", "verifying", "finalizing", "completed", "failed"}:
                    queries_in_flight = 0
                else:
                    queries_in_flight = int(latest_telemetry.get("queries_in_flight", 0) or 0)
                unique_after_dedupe = max(
                    previous_unique,
                    int(event.get("unique_after_dedupe", previous_unique) or 0),
                )
                raw_found = max(
                    previous_raw_found,
                    int(event.get("raw_found", previous_raw_found) or 0),
                    unique_after_dedupe,
                )
                reranked_count = max(
                    previous_reranked,
                    int(event.get("reranked_count", previous_reranked) or 0),
                )
                rerank_target = max(
                    previous_rerank_target,
                    int(event.get("rerank_target", previous_rerank_target) or 0),
                    reranked_count,
                )
                if stage == "rerank" and rerank_target <= 0:
                    rerank_target = max(
                        previous_rerank_target,
                        min(
                            max(1, unique_after_dedupe),
                            max(1, int(latest_telemetry.get("target", requested_limit) or requested_limit or 1)),
                        ),
                    )
                raw_finalized_count = max(
                    previous_finalized,
                    int(event.get("finalized_count", previous_finalized) or 0),
                )
                if stage in {"retrieval", "dedupe", "rerank", "verifying"}:
                    finalized_count = min(previous_finalized, max(0, requested_limit))
                else:
                    finalized_count = min(raw_finalized_count, max(0, requested_limit))
                verified_count = max(0, int(event.get("verified_count", previous_verified) or 0))
                review_count = max(0, int(event.get("review_count", previous_review) or 0))
                reject_count = max(0, int(event.get("reject_count", previous_reject) or 0))
                verifying_count = max(
                    0,
                    int(event.get("verifying_count", previous_verifying) or 0),
                )
                estimated_total_seconds = int(event.get("estimated_total_seconds", latest_telemetry.get("estimated_total_seconds", 0)) or 0)
                if stage in {"rerank", "verifying", "finalizing"}:
                    queries_in_flight = 0

                explicit_percent = _resolve_pipeline_progress_percent(
                    stage=stage,
                    explicit_percent=event.get("percent"),
                    previous_percent=int(latest_telemetry.get("percent", 5) or 5),
                    queries_completed=queries_completed,
                    queries_total=queries_total,
                )

                _push_progress(
                    {
                        "stage": stage,
                        "stage_label": stage_label_map.get(stage, stage.title()),
                        "queries_total": queries_total,
                        "queries_completed": queries_completed,
                        "queries_in_flight": queries_in_flight,
                        "raw_found": raw_found,
                        "unique_after_dedupe": unique_after_dedupe,
                        "reranked_count": reranked_count,
                        "rerank_target": rerank_target,
                        "finalized_count": finalized_count,
                        "verified_count": verified_count,
                        "review_count": review_count,
                        "reject_count": reject_count,
                        "verifying_count": verifying_count,
                        "stage_elapsed_seconds": event.get("stage_elapsed_seconds"),
                        "estimated_total_seconds": estimated_total_seconds,
                        "round": round_number,
                        "percent": explicit_percent,
                        "message": str(event.get("message", latest_telemetry.get("message", "")) or ""),
                    },
                    checkpoint_patch={"event": "progress"},
                )

            _push_progress(
                {
                    "stage": "retrieval",
                    "stage_label": "Retrieval",
                    "percent": 4,
                    "message": "Preparing search pipeline.",
                },
                checkpoint_patch={"event": "started"},
                force=True,
            )

            if project_id:
                save_project_brief(
                    actor,
                    project_id=project_id,
                    brief_json=ui_payload["brief_config"],
                    role_title=brief.role_title,
                    target_geography=target_geography,
                )
            exclude_report_paths = [Path(value) for value in payload.get("exclude_report_paths", [])]
            exclude_history_dirs = [Path(value) for value in payload.get("exclude_history_dirs", [])]
            execution_backend = "local_engine"
            remote_error_message = ""
            if remote_client.is_configured():
                _push_progress(
                    {
                        "stage": "retrieval",
                        "stage_label": "Retrieval",
                        "message": "Running remote sourcing request.",
                        "percent": 8,
                    },
                    checkpoint_patch={"event": "remote_request"},
                )
                try:
                    report = await remote_client.run_search(payload, ui_payload)
                    execution_backend = "remote_api"
                except Exception as exc:
                    if remote_client.is_required() and not _remote_error_allows_local_fallback(exc):
                        raise
                    remote_error_message = str(exc)
                    report = await _run_local_search(
                        brief,
                        providers=list(ui_payload["providers"]),
                        limit=internal_fetch_limit,
                        dry_run=bool(payload.get("dry_run", False)),
                        exclude_report_paths=exclude_report_paths,
                        exclude_history_dirs=exclude_history_dirs,
                        progress_callback=_on_pipeline_progress,
                    )
                    execution_backend = "local_fallback"
            else:
                report = await _run_local_search(
                    brief,
                    providers=list(ui_payload["providers"]),
                    limit=internal_fetch_limit,
                    dry_run=bool(payload.get("dry_run", False)),
                    exclude_report_paths=exclude_report_paths,
                    exclude_history_dirs=exclude_history_dirs,
                    progress_callback=_on_pipeline_progress,
                )
            report, internal_fetch_limit = await _expand_report_to_requested_limit(
                report,
                payload=payload,
                ui_payload=ui_payload,
                requested_limit=requested_limit,
                internal_fetch_limit=internal_fetch_limit,
                execution_backend=execution_backend,
                exclude_report_paths=exclude_report_paths,
                exclude_history_dirs=exclude_history_dirs,
                progress_callback=_on_pipeline_progress,
            )
            verification_stats = None
            if verification_enabled and verification_target > 0:
                report.candidates = prioritize_verification_candidates(
                    report.candidates,
                    brief=brief,
                    company_required=bool(brief.company_targets),
                )
                verification_target_plan = _resolve_effective_verification_target(
                    report.candidates,
                    requested_limit=requested_limit,
                    verification_target=verification_target,
                    company_required=bool(brief.company_targets),
                )
                effective_verification_target = int(verification_target_plan["effective_target"] or 0)
                report.summary = dict(report.summary or {})
                report.summary["verification_requested_target"] = int(
                    verification_target_plan["requested_target"] or 0
                )
                report.summary["verification_effective_target"] = effective_verification_target
                verifier = PublicEvidenceVerifier(
                    {
                        **dict(brief.provider_settings.get("scrapingbee_google", {}) or {}),
                        **verification_settings,
                    }
                )
                if verifier.is_configured():
                    verification_pipeline_metrics = dict(report.summary.get("pipeline_metrics", {}) or {})
                    verification_progress_base = _verification_progress_base(
                        verification_pipeline_metrics,
                        latest_telemetry,
                    )

                    def _on_verification_progress(event: Dict[str, Any]) -> None:
                        checked = max(0, int(event.get("candidates_checked", 0) or 0))
                        total = max(
                            1,
                            int(
                                event.get("candidates_total", effective_verification_target)
                                or effective_verification_target
                                or 1
                            ),
                        )
                        coverage = min(1.0, max(0.0, checked / max(1, total)))
                        _push_progress(
                            {
                                "stage": "verifying",
                                "stage_label": "Verifying",
                                **verification_progress_base,
                                "queries_in_flight": 0,
                                "verification_target": total,
                                "verified_candidates_checked": checked,
                                "verification_requests_used": int(event.get("requests_used", 0) or 0),
                                "verifying_count": int(event.get("verifying_count", max(0, total - checked)) or 0),
                                "verified_count": int(event.get("verified_count", 0) or 0),
                                "review_count": int(event.get("review_count", 0) or 0),
                                "reject_count": int(event.get("reject_count", 0) or 0),
                                "percent": max(92, min(98, 92 + int(round(coverage * 6)))),
                                "message": (
                                    "Checking public evidence for top candidates. "
                                    f"{checked}/{total} reviewed."
                                ),
                            },
                            checkpoint_patch={"event": "verifying"},
                        )

                    _push_progress(
                        {
                            "stage": "verifying",
                            "stage_label": "Verifying",
                            **verification_progress_base,
                            "queries_in_flight": 0,
                            "verification_target": effective_verification_target,
                            "verified_candidates_checked": 0,
                            "verification_requests_used": 0,
                            "verifying_count": effective_verification_target,
                            "verified_count": 0,
                            "review_count": 0,
                            "reject_count": 0,
                            "percent": 92,
                            "message": "Checking public evidence for top candidates.",
                        },
                        checkpoint_patch={"event": "verifying_started"},
                        force=True,
                    )
                    verification_stats = await verifier.verify_candidates(
                        report.candidates,
                        brief,
                        limit=effective_verification_target,
                        progress_callback=_on_verification_progress,
                    )
                    refresh_report_summary(report, verification_stats, brief=brief)
            quality_recovery_settings = _quality_recovery_settings(
                brief,
                requested_limit=requested_limit,
                current_fetch_limit=internal_fetch_limit,
            )
            quality_recovery_summary = {
                "enabled": bool(quality_recovery_settings.get("enabled", False)),
                "rounds": 0,
                "notes": [],
            }
            if quality_recovery_summary["enabled"] and not bool(payload.get("dry_run", False)):
                quality_recovery_gap = _quality_recovery_gap(report.summary, quality_recovery_settings)
                stagnant_quality_rounds = 0
                while (
                    quality_recovery_gap["should_retry"]
                    and quality_recovery_summary["rounds"] < int(quality_recovery_settings["max_rounds"])
                ):
                    quality_recovery_summary["rounds"] += 1
                    recovery_round = int(quality_recovery_summary["rounds"])
                    next_fetch_limit = max(
                        compute_top_up_fetch_limit(requested_limit, internal_fetch_limit),
                        internal_fetch_limit + int(quality_recovery_settings["fetch_limit_increment"]),
                    )
                    recovery_payload = dict(payload)
                    recovery_payload["internal_fetch_limit_override"] = next_fetch_limit
                    recovery_payload["top_up_round"] = max(
                        recovery_round,
                        int(report.summary.get("top_up_rounds", 0) or 0) + recovery_round,
                    )
                    recovery_payload["jd_breakdown"] = dict(ui_payload.get("job_description_breakdown", {}))
                    recovery_payload["verification_enabled"] = True
                    recovery_payload["verification_top_n"] = max(
                        int(quality_recovery_settings["verification_top_n"]),
                        int(verification_target or 0),
                    )
                    recovery_payload["verification_parallel_candidates"] = max(
                        int(quality_recovery_settings["verification_parallel_candidates"]),
                        int(verification_settings.get("parallel_candidates", 0) or 0),
                    )
                    recovery_payload["provider_parallel_requests"] = max(
                        int(quality_recovery_settings["parallel_requests"]),
                        int(payload.get("provider_parallel_requests", 0) or 0),
                    )
                    recovery_payload["scrapingbee_max_queries"] = max(
                        int(quality_recovery_settings["max_queries"]),
                        int(payload.get("scrapingbee_max_queries", 0) or 0),
                    )
                    recovery_payload["max_geo_groups"] = max(
                        int(quality_recovery_settings["max_geo_groups"]),
                        int(payload.get("max_geo_groups", 0) or 0),
                    )
                    recovery_payload["reranker_top_n"] = max(
                        int(quality_recovery_settings["reranker_top_n"]),
                        int(payload.get("reranker_top_n", 0) or 0),
                    )
                    if bool(quality_recovery_settings.get("disable_history_slices", False)):
                        recovery_payload["include_history_slices"] = False
                    if bool(quality_recovery_settings.get("disable_registry_memory", False)):
                        recovery_payload["registry_memory_enabled"] = False
                    if bool(quality_recovery_settings.get("force_discovery_slices", False)):
                        recovery_payload["include_discovery_slices"] = True
                    if bool(quality_recovery_settings.get("force_geo_fanout", False)):
                        recovery_payload["geo_fanout_enabled"] = True
                    if bool(
                        quality_recovery_settings.get("force_adjacent_titles", False)
                        or quality_recovery_settings.get("force_country_only_queries", False)
                    ):
                        recovery_clarifications = dict(recovery_payload.get("brief_clarifications", {}) or {})
                        recovery_clarifications["expand_search_when_thin"] = True
                        if bool(quality_recovery_settings.get("force_country_only_queries", False)):
                            recovery_clarifications["strict_market_scope"] = False
                    else:
                        recovery_clarifications = None
                    if recovery_clarifications is not None and bool(
                        quality_recovery_settings.get("force_adjacent_titles", False)
                    ):
                        recovery_clarifications["allow_adjacent_titles"] = True
                    if recovery_clarifications is not None:
                        recovery_payload["brief_clarifications"] = recovery_clarifications

                    _push_progress(
                        {
                            "stage": "retrieval",
                            "stage_label": "Retrieval",
                            "queries_completed": int(latest_telemetry.get("queries_completed", 0) or 0),
                            "queries_total": int(latest_telemetry.get("queries_total", 0) or 0),
                            "raw_found": int(latest_telemetry.get("raw_found", 0) or 0),
                            "unique_after_dedupe": len(report.candidates),
                            "reranked_count": int(latest_telemetry.get("reranked_count", 0) or 0),
                            "finalized_count": len(report.candidates[:requested_limit]),
                            "verified_count": int(report.summary.get("verified_count", 0) or 0),
                            "review_count": int(report.summary.get("review_count", 0) or 0),
                            "reject_count": int(report.summary.get("reject_count", 0) or 0),
                            "percent": 80,
                            "round": recovery_round,
                            "message": (
                                "Verified yield is below target, so HR Hunter is expanding to fresh public candidates. "
                                f"Recovery round {recovery_round}."
                            ),
                        },
                        checkpoint_patch={"event": "quality_recovery"},
                        force=True,
                    )
                    recovery_ui_payload = build_ui_brief_payload(recovery_payload)
                    recovery_brief = build_search_brief(recovery_ui_payload["brief_config"])
                    supplemental_report = await _run_local_search(
                        recovery_brief,
                        providers=list(recovery_ui_payload["providers"]),
                        limit=next_fetch_limit,
                        dry_run=bool(payload.get("dry_run", False)),
                        exclude_report_paths=exclude_report_paths,
                        exclude_history_dirs=exclude_history_dirs,
                        extra_exclude_candidate_keys=_collect_candidate_keys_from_report(report),
                        extra_exclude_provider_queries=_collect_provider_query_exclusions_from_report(report),
                        progress_callback=_on_pipeline_progress,
                    )
                    previous_candidate_count = len(report.candidates)
                    report = _merge_ranked_report_candidates(report, supplemental_report, brief=recovery_brief)
                    net_new_candidates = max(0, len(report.candidates) - previous_candidate_count)
                    if net_new_candidates <= 0:
                        stagnant_quality_rounds += 1
                    else:
                        stagnant_quality_rounds = 0
                    brief = recovery_brief
                    rerank_window_target = max(
                        int(quality_recovery_settings["reranker_top_n"] or 0),
                        int(len(report.candidates) or 0),
                    )
                    _push_progress(
                        {
                            "stage": "rerank",
                            "stage_label": "Rerank",
                            "queries_completed": int(latest_telemetry.get("queries_completed", 0) or 0),
                            "queries_total": int(latest_telemetry.get("queries_total", 0) or 0),
                            "raw_found": max(
                                int(latest_telemetry.get("raw_found", 0) or 0),
                                len(report.candidates),
                            ),
                            "unique_after_dedupe": len(report.candidates),
                            "reranked_count": 0,
                            "rerank_target": min(len(report.candidates), max(1, rerank_window_target)),
                            "finalized_count": min(requested_limit, len(report.candidates)),
                            "verified_count": int(report.summary.get("verified_count", 0) or 0),
                            "review_count": int(report.summary.get("review_count", 0) or 0),
                            "reject_count": int(report.summary.get("reject_count", 0) or 0),
                            "percent": 88,
                            "round": recovery_round,
                            "message": (
                                "Re-ranking the combined candidate pool after expanding recovery round "
                                f"{recovery_round}."
                            ),
                        },
                        checkpoint_patch={"event": "quality_recovery_rerank"},
                        force=True,
                    )
                    report, rerank_metrics = await asyncio.to_thread(
                        _rerank_merged_report_candidates,
                        report,
                        brief=brief,
                        rerank_top_n=int(quality_recovery_settings["reranker_top_n"] or 0),
                    )
                    report.summary = dict(report.summary or {})
                    rerank_pipeline_metrics = dict(report.summary.get("pipeline_metrics", {}) or {})
                    rerank_pipeline_metrics["raw_found"] = max(
                        len(report.candidates),
                        int(rerank_pipeline_metrics.get("raw_found", 0) or 0),
                    )
                    rerank_pipeline_metrics["unique_after_dedupe"] = max(
                        len(report.candidates),
                        int(rerank_pipeline_metrics.get("unique_after_dedupe", 0) or 0),
                    )
                    rerank_pipeline_metrics["rerank_target"] = max(
                        int(rerank_pipeline_metrics.get("rerank_target", 0) or 0),
                        int(rerank_metrics.get("rerank_target", 0) or 0),
                    )
                    rerank_pipeline_metrics["reranked_count"] = max(
                        int(rerank_pipeline_metrics.get("reranked_count", 0) or 0),
                        int(rerank_metrics.get("reranked_count", 0) or 0),
                    )
                    report.summary["pipeline_metrics"] = rerank_pipeline_metrics
                    _push_progress(
                        {
                            "stage": "rerank",
                            "stage_label": "Rerank",
                            "queries_completed": int(latest_telemetry.get("queries_completed", 0) or 0),
                            "queries_total": int(latest_telemetry.get("queries_total", 0) or 0),
                            "raw_found": int(rerank_pipeline_metrics.get("raw_found", 0) or 0),
                            "unique_after_dedupe": int(rerank_pipeline_metrics.get("unique_after_dedupe", 0) or 0),
                            "reranked_count": int(rerank_pipeline_metrics.get("reranked_count", 0) or 0),
                            "rerank_target": int(rerank_pipeline_metrics.get("rerank_target", 0) or 0),
                            "finalized_count": min(requested_limit, len(report.candidates)),
                            "verified_count": int(report.summary.get("verified_count", 0) or 0),
                            "review_count": int(report.summary.get("review_count", 0) or 0),
                            "reject_count": int(report.summary.get("reject_count", 0) or 0),
                            "percent": 91,
                            "round": recovery_round,
                            "message": (
                                "Combined recovery pool reranked. Stronger fresh candidates are now moved up for "
                                f"verification in round {recovery_round}."
                            ),
                        },
                        checkpoint_patch={"event": "quality_recovery_rerank_complete"},
                        force=True,
                    )
                    verification_settings = dict(brief.provider_settings.get("verification", {}) or {})
                    report.candidates = prioritize_verification_candidates(
                        report.candidates,
                        brief=brief,
                        company_required=bool(brief.company_targets),
                    )
                    recovery_verifier = PublicEvidenceVerifier(
                        {
                            **dict(brief.provider_settings.get("scrapingbee_google", {}) or {}),
                            **verification_settings,
                        }
                    )
                    recovery_verify_limit = max(
                        0,
                        min(
                            len(report.candidates),
                            int(quality_recovery_settings["verification_top_n"] or 0),
                        ),
                    )
                    recovery_verification_candidates = _quality_recovery_verification_candidates(
                        report.candidates,
                        limit=recovery_verify_limit,
                        brief=brief,
                    )
                    recovery_verification_stats = None
                    if recovery_verification_candidates and recovery_verifier.is_configured():
                        recovery_progress_base = _verification_progress_base(
                            dict(report.summary.get("pipeline_metrics", {}) or {}),
                            latest_telemetry,
                        )

                        def _on_recovery_verification_progress(event: Dict[str, Any]) -> None:
                            checked = max(0, int(event.get("candidates_checked", 0) or 0))
                            total = max(
                                1,
                                int(
                                    event.get("candidates_total", len(recovery_verification_candidates))
                                    or len(recovery_verification_candidates)
                                    or 1
                                ),
                            )
                            coverage = min(1.0, max(0.0, checked / max(1, total)))
                            _push_progress(
                                {
                                    "stage": "verifying",
                                    "stage_label": "Verifying",
                                    **recovery_progress_base,
                                    "queries_in_flight": 0,
                                    "verification_target": total,
                                    "verified_candidates_checked": checked,
                                    "verification_requests_used": int(event.get("requests_used", 0) or 0),
                                    "verifying_count": int(event.get("verifying_count", max(0, total - checked)) or 0),
                                    "verified_count": int(event.get("verified_count", 0) or 0),
                                    "review_count": int(event.get("review_count", 0) or 0),
                                    "reject_count": int(event.get("reject_count", 0) or 0),
                                    "percent": max(92, min(98, 92 + int(round(coverage * 6)))),
                                    "message": (
                                        "Checking fresh public evidence on new candidates from recovery round "
                                        f"{recovery_round}. {checked}/{total} reviewed."
                                    ),
                                },
                                checkpoint_patch={"event": "quality_recovery_verifying"},
                            )

                        recovery_verification_stats = await recovery_verifier.verify_candidates(
                            recovery_verification_candidates,
                            brief,
                            limit=len(recovery_verification_candidates),
                            progress_callback=_on_recovery_verification_progress,
                        )
                        refresh_report_summary(report, recovery_verification_stats, brief=brief)
                    report = _finalize_report_for_limit(
                        report,
                        requested_limit=requested_limit,
                        internal_fetch_limit=next_fetch_limit,
                        brief=brief,
                    )
                    internal_fetch_limit = next_fetch_limit
                    quality_recovery_gap = _quality_recovery_gap(report.summary, quality_recovery_settings)
                    quality_recovery_summary["notes"].append(
                        (
                            f"round {recovery_round}: +{net_new_candidates} fresh candidates, "
                            f"{quality_recovery_gap['verified_count']} verified, "
                            f"{quality_recovery_gap['reject_count']} rejected"
                        )
                    )
                    if stagnant_quality_rounds >= 2:
                        quality_recovery_summary["notes"].append(
                            "stopped after repeated recovery rounds failed to add fresh candidates"
                        )
                        break
                report.summary = dict(report.summary or {})
                report.summary["quality_recovery"] = quality_recovery_summary
            report = _finalize_report_for_limit(
                report,
                requested_limit=requested_limit,
                internal_fetch_limit=internal_fetch_limit,
                brief=brief,
            )
            pipeline_metrics = dict(report.summary.get("pipeline_metrics", {}) or {})
            _push_progress(
                {
                    "stage": "finalizing",
                    "stage_label": "Finalizing",
                    "percent": 97,
                    "queries_in_flight": 0,
                    "rerank_target": int(pipeline_metrics.get("rerank_target", 0) or 0),
                    "reranked_count": int(pipeline_metrics.get("reranked_count", 0) or 0),
                    "finalized_count": len(report.candidates),
                    "verified_candidates_checked": int(
                        (verification_stats or {}).get("candidates_checked", latest_telemetry.get("verified_candidates_checked", 0))
                        or 0
                    ),
                    "verification_target": int(latest_telemetry.get("verification_target", verification_target) or verification_target or 0),
                    "verification_requests_used": int(
                        (verification_stats or {}).get("requests_used", latest_telemetry.get("verification_requests_used", 0))
                        or 0
                    ),
                    "verifying_count": 0,
                    "verified_count": int(report.summary.get("verified_count", latest_telemetry.get("verified_count", 0)) or 0),
                    "review_count": int(report.summary.get("review_count", latest_telemetry.get("review_count", 0)) or 0),
                    "reject_count": int(report.summary.get("reject_count", latest_telemetry.get("reject_count", 0)) or 0),
                    "message": "Persisting final results and artifacts.",
                },
                checkpoint_patch={"event": "finalizing"},
                force=True,
            )
            output_dir = Path(ui_payload["output_dir"])
            json_path, csv_path = write_report(
                report,
                output_dir,
                csv_candidate_limit=int(ui_payload["csv_export_limit"]),
            )
            state_record = persist_search_run(
                brief,
                report,
                provider_names=list(ui_payload["providers"]),
                limit_requested=requested_limit,
                json_report_path=json_path,
                csv_report_path=csv_path,
                owner_id=str(payload.get("recruiter_id", "")),
                owner_name=str(payload.get("recruiter_name", "")),
                team_id=str(payload.get("team_id", "")),
                execution_backend=execution_backend,
                mandate_id_override=project_id,
            )
            project_payload = {}
            if project_id:
                project_payload = attach_project_run(
                    actor,
                    project_id=project_id,
                    run_id=report.run_id,
                    brief_json=ui_payload["brief_config"],
                )
            complete_job(
                job_id,
                {
                    "run_id": report.run_id,
                    "summary": report.summary,
                    "report_paths": {"json": str(json_path), "csv": str(csv_path)},
                    "execution_backend": execution_backend,
                    "remote_error": remote_error_message,
                    "state": state_record,
                    "project": project_payload,
                },
            )
        except Exception as exc:
            fail_job(job_id, f"{str(exc).strip() or 'The search failed unexpectedly.'} Please retry the search.")

    async def _train_ranker_job_runner(job_id: str, payload: Dict[str, Any]) -> None:
        try:
            start_job(job_id)
            db_path = Path(str(payload.get("feedback_db", resolve_feedback_db_path()))).expanduser().resolve()
            model_dir = Path(str(payload.get("model_dir", resolve_ranker_model_dir()))).expanduser().resolve()
            init_feedback_db(db_path)
            training_rows = load_ranker_training_rows(db_path)
            result = train_learned_ranker(
                training_rows,
                model_dir=model_dir,
                n_estimators=int(payload.get("n_estimators", 80)),
                learning_rate=float(payload.get("learning_rate", 0.1)),
                num_leaves=int(payload.get("num_leaves", 31)),
            )
            complete_job(job_id, result)
        except Exception as exc:
            fail_job(job_id, f"{str(exc).strip() or 'The training job failed unexpectedly.'} Please retry.")

    async def _resume_pending_jobs() -> None:
        pending: List[Dict[str, Any]] = []
        pending.extend(list_jobs(limit=500, status="queued"))
        pending.extend(list_jobs(limit=500, status="running"))
        seen_ids: set[str] = set()
        for job in pending:
            job_id = str(job.get("job_id", "")).strip()
            if not job_id or job_id in seen_ids:
                continue
            seen_ids.add(job_id)
            if str(job.get("job_type", "")).strip().lower() != "search":
                continue
            payload = dict(job.get("payload") or {})
            if not payload:
                fail_job(job_id, "The job payload is missing, so this run cannot be resumed. Please retry.")
                continue
            update_job_progress(
                job_id,
                {
                    "stage": "running",
                    "stage_label": "Running",
                    "status": "running",
                    "message": "Resuming search after app restart.",
                    "percent": max(3, int(job.get("progress", {}).get("percent", 3) or 3)),
                },
                checkpoint={"event": "resumed_after_restart"},
            )
            _spawn_background_job(job_id, lambda job_id=job_id, payload=payload: _search_job_runner(job_id, payload))

    @app.on_event("startup")
    async def _app_startup_resume_jobs() -> None:
        await _resume_pending_jobs()

    @app.get("/")
    async def home() -> FileResponse:
        if not ui_dir.exists():
            raise HTTPException(status_code=404, detail="UI assets are not installed.")
        return FileResponse(
            ui_dir / "index.html",
            headers={
                "Cache-Control": "no-store, max-age=0, must-revalidate",
                "Pragma": "no-cache",
            },
        )

    @app.get("/healthz")
    async def healthz() -> Dict[str, str]:
        return {"status": "ok"}

    @app.get("/app-config")
    async def app_config() -> Dict[str, Any]:
        bootstrap = build_app_bootstrap()
        bootstrap["project_statuses"] = PROJECT_STATUS_OPTIONS
        storage = _runtime_storage_snapshot()
        bootstrap["paths"] = {
            "workspace_root": str(workspace_root),
            "output_dir": str(resolve_output_dir()),
            "feedback_db": str(resolve_feedback_db_path()),
            "model_dir": str(resolve_ranker_model_dir()),
            "state_db": storage["state"]["display_locator"],
            "workspace_db": storage["workspace"]["display_locator"],
        }
        bootstrap["storage"] = storage
        bootstrap["remote_sourcing"] = {
            "configured": remote_client.is_configured(),
            "required": remote_client.is_required(),
            "base_url": remote_client.base_url if remote_client.is_configured() else "",
        }
        bootstrap["private_api_auth"] = {"enabled": _private_api_auth_configured()}
        return bootstrap

    @app.post("/app/auth/login")
    async def app_auth_login(payload: Dict[str, Any], response: Response) -> Dict[str, Any]:
        try:
            result = authenticate_user(
                str(payload.get("email", "")),
                str(payload.get("otp_code", payload.get("totp_code", ""))),
            )
        except Exception as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc
        response.set_cookie(
            "hr_hunter_session",
            result["session_token"],
            httponly=True,
            samesite="lax",
            max_age=SESSION_TTL_DAYS * 24 * 60 * 60,
            path="/",
        )
        result["projects"] = list_projects(result["user"], limit=40)
        return result

    @app.post("/app/auth/login-form")
    async def app_auth_login_form(request: Request) -> RedirectResponse:
        form = await request.form()
        try:
            result = authenticate_user(
                str(form.get("email", "")),
                str(form.get("otp_code", form.get("totp_code", ""))),
            )
        except Exception:
            return RedirectResponse(url="/?session=auth-failed", status_code=303)

        response = RedirectResponse(url="/", status_code=303)
        response.set_cookie(
            "hr_hunter_session",
            result["session_token"],
            httponly=True,
            samesite="lax",
            max_age=SESSION_TTL_DAYS * 24 * 60 * 60,
            path="/",
        )
        return response

    @app.get("/app/auth/session")
    async def app_auth_session(request: Request) -> Dict[str, Any]:
        user = _require_app_user(request)
        projects = list_projects(user, limit=40)
        return {
            "user": user,
            "projects": projects,
        }

    @app.post("/app/auth/logout")
    async def app_auth_logout(request: Request, response: Response) -> Dict[str, bool]:
        token = _session_token_from_request(request)
        if token:
            revoke_session(token)
        response.delete_cookie("hr_hunter_session", path="/")
        return {"ok": True}

    @app.get("/app/users")
    async def app_users(request: Request, query: str = "") -> Dict[str, Any]:
        _require_app_user(request)
        return {"users": list_users(query=query)}

    @app.post("/app/admin/users")
    async def app_admin_create_user(request: Request, payload: Dict[str, Any]) -> Dict[str, Any]:
        admin_user = _require_admin_user(request)
        try:
            user = create_user_account(
                email=str(payload.get("email", "")),
                password=str(payload.get("password", "")),
                full_name=str(payload.get("full_name", payload.get("name", ""))),
                team_id=str(payload.get("team_id", "")),
                role=str(payload.get("role", "recruiter")),
                created_by=admin_user["id"],
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"user": user}

    @app.post("/app/admin/users/{user_id}/totp")
    async def app_admin_user_totp(request: Request, user_id: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        _require_admin_user(request)
        rotate = bool((payload or {}).get("rotate", False))
        try:
            return get_user_totp_setup(user_id=user_id, rotate=rotate)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/app/projects")
    async def app_projects(request: Request, query: str = "", limit: int = 50) -> Dict[str, Any]:
        user = _require_app_user(request)
        return {"projects": list_projects(user, query=query, limit=max(1, min(int(limit), 200)))}

    @app.post("/app/projects")
    async def app_create_project(request: Request, payload: Dict[str, Any]) -> Dict[str, Any]:
        user = _require_app_user(request)
        try:
            project = create_project(
                user,
                name=str(payload.get("name", "")),
                client_name=str(payload.get("client_name", "")),
                role_title=str(payload.get("role_title", "")),
                target_geography=str(payload.get("target_geography", "")),
                status=str(payload.get("status", "active")),
                notes=str(payload.get("notes", "")),
                brief_json=payload.get("brief_json") if isinstance(payload.get("brief_json"), dict) else {},
                assigned_user_ids=payload.get("assigned_user_ids", []),
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"project": project}

    @app.get("/app/projects/{project_id}")
    async def app_project_detail(request: Request, project_id: str) -> Dict[str, Any]:
        user = _require_app_user(request)
        try:
            project = get_project(user, project_id)
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"project": project}

    @app.post("/app/projects/{project_id}")
    async def app_update_project(request: Request, project_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        user = _require_app_user(request)
        try:
            project = update_project(
                user,
                project_id=project_id,
                name=str(payload.get("name", "")),
                client_name=str(payload.get("client_name", "")),
                role_title=str(payload.get("role_title", "")),
                target_geography=str(payload.get("target_geography", "")),
                status=str(payload.get("status", "active")),
                notes=str(payload.get("notes", "")),
                brief_json=payload.get("brief_json") if isinstance(payload.get("brief_json"), dict) else {},
                assigned_user_ids=payload.get("assigned_user_ids") if isinstance(payload.get("assigned_user_ids"), list) else None,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"project": project}

    @app.delete("/app/projects/{project_id}")
    async def app_delete_project(request: Request, project_id: str) -> Dict[str, Any]:
        user = _require_app_user(request)
        try:
            deleted = delete_project(user, project_id=project_id)
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"deleted": deleted}

    @app.post("/app/projects/{project_id}/save-brief")
    async def app_project_save_brief(request: Request, project_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        user = _require_app_user(request)
        try:
            project = save_project_brief(
                user,
                project_id=project_id,
                brief_json=payload.get("brief_json") if isinstance(payload.get("brief_json"), dict) else {},
                role_title=str(payload.get("role_title", "")),
                target_geography=str(payload.get("target_geography", "")),
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"project": project}

    @app.get("/app/projects/{project_id}/runs")
    async def app_project_runs(request: Request, project_id: str, limit: int = 25) -> Dict[str, Any]:
        user = _require_app_user(request)
        try:
            runs = list_project_runs(user, project_id=project_id, limit=max(1, min(int(limit), 100)))
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"runs": runs}

    @app.delete("/app/projects/{project_id}/runs/{run_id}")
    async def app_project_delete_run(request: Request, project_id: str, run_id: str) -> Dict[str, Any]:
        user = _require_admin_user(request)
        try:
            deleted = delete_project_run(user, project_id=project_id, run_id=run_id)
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"deleted": deleted}

    @app.get("/app/projects/{project_id}/run")
    async def app_project_run(request: Request, project_id: str, run_id: str = "") -> Dict[str, Any]:
        user = _require_app_user(request)
        try:
            payload = get_project_run_report(user, project_id=project_id, run_id=run_id)
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        if not payload:
            return {}
        return payload

    @app.post("/brief/parse")
    async def brief_parse(request: Request, payload: Dict[str, Any]) -> Dict[str, Any]:
        _enforce_private_api_auth(request)
        raw_text = str(payload.get("rawText", payload.get("job_description", "")))
        title = str(payload.get("title", payload.get("role_title", "")))
        criteria = dict(payload.get("criteria", {})) if isinstance(payload.get("criteria", {}), dict) else {}
        local_payload = {
            "role_title": title,
            "titles": criteria.get("jobTitles", criteria.get("titles", [])),
            "countries": [
                str(scope.get("country", ""))
                for scope in criteria.get("locationScopes", [])
                if str(scope.get("country", "")).strip()
            ],
            "cities": [
                str(scope.get("city", scope.get("value", "")))
                for scope in criteria.get("locationScopes", [])
                if str(scope.get("city", scope.get("value", ""))).strip() and str(scope.get("type", "")).strip() == "city_radius"
            ],
            "company_targets": [
                str(target.get("name", ""))
                for target in criteria.get("companyTargets", [])
                if str(target.get("name", "")).strip()
            ],
            "industry_keywords": criteria.get("industries", []),
            "must_have_keywords": criteria.get("mustHaveKeywords", []),
            "anchors": criteria.get("anchors", {}),
            "job_description": raw_text,
        }
        breakdown = extract_job_description_breakdown(raw_text, role_title=title)
        return {
            "summary": breakdown.get("summary", ""),
            "extractedPoints": breakdown.get("key_experience_points", []),
            "requiredKeywords": breakdown.get("required_keywords", []),
            "preferredKeywords": breakdown.get("preferred_keywords", []),
            "industries": breakdown.get("industry_keywords", []),
            "jobTitles": breakdown.get("titles", []),
            "seniorityLevels": breakdown.get("seniority_levels", []),
            "experience": {
                "mode": breakdown.get("years", {}).get("mode", "range"),
                "targetYears": breakdown.get("years", {}).get("value"),
                "minYears": breakdown.get("years", {}).get("min"),
                "maxYears": breakdown.get("years", {}).get("max"),
                "toleranceYears": breakdown.get("years", {}).get("tolerance", 0),
            },
            "suggestedAnchors": breakdown.get("suggested_anchors", {}),
            "normalizedCriteria": local_payload,
        }

    @app.post("/search")
    async def search(request: Request, payload: Dict[str, Any]) -> Dict[str, Any]:
        _enforce_private_api_auth(request)
        brief_config = payload.get("brief")
        brief_path = payload.get("brief_path")
        providers = payload.get("providers", ["scrapingbee_google"])
        limit = int(payload.get("limit", 100))
        requested_limit = int(payload.get("requestedLimit", limit) or limit)
        dry_run = bool(payload.get("dry_run", False))
        exclude_report_paths = payload.get("exclude_report_paths", [])
        exclude_history_dirs = payload.get("exclude_history_dirs", [])

        if isinstance(payload.get("criteria"), dict):
            brief_config = _brief_config_from_remote_payload(payload)
            providers = payload.get("providers", ["scrapingbee_google"])
            limit = int(payload.get("limit", 100))
            requested_limit = int(payload.get("requestedLimit", limit) or limit)

        if brief_path:
            brief_config = load_yaml_file(Path(brief_path))
        if not isinstance(brief_config, dict):
            raise HTTPException(status_code=400, detail="Provide `brief` or `brief_path`.")

        brief = build_search_brief(brief_config)
        report = await _run_local_search(
            brief,
            providers=list(providers),
            limit=limit,
            dry_run=dry_run,
            exclude_report_paths=[Path(value) for value in exclude_report_paths],
            exclude_history_dirs=[Path(value) for value in exclude_history_dirs],
        )
        report = _finalize_report_for_limit(
            report,
            requested_limit=requested_limit,
            internal_fetch_limit=limit,
            brief=brief,
        )
        persist_search_run(
            brief,
            report,
            provider_names=list(providers),
            limit_requested=requested_limit,
            execution_backend="private_api",
            owner_id=str(payload.get("orgId", payload.get("org_id", ""))),
        )
        return {
            "runId": report.run_id,
            "summary": report.summary,
            "candidates": [asdict(candidate) for candidate in report.candidates],
            "provider_results": [asdict(result) for result in report.provider_results],
        }

    @app.post("/app/jd-breakdown")
    async def app_jd_breakdown(request: Request, payload: Dict[str, Any]) -> Dict[str, Any]:
        _require_app_user(request)
        source = resolve_job_description_source(
            typed_text=str(payload.get("job_description", "")),
            uploaded_text=str(payload.get("uploaded_job_description_text", "")),
            uploaded_file_name=str(payload.get("uploaded_job_description_name", "")),
        )
        parse_payload = dict(payload)
        parse_payload["job_description"] = source["combined_text"]
        breakdown: Dict[str, Any] | None = None
        if remote_client.is_configured():
            try:
                breakdown = await remote_client.parse_brief(parse_payload)
            except Exception as exc:
                if remote_client.is_required() and not _remote_error_allows_local_fallback(exc):
                    raise HTTPException(status_code=502, detail=f"Remote brief parsing failed: {exc}") from exc
        if breakdown is None:
            breakdown = extract_job_description_breakdown(
                source["combined_text"],
                role_title=str(payload.get("role_title", "")),
            )
        breakdown = ensure_structured_jd_breakdown(
            breakdown,
            job_description=source["combined_text"],
            role_title=str(payload.get("role_title", "")),
        )
        breakdown["source"] = source["source"]
        breakdown["uploaded_file_name"] = source["file_name"]
        return breakdown

    @app.post("/app/jd-upload")
    async def app_jd_upload(
        request: Request,
        file: UploadFile = File(...),
        role_title: str = Form(""),
        job_description_notes: str = Form(""),
    ) -> Dict[str, Any]:
        _require_app_user(request)
        file_name = str(file.filename or "").strip()
        if not file_name:
            raise HTTPException(status_code=400, detail="Choose a JD file to upload.")

        content = await file.read()
        if not content:
            raise HTTPException(status_code=400, detail="The uploaded JD file is empty.")

        try:
            extracted = extract_document_text_from_bytes(file_name, content)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        source = resolve_job_description_source(
            typed_text=job_description_notes,
            uploaded_text=str(extracted.get("text", "")),
            uploaded_file_name=file_name,
        )
        parse_payload = {
            "role_title": role_title,
            "job_description": source["combined_text"],
        }
        if remote_client.is_configured():
            try:
                breakdown = await remote_client.parse_brief(parse_payload)
            except Exception as exc:
                if remote_client.is_required() and not _remote_error_allows_local_fallback(exc):
                    raise HTTPException(status_code=502, detail=f"Remote brief parsing failed: {exc}") from exc
                breakdown = extract_job_description_breakdown(source["combined_text"], role_title=role_title)
        else:
            breakdown = extract_job_description_breakdown(source["combined_text"], role_title=role_title)

        breakdown = ensure_structured_jd_breakdown(
            breakdown,
            job_description=source["combined_text"],
            role_title=role_title,
        )

        if not role_title and isinstance(breakdown.get("titles"), list) and breakdown["titles"]:
            parse_payload["role_title"] = str(breakdown["titles"][0])

        breakdown["source"] = source["source"]
        breakdown["uploaded_file_name"] = file_name
        return {
            "uploaded_file_name": file_name,
            "uploaded_file_extension": extracted.get("file_extension", ""),
            "uploaded_parser": extracted.get("parser", ""),
            "uploaded_job_description_text": extracted.get("text", ""),
            "job_description": job_description_notes,
            "effective_job_description": source["combined_text"],
            "breakdown": breakdown,
        }

    @app.post("/app/brief-quality")
    async def app_brief_quality(request: Request, payload: Dict[str, Any]) -> Dict[str, Any]:
        _require_app_user(request)
        try:
            ui_payload = build_ui_brief_payload(payload)
            quality = assess_ui_brief_quality(ui_payload["brief_config"])
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"quality": quality}

    @app.post("/app/search")
    async def app_search(request: Request, payload: Dict[str, Any]) -> Dict[str, Any]:
        user = _require_app_user(request)
        project_id = str(payload.get("project_id", "")).strip()
        if not project_id:
            raise HTTPException(status_code=400, detail="Select or create a project before running search.")
        try:
            project = get_project(user, project_id)
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        try:
            ui_payload = build_ui_brief_payload(payload)
            quality = assess_ui_brief_quality(ui_payload["brief_config"])
            if not quality.get("ok"):
                raise ValueError(str(quality.get("message", "Hunt brief details are not enough.")))
            brief = build_search_brief(ui_payload["brief_config"])
            requested_limit = int(ui_payload["limit"])
            internal_fetch_limit = int(ui_payload.get("internal_fetch_limit", requested_limit))
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        if not brief.role_title.strip():
            raise HTTPException(status_code=400, detail="Role title is required.")
        target_geography = _summarize_target_geography(ui_payload, brief)
        save_project_brief(
            user,
            project_id=project_id,
            brief_json=ui_payload["brief_config"],
            role_title=brief.role_title,
            target_geography=target_geography,
        )

        exclude_report_paths = [Path(value) for value in payload.get("exclude_report_paths", [])]
        exclude_history_dirs = [Path(value) for value in payload.get("exclude_history_dirs", [])]
        execution_backend = "local_engine"
        remote_error_message = ""
        if remote_client.is_configured():
            try:
                report = await remote_client.run_search(payload, ui_payload)
                execution_backend = "remote_api"
            except Exception as exc:
                if remote_client.is_required() and not _remote_error_allows_local_fallback(exc):
                    raise HTTPException(status_code=502, detail=f"Remote sourcing failed: {exc}") from exc
                remote_error_message = str(exc)
                report = await _run_local_search(
                    brief,
                    providers=list(ui_payload["providers"]),
                    limit=internal_fetch_limit,
                    dry_run=bool(payload.get("dry_run", False)),
                    exclude_report_paths=exclude_report_paths,
                    exclude_history_dirs=exclude_history_dirs,
                )
                execution_backend = "local_fallback"
        else:
            report = await _run_local_search(
                brief,
                providers=list(ui_payload["providers"]),
                limit=internal_fetch_limit,
                dry_run=bool(payload.get("dry_run", False)),
                exclude_report_paths=exclude_report_paths,
                exclude_history_dirs=exclude_history_dirs,
            )
        report, internal_fetch_limit = await _expand_report_to_requested_limit(
            report,
            payload=payload,
            ui_payload=ui_payload,
            requested_limit=requested_limit,
            internal_fetch_limit=internal_fetch_limit,
            execution_backend=execution_backend,
            exclude_report_paths=exclude_report_paths,
            exclude_history_dirs=exclude_history_dirs,
        )
        report = _finalize_report_for_limit(
            report,
            requested_limit=requested_limit,
            internal_fetch_limit=internal_fetch_limit,
            brief=brief,
        )
        output_dir = Path(ui_payload["output_dir"])
        json_path, csv_path = write_report(
            report,
            output_dir,
            csv_candidate_limit=int(ui_payload["csv_export_limit"]),
        )
        state_record = persist_search_run(
            brief,
            report,
            provider_names=list(ui_payload["providers"]),
            limit_requested=requested_limit,
            json_report_path=json_path,
            csv_report_path=csv_path,
            owner_id=user["id"],
            owner_name=user["full_name"],
            team_id=user.get("team_id", ""),
            execution_backend=execution_backend,
            mandate_id_override=project_id,
        )
        project = attach_project_run(
            user,
            project_id=project_id,
            run_id=report.run_id,
            brief_json=ui_payload["brief_config"],
        )
        return {
            "summary": report.summary,
            "candidates": [asdict(candidate) for candidate in report.candidates],
            "provider_results": [asdict(result) for result in report.provider_results],
            "report_paths": {
                "json": str(json_path),
                "csv": str(csv_path),
            },
            "csv_export_limit": int(ui_payload["csv_export_limit"]),
            "feedback_db": ui_payload["feedback_db"],
            "model_dir": ui_payload["model_dir"],
            "brief": ui_payload["brief_config"],
            "jd_breakdown": ui_payload["job_description_breakdown"],
            "execution_backend": execution_backend,
            "remote_error": remote_error_message,
            "state": state_record,
            "project": project,
        }

    @app.get("/app/artifact")
    async def app_artifact(request: Request, path: str) -> FileResponse:
        _require_app_user(request)
        try:
            artifact_path = safe_artifact_path(path, workspace_root=workspace_root)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if not artifact_path.exists() or not artifact_path.is_file():
            raise HTTPException(status_code=404, detail="Artifact not found.")
        return FileResponse(artifact_path)

    @app.get("/app/history/runs")
    async def app_history_runs(request: Request, limit: int = 25, project_id: str = "") -> Dict[str, Any]:
        user = _require_app_user(request)
        mandate_filter = ""
        if project_id.strip():
            get_project(user, project_id.strip())
            mandate_filter = project_id.strip()
            return {"runs": list_run_history(limit=max(1, min(int(limit), 100)), mandate_id=mandate_filter)}
        accessible_projects = {project["id"] for project in list_projects(user, limit=200)}
        runs = [
            run
            for run in list_run_history(limit=300)
            if run["mandate_id"] in accessible_projects
        ][: max(1, min(int(limit), 100))]
        return {"runs": runs}

    @app.get("/app/ops")
    async def app_ops(request: Request) -> Dict[str, Any]:
        _require_admin_user(request)
        summary = summarize_system_state()
        summary["workspace_storage"] = _runtime_storage_snapshot()["workspace"]
        summary["remote_sourcing"] = {
            "configured": remote_client.is_configured(),
            "required": remote_client.is_required(),
            "base_url": remote_client.base_url if remote_client.is_configured() else "",
        }
        summary["private_api_auth"] = {"enabled": _private_api_auth_configured()}
        return summary

    @app.get("/app/reviews")
    async def app_reviews(
        request: Request,
        limit: int = 25,
        mandate_id: str = "",
        candidate_id: str = "",
        project_id: str = "",
    ) -> Dict[str, Any]:
        user = _require_app_user(request)
        resolved_mandate = mandate_id.strip() or project_id.strip()
        if resolved_mandate:
            get_project(user, resolved_mandate)
            reviews = list_review_history(
                limit=max(1, min(int(limit), 100)),
                mandate_id=resolved_mandate,
                candidate_id=candidate_id.strip(),
            )
            return {"reviews": reviews}
        accessible_projects = {project["id"] for project in list_projects(user, limit=200)}
        reviews = [
            review
            for review in list_review_history(limit=300, candidate_id=candidate_id.strip())
            if review["mandate_id"] in accessible_projects
        ][: max(1, min(int(limit), 100))]
        return {"reviews": reviews}

    @app.post("/app/review")
    async def app_review(request: Request, payload: Dict[str, Any]) -> Dict[str, Any]:
        user = _require_app_user(request)
        mandate_id = str(payload.get("mandate_id", payload.get("project_id", ""))).strip()
        if mandate_id:
            get_project(user, mandate_id)
        try:
            return review_candidate(
                mandate_id=mandate_id or f"mandate:local:{payload.get('brief_id', '')}",
                run_id=str(payload.get("run_id", "")).strip(),
                candidate_id=str(payload.get("candidate_id", "")).strip(),
                reviewer_id=user["id"],
                reviewer_name=user["full_name"],
                owner_id=str(payload.get("owner_id", user["id"])).strip(),
                owner_name=str(payload.get("owner_name", user["full_name"])).strip(),
                action=str(payload.get("action", "")).strip(),
                reason_code=str(payload.get("reason_code", "")).strip(),
                note=str(payload.get("note", "")).strip(),
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/app/similar-candidates")
    async def app_similar_candidates(request: Request, payload: Dict[str, Any]) -> Dict[str, Any]:
        _require_app_user(request)
        candidate_payload = payload.get("candidate", {})
        if not isinstance(candidate_payload, dict):
            raise HTTPException(status_code=400, detail="`candidate` payload is required.")
        try:
            profile = candidate_from_remote(candidate_payload)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "similar_candidates": find_similar_candidates(
                profile,
                limit=int(payload.get("limit", 5) or 5),
            )
        }

    @app.post("/app/search-jobs")
    async def app_search_jobs(request: Request, payload: Dict[str, Any], background_tasks: BackgroundTasks) -> Dict[str, Any]:
        user = _require_app_user(request)
        expire_stale_jobs(max_age_seconds=job_stale_seconds)
        if not str(payload.get("project_id", "")).strip():
            raise HTTPException(status_code=400, detail="Select or create a project before running search.")
        try:
            ui_payload = build_ui_brief_payload(payload)
            quality = assess_ui_brief_quality(ui_payload["brief_config"])
            if not quality.get("ok"):
                raise ValueError(str(quality.get("message", "Hunt brief details are not enough.")))
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        get_project(user, str(payload.get("project_id", "")).strip())
        payload = dict(payload)
        payload["recruiter_id"] = user["id"]
        payload["recruiter_name"] = user["full_name"]
        payload["team_id"] = user.get("team_id", "")
        payload["recruiter_is_admin"] = bool(user.get("is_admin"))
        job = enqueue_job("search", payload)
        _spawn_background_job(job["job_id"], lambda job_id=job["job_id"], payload=payload: _search_job_runner(job_id, payload))
        return job

    @app.post("/app/train-ranker-jobs")
    async def app_train_ranker_jobs(request: Request, payload: Dict[str, Any], background_tasks: BackgroundTasks) -> Dict[str, Any]:
        _require_admin_user(request)
        job = enqueue_job("train_ranker", payload)
        _spawn_background_job(job["job_id"], lambda job_id=job["job_id"], payload=payload: _train_ranker_job_runner(job_id, payload))
        return job

    @app.get("/app/jobs/{job_id}")
    async def app_job_status(request: Request, job_id: str) -> Dict[str, Any]:
        _require_app_user(request)
        expire_stale_jobs(max_age_seconds=job_stale_seconds)
        job = load_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found.")
        return job

    @app.get("/app/projects/{project_id}/latest-job")
    async def app_project_latest_job(request: Request, project_id: str) -> Dict[str, Any]:
        user = _require_app_user(request)
        get_project(user, project_id)
        expire_stale_jobs(max_age_seconds=job_stale_seconds)
        return {"job": latest_project_job(project_id)}

    @app.post("/app/jobs/{job_id}/stop")
    async def app_stop_job(request: Request, job_id: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        _require_admin_user(request)
        expire_stale_jobs(max_age_seconds=job_stale_seconds)
        reason = str((payload or {}).get("reason", "")).strip() or "Stopped by admin. Retry when ready."
        job = stop_job(job_id, reason=reason)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found.")
        return job

    @app.post("/feedback")
    async def feedback(payload: Dict[str, Any]) -> Dict[str, Any]:
        brief = None
        brief_config = payload.get("brief")
        brief_path = payload.get("brief_path")
        if brief_path:
            brief_config = load_yaml_file(Path(brief_path))
        if isinstance(brief_config, dict):
            brief = build_search_brief(brief_config)

        try:
            return log_feedback(
                report_path=Path(str(payload.get("report_path", ""))).expanduser().resolve(),
                candidate_ref=str(payload.get("candidate_ref", "")),
                recruiter_id=str(payload.get("recruiter_id", "")),
                action=str(payload.get("action", "")),
                reason_code=str(payload.get("reason_code", "")),
                note=str(payload.get("note", "")),
                recruiter_name=str(payload.get("recruiter_name", "")),
                team_id=str(payload.get("team_id", "")),
                db_path=Path(str(payload["feedback_db"])).expanduser().resolve() if payload.get("feedback_db") else None,
                brief=brief,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/app/feedback")
    async def app_feedback(request: Request, payload: Dict[str, Any]) -> Dict[str, Any]:
        user = _require_app_user(request)
        project_id = str(payload.get("project_id", "")).strip()
        if project_id:
            get_project(user, project_id)
        brief_config = payload.get("brief")
        brief = build_search_brief(brief_config) if isinstance(brief_config, dict) else None
        try:
            result = log_feedback(
                report_path=Path(str(payload.get("report_path", ""))).expanduser().resolve(),
                candidate_ref=str(payload.get("candidate_ref", "")),
                recruiter_id=user["id"],
                action=str(payload.get("action", "")),
                reason_code=str(payload.get("reason_code", "")),
                note=str(payload.get("note", "")),
                recruiter_name=user["full_name"],
                team_id=user.get("team_id", ""),
                db_path=Path(str(payload.get("feedback_db", resolve_feedback_db_path()))).expanduser().resolve(),
                brief=brief,
                mandate_id=project_id,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        result["feedback_db"] = str(Path(str(payload.get("feedback_db", resolve_feedback_db_path()))).expanduser().resolve())
        return result

    @app.post("/feedback/export")
    async def feedback_export(payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            db_path = Path(str(payload["feedback_db"])).expanduser().resolve() if payload.get("feedback_db") else None
            output_path = export_training_rows(
                Path(str(payload.get("output_path", ""))).expanduser().resolve(),
                db_path=db_path,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "output_path": str(output_path),
            "row_count": len(load_ranker_training_rows(db_path)),
        }

    @app.post("/train-ranker")
    async def train_ranker(payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            db_path = Path(str(payload["feedback_db"])).expanduser().resolve() if payload.get("feedback_db") else None
            init_feedback_db(db_path)
            training_rows = load_ranker_training_rows(db_path)
            return train_learned_ranker(
                training_rows,
                model_dir=Path(str(payload["model_dir"])).expanduser().resolve() if payload.get("model_dir") else None,
                n_estimators=int(payload.get("n_estimators", 80)),
                learning_rate=float(payload.get("learning_rate", 0.1)),
                num_leaves=int(payload.get("num_leaves", 31)),
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/app/train-ranker")
    async def app_train_ranker(request: Request, payload: Dict[str, Any]) -> Dict[str, Any]:
        _require_admin_user(request)
        try:
            db_path = Path(str(payload.get("feedback_db", resolve_feedback_db_path()))).expanduser().resolve()
            model_dir = Path(str(payload.get("model_dir", resolve_ranker_model_dir()))).expanduser().resolve()
            init_feedback_db(db_path)
            training_rows = load_ranker_training_rows(db_path)
            return train_learned_ranker(
                training_rows,
                model_dir=model_dir,
                n_estimators=int(payload.get("n_estimators", 80)),
                learning_rate=float(payload.get("learning_rate", 0.1)),
                num_leaves=int(payload.get("num_leaves", 31)),
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/app/support-request")
    async def app_support_request(request: Request, payload: Dict[str, Any]) -> Dict[str, str]:
        user = _require_app_user(request)
        try:
            return _write_app_request("support_requests", {**payload, "actor": user})
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/app/feature-request")
    async def app_feature_request(request: Request, payload: Dict[str, Any]) -> Dict[str, str]:
        user = _require_app_user(request)
        try:
            return _write_app_request("feature_requests", {**payload, "actor": user})
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    return app
