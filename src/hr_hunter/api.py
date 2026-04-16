from __future__ import annotations

import asyncio
import json
import os
import threading
import time
import traceback
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Set

from hr_hunter.briefing import build_search_brief
from hr_hunter.config import (
    env_flag,
    load_env_file,
    load_yaml_file,
    resolve_feedback_db_path,
    resolve_output_dir,
    resolve_ranker_model_dir,
    resolve_state_db_path,
)
from hr_hunter.candidate_order import STATUS_RANK, candidate_priority_sort_tuple
from hr_hunter.db import describe_database_target, resolve_database_target
from hr_hunter.engine import SearchEngine, dedupe_candidates
from hr_hunter.feedback import export_training_rows, init_feedback_db, load_ranker_training_rows, log_feedback
from hr_hunter.identity import candidate_identity_keys
from hr_hunter.output import (
    build_scope_progress_counts,
    build_reporting_summary,
    collect_seen_candidate_keys,
    collect_seen_provider_queries,
    prepare_verification_candidate_order,
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
from hr_hunter.transformer_bridge import (
    run_transformer_search,
    transformer_available,
    transformer_runtime_status,
    warm_transformer_runtime_background,
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


def _normalize_transformer_role_hint(value: Any) -> str:
    return " ".join(str(value or "").lower().replace("/", " ").replace("-", " ").split())


def _infer_transformer_role_family(payload: Dict[str, Any]) -> str:
    values: List[str] = [
        str(payload.get("role_title", "")),
        *[str(value) for value in (payload.get("titles", []) or [])],
        *[str(value) for value in (payload.get("must_have_keywords", []) or [])],
        *[str(value) for value in (payload.get("nice_to_have_keywords", []) or [])],
        *[str(value) for value in (payload.get("industry_keywords", []) or [])],
    ]
    haystack = " ".join(_normalize_transformer_role_hint(value) for value in values if str(value).strip())
    family_hints = {
        "executive": ("chief executive officer", "ceo", "president", "managing director", "general manager", "vice president"),
        "operations_process": ("operations manager", "business operations manager", "process analyst", "service operations manager"),
        "supply_chain": ("supply chain manager", "demand planning", "supply planning", "logistics manager", "procurement manager"),
        "finance": ("accountant", "senior accountant", "finance manager", "financial controller", "accounts manager"),
        "sales_business_development": ("sales manager", "sales executive", "business development manager", "account manager", "partnerships manager"),
        "technical_ai": ("ai engineer", "machine learning engineer", "ml engineer", "llm engineer", "applied ai engineer", "generative ai engineer", "nlp engineer", "mlops engineer", "pytorch", "rag"),
        "data": ("data analyst", "data scientist", "analytics manager", "business intelligence analyst"),
        "marketing": ("digital marketing manager", "marketing manager", "growth manager", "performance marketing manager", "demand generation"),
        "customer_service_success": ("customer success manager", "customer support manager", "client success manager", "support lead"),
        "hr_talent": ("recruiter", "talent acquisition", "talent partner", "hr manager", "hr business partner"),
        "product_management": ("product manager", "senior product manager", "product owner", "platform product manager"),
        "project_program_management": ("project manager", "program manager", "scrum master", "pmo analyst", "delivery manager"),
        "procurement_sourcing": ("procurement manager", "strategic sourcing manager", "buyer", "category manager", "sourcing specialist"),
        "manufacturing_production": ("production manager", "plant manager", "production supervisor", "manufacturing engineer"),
        "engineering_non_it": ("mechanical engineer", "electrical engineer", "civil engineer", "industrial engineer"),
        "construction_facilities": ("site engineer", "facilities manager", "maintenance manager", "construction manager"),
        "healthcare_medical": ("doctor", "physician", "nurse", "pharmacist", "clinic manager"),
        "education_training": ("teacher", "lecturer", "trainer", "instructional designer", "academic coordinator"),
        "legal_compliance": ("legal counsel", "lawyer", "compliance officer", "contracts manager"),
        "risk_audit_security": ("internal auditor", "risk manager", "cybersecurity analyst", "information security manager"),
        "research_development": ("research scientist", "r&d engineer", "innovation manager", "applied researcher"),
        "design_creative": ("graphic designer", "ux designer", "ui designer", "motion designer", "video editor"),
        "design_architecture": ("interior designer", "senior interior designer", "interior design manager", "architect", "project architect", "design manager", "design director", "fit out"),
        "media_communications": ("communications manager", "pr manager", "journalist", "corporate communications lead"),
        "admin_office_support": ("admin assistant", "executive assistant", "office manager", "office coordinator"),
        "hospitality_tourism": ("hotel manager", "guest relations manager", "front office manager", "travel consultant", "restaurant manager"),
        "retail_merchandising": ("store manager", "merchandiser", "retail operations manager", "category executive"),
        "real_estate_property": ("property manager", "leasing manager", "real estate consultant", "property consultant"),
        "public_sector_government": ("policy analyst", "civil servant", "public administration officer", "municipal operations manager"),
        "agriculture_environment": ("sustainability manager", "esg manager", "environmental manager", "agronomist", "hse manager"),
        "transportation_mobility": ("fleet manager", "dispatcher", "aviation operations manager", "transport manager"),
    }
    for family, hints in family_hints.items():
        if any(_normalize_transformer_role_hint(hint) in haystack for hint in hints):
            return family
    return "other"


def _resolve_transformer_tuning(payload: Dict[str, Any], requested_limit: int) -> Dict[str, int | str]:
    family = _infer_transformer_role_family(payload)
    scale = max(0.75, min(2.0, max(1, int(requested_limit or 1)) / 300))
    profile_map: Dict[str, Dict[str, int]] = {
        "supply_chain": {"max_queries": 54, "pages_per_query": 1, "parallel_requests": 10},
        "finance": {"max_queries": 54, "pages_per_query": 1, "parallel_requests": 10},
        "operations_process": {"max_queries": 72, "pages_per_query": 1, "parallel_requests": 10},
        "sales_business_development": {"max_queries": 72, "pages_per_query": 1, "parallel_requests": 10},
        "customer_service_success": {"max_queries": 64, "pages_per_query": 1, "parallel_requests": 10},
        "hr_talent": {"max_queries": 70, "pages_per_query": 1, "parallel_requests": 10},
        "data": {"max_queries": 60, "pages_per_query": 1, "parallel_requests": 10},
        "marketing": {"max_queries": 60, "pages_per_query": 1, "parallel_requests": 10},
        "product_management": {"max_queries": 80, "pages_per_query": 1, "parallel_requests": 10},
        "project_program_management": {"max_queries": 80, "pages_per_query": 1, "parallel_requests": 10},
        "procurement_sourcing": {"max_queries": 72, "pages_per_query": 1, "parallel_requests": 10},
        "manufacturing_production": {"max_queries": 90, "pages_per_query": 1, "parallel_requests": 10},
        "engineering_non_it": {"max_queries": 100, "pages_per_query": 2, "parallel_requests": 8},
        "construction_facilities": {"max_queries": 100, "pages_per_query": 2, "parallel_requests": 8},
        "healthcare_medical": {"max_queries": 100, "pages_per_query": 2, "parallel_requests": 8},
        "education_training": {"max_queries": 70, "pages_per_query": 1, "parallel_requests": 10},
        "legal_compliance": {"max_queries": 84, "pages_per_query": 1, "parallel_requests": 8},
        "risk_audit_security": {"max_queries": 84, "pages_per_query": 1, "parallel_requests": 8},
        "research_development": {"max_queries": 100, "pages_per_query": 2, "parallel_requests": 8},
        "design_creative": {"max_queries": 84, "pages_per_query": 2, "parallel_requests": 8},
        "design_architecture": {"max_queries": 100, "pages_per_query": 2, "parallel_requests": 8},
        "media_communications": {"max_queries": 72, "pages_per_query": 1, "parallel_requests": 10},
        "admin_office_support": {"max_queries": 60, "pages_per_query": 1, "parallel_requests": 10},
        "hospitality_tourism": {"max_queries": 80, "pages_per_query": 1, "parallel_requests": 10},
        "retail_merchandising": {"max_queries": 80, "pages_per_query": 1, "parallel_requests": 10},
        "real_estate_property": {"max_queries": 90, "pages_per_query": 1, "parallel_requests": 10},
        "public_sector_government": {"max_queries": 84, "pages_per_query": 1, "parallel_requests": 8},
        "agriculture_environment": {"max_queries": 100, "pages_per_query": 2, "parallel_requests": 8},
        "transportation_mobility": {"max_queries": 84, "pages_per_query": 1, "parallel_requests": 10},
        "technical_ai": {"max_queries": 140, "pages_per_query": 2, "parallel_requests": 8},
        "executive": {"max_queries": 180, "pages_per_query": 2, "parallel_requests": 6},
        "other": {"max_queries": 120, "pages_per_query": 2, "parallel_requests": 8},
    }
    base = profile_map.get(family, profile_map["other"])
    return {
        "role_family": family,
        "max_queries": max(24, int(round(base["max_queries"] * scale))),
        "pages_per_query": int(base["pages_per_query"]),
        "parallel_requests": int(base["parallel_requests"]),
    }


def _completed_telemetry_elapsed_seconds(summary: Dict[str, Any]) -> int:
    telemetry_events = summary.get("telemetry_events", [])
    if not isinstance(telemetry_events, list):
        return 0
    elapsed_values: List[int] = []
    for event in telemetry_events:
        if not isinstance(event, dict):
            continue
        try:
            elapsed_values.append(max(0, int(event.get("elapsed_seconds", 0) or 0)))
        except (TypeError, ValueError):
            continue
    return max(elapsed_values, default=0)


def _attach_report_runtime_metadata(
    report: SearchRunReport,
    *,
    elapsed_seconds: int,
    runtime_target_seconds: int,
    execution_backend: str,
) -> SearchRunReport:
    """Persist the same wall-clock runtime that the backend job progress uses."""

    elapsed_seconds = max(0, int(elapsed_seconds or 0))
    runtime_target_seconds = max(0, int(runtime_target_seconds or 0))
    summary = dict(report.summary or {})
    pipeline_elapsed_seconds = int(
        summary.get("pipeline_elapsed_seconds")
        or _completed_telemetry_elapsed_seconds(summary)
        or 0
    )

    summary.update(
        {
            "execution_backend": execution_backend,
            "runtime_seconds": elapsed_seconds,
            "wall_clock_seconds": elapsed_seconds,
            "job_elapsed_seconds": elapsed_seconds,
            "pipeline_elapsed_seconds": pipeline_elapsed_seconds,
            "target_runtime_seconds": runtime_target_seconds,
            "runtime_display_source": "job_wall_clock",
        }
    )
    pipeline_metrics = dict(summary.get("pipeline_metrics", {}) or {})
    pipeline_metrics.update(
        {
            "runtime_seconds": elapsed_seconds,
            "wall_clock_seconds": elapsed_seconds,
            "job_elapsed_seconds": elapsed_seconds,
            "pipeline_elapsed_seconds": pipeline_elapsed_seconds,
            "target_runtime_seconds": runtime_target_seconds,
        }
    )
    summary["pipeline_metrics"] = pipeline_metrics

    telemetry_events = []
    for event in summary.get("telemetry_events", []) or []:
        if not isinstance(event, dict):
            continue
        event_copy = dict(event)
        event_elapsed = max(0, int(event_copy.get("elapsed_seconds", 0) or 0))
        event_copy.setdefault("pipeline_elapsed_seconds", event_elapsed)
        if str(event_copy.get("stage", "")).strip().lower() == "completed":
            event_copy["elapsed_seconds"] = elapsed_seconds
            event_copy["job_elapsed_seconds"] = elapsed_seconds
            event_copy["pipeline_elapsed_seconds"] = event_elapsed
        telemetry_events.append(event_copy)
    if telemetry_events:
        summary["telemetry_events"] = telemetry_events

    report.summary = summary
    return report


def _resolve_effective_verification_target(
    candidates: List[Any],
    *,
    requested_limit: int,
    verification_target: int,
    scope_target: int = 0,
    company_required: bool = False,
) -> Dict[str, int]:
    shortlist_limit = max(0, min(int(verification_target or 0), len(candidates)))
    if shortlist_limit <= 0:
        return {
            "requested_target": 0,
            "effective_target": 0,
            "shortlist_scope_count": 0,
            "shortlist_precise_scope_count": 0,
        }

    requested = max(1, int(requested_limit or 1))
    verification_floor = min(
        shortlist_limit,
        max(16, min(48, int(round(requested * 0.2)))),
    )
    effective_target = shortlist_limit if shortlist_limit > 0 else verification_floor
    return {
        "requested_target": shortlist_limit,
        "effective_target": min(shortlist_limit, max(verification_floor, effective_target)),
        "shortlist_scope_count": 0,
        "shortlist_precise_scope_count": 0,
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
    ordered_candidates = sorted(
        list(report.candidates),
        key=lambda candidate: (
            STATUS_RANK.get(str(getattr(candidate, "verification_status", "") or "").lower(), 9),
            *candidate_priority_sort_tuple(candidate, brief, phase="final"),
        ),
    )
    report.candidates = ordered_candidates[:requested]
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
        if stage in {"retrieval", "retrieval_running"} and queries_total > 0:
            resolved_percent = max(5, min(70, int(round((queries_completed / max(1, queries_total)) * 65 + 5))))
        elif stage in {"dedupe", "extraction_running"}:
            resolved_percent = 72
        elif stage in {"rerank", "entity_resolution"}:
            resolved_percent = 84
        elif stage in {"scoring"}:
            resolved_percent = 88
        elif stage in {"verifying", "verification"}:
            resolved_percent = 92
        elif stage in {"brief_normalized"}:
            resolved_percent = 2
        elif stage in {"role_understood"}:
            resolved_percent = 6
        elif stage in {"queries_planned"}:
            resolved_percent = 10
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
        exclude_provider_queries = collect_seen_provider_queries(exclusion_sources)
        if extra_exclude_candidate_keys:
            exclude_candidate_keys.update(extra_exclude_candidate_keys)
        if extra_exclude_provider_queries:
            for provider_name, queries in extra_exclude_provider_queries.items():
                if not queries:
                    continue
                exclude_provider_queries.setdefault(str(provider_name), set()).update(set(queries))
        return await engine.run(
            brief,
            list(providers),
            limit=limit,
            dry_run=dry_run,
            exclude_candidate_keys=exclude_candidate_keys,
            exclude_provider_queries=exclude_provider_queries,
            progress_callback=progress_callback,
        )

    def _report_candidate_keys(report: Any) -> Set[str]:
        keys: Set[str] = set()
        for candidate in list(getattr(report, "candidates", []) or []):
            keys.update(candidate_identity_keys(candidate))
        for provider_result in list(getattr(report, "provider_results", []) or []):
            for candidate in list(getattr(provider_result, "candidates", []) or []):
                keys.update(candidate_identity_keys(candidate))
        return keys

    def _report_provider_queries(report: Any) -> Dict[str, Set[str]]:
        seen: Dict[str, Set[str]] = {}
        for provider_result in list(getattr(report, "provider_results", []) or []):
            provider_name = str(getattr(provider_result, "provider_name", "") or "").strip()
            if not provider_name:
                continue
            provider_seen = seen.setdefault(provider_name, set())
            diagnostics = dict(getattr(provider_result, "diagnostics", {}) or {})
            diagnostics_queries = diagnostics.get("queries", [])
            if not isinstance(diagnostics_queries, list):
                continue
            for item in diagnostics_queries:
                if not isinstance(item, dict):
                    continue
                search_query = str(item.get("search", "")).strip()
                if search_query:
                    provider_seen.add(search_query)
                    continue
                fingerprint = str(item.get("fingerprint", "")).strip()
                if fingerprint:
                    provider_seen.add(fingerprint)
        return seen

    def _report_verification_counts(report: Any) -> Dict[str, int]:
        candidates = list(getattr(report, "candidates", []) or [])
        verified_count = len(
            [candidate for candidate in candidates if getattr(candidate, "verification_status", "") == "verified"]
        )
        review_count = len(
            [candidate for candidate in candidates if getattr(candidate, "verification_status", "") == "review"]
        )
        reject_count = len(
            [candidate for candidate in candidates if getattr(candidate, "verification_status", "") == "reject"]
        )
        return {
            "verified": verified_count,
            "review": review_count,
            "reject": reject_count,
            "accepted": verified_count + review_count,
        }

    def _merge_verification_stats(base: Dict[str, Any] | None, extra: Dict[str, Any] | None) -> Dict[str, Any]:
        merged = dict(base or {})
        if not extra:
            return merged
        for key in (
            "candidates_checked",
            "requests_used",
            "promoted_to_verified",
            "promoted_to_review",
            "verified_count",
            "review_count",
            "reject_count",
        ):
            merged[key] = int(merged.get(key, 0) or 0) + int(extra.get(key, 0) or 0)
        merged["verifying_count"] = int(extra.get("verifying_count", 0) or 0)
        return merged

    async def _recover_report_after_verification(
        report: Any,
        *,
        brief: Any,
        verifier: Any,
        payload: Dict[str, Any],
        ui_payload: Dict[str, Any],
        requested_limit: int,
        internal_fetch_limit: int,
        exclude_report_paths: List[Path],
        exclude_history_dirs: List[Path],
        verification_stats: Dict[str, Any] | None,
        progress_callback: Any = None,
    ) -> tuple[Any, Dict[str, Any] | None, int]:
        if verifier is None or not verifier.is_configured():
            return report, verification_stats, internal_fetch_limit

        requested_limit_value = max(1, int(requested_limit or 1))
        default_recovery_rounds = 6 if requested_limit_value >= 180 else 3
        max_rounds = max(0, int(payload.get("post_verification_recovery_rounds", default_recovery_rounds) or default_recovery_rounds))
        reject_threshold = max(0, int(payload.get("rejection_refill_threshold", 30) or 30))
        default_stall_round_limit = 2
        default_no_gain_round_limit = 2
        stall_round_limit = max(1, int(payload.get("post_verification_recovery_stall_rounds", default_stall_round_limit) or default_stall_round_limit))
        no_accepted_gain_limit = max(1, int(payload.get("post_verification_recovery_no_gain_rounds", default_no_gain_round_limit) or default_no_gain_round_limit))
        current_fetch_limit = max(requested_limit, int(internal_fetch_limit or requested_limit))
        aggregated_stats = dict(verification_stats or {})
        recovery_rounds = 0
        recovery_notes: List[str] = []
        stalled_candidate_rounds = 0
        stalled_accepted_rounds = 0

        while recovery_rounds < max_rounds:
            counts = _report_verification_counts(report)
            current_total = len(report.candidates)
            accepted_count = counts["accepted"]
            excessive_rejects = max(0, counts["reject"] - reject_threshold)
            accepted_gap = max(0, requested_limit_value - accepted_count)
            recovery_gap = max(accepted_gap, excessive_rejects)
            if recovery_gap <= 0:
                break

            recovery_rounds += 1
            prior_keys = _report_candidate_keys(report)
            prior_provider_queries = _report_provider_queries(report)
            next_fetch_limit = max(
                current_fetch_limit,
                compute_top_up_fetch_limit(max(requested_limit_value, accepted_count + recovery_gap), current_fetch_limit),
                current_total + recovery_gap + 20,
                requested_limit_value + recovery_gap + 20,
            )
            payload_override = dict(payload)
            payload_override["internal_fetch_limit_override"] = next_fetch_limit
            payload_override["registry_memory_enabled"] = False
            payload_override["top_up_round"] = int(payload.get("top_up_round", 0) or 0) + recovery_rounds
            recovery_ui_payload = build_ui_brief_payload(payload_override)
            recovery_brief = build_search_brief(recovery_ui_payload["brief_config"])

            if progress_callback:
                progress_callback(
                    {
                        "stage": "retrieval",
                        "stage_label": "Retrieval",
                        "message": (
                            f"Recovery round {recovery_rounds} started to replace rejected candidates "
                            f"and close the remaining accepted gap of {accepted_gap}."
                        ),
                        "round": recovery_rounds,
                        "target": requested_limit,
                        "finalized_count": current_total,
                        "verified_count": counts["verified"],
                        "review_count": counts["review"],
                        "reject_count": counts["reject"],
                        "percent": 93,
                    }
                )

            supplemental_report = await _run_local_search(
                recovery_brief,
                providers=list(recovery_ui_payload["providers"]),
                limit=next_fetch_limit,
                dry_run=bool(payload.get("dry_run", False)),
                exclude_report_paths=exclude_report_paths,
                exclude_history_dirs=exclude_history_dirs,
                extra_exclude_candidate_keys=prior_keys,
                extra_exclude_provider_queries=prior_provider_queries,
                progress_callback=progress_callback,
            )
            report = _merge_ranked_report_candidates(report, supplemental_report, brief=recovery_brief)
            new_candidates = [
                candidate
                for candidate in list(report.candidates or [])
                if candidate_identity_keys(candidate).isdisjoint(prior_keys)
            ]
            if not new_candidates:
                stalled_candidate_rounds += 1
                recovery_notes.append(
                    f"Recovery round {recovery_rounds} found no net-new candidates after dedupe."
                )
                current_fetch_limit = next_fetch_limit
                if stalled_candidate_rounds >= stall_round_limit:
                    recovery_notes.append(
                        "Stopped reject-replacement recovery after consecutive rounds with no net-new candidates."
                    )
                    break
                continue
            stalled_candidate_rounds = 0

            additional_verification_target = min(
                len(new_candidates),
                max(24, min(240, recovery_gap + 30)),
            )
            prioritized_new_candidates = prepare_verification_candidate_order(
                new_candidates,
                brief=recovery_brief,
                company_required=bool(recovery_brief.company_targets),
                verification_limit=additional_verification_target,
                scope_target=0,
            )
            round_stats = await verifier.verify_candidates(
                prioritized_new_candidates,
                recovery_brief,
                limit=additional_verification_target,
                progress_callback=None,
            )
            aggregated_stats = _merge_verification_stats(aggregated_stats, round_stats)
            refresh_report_summary(report, aggregated_stats, brief=brief)
            current_fetch_limit = next_fetch_limit

            updated_counts = _report_verification_counts(report)
            accepted_gain = max(0, updated_counts["accepted"] - accepted_count)
            recovery_notes.append(
                (
                    f"Recovery round {recovery_rounds} added {len(new_candidates)} net-new candidates; "
                    f"current mix is {updated_counts['verified']} verified, {updated_counts['review']} review, "
                    f"{updated_counts['reject']} reject with {accepted_gain} net-new accepted."
                )
            )
            if accepted_gain <= 0:
                stalled_accepted_rounds += 1
                if stalled_accepted_rounds >= no_accepted_gain_limit:
                    recovery_notes.append(
                        "Stopped reject-replacement recovery after consecutive rounds produced no new verified or review candidates."
                    )
                    break
            else:
                stalled_accepted_rounds = 0

        if recovery_rounds:
            report.summary = dict(report.summary or {})
            report.summary["post_verification_recovery_rounds"] = recovery_rounds
            report.summary["post_verification_recovery_reject_threshold"] = reject_threshold
            report.summary["post_verification_recovery_target_mode"] = "accepted_candidates"
            if recovery_notes:
                report.summary["post_verification_recovery_notes"] = recovery_notes[-4:]
        return report, aggregated_stats, current_fetch_limit

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
        scope_counts = build_scope_progress_counts(report.candidates)
        top_up_rounds = 0
        top_up_notes: List[str] = []
        default_top_up_rounds = 8
        max_rounds = max(1, int(payload.get("top_up_max_rounds", default_top_up_rounds) or default_top_up_rounds))
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
            prior_keys = _report_candidate_keys(report)
            prior_provider_queries = _report_provider_queries(report)

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
                        "precise_in_scope_count": int(scope_counts.get("precise_in_scope_count", 0) or 0),
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
                local_prior_keys = _report_candidate_keys(report)
                local_prior_provider_queries = _report_provider_queries(report)
                supplemental_report = await _run_local_search(
                    top_up_brief,
                    providers=list(top_up_ui_payload["providers"]),
                    limit=next_fetch_limit,
                    dry_run=bool(payload.get("dry_run", False)),
                    exclude_report_paths=exclude_report_paths,
                    exclude_history_dirs=exclude_history_dirs,
                    extra_exclude_candidate_keys=local_prior_keys,
                    extra_exclude_provider_queries=local_prior_provider_queries,
                    progress_callback=progress_callback,
                )
                before_merge = len(report.candidates)
                report = _merge_ranked_report_candidates(report, supplemental_report, brief=top_up_brief)
                round_growth += max(0, len(report.candidates) - before_merge)

            current_fetch_limit = next_fetch_limit
            updated_unique_count = len(report.candidates)
            scope_counts = build_scope_progress_counts(report.candidates)
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
            requested_backend = str(payload.get("search_backend", "transformer") or "transformer").strip().lower()
            transformer_tuning = _resolve_transformer_tuning(payload, requested_limit)
            transformer_max_queries = int(transformer_tuning["max_queries"])
            transformer_pages_per_query = int(transformer_tuning["pages_per_query"])
            transformer_parallel_requests = int(transformer_tuning["parallel_requests"])
            payload["transformer_max_queries"] = transformer_max_queries
            payload["transformer_pages_per_query"] = transformer_pages_per_query
            payload["transformer_parallel_requests"] = transformer_parallel_requests
            payload["transformer_role_family"] = str(transformer_tuning["role_family"])
            candidate_runtime_target_seconds = max(60, int(round(max(1, requested_limit) * 3)))
            if requested_backend == "transformer":
                query_runtime_target_seconds = int(
                    round(
                        ((transformer_max_queries * transformer_pages_per_query) / max(1, transformer_parallel_requests)) * 7
                    )
                )
                runtime_target_seconds = max(candidate_runtime_target_seconds, query_runtime_target_seconds)
            else:
                runtime_target_seconds = candidate_runtime_target_seconds
            latest_telemetry: Dict[str, Any] = {
                "stage": "running",
                "stage_label": "Running",
                "queries_completed": 0,
                "queries_total": 0,
                "queries_in_flight": 0,
                "raw_found": 0,
                "unique_after_dedupe": 0,
                "in_scope_count": 0,
                "precise_in_scope_count": 0,
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
                        "in_scope_count": int(latest_telemetry.get("in_scope_count", 0) or 0),
                        "precise_in_scope_count": int(latest_telemetry.get("precise_in_scope_count", 0) or 0),
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
                round_reset = round_number > previous_round and stage in {"retrieval", "retrieval_running"}
                stage_label_map = {
                    "brief_normalized": "Brief Ready",
                    "role_understood": "Role Understood",
                    "queries_planned": "Queries Planned",
                    "retrieval": "Retrieval",
                    "retrieval_running": "Retrieval",
                    "dedupe": "Dedupe",
                    "extraction_running": "Extraction",
                    "entity_resolution": "Entity Resolution",
                    "rerank": "Rerank",
                    "scoring": "Scoring",
                    "verifying": "Verifying",
                    "verification": "Verification",
                    "finalizing": "Finalizing",
                    "completed": "Completed",
                    "running": "Running",
                }
                previous_queries_total = 0 if round_reset else int(latest_telemetry.get("queries_total", 0) or 0)
                previous_queries_completed = 0 if round_reset else int(latest_telemetry.get("queries_completed", 0) or 0)
                previous_raw_found = 0 if round_reset else int(latest_telemetry.get("raw_found", 0) or 0)
                previous_unique = int(latest_telemetry.get("unique_after_dedupe", 0) or 0)
                previous_in_scope = int(latest_telemetry.get("in_scope_count", 0) or 0)
                previous_precise_in_scope = int(latest_telemetry.get("precise_in_scope_count", 0) or 0)
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
                elif stage in {"dedupe", "extraction_running", "entity_resolution", "rerank", "scoring", "verifying", "verification", "finalizing", "completed", "failed"}:
                    queries_in_flight = 0
                else:
                    queries_in_flight = int(latest_telemetry.get("queries_in_flight", 0) or 0)
                unique_after_dedupe = max(
                    previous_unique,
                    int(event.get("unique_after_dedupe", previous_unique) or 0),
                )
                in_scope_count = max(
                    0,
                    int(event.get("in_scope_count", previous_in_scope) or 0),
                )
                precise_in_scope_count = max(
                    0,
                    int(event.get("precise_in_scope_count", previous_precise_in_scope) or 0),
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
                if stage in {"rerank", "scoring", "verifying", "verification", "finalizing"}:
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
                        "in_scope_count": in_scope_count,
                        "precise_in_scope_count": precise_in_scope_count,
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
                    "stage": "query_planning",
                    "stage_label": "Planning",
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
            verification_stats = None
            if requested_backend == "transformer":
                if not transformer_available():
                    raise RuntimeError("Transformer mode is not available in the local workspace.")
                execution_backend = "transformer_v2"
                remote_error_message = ""
                _push_progress(
                    {
                        "stage": "query_planning",
                        "stage_label": "Transformer Planning",
                        "message": "Preparing Transformer V2 query plan and retrieval workers.",
                        "percent": 8,
                    },
                    checkpoint_patch={"event": "transformer_request"},
                )
                report = await run_transformer_search(
                    brief=brief,
                    requested_limit=requested_limit,
                    reranker_enabled=bool(payload.get("reranker_enabled", True)),
                    max_queries=transformer_max_queries,
                    pages_per_query=transformer_pages_per_query,
                    parallel_requests=transformer_parallel_requests,
                    payload=payload,
                    job_id=job_id,
                    project_id=project_id,
                    progress_callback=_on_pipeline_progress,
                )
            else:
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
                scope_progress_counts = build_scope_progress_counts(report.candidates)
                if verification_enabled and verification_target > 0:
                    report.candidates = prepare_verification_candidate_order(
                        report.candidates,
                        brief=brief,
                        company_required=bool(brief.company_targets),
                        verification_limit=verification_target,
                        scope_target=0,
                    )
                    verification_target_plan = _resolve_effective_verification_target(
                        report.candidates,
                        requested_limit=requested_limit,
                        verification_target=verification_target,
                        scope_target=0,
                        company_required=bool(brief.company_targets),
                    )
                    effective_verification_target = int(verification_target_plan["effective_target"] or 0)
                    report.summary = dict(report.summary or {})
                    report.summary["verification_requested_target"] = int(
                        verification_target_plan["requested_target"] or 0
                    )
                    report.summary["verification_effective_target"] = effective_verification_target
                    report.summary["verification_shortlist_scope_count"] = int(
                        verification_target_plan["shortlist_scope_count"] or 0
                    )
                    report.summary["verification_shortlist_precise_scope_count"] = int(
                        verification_target_plan["shortlist_precise_scope_count"] or 0
                    )
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
                                    "in_scope_count": int(scope_progress_counts.get("in_scope_count", 0) or 0),
                                    "precise_in_scope_count": int(scope_progress_counts.get("precise_in_scope_count", 0) or 0),
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
                                "in_scope_count": int(scope_progress_counts.get("in_scope_count", 0) or 0),
                                "precise_in_scope_count": int(scope_progress_counts.get("precise_in_scope_count", 0) or 0),
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
                        scope_progress_counts = build_scope_progress_counts(report.candidates)
                        report, verification_stats, internal_fetch_limit = await _recover_report_after_verification(
                            report,
                            brief=brief,
                            verifier=verifier,
                            payload=payload,
                            ui_payload=ui_payload,
                            requested_limit=requested_limit,
                            internal_fetch_limit=internal_fetch_limit,
                            exclude_report_paths=exclude_report_paths,
                            exclude_history_dirs=exclude_history_dirs,
                            verification_stats=verification_stats,
                            progress_callback=_on_pipeline_progress,
                        )
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
                    "in_scope_count": int(report.summary.get("in_scope_count", latest_telemetry.get("in_scope_count", 0)) or 0),
                    "precise_in_scope_count": int(report.summary.get("precise_in_scope_count", latest_telemetry.get("precise_in_scope_count", 0)) or 0),
                    "message": "Persisting final results and artifacts.",
                },
                checkpoint_patch={"event": "finalizing"},
                force=True,
            )
            report = _attach_report_runtime_metadata(
                report,
                elapsed_seconds=int(time.monotonic() - job_started_monotonic),
                runtime_target_seconds=runtime_target_seconds,
                execution_backend=execution_backend,
            )
            output_dir = Path(ui_payload["output_dir"])
            json_path, csv_path = write_report(
                report,
                output_dir,
                csv_candidate_limit=int(requested_limit),
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
            traceback.print_exc()
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
        if env_flag("HR_HUNTER_WARM_TRANSFORMER_ON_STARTUP", default=False):
            warm_transformer_runtime_background(use_transformer=True)

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
        bootstrap["defaults"]["search_backend"] = "transformer"
        bootstrap["defaults"]["limit"] = 1000
        bootstrap["transformer"] = {
            "available": transformer_available(),
            "label": "HR Hunter Transformer V2",
            "runtime": transformer_runtime_status(),
        }
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
            csv_candidate_limit=int(requested_limit),
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
        served_path = artifact_path
        if artifact_path.suffix.lower() == ".csv":
            client_csv_path = artifact_path.with_name(f"{artifact_path.stem}-client.csv")
            if not client_csv_path.exists():
                json_path = artifact_path.with_suffix(".json")
                if json_path.exists():
                    try:
                        from hr_hunter.output import load_report, write_candidates_csv

                        report = load_report(json_path)
                        write_candidates_csv(report.candidates, client_csv_path)
                    except Exception:
                        client_csv_path = artifact_path
            if client_csv_path.exists():
                served_path = client_csv_path
        media_type = None
        if served_path.suffix.lower() == ".csv":
            media_type = "text/csv"
        elif served_path.suffix.lower() == ".json":
            media_type = "application/json"
        return FileResponse(served_path, filename=artifact_path.name, media_type=media_type)

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
        summary["transformer_runtime"] = transformer_runtime_status()
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
