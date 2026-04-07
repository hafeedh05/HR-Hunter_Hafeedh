from __future__ import annotations

import base64
import os
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, List

import httpx

from hr_hunter.models import CandidateProfile, ProviderRunResult, SearchRunReport


DEFAULT_REMOTE_BASE_URL = base64.b64decode(
    b"aHR0cHM6Ly9vcGVuY2xhdy5oeXZlbGFicy50ZWNoL2FwaS9oci1odW50ZXI="
).decode("utf-8")


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_int(value: Any, default: int | None = None) -> int | None:
    if value in (None, ""):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _clean_list(value: Any) -> List[str]:
    if isinstance(value, list):
        raw_values = value
    else:
        raw_values = str(value or "").split(",")
    ordered: List[str] = []
    seen = set()
    for raw in raw_values:
        text = str(raw or "").strip()
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        ordered.append(text)
    return ordered


def _normalize_company_targets(payload: Dict[str, Any]) -> List[Dict[str, str]]:
    match_mode = str(payload.get("company_match_mode", "both") or "both").strip().lower()
    employment_mode = {
        "current_only": "current",
        "past_only": "past",
        "both": "both",
    }.get(match_mode, "both")
    return [
        {"name": company, "employmentMode": employment_mode}
        for company in _clean_list(payload.get("company_targets"))
    ]


def _normalize_location_scopes(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    scopes: List[Dict[str, Any]] = []
    radius = _parse_float(payload.get("radius_miles"), 0.0)
    for country in _clean_list(payload.get("countries")):
        scopes.append(
            {
                "type": "country",
                "value": country,
                "country": country,
                "city": "",
                "radiusMiles": 0,
                "locationHints": [],
            }
        )
    for continent in _clean_list(payload.get("continents")):
        scopes.append(
            {
                "type": "continent",
                "value": continent,
                "country": "",
                "city": "",
                "radiusMiles": 0,
                "locationHints": [],
            }
        )
    for city in _clean_list(payload.get("cities")):
        scopes.append(
            {
                "type": "city_radius",
                "value": city,
                "country": "",
                "city": city,
                "radiusMiles": radius,
                "locationHints": [],
            }
        )
    return scopes


class RemoteSourcingError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        recoverable: bool = False,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.recoverable = recoverable
        self.status_code = status_code


def build_remote_criteria(payload: Dict[str, Any], ui_payload: Dict[str, Any]) -> Dict[str, Any]:
    brief_config = dict(ui_payload.get("brief_config", {}))
    breakdown = dict(ui_payload.get("job_description_breakdown", {}))
    return {
        "roleTitle": brief_config.get("role_title", ""),
        "jobTitles": list(brief_config.get("titles", [])),
        "titles": list(brief_config.get("titles", [])),
        "locationScopes": _normalize_location_scopes(payload),
        "companyTargets": _normalize_company_targets(payload),
        "employmentStatus": str(
            brief_config.get("employment_status_mode", payload.get("employment_status_mode", "any"))
        ),
        "industries": list(brief_config.get("industry_keywords", [])),
        "mustHaveKeywords": list(brief_config.get("required_keywords", [])),
        "niceToHaveKeywords": list(brief_config.get("preferred_keywords", [])),
        "excludeTitles": list(brief_config.get("exclude_title_keywords", [])),
        "excludeCompanies": list(brief_config.get("exclude_company_keywords", [])),
        "experience": {
            "targetYears": brief_config.get("years_target"),
            "toleranceYears": brief_config.get("years_tolerance", 0),
        },
        "jobDescription": {
            "text": brief_config.get("document_text", ""),
            "extractedPoints": list(breakdown.get("key_experience_points", [])),
        },
        "anchors": dict(brief_config.get("anchors", payload.get("anchors", {})) or payload.get("anchors", {})),
        "languages": _clean_list(payload.get("languages")),
        "seniority": ", ".join(brief_config.get("seniority_levels", [])),
        "roleLocation": ", ".join(_clean_list(payload.get("cities")) or _clean_list(payload.get("countries"))[:1]),
        "booleanKeywords": " ".join(list(brief_config.get("required_keywords", []))[:8]),
    }


def normalize_remote_parse_response(payload: Dict[str, Any]) -> Dict[str, Any]:
    extracted_points = list(payload.get("extractedPoints", payload.get("key_experience_points", [])) or [])
    suggested_anchors = payload.get("suggestedAnchors", payload.get("anchors", {})) or {}
    experience = payload.get("experience", {}) or {}
    return {
        "summary": payload.get("summary", ""),
        "key_experience_points": extracted_points,
        "required_keywords": list(payload.get("requiredKeywords", payload.get("required_keywords", [])) or []),
        "preferred_keywords": list(payload.get("preferredKeywords", payload.get("preferred_keywords", [])) or []),
        "industry_keywords": list(payload.get("industries", payload.get("industry_keywords", [])) or []),
        "titles": list(payload.get("jobTitles", payload.get("titles", [])) or []),
        "seniority_levels": _clean_list(payload.get("seniorityLevels", payload.get("seniority_levels", []))),
        "years": {
            "mode": payload.get("yearsMode", experience.get("mode", "range")) or "range",
            "value": experience.get("targetYears"),
            "min": experience.get("minYears"),
            "max": experience.get("maxYears"),
            "tolerance": experience.get("toleranceYears", 0) or 0,
        },
        "suggested_anchors": suggested_anchors if isinstance(suggested_anchors, dict) else {},
    }


def candidate_from_remote(payload: Dict[str, Any]) -> CandidateProfile:
    return CandidateProfile(
        full_name=str(payload.get("full_name", payload.get("name", ""))),
        current_title=str(payload.get("current_title", payload.get("title", ""))),
        current_company=str(payload.get("current_company", payload.get("company", ""))),
        location_name=str(payload.get("location_name", payload.get("location", ""))),
        linkedin_url=payload.get("linkedin_url", payload.get("linkedinUrl")),
        source=str(payload.get("source", "remote_api")),
        source_url=payload.get("source_url", payload.get("sourceUrl")),
        summary=str(payload.get("summary", "")),
        years_experience=payload.get("years_experience"),
        industry=payload.get("industry"),
        experience=list(payload.get("experience", [])),
        matched_titles=list(payload.get("matched_titles", payload.get("matchedTitles", [])) or []),
        matched_companies=list(payload.get("matched_companies", payload.get("matchedCompanies", [])) or []),
        current_target_company_match=bool(payload.get("current_target_company_match", False)),
        target_company_history_match=bool(payload.get("target_company_history_match", False)),
        current_title_match=bool(payload.get("current_title_match", False)),
        industry_aligned=bool(payload.get("industry_aligned", False)),
        location_aligned=bool(payload.get("location_aligned", False)),
        verification_status=str(payload.get("verification_status", "review")),
        qualification_tier=str(payload.get("qualification_tier", "weak")),
        cap_reasons=list(payload.get("cap_reasons", [])),
        disqualifier_reasons=list(payload.get("disqualifier_reasons", [])),
        matched_title_family=str(payload.get("matched_title_family", "")),
        location_precision_bucket=str(payload.get("location_precision_bucket", "unknown")),
        current_role_proof_count=int(payload.get("current_role_proof_count", 0) or 0),
        source_quality_score=float(payload.get("source_quality_score", 0.0) or 0.0),
        evidence_freshness_year=payload.get("evidence_freshness_year"),
        current_function_fit=float(payload.get("current_function_fit", 0.0) or 0.0),
        current_fmcg_fit=float(payload.get("current_fmcg_fit", 0.0) or 0.0),
        parser_confidence=float(payload.get("parser_confidence", 0.0) or 0.0),
        evidence_quality_score=float(payload.get("evidence_quality_score", 0.0) or 0.0),
        title_similarity_score=float(payload.get("title_similarity_score", 0.0) or 0.0),
        company_match_score=float(payload.get("company_match_score", 0.0) or 0.0),
        location_match_score=float(payload.get("location_match_score", 0.0) or 0.0),
        skill_overlap_score=float(payload.get("skill_overlap_score", 0.0) or 0.0),
        industry_fit_score=float(payload.get("industry_fit_score", 0.0) or 0.0),
        years_fit_score=float(payload.get("years_fit_score", 0.0) or 0.0),
        years_experience_gap=payload.get("years_experience_gap"),
        semantic_similarity_score=float(payload.get("semantic_similarity_score", 0.0) or 0.0),
        reranker_score=float(payload.get("reranker_score", 0.0) or 0.0),
        ranking_model_version=str(payload.get("ranking_model_version", "")),
        feature_scores=dict(payload.get("feature_scores", payload.get("dimension_scores", {})) or {}),
        anchor_scores=dict(payload.get("anchor_scores", payload.get("anchor_hits", {})) or {}),
        verification_notes=list(payload.get("verification_notes", payload.get("notes", [])) or []),
        search_strategies=list(payload.get("search_strategies", payload.get("matchedIn", [])) or []),
        score=float(payload.get("score", 0.0) or 0.0),
        raw=dict(payload),
    )


def provider_result_from_remote(payload: Dict[str, Any]) -> ProviderRunResult:
    return ProviderRunResult(
        provider_name=str(payload.get("provider_name", payload.get("providerName", "remote_api"))),
        executed=bool(payload.get("executed", True)),
        dry_run=bool(payload.get("dry_run", False)),
        request_count=int(payload.get("request_count", payload.get("requestCount", 0)) or 0),
        candidate_count=int(payload.get("candidate_count", payload.get("candidateCount", 0)) or 0),
        candidates=[candidate_from_remote(candidate) for candidate in payload.get("candidates", [])],
        diagnostics=dict(payload.get("diagnostics", {})),
        errors=list(payload.get("errors", [])),
    )


class RemoteSourcingClient:
    def __init__(self) -> None:
        self.api_key = str(os.getenv("REMOTE_SOURCING_API_KEY") or os.getenv("HR_HUNTER_API_KEY") or "").strip()
        self.bearer_token = str(
            os.getenv("REMOTE_SOURCING_BEARER_TOKEN") or os.getenv("HR_HUNTER_API_BEARER_TOKEN") or ""
        ).strip()
        self.required = _truthy(os.getenv("REMOTE_SOURCING_REQUIRED") or os.getenv("HR_HUNTER_API_REQUIRED"))
        explicit_base_url = str(
            os.getenv("REMOTE_SOURCING_API_URL") or os.getenv("HR_HUNTER_API_URL") or ""
        ).strip()
        use_default_base_url = bool(self.required or self.api_key or self.bearer_token)
        self.base_url = (explicit_base_url or (DEFAULT_REMOTE_BASE_URL if use_default_base_url else "")).rstrip("/")
        self.timeout_ms = _parse_int(
            os.getenv("REMOTE_SOURCING_TIMEOUT_MS") or os.getenv("HR_HUNTER_API_TIMEOUT_MS"),
            120000,
        ) or 120000

    def is_configured(self) -> bool:
        return bool(self.base_url)

    def is_required(self) -> bool:
        return self.required

    def headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        return headers

    async def request(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.is_configured():
            raise RuntimeError("Remote HR Hunter API is required but not configured.")
        timeout = httpx.Timeout(self.timeout_ms / 1000.0)
        url = f"{self.base_url}{path}"
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(url, headers=self.headers(), json=payload)
            response.raise_for_status()
        except httpx.TimeoutException as exc:
            raise RemoteSourcingError(
                f"Remote sourcing timed out while calling '{url}'.",
                recoverable=True,
            ) from exc
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            recoverable = status_code >= 500 or status_code == 429
            raise RemoteSourcingError(
                f"Server error '{status_code} {exc.response.reason_phrase}' for url '{url}'",
                recoverable=recoverable,
                status_code=status_code,
            ) from exc
        except httpx.RequestError as exc:
            raise RemoteSourcingError(
                f"Remote sourcing request failed for '{url}': {exc}",
                recoverable=True,
            ) from exc
        return dict(response.json() or {})

    async def parse_brief(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        response = await self.request(
            "/brief/parse",
            {
                "rawText": str(payload.get("job_description", "")),
                "searchId": str(payload.get("search_id", "")).strip() or str(payload.get("role_title", "")).strip(),
                "title": str(payload.get("role_title", "")).strip(),
                "criteria": {
                    "jobTitles": _clean_list(payload.get("titles")) or [str(payload.get("role_title", "")).strip()],
                    "locationScopes": _normalize_location_scopes(payload),
                    "companyTargets": _normalize_company_targets(payload),
                    "industries": _clean_list(payload.get("industry_keywords")),
                    "mustHaveKeywords": _clean_list(payload.get("must_have_keywords")),
                    "anchors": dict(payload.get("anchors", {})),
                },
            },
        )
        return normalize_remote_parse_response(response)

    async def run_search(self, payload: Dict[str, Any], ui_payload: Dict[str, Any]) -> SearchRunReport:
        brief_config = dict(ui_payload.get("brief_config", {}))
        requested_limit = int(ui_payload.get("limit", 20) or 20)
        internal_fetch_limit = int(ui_payload.get("internal_fetch_limit", requested_limit) or requested_limit)
        remote_payload = {
            "runId": payload.get("run_id") or f"{brief_config.get('id', 'search')}-{uuid.uuid4().hex[:8]}",
            "searchId": brief_config.get("id", "search"),
            "orgId": str(payload.get("org_id", "local")),
            "title": brief_config.get("role_title", payload.get("role_title", "")),
            "mode": str(payload.get("mode", "standard")),
            "isExploratory": bool(payload.get("is_exploratory", False)),
            "limit": internal_fetch_limit,
            "requestedLimit": requested_limit,
            "criteria": build_remote_criteria(payload, ui_payload),
        }
        response = await self.request("/search", remote_payload)
        provider_results = [
            provider_result_from_remote(result)
            for result in response.get("provider_results", response.get("providerResults", []))
        ]
        candidates = [candidate_from_remote(candidate) for candidate in response.get("candidates", [])]
        return SearchRunReport(
            run_id=str(response.get("runId", response.get("run_id", remote_payload["runId"]))),
            brief_id=str(brief_config.get("id", remote_payload["searchId"])),
            dry_run=False,
            generated_at=str(response.get("generated_at", _now())),
            provider_results=provider_results,
            candidates=candidates,
            summary=dict(response.get("summary", {})),
        )


def remote_client_from_env() -> RemoteSourcingClient:
    return RemoteSourcingClient()
