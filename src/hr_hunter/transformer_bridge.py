from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple
from uuid import uuid4

from hr_hunter.models import CandidateProfile, EvidenceRecord, ProviderRunResult, SearchBrief, SearchRunReport


def _transformer_src_path() -> Path:
    return Path(__file__).resolve().parents[2] / "hr_hunter_transformer"


def _external_transformer_src_path() -> Path:
    return Path(__file__).resolve().parents[2].parent / "Hr Hunter Transformer" / "src"


def _ensure_transformer_import_path() -> None:
    if importlib.util.find_spec("hr_hunter_transformer") is not None:
        return
    transformer_src = _external_transformer_src_path()
    if transformer_src.exists():
        path_str = str(transformer_src)
        if path_str not in sys.path:
            sys.path.insert(0, path_str)


def transformer_available() -> bool:
    if importlib.util.find_spec("hr_hunter_transformer") is not None:
        return True
    transformer_src = _external_transformer_src_path()
    return transformer_src.exists()


def _normalize_locations(brief: SearchBrief, payload: Dict[str, Any] | None = None) -> tuple[list[str], list[str]]:
    payload = dict(payload or {})
    countries: list[str] = []
    cities: list[str] = []
    for value in payload.get("countries", []) or []:
        text = str(value or "").strip()
        if text and text not in countries:
            countries.append(text)
    for value in payload.get("continents", []) or []:
        text = str(value or "").strip()
        if text and text not in countries:
            countries.append(text)
    for value in payload.get("cities", []) or []:
        text = str(value or "").strip()
        if text and text not in cities:
            cities.append(text)
    if brief.geography.country and brief.geography.country not in countries:
        countries.append(brief.geography.country)
    for value in brief.location_targets:
        text = str(value or "").strip()
        if not text:
            continue
        if text in countries or text in cities:
            continue
        if " " in text and text.lower() not in {"middle east", "north america", "south america"}:
            cities.append(text)
        else:
            countries.append(text)
    return countries, cities


def _build_transformer_brief(brief: SearchBrief, *, requested_limit: int, payload: Dict[str, Any] | None = None) -> Any:
    _ensure_transformer_import_path()
    from hr_hunter_transformer.models import SearchBrief as TransformerSearchBrief

    countries, cities = _normalize_locations(brief, payload)
    return TransformerSearchBrief(
        role_title=brief.role_title,
        titles=list(brief.titles),
        countries=countries,
        cities=cities,
        company_targets=list(brief.company_targets or brief.peer_company_targets),
        required_keywords=list(brief.required_keywords),
        preferred_keywords=list(brief.preferred_keywords),
        industry_keywords=list(brief.industry_keywords),
        target_count=max(1, int(requested_limit or brief.max_profiles or 300)),
    )


def _qualification_tier(status: str) -> str:
    if status == "verified":
        return "strong"
    if status == "review":
        return "good"
    return "weak"


def _sanitize_transformer_name(value: str) -> str:
    text = str(value or "").split("(")[0].strip()
    parts = [part for part in text.split() if part]
    while parts and (any(char.isdigit() for char in parts[-1]) or len(parts[-1]) >= 6 and all(ch in "0123456789abcdefABCDEF" for ch in parts[-1])):
        parts.pop()
    while parts and parts[-1].lower() in {"mba", "cscp", "cppm", "cscm", "pmp", "phd"}:
        parts.pop()
    return " ".join(parts[:5]).strip()


def _looks_like_bad_company(value: str, current_title: str = "") -> bool:
    lowered = " ".join(str(value or "").strip().lower().split())
    if not lowered:
        return True
    if lowered in {"at", "@", "dr", "experience", "profile", "educational"}:
        return True
    if any(lowered.startswith(prefix) for prefix in ("jan ", "feb ", "mar ", "apr ", "may ", "jun ", "jul ", "aug ", "sep ", "oct ", "nov ", "dec ")):
        return True
    if any(fragment in lowered for fragment in ("view org chart", "view manager", " is a ", "manager at", "engineer at", "planner at", "from may ", "from jan ", "from feb ", "from mar ", "from apr ", "from jun ", "from jul ", "from aug ", "from sep ", "from oct ", "from nov ", "from dec ")):
        return True
    if current_title and lowered == " ".join(str(current_title).strip().lower().split()):
        return True
    if lowered.endswith((" manager", " engineer", " planner", " lead", " director", " analyst", " specialist")):
        return True
    return False


def _sanitize_transformer_company(entity: Any, top_evidence: List[Any]) -> str:
    candidates: List[str] = []
    if entity.current_company and not _looks_like_bad_company(entity.current_company, entity.current_title):
        candidates.append(str(entity.current_company).strip())
    for evidence in top_evidence:
        current_company = str(getattr(evidence, "current_company", "") or "").strip()
        if current_company and not _looks_like_bad_company(current_company, entity.current_title):
            candidates.append(current_company)
        source_url = str(getattr(evidence, "source_url", "") or "")
        if "theorg.com/org/" in source_url.lower():
            try:
                slug = source_url.split("/org/", 1)[1].split("/", 1)[0]
                inferred = " ".join(part.capitalize() for part in slug.replace("_", "-").split("-") if part).strip()
                if inferred and not _looks_like_bad_company(inferred, entity.current_title):
                    candidates.append(inferred)
            except Exception:
                pass
    return candidates[0] if candidates else ""


def _candidate_from_transformer_entity(entity: Any, brief: SearchBrief) -> CandidateProfile:
    top_evidence = list(entity.evidence[:3])
    primary = top_evidence[0] if top_evidence else None
    location_name = entity.current_location or (primary.current_location if primary else "")
    full_name = _sanitize_transformer_name(entity.full_name)
    current_company = _sanitize_transformer_company(entity, top_evidence)
    source_url = primary.source_url if primary else ""
    source_domain = primary.source_domain if primary else ""
    verification_notes = list(entity.notes or [])
    verification_notes.extend(list(entity.diagnostics or []))
    if entity.verification_status == "review" and not entity.current_company_confirmed:
        verification_notes.append("needs_current_company_confirmation")
    if entity.verification_status == "review" and not entity.current_location_confirmed:
        verification_notes.append("needs_precise_location_confirmation")
    evidence_records = [
        EvidenceRecord(
            query="",
            source_url=evidence.source_url,
            source_domain=evidence.source_domain,
            title=evidence.page_title,
            snippet=evidence.page_snippet,
            source_type=evidence.source_type,
            name_match=True,
            company_match=evidence.current_company if evidence.company_match else "",
            title_matches=[evidence.current_title] if evidence.current_title else [],
            location_match=evidence.location_match,
            location_match_text=evidence.current_location,
            precise_location_match=evidence.location_match,
            profile_signal=True,
            current_employment_signal=evidence.current_role_signal,
            confidence=evidence.confidence,
            raw={},
        )
        for evidence in top_evidence
    ]
    return CandidateProfile(
        full_name=full_name,
        current_title=entity.current_title,
        current_company=current_company,
        location_name=location_name,
        linkedin_url=source_url if "linkedin" in source_domain else None,
        source="transformer_scrapingbee",
        source_url=source_url,
        summary=primary.page_snippet if primary else "",
        matched_titles=[entity.current_title] if entity.current_title else [brief.role_title],
        matched_companies=[current_company] if current_company else [],
        current_target_company_match=bool(entity.company_match),
        target_company_history_match=False,
        current_title_match=bool(entity.title_match),
        in_scope=False,
        precise_market_in_scope=False,
        scope_bucket="out_of_scope",
        industry_aligned=bool(primary and primary.supporting_keywords),
        location_aligned=bool(entity.location_match),
        current_company_confirmed=bool(entity.current_company_confirmed),
        current_title_confirmed=bool(entity.current_title_confirmed),
        current_location_confirmed=bool(entity.current_location_confirmed),
        precise_location_confirmed=bool(entity.current_location_confirmed),
        current_employment_confirmed=bool(entity.current_role_proof_count > 0),
        verification_status=entity.verification_status,
        qualification_tier=_qualification_tier(entity.verification_status),
        cap_reasons=[],
        disqualifier_reasons=[] if entity.verification_status != "reject" else ["insufficient_current_role_proof"],
        matched_title_family=entity.role_family,
        location_precision_bucket="precise" if entity.current_location_confirmed else "unknown",
        current_role_proof_count=int(entity.current_role_proof_count or 0),
        source_quality_score=float(min(1.0, len(entity.source_domains) / 4.0)),
        current_function_fit=1.0 if entity.title_match else 0.0,
        parser_confidence=float(primary.confidence if primary else 0.0),
        evidence_quality_score=float(max((evidence.confidence for evidence in top_evidence), default=0.0)),
        title_similarity_score=1.0 if entity.title_match else 0.0,
        company_match_score=1.0 if entity.company_match else 0.0,
        location_match_score=1.0 if entity.location_match else 0.0,
        semantic_similarity_score=float(entity.semantic_similarity or 0.0),
        reranker_score=float(entity.score or 0.0),
        ranking_model_version="transformer_v2",
        feature_scores={},
        anchor_scores={
            "semantic_fit": float(entity.semantic_fit or 0.0),
            "title_match": float(entity.title_match_score or 0.0),
            "skill_match": float(entity.skill_match_score or 0.0),
            "company_match": float(entity.company_match_score or 0.0),
            "location_match": float(entity.location_match_score or 0.0),
            "seniority_match": float(entity.seniority_match_score or 0.0),
            "currentness": float(entity.currentness_score or 0.0),
            "source_trust": float(entity.source_trust_score or 0.0),
            "verification_confidence": float(entity.verification_confidence or 0.0),
        },
        verification_notes=verification_notes,
        search_strategies=list(entity.source_domains),
        evidence_records=evidence_records,
        evidence_confidence=float(max((evidence.confidence for evidence in top_evidence), default=0.0)),
        evidence_verdict=entity.verification_status,
        stale_data_risk=False,
        last_verified_at=datetime.now(timezone.utc).isoformat(),
        score=float(entity.score or 0.0),
        raw={
            "transformer_mode": True,
            "source_domains": list(entity.source_domains),
            "role_family": entity.role_family,
            "role_subfamily": "",
        },
    )


def _private_usage_log_path() -> Path:
    return Path(__file__).resolve().parents[2] / "output" / "private" / "transformer_usage.jsonl"


def _append_private_usage_log(entry: Dict[str, Any]) -> None:
    path = _private_usage_log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=True) + "\n")


async def run_transformer_search(
    *,
    brief: SearchBrief,
    requested_limit: int,
    reranker_enabled: bool = True,
    max_queries: int = 60,
    pages_per_query: int = 2,
    parallel_requests: int = 6,
    payload: Dict[str, Any] | None = None,
    job_id: str | None = None,
    project_id: str | None = None,
    progress_callback: Callable[[Dict[str, Any]], None] | None = None,
) -> SearchRunReport:
    _ensure_transformer_import_path()
    from hr_hunter_transformer.export import build_run_summary
    from hr_hunter_transformer.pipeline import CandidateIntelligencePipeline
    from hr_hunter_transformer.scrapingbee_adapter import ScrapingBeeSearchConfig, ScrapingBeeTransformerRetriever

    transformer_brief = _build_transformer_brief(brief, requested_limit=requested_limit, payload=payload)
    pipeline = CandidateIntelligencePipeline(use_transformer=bool(reranker_enabled))
    query_plan = pipeline.build_query_plan(transformer_brief)
    retriever = ScrapingBeeTransformerRetriever(
        ScrapingBeeSearchConfig(
            max_queries=query_plan.max_queries,
            pages_per_query=query_plan.pages_per_query,
            parallel_requests=query_plan.parallel_requests,
        )
    )
    if progress_callback:
        progress_callback(
            {
                "stage": "brief_normalized",
                "message": "Brief normalized for transformer pipeline.",
                "queries_total": 0,
                "queries_completed": 0,
                "queries_in_flight": 0,
                "raw_found": 0,
                "percent": 2,
            }
        )
        progress_callback(
            {
                "stage": "role_understood",
                "message": f"Mapped role to {query_plan.role_understanding.role_family}/{query_plan.role_understanding.role_subfamily}.",
                "queries_total": 0,
                "queries_completed": 0,
                "queries_in_flight": 0,
                "raw_found": 0,
                "percent": 6,
            }
        )
        progress_callback(
            {
                "stage": "queries_planned",
                "message": f"Planned {len(query_plan.queries)} transformer queries.",
                "queries_total": len(query_plan.queries),
                "queries_completed": 0,
                "queries_in_flight": 0,
                "raw_found": 0,
                "percent": 10,
            }
        )
    queries, hits = await retriever.search_async(
        transformer_brief,
        query_plan=query_plan,
        progress_callback=progress_callback,
    )
    result = pipeline.run(transformer_brief, hits, query_plan=query_plan, progress_callback=progress_callback)
    candidates = [_candidate_from_transformer_entity(entity, brief) for entity in result.candidates[:requested_limit]]
    run_id = f"{brief.role_title.lower().replace(' ', '-')}-{uuid4().hex[:8]}"
    report = SearchRunReport(
        run_id=run_id,
        brief_id=brief.id,
        dry_run=False,
        generated_at=datetime.now(timezone.utc).isoformat(),
        provider_results=[
            ProviderRunResult(
                provider_name="transformer_scrapingbee",
                executed=True,
                dry_run=False,
                request_count=len(queries),
                candidate_count=len(candidates),
                candidates=[],
                diagnostics={},
                errors=[],
            )
        ],
        candidates=candidates,
        summary=build_run_summary(transformer_brief, query_plan, result),
    )
    retriever_usage = retriever.usage_summary()
    pipeline_usage = pipeline.usage_summary()
    _append_private_usage_log(
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "job_id": str(job_id or ""),
            "project_id": str(project_id or ""),
            "run_id": run_id,
            "role_title": brief.role_title,
            "requested_limit": int(requested_limit),
            "search_backend": "transformer_v2",
            "scrapingbee": {
                "logical_queries": int(len(queries)),
                "pages_per_query": int(pages_per_query),
                "parallel_requests": int(parallel_requests),
                **retriever_usage,
            },
            "transformer": pipeline_usage,
            "result": {
                "candidate_count": int(report.summary.get("candidate_count", len(candidates)) or len(candidates)),
                "verified_count": int(report.summary.get("verified_count", 0) or 0),
                "review_count": int(report.summary.get("review_count", 0) or 0),
                "reject_count": int(report.summary.get("reject_count", 0) or 0),
                "raw_found": int(report.summary.get("raw_found", len(hits)) or len(hits)),
                "unique_after_dedupe": int(report.summary.get("unique_after_dedupe", len(candidates)) or len(candidates)),
            },
        }
    )
    return report
