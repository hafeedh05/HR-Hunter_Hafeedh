from __future__ import annotations

import csv
import json
from dataclasses import asdict
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Set, Tuple

from hr_hunter.identity import candidate_identity_keys
from hr_hunter.models import (
    CandidateProfile,
    EvidenceRecord,
    ProviderRunResult,
    SearchRunReport,
)


TITLE_FAMILY_KEYWORDS = (
    ("product_marketing", ("product marketing",)),
    ("product_portfolio", ("portfolio",)),
    ("category", ("category",)),
    ("brand", ("brand",)),
    ("product", ("product",)),
)


def _clean_reason_values(values: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    ordered: List[str] = []
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        if text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


def _title_family_text(candidate: CandidateProfile) -> str:
    parts = [candidate.current_title, *candidate.matched_titles]
    return " ".join(part.strip().lower() for part in parts if str(part).strip())


def _infer_matched_title_family(candidate: CandidateProfile) -> str:
    if candidate.matched_title_family:
        return candidate.matched_title_family
    haystack = _title_family_text(candidate)
    if not haystack:
        return "other"
    for family, keywords in TITLE_FAMILY_KEYWORDS:
        if any(keyword in haystack for keyword in keywords):
            return family
    return "other"


def _infer_location_precision_bucket(candidate: CandidateProfile) -> str:
    if candidate.location_precision_bucket and candidate.location_precision_bucket != "unknown":
        return candidate.location_precision_bucket
    if candidate.distance_miles is not None:
        return "geo_distance"
    if candidate.current_location_confirmed:
        return "current_evidence"
    if candidate.location_aligned:
        return "text_aligned"
    if candidate.location_name:
        return "profile_text"
    return "unknown"


def _infer_current_role_proof_count(candidate: CandidateProfile) -> int:
    if candidate.current_role_proof_count:
        return candidate.current_role_proof_count
    return sum(1 for record in candidate.evidence_records if record.current_employment_signal)


def _infer_source_quality_score(candidate: CandidateProfile, proof_count: int) -> float:
    if candidate.source_quality_score > 0.0:
        return candidate.source_quality_score
    score = 0.0
    source = (candidate.source or "").strip().lower()
    source_url = (candidate.source_url or "").strip().lower()
    linkedin_url = (candidate.linkedin_url or "").strip().lower()

    if source == "pdl":
        score += 0.35
    elif source:
        score += 0.2

    if "linkedin.com/in/" in linkedin_url or "linkedin.com/in/" in source_url:
        score += 0.25

    if any(record.profile_signal for record in candidate.evidence_records):
        score += 0.15
    if proof_count > 0:
        score += 0.2
    if candidate.current_company_confirmed or candidate.current_title_confirmed:
        score += 0.05

    return round(min(score, 1.0), 3)


def _infer_evidence_freshness_year(candidate: CandidateProfile) -> int | None:
    if candidate.evidence_freshness_year is not None:
        return candidate.evidence_freshness_year
    years = [
        int(record.recency_year)
        for record in candidate.evidence_records
        if isinstance(record.recency_year, int)
    ]
    return max(years) if years else None


def _infer_current_function_fit(candidate: CandidateProfile, matched_title_family: str) -> float:
    if candidate.current_function_fit > 0.0:
        return candidate.current_function_fit
    if candidate.current_title_match:
        return 1.0
    if matched_title_family != "other" and candidate.matched_titles:
        return 0.85
    if matched_title_family != "other":
        return 0.7
    if candidate.matched_titles:
        return 0.55
    return 0.0


def _infer_current_fmcg_fit(candidate: CandidateProfile) -> float:
    if candidate.current_fmcg_fit > 0.0:
        return candidate.current_fmcg_fit
    if candidate.current_target_company_match and candidate.industry_aligned:
        return 1.0
    if candidate.current_target_company_match:
        return 0.85
    if candidate.industry_aligned:
        return 0.75
    if candidate.target_company_history_match:
        return 0.5
    return 0.0


def _infer_qualification_tier(candidate: CandidateProfile, function_fit: float, fmcg_fit: float) -> str:
    if candidate.verification_status == "verified":
        return "strict_verified"
    if candidate.verification_status == "review":
        return "search_qualified"
    if candidate.score >= 45.0 and candidate.location_aligned and function_fit >= 0.7 and fmcg_fit >= 0.7:
        return "search_qualified"
    return "weak"


def _infer_cap_reasons(
    candidate: CandidateProfile,
    qualification_tier: str,
    proof_count: int,
) -> List[str]:
    if candidate.cap_reasons:
        return _clean_reason_values(candidate.cap_reasons)
    reasons: List[str] = []
    if qualification_tier != "strict_verified":
        if not candidate.current_company_confirmed:
            reasons.append("missing_current_company_confirmation")
        if not candidate.current_title_confirmed:
            reasons.append("missing_current_title_confirmation")
        if not candidate.current_location_confirmed:
            reasons.append("missing_precise_location_confirmation")
        if proof_count == 0 or not candidate.current_employment_confirmed:
            reasons.append("missing_current_role_proof")
        if candidate.stale_data_risk:
            reasons.append("stale_evidence_risk")
        if candidate.target_company_history_match and not candidate.current_target_company_match:
            reasons.append("target_company_history_only")
    return _clean_reason_values(reasons)


def _infer_disqualifier_reasons(
    candidate: CandidateProfile,
    qualification_tier: str,
    function_fit: float,
    fmcg_fit: float,
) -> List[str]:
    if candidate.disqualifier_reasons:
        return _clean_reason_values(candidate.disqualifier_reasons)
    reasons: List[str] = []
    if qualification_tier == "weak":
        if not candidate.location_aligned:
            reasons.append("location_not_aligned")
        if function_fit < 0.6:
            reasons.append("low_function_fit")
        if fmcg_fit < 0.6:
            reasons.append("low_fmcg_fit")
        if not candidate.current_target_company_match and not candidate.target_company_history_match:
            reasons.append("no_target_company_signal")
        if not candidate.current_employment_confirmed:
            reasons.append("missing_current_role_proof")
        if candidate.stale_data_risk:
            reasons.append("stale_evidence_risk")
    return _clean_reason_values(reasons)


def hydrate_candidate_reporting(candidate: CandidateProfile) -> CandidateProfile:
    matched_title_family = _infer_matched_title_family(candidate)
    current_role_proof_count = _infer_current_role_proof_count(candidate)
    evidence_freshness_year = _infer_evidence_freshness_year(candidate)
    current_function_fit = _infer_current_function_fit(candidate, matched_title_family)
    current_fmcg_fit = _infer_current_fmcg_fit(candidate)
    qualification_tier = _infer_qualification_tier(candidate, current_function_fit, current_fmcg_fit)

    candidate.matched_title_family = matched_title_family
    candidate.location_precision_bucket = _infer_location_precision_bucket(candidate)
    candidate.current_role_proof_count = current_role_proof_count
    candidate.source_quality_score = _infer_source_quality_score(candidate, current_role_proof_count)
    candidate.evidence_freshness_year = evidence_freshness_year
    candidate.current_function_fit = round(current_function_fit, 3)
    candidate.current_fmcg_fit = round(current_fmcg_fit, 3)
    candidate.qualification_tier = qualification_tier
    candidate.cap_reasons = _infer_cap_reasons(candidate, qualification_tier, current_role_proof_count)
    candidate.disqualifier_reasons = _infer_disqualifier_reasons(
        candidate,
        qualification_tier,
        current_function_fit,
        current_fmcg_fit,
    )
    return candidate


def hydrate_report_reporting(report: SearchRunReport) -> SearchRunReport:
    report.candidates = [hydrate_candidate_reporting(candidate) for candidate in report.candidates]
    for result in report.provider_results:
        result.candidates = [hydrate_candidate_reporting(candidate) for candidate in result.candidates]
    report.summary = build_reporting_summary(report.candidates, report.summary)
    return report


def build_reporting_summary(
    candidates: Iterable[CandidateProfile],
    base_summary: Dict[str, object] | None = None,
) -> Dict[str, object]:
    hydrated_candidates = [hydrate_candidate_reporting(candidate) for candidate in candidates]
    verification_counts = {
        "verified": len([candidate for candidate in hydrated_candidates if candidate.verification_status == "verified"]),
        "review": len([candidate for candidate in hydrated_candidates if candidate.verification_status == "review"]),
        "reject": len([candidate for candidate in hydrated_candidates if candidate.verification_status == "reject"]),
    }
    qualification_counts = {
        "strict_verified": len(
            [candidate for candidate in hydrated_candidates if candidate.qualification_tier == "strict_verified"]
        ),
        "search_qualified": len(
            [candidate for candidate in hydrated_candidates if candidate.qualification_tier == "search_qualified"]
        ),
        "weak": len([candidate for candidate in hydrated_candidates if candidate.qualification_tier == "weak"]),
    }
    summary = dict(base_summary or {})
    summary.update(
        {
            "candidate_count": len(hydrated_candidates),
            "verified_count": verification_counts["verified"],
            "review_count": verification_counts["review"],
            "reject_count": verification_counts["reject"],
            "strict_verified_count": qualification_counts["strict_verified"],
            "search_qualified_count": qualification_counts["search_qualified"],
            "weak_count": qualification_counts["weak"],
            "verification_status_counts": verification_counts,
            "qualification_tier_counts": qualification_counts,
        }
    )
    return summary


def iter_report_paths(paths: Iterable[Path]) -> Iterator[Path]:
    for path in paths:
        resolved = path.expanduser().resolve()
        if resolved.is_file() and resolved.suffix.lower() == ".json":
            yield resolved
            continue
        if resolved.is_dir():
            for child in sorted(resolved.glob("*.json")):
                if child.is_file():
                    yield child.resolve()


def collect_seen_candidate_keys(paths: Iterable[Path]) -> Set[str]:
    seen: Set[str] = set()
    for path in iter_report_paths(paths):
        try:
            report = load_report(path)
        except Exception:
            continue
        for candidate in report.candidates:
            seen.update(candidate_identity_keys(candidate))
    return seen


def collect_seen_provider_queries(paths: Iterable[Path]) -> Dict[str, Set[str]]:
    seen: Dict[str, Set[str]] = {}
    for path in iter_report_paths(paths):
        try:
            report = load_report(path)
        except Exception:
            continue
        for result in report.provider_results:
            provider_seen = seen.setdefault(result.provider_name, set())
            diagnostics_queries = result.diagnostics.get("queries", [])
            if not isinstance(diagnostics_queries, list):
                continue
            for item in diagnostics_queries:
                if not isinstance(item, dict):
                    continue
                search_query = str(item.get("search", "")).strip()
                if search_query:
                    provider_seen.add(search_query)
                    continue
                query_payload = item.get("query")
                if query_payload:
                    provider_seen.add(json.dumps(query_payload, sort_keys=True))
    return seen


def write_report(report: SearchRunReport, output_dir: Path) -> Tuple[Path, Path]:
    hydrate_report_reporting(report)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{report.run_id}.json"
    csv_path = output_dir / f"{report.run_id}.csv"

    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(asdict(report), handle, indent=2)

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "full_name",
                "current_title",
                "current_company",
                "location_name",
                "distance_miles",
                "current_target_company_match",
                "target_company_history_match",
                "current_title_match",
                "industry_aligned",
                "location_aligned",
                "current_company_confirmed",
                "current_title_confirmed",
                "current_location_confirmed",
                "precise_location_confirmed",
                "current_employment_confirmed",
                "verification_status",
                "qualification_tier",
                "score",
                "evidence_confidence",
                "evidence_verdict",
                "stale_data_risk",
                "cap_reasons",
                "disqualifier_reasons",
                "matched_title_family",
                "location_precision_bucket",
                "current_role_proof_count",
                "source_quality_score",
                "evidence_freshness_year",
                "current_function_fit",
                "current_fmcg_fit",
                "source",
                "linkedin_url",
                "source_url",
                "matched_titles",
                "matched_companies",
                "verification_notes",
            ],
        )
        writer.writeheader()
        for candidate in report.candidates:
            writer.writerow(
                {
                    "full_name": candidate.full_name,
                    "current_title": candidate.current_title,
                    "current_company": candidate.current_company,
                    "location_name": candidate.location_name,
                    "distance_miles": candidate.distance_miles,
                    "current_target_company_match": candidate.current_target_company_match,
                    "target_company_history_match": candidate.target_company_history_match,
                    "current_title_match": candidate.current_title_match,
                    "industry_aligned": candidate.industry_aligned,
                    "location_aligned": candidate.location_aligned,
                    "current_company_confirmed": candidate.current_company_confirmed,
                    "current_title_confirmed": candidate.current_title_confirmed,
                    "current_location_confirmed": candidate.current_location_confirmed,
                    "precise_location_confirmed": candidate.precise_location_confirmed,
                    "current_employment_confirmed": candidate.current_employment_confirmed,
                    "verification_status": candidate.verification_status,
                    "qualification_tier": candidate.qualification_tier,
                    "score": candidate.score,
                    "evidence_confidence": candidate.evidence_confidence,
                    "evidence_verdict": candidate.evidence_verdict,
                    "stale_data_risk": candidate.stale_data_risk,
                    "cap_reasons": "; ".join(candidate.cap_reasons),
                    "disqualifier_reasons": "; ".join(candidate.disqualifier_reasons),
                    "matched_title_family": candidate.matched_title_family,
                    "location_precision_bucket": candidate.location_precision_bucket,
                    "current_role_proof_count": candidate.current_role_proof_count,
                    "source_quality_score": candidate.source_quality_score,
                    "evidence_freshness_year": candidate.evidence_freshness_year,
                    "current_function_fit": candidate.current_function_fit,
                    "current_fmcg_fit": candidate.current_fmcg_fit,
                    "source": candidate.source,
                    "linkedin_url": candidate.linkedin_url,
                    "source_url": candidate.source_url,
                    "matched_titles": "; ".join(candidate.matched_titles),
                    "matched_companies": "; ".join(candidate.matched_companies),
                    "verification_notes": "; ".join(candidate.verification_notes),
                }
            )

    return json_path, csv_path


def load_report(path: Path) -> SearchRunReport:
    payload = json.loads(path.read_text(encoding="utf-8"))
    provider_results = [
        ProviderRunResult(
            provider_name=result.get("provider_name", ""),
            executed=bool(result.get("executed", False)),
            dry_run=bool(result.get("dry_run", False)),
            request_count=int(result.get("request_count", 0)),
            candidate_count=int(result.get("candidate_count", 0)),
            candidates=[build_candidate(candidate) for candidate in result.get("candidates", [])],
            diagnostics=result.get("diagnostics", {}),
            errors=list(result.get("errors", [])),
        )
        for result in payload.get("provider_results", [])
    ]
    report = SearchRunReport(
        run_id=payload.get("run_id", ""),
        brief_id=payload.get("brief_id", ""),
        dry_run=bool(payload.get("dry_run", False)),
        generated_at=payload.get("generated_at", ""),
        provider_results=provider_results,
        candidates=[build_candidate(candidate) for candidate in payload.get("candidates", [])],
        summary=payload.get("summary", {}),
    )
    return hydrate_report_reporting(report)


def build_candidate(payload: dict) -> CandidateProfile:
    candidate = CandidateProfile(
        full_name=payload.get("full_name", ""),
        current_title=payload.get("current_title", ""),
        current_company=payload.get("current_company", ""),
        location_name=payload.get("location_name", ""),
        location_geo=payload.get("location_geo"),
        linkedin_url=payload.get("linkedin_url"),
        source=payload.get("source", ""),
        source_url=payload.get("source_url"),
        summary=payload.get("summary", ""),
        years_experience=payload.get("years_experience"),
        industry=payload.get("industry"),
        experience=list(payload.get("experience", [])),
        matched_titles=list(payload.get("matched_titles", [])),
        matched_companies=list(payload.get("matched_companies", [])),
        distance_miles=payload.get("distance_miles"),
        current_target_company_match=bool(payload.get("current_target_company_match", False)),
        target_company_history_match=bool(payload.get("target_company_history_match", False)),
        current_title_match=bool(payload.get("current_title_match", False)),
        industry_aligned=bool(payload.get("industry_aligned", False)),
        location_aligned=bool(payload.get("location_aligned", False)),
        current_company_confirmed=bool(payload.get("current_company_confirmed", False)),
        current_title_confirmed=bool(payload.get("current_title_confirmed", False)),
        current_location_confirmed=bool(payload.get("current_location_confirmed", False)),
        precise_location_confirmed=bool(payload.get("precise_location_confirmed", False)),
        current_employment_confirmed=bool(payload.get("current_employment_confirmed", False)),
        verification_status=payload.get("verification_status", "review"),
        qualification_tier=payload.get("qualification_tier", "weak"),
        cap_reasons=list(payload.get("cap_reasons", [])),
        disqualifier_reasons=list(payload.get("disqualifier_reasons", [])),
        matched_title_family=payload.get("matched_title_family", ""),
        location_precision_bucket=payload.get("location_precision_bucket", "unknown"),
        current_role_proof_count=int(payload.get("current_role_proof_count", 0) or 0),
        source_quality_score=float(payload.get("source_quality_score", 0.0)),
        evidence_freshness_year=payload.get("evidence_freshness_year"),
        current_function_fit=float(payload.get("current_function_fit", 0.0)),
        current_fmcg_fit=float(payload.get("current_fmcg_fit", 0.0)),
        verification_notes=list(payload.get("verification_notes", [])),
        evidence_records=[
            EvidenceRecord(
                query=record.get("query", ""),
                source_url=record.get("source_url", ""),
                source_domain=record.get("source_domain", ""),
                title=record.get("title", ""),
                snippet=record.get("snippet", ""),
                source_type=record.get("source_type", "search_result"),
                name_match=bool(record.get("name_match", False)),
                company_match=record.get("company_match", ""),
                title_matches=list(record.get("title_matches", [])),
                location_match=bool(record.get("location_match", False)),
                location_match_text=record.get("location_match_text", ""),
                precise_location_match=bool(record.get("precise_location_match", False)),
                profile_signal=bool(record.get("profile_signal", False)),
                current_employment_signal=bool(record.get("current_employment_signal", False)),
                recency_year=record.get("recency_year"),
                confidence=float(record.get("confidence", 0.0)),
                raw=record.get("raw", {}),
            )
            for record in payload.get("evidence_records", [])
        ],
        evidence_confidence=float(payload.get("evidence_confidence", 0.0)),
        evidence_verdict=payload.get("evidence_verdict", ""),
        stale_data_risk=bool(payload.get("stale_data_risk", False)),
        last_verified_at=payload.get("last_verified_at"),
        score=float(payload.get("score", 0.0)),
        raw=payload.get("raw", {}),
    )
    return hydrate_candidate_reporting(candidate)
