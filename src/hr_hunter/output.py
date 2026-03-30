from __future__ import annotations

import csv
import json
from dataclasses import asdict
from pathlib import Path
from typing import Tuple

from hr_hunter.models import (
    CandidateProfile,
    EvidenceRecord,
    ProviderRunResult,
    SearchRunReport,
)


def write_report(report: SearchRunReport, output_dir: Path) -> Tuple[Path, Path]:
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
                "verification_status",
                "score",
                "evidence_confidence",
                "evidence_verdict",
                "stale_data_risk",
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
                    "verification_status": candidate.verification_status,
                    "score": candidate.score,
                    "evidence_confidence": candidate.evidence_confidence,
                    "evidence_verdict": candidate.evidence_verdict,
                    "stale_data_risk": candidate.stale_data_risk,
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
    return SearchRunReport(
        run_id=payload.get("run_id", ""),
        brief_id=payload.get("brief_id", ""),
        dry_run=bool(payload.get("dry_run", False)),
        generated_at=payload.get("generated_at", ""),
        provider_results=provider_results,
        candidates=[build_candidate(candidate) for candidate in payload.get("candidates", [])],
        summary=payload.get("summary", {}),
    )


def build_candidate(payload: dict) -> CandidateProfile:
    return CandidateProfile(
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
        verification_status=payload.get("verification_status", "review"),
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
