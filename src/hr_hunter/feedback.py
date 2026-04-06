from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from hr_hunter.config import resolve_feedback_db_path
from hr_hunter.identity import canonicalize_profile_url, candidate_identity_keys, candidate_primary_key, normalize_identity_text
from hr_hunter.models import CandidateProfile, GeoSpec, SearchBrief, SearchRunReport
from hr_hunter.output import load_report
from hr_hunter.ranker import build_learned_feature_map


FEEDBACK_ACTIONS = {
    "shortlist",
    "reject",
    "promote_to_verified",
    "demote_to_review",
    "wrong_location",
    "wrong_function",
    "too_senior",
    "too_junior",
    "good_fit",
    "interviewed",
    "hired",
}
FEEDBACK_LABELS = {
    "reject": 0,
    "wrong_location": 0,
    "wrong_function": 0,
    "too_senior": 0,
    "too_junior": 0,
    "demote_to_review": 1,
    "good_fit": 2,
    "shortlist": 3,
    "promote_to_verified": 3,
    "interviewed": 4,
    "hired": 5,
}
POSITIVE_ACTIONS = {"good_fit", "shortlist", "promote_to_verified", "interviewed", "hired"}
NEGATIVE_ACTIONS = {"reject", "wrong_location", "wrong_function", "too_senior", "too_junior", "demote_to_review"}


@dataclass
class FeedbackEvent:
    brief_id: str
    candidate_id: str
    recruiter_id: str
    action: str
    reason_code: str = ""
    note: str = ""
    old_status: str = ""
    new_status: str = ""
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class TrainingPair:
    brief_id: str
    preferred_candidate_id: str
    other_candidate_id: str
    recruiter_id: str
    label: str
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


def validate_feedback_action(action: str) -> str:
    normalized = str(action).strip().lower()
    if normalized not in FEEDBACK_ACTIONS:
        raise ValueError(f"Unsupported feedback action: {action}")
    return normalized


def event_to_record(event: FeedbackEvent) -> Dict[str, Optional[str]]:
    return {
        "brief_id": event.brief_id,
        "candidate_id": event.candidate_id,
        "recruiter_id": event.recruiter_id,
        "action": validate_feedback_action(event.action),
        "reason_code": event.reason_code,
        "note": event.note,
        "old_status": event.old_status,
        "new_status": event.new_status,
        "created_at": event.created_at,
    }


def training_pair_to_record(pair: TrainingPair) -> Dict[str, str]:
    return {
        "brief_id": pair.brief_id,
        "preferred_candidate_id": pair.preferred_candidate_id,
        "other_candidate_id": pair.other_candidate_id,
        "recruiter_id": pair.recruiter_id,
        "label": pair.label,
        "created_at": pair.created_at,
    }


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(db_path))
    connection.row_factory = sqlite3.Row
    return connection


def init_feedback_db(db_path: Path | None = None) -> Path:
    resolved = resolve_feedback_db_path(str(db_path) if db_path else None)
    with _connect(resolved) as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS recruiters (
                id TEXT PRIMARY KEY,
                name TEXT DEFAULT '',
                team_id TEXT DEFAULT '',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS briefs (
                id TEXT PRIMARY KEY,
                created_by TEXT DEFAULT '',
                role_title TEXT DEFAULT '',
                job_description TEXT DEFAULT '',
                titles_json TEXT DEFAULT '[]',
                companies_json TEXT DEFAULT '[]',
                locations_json TEXT DEFAULT '[]',
                min_years INTEGER,
                max_years INTEGER,
                anchors_json TEXT DEFAULT '{}',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS candidates (
                id TEXT PRIMARY KEY,
                identity_key TEXT UNIQUE,
                full_name TEXT DEFAULT '',
                current_title TEXT DEFAULT '',
                current_company TEXT DEFAULT '',
                location_name TEXT DEFAULT '',
                linkedin_url TEXT DEFAULT '',
                source_url TEXT DEFAULT '',
                raw_profile_json TEXT DEFAULT '{}',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS candidate_evidence (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                candidate_id TEXT NOT NULL,
                source_domain TEXT DEFAULT '',
                source_url TEXT DEFAULT '',
                snippet TEXT DEFAULT '',
                evidence_type TEXT DEFAULT '',
                confidence REAL DEFAULT 0,
                raw_json TEXT DEFAULT '{}',
                UNIQUE(candidate_id, source_url, evidence_type, snippet)
            );

            CREATE TABLE IF NOT EXISTS candidate_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brief_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                retrieval_score REAL DEFAULT 0,
                reranker_score REAL DEFAULT 0,
                final_score REAL DEFAULT 0,
                status TEXT DEFAULT '',
                feature_json TEXT DEFAULT '{}',
                model_version TEXT DEFAULT '',
                report_path TEXT DEFAULT '',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS feedback_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brief_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                recruiter_id TEXT NOT NULL,
                action TEXT NOT NULL,
                reason_code TEXT DEFAULT '',
                note TEXT DEFAULT '',
                old_status TEXT DEFAULT '',
                new_status TEXT DEFAULT '',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS training_pairs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brief_id TEXT NOT NULL,
                preferred_candidate_id TEXT NOT NULL,
                other_candidate_id TEXT NOT NULL,
                recruiter_id TEXT NOT NULL,
                label TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(brief_id, preferred_candidate_id, other_candidate_id, recruiter_id, label)
            );
            """
        )
    return resolved


def _brief_to_record(brief: SearchBrief, created_by: str = "") -> Dict[str, Any]:
    locations = brief.location_targets or [value for value in [brief.geography.location_name, brief.geography.country] if value]
    return {
        "id": brief.id,
        "created_by": created_by,
        "role_title": brief.role_title,
        "job_description": brief.brief_summary or brief.document_text,
        "titles_json": json.dumps(brief.titles),
        "companies_json": json.dumps(brief.company_targets),
        "locations_json": json.dumps(locations),
        "min_years": brief.minimum_years_experience,
        "max_years": brief.maximum_years_experience,
        "anchors_json": json.dumps(brief.anchor_weights, sort_keys=True),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def _candidate_feature_payload(candidate: CandidateProfile, brief: SearchBrief) -> Dict[str, Any]:
    return {
        **build_learned_feature_map(candidate, brief),
        "feature_scores": dict(candidate.feature_scores),
        "anchor_scores": dict(candidate.anchor_scores),
        "title_similarity_score": candidate.title_similarity_score,
        "company_match_score": candidate.company_match_score,
        "location_match_score": candidate.location_match_score,
        "skill_overlap_score": candidate.skill_overlap_score,
        "industry_fit_score": candidate.industry_fit_score,
        "years_fit_score": candidate.years_fit_score,
        "years_experience_gap": candidate.years_experience_gap,
        "parser_confidence": candidate.parser_confidence,
        "evidence_quality_score": candidate.evidence_quality_score,
        "semantic_similarity_score": candidate.semantic_similarity_score,
        "reranker_score": candidate.reranker_score,
        "source_quality_score": candidate.source_quality_score,
        "current_function_fit": candidate.current_function_fit,
        "status": candidate.verification_status,
        "cap_reasons": candidate.cap_reasons,
        "disqualifier_reasons": candidate.disqualifier_reasons,
    }


def _upsert_recruiter(connection: sqlite3.Connection, recruiter_id: str, recruiter_name: str = "", team_id: str = "") -> None:
    connection.execute(
        """
        INSERT INTO recruiters (id, name, team_id, created_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name = CASE WHEN excluded.name != '' THEN excluded.name ELSE recruiters.name END,
            team_id = CASE WHEN excluded.team_id != '' THEN excluded.team_id ELSE recruiters.team_id END
        """,
        (recruiter_id, recruiter_name, team_id, datetime.now(timezone.utc).isoformat()),
    )


def _upsert_brief(connection: sqlite3.Connection, brief: SearchBrief, created_by: str = "") -> None:
    record = _brief_to_record(brief, created_by=created_by)
    connection.execute(
        """
        INSERT INTO briefs (
            id, created_by, role_title, job_description, titles_json, companies_json,
            locations_json, min_years, max_years, anchors_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            created_by = CASE WHEN excluded.created_by != '' THEN excluded.created_by ELSE briefs.created_by END,
            role_title = CASE WHEN excluded.role_title != '' THEN excluded.role_title ELSE briefs.role_title END,
            job_description = CASE WHEN excluded.job_description != '' THEN excluded.job_description ELSE briefs.job_description END,
            titles_json = excluded.titles_json,
            companies_json = excluded.companies_json,
            locations_json = excluded.locations_json,
            min_years = excluded.min_years,
            max_years = excluded.max_years,
            anchors_json = excluded.anchors_json
        """,
        (
            record["id"],
            record["created_by"],
            record["role_title"],
            record["job_description"],
            record["titles_json"],
            record["companies_json"],
            record["locations_json"],
            record["min_years"],
            record["max_years"],
            record["anchors_json"],
            record["created_at"],
        ),
    )


def _upsert_candidate(connection: sqlite3.Connection, candidate: CandidateProfile) -> str:
    candidate_id = candidate_primary_key(candidate)
    if not candidate_id:
        raise ValueError("Candidate must have a stable identity key before logging feedback.")
    connection.execute(
        """
        INSERT INTO candidates (
            id, identity_key, full_name, current_title, current_company, location_name,
            linkedin_url, source_url, raw_profile_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            full_name = excluded.full_name,
            current_title = excluded.current_title,
            current_company = excluded.current_company,
            location_name = excluded.location_name,
            linkedin_url = excluded.linkedin_url,
            source_url = excluded.source_url,
            raw_profile_json = excluded.raw_profile_json
        """,
        (
            candidate_id,
            candidate_id,
            candidate.full_name,
            candidate.current_title,
            candidate.current_company,
            candidate.location_name,
            candidate.linkedin_url or "",
            candidate.source_url or "",
            json.dumps(candidate.raw or {}, sort_keys=True),
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    for record in candidate.evidence_records:
        connection.execute(
            """
            INSERT OR IGNORE INTO candidate_evidence (
                candidate_id, source_domain, source_url, snippet, evidence_type, confidence, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                candidate_id,
                record.source_domain,
                record.source_url,
                record.snippet,
                record.source_type,
                record.confidence,
                json.dumps(record.raw or {}, sort_keys=True),
            ),
        )
    return candidate_id


def _insert_candidate_score(
    connection: sqlite3.Connection,
    brief: SearchBrief,
    brief_id: str,
    candidate_id: str,
    candidate: CandidateProfile,
    report_path: str,
) -> None:
    connection.execute(
        """
        INSERT INTO candidate_scores (
            brief_id, candidate_id, retrieval_score, reranker_score, final_score, status,
            feature_json, model_version, report_path, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            brief_id,
            candidate_id,
            candidate.raw.get("retrieval_score", candidate.feature_scores.get("semantic_similarity", 0.0)),
            candidate.reranker_score,
            candidate.score,
            candidate.verification_status,
            json.dumps(_candidate_feature_payload(candidate, brief), sort_keys=True),
            candidate.ranking_model_version,
            report_path,
            datetime.now(timezone.utc).isoformat(),
        ),
    )


def _match_candidate(report: SearchRunReport, candidate_ref: str) -> CandidateProfile:
    normalized_ref = normalize_identity_text(candidate_ref)
    canonical_ref_url = canonicalize_profile_url(candidate_ref)
    for candidate in report.candidates:
        if candidate_ref == candidate_primary_key(candidate):
            return candidate
        if candidate_ref in candidate_identity_keys(candidate):
            return candidate
        if canonical_ref_url and canonical_ref_url in {
            canonicalize_profile_url(candidate.linkedin_url),
            canonicalize_profile_url(candidate.source_url),
        }:
            return candidate
        if normalized_ref and normalized_ref in {
            normalize_identity_text(candidate.full_name),
            normalize_identity_text(candidate.current_company),
        }:
            return candidate
    raise ValueError(f"Candidate `{candidate_ref}` was not found in the report.")


def load_feedback_events(
    db_path: Path | None = None,
    *,
    recruiter_id: str | None = None,
    brief_id: str | None = None,
) -> List[FeedbackEvent]:
    resolved = init_feedback_db(db_path)
    query = "SELECT brief_id, candidate_id, recruiter_id, action, reason_code, note, old_status, new_status, created_at FROM feedback_events"
    clauses: List[str] = []
    params: List[str] = []
    if recruiter_id:
        clauses.append("recruiter_id = ?")
        params.append(recruiter_id)
    if brief_id:
        clauses.append("brief_id = ?")
        params.append(brief_id)
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY created_at ASC"
    with _connect(resolved) as connection:
        rows = connection.execute(query, params).fetchall()
    return [
        FeedbackEvent(
            brief_id=row["brief_id"],
            candidate_id=row["candidate_id"],
            recruiter_id=row["recruiter_id"],
            action=row["action"],
            reason_code=row["reason_code"],
            note=row["note"],
            old_status=row["old_status"],
            new_status=row["new_status"],
            created_at=row["created_at"],
        )
        for row in rows
    ]


def refresh_training_pairs(db_path: Path | None = None) -> int:
    resolved = init_feedback_db(db_path)
    events = load_feedback_events(resolved)
    grouped: Dict[tuple[str, str], List[FeedbackEvent]] = {}
    for event in events:
        grouped.setdefault((event.brief_id, event.recruiter_id), []).append(event)

    pairs: List[TrainingPair] = []
    for (brief_id, recruiter_id), group_events in grouped.items():
        labels_by_candidate: Dict[str, int] = {}
        for event in group_events:
            labels_by_candidate[event.candidate_id] = max(
                labels_by_candidate.get(event.candidate_id, -1),
                FEEDBACK_LABELS.get(validate_feedback_action(event.action), -1),
            )
        candidate_ids = list(labels_by_candidate.keys())
        for preferred_id in candidate_ids:
            for other_id in candidate_ids:
                if preferred_id == other_id:
                    continue
                if labels_by_candidate[preferred_id] <= labels_by_candidate[other_id]:
                    continue
                pairs.append(
                    TrainingPair(
                        brief_id=brief_id,
                        preferred_candidate_id=preferred_id,
                        other_candidate_id=other_id,
                        recruiter_id=recruiter_id,
                        label="preferred_over_other",
                    )
                )

    with _connect(resolved) as connection:
        connection.execute("DELETE FROM training_pairs")
        for pair in pairs:
            record = training_pair_to_record(pair)
            connection.execute(
                """
                INSERT OR IGNORE INTO training_pairs (
                    brief_id, preferred_candidate_id, other_candidate_id, recruiter_id, label, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    record["brief_id"],
                    record["preferred_candidate_id"],
                    record["other_candidate_id"],
                    record["recruiter_id"],
                    record["label"],
                    record["created_at"],
                ),
            )
    return len(pairs)


def log_feedback(
    *,
    report_path: Path,
    candidate_ref: str,
    recruiter_id: str,
    action: str,
    reason_code: str = "",
    note: str = "",
    recruiter_name: str = "",
    team_id: str = "",
    db_path: Path | None = None,
    brief: SearchBrief | None = None,
) -> Dict[str, Any]:
    resolved = init_feedback_db(db_path)
    report = load_report(report_path)
    candidate = _match_candidate(report, candidate_ref)
    candidate_id = candidate_primary_key(candidate)
    action_name = validate_feedback_action(action)
    effective_brief = brief
    if effective_brief is None:
        summary = report.summary if isinstance(report.summary, dict) else {}
        effective_brief = SearchBrief(
            id=report.brief_id,
            role_title=str(summary.get("role_title", report.brief_id)),
            brief_document_path=None,
            brief_summary="",
            titles=[],
            title_keywords=[],
            company_targets=[],
            company_aliases={},
            geography=GeoSpec(location_name=""),
            required_keywords=[],
            preferred_keywords=[],
            portfolio_keywords=[],
            commercial_keywords=[],
            leadership_keywords=[],
            scope_keywords=[],
            seniority_levels=[],
            minimum_years_experience=None,
            maximum_years_experience=None,
            result_target_min=0,
            result_target_max=0,
            max_profiles=0,
            industry_keywords=[],
            exclude_title_keywords=[],
            exclude_company_keywords=[],
            location_targets=list(summary.get("location_targets", [])) if isinstance(summary.get("location_targets", []), list) else [],
            company_match_mode=str(summary.get("company_match_mode", "both")),
            years_mode=str(summary.get("years_mode", "range")),
            years_target=summary.get("years_target"),
            years_tolerance=int(summary.get("years_tolerance", 0) or 0),
            jd_breakdown=dict(summary.get("jd_breakdown", {})) if isinstance(summary.get("jd_breakdown", {}), dict) else {},
            anchor_weights=dict(summary.get("anchor_weights", {})),
            provider_settings={},
            document_text="",
        )

    event = FeedbackEvent(
        brief_id=report.brief_id,
        candidate_id=candidate_id,
        recruiter_id=recruiter_id,
        action=action_name,
        reason_code=reason_code,
        note=note,
        old_status=candidate.verification_status,
        new_status="verified" if action_name in {"promote_to_verified", "hired"} else (
            "review" if action_name == "demote_to_review" else candidate.verification_status
        ),
    )

    with _connect(resolved) as connection:
        _upsert_recruiter(connection, recruiter_id, recruiter_name=recruiter_name, team_id=team_id)
        _upsert_brief(connection, effective_brief, created_by=recruiter_id)
        _upsert_candidate(connection, candidate)
        _insert_candidate_score(connection, effective_brief, report.brief_id, candidate_id, candidate, str(report_path))
        record = event_to_record(event)
        connection.execute(
            """
            INSERT INTO feedback_events (
                brief_id, candidate_id, recruiter_id, action, reason_code, note, old_status, new_status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["brief_id"],
                record["candidate_id"],
                record["recruiter_id"],
                record["action"],
                record["reason_code"],
                record["note"],
                record["old_status"],
                record["new_status"],
                record["created_at"],
            ),
        )
    pair_count = refresh_training_pairs(resolved)
    return {
        "db_path": str(resolved),
        "brief_id": report.brief_id,
        "candidate_id": candidate_id,
        "candidate_name": candidate.full_name,
        "action": action_name,
        "training_pair_count": pair_count,
    }


def load_ranker_training_rows(db_path: Path | None = None) -> List[Dict[str, Any]]:
    resolved = init_feedback_db(db_path)
    with _connect(resolved) as connection:
        score_rows = connection.execute(
            """
            SELECT cs.*, c.full_name
            FROM candidate_scores cs
            JOIN (
                SELECT brief_id, candidate_id, MAX(id) AS max_id
                FROM candidate_scores
                GROUP BY brief_id, candidate_id
            ) latest
                ON cs.id = latest.max_id
            JOIN candidates c ON c.id = cs.candidate_id
            """
        ).fetchall()
        events = connection.execute(
            """
            SELECT brief_id, candidate_id, recruiter_id, action
            FROM feedback_events
            ORDER BY created_at ASC
            """
        ).fetchall()

    labels_by_key: Dict[tuple[str, str, str], int] = {}
    for row in events:
        key = (row["brief_id"], row["candidate_id"], row["recruiter_id"])
        labels_by_key[key] = max(labels_by_key.get(key, -1), FEEDBACK_LABELS.get(row["action"], -1))

    training_rows: List[Dict[str, Any]] = []
    for score_row in score_rows:
        for (brief_id, candidate_id, recruiter_id), label in labels_by_key.items():
            if brief_id != score_row["brief_id"] or candidate_id != score_row["candidate_id"]:
                continue
            feature_payload = json.loads(score_row["feature_json"] or "{}")
            training_rows.append(
                {
                    "query_id": f"{brief_id}:{recruiter_id}",
                    "brief_id": brief_id,
                    "candidate_id": candidate_id,
                    "candidate_name": score_row["full_name"],
                    "recruiter_id": recruiter_id,
                    "label": label,
                    "feature_json": feature_payload,
                    "model_version": score_row["model_version"],
                    "final_score": float(score_row["final_score"] or 0.0),
                    "reranker_score": float(score_row["reranker_score"] or 0.0),
                    "status": score_row["status"],
                }
            )
    return training_rows


def export_training_rows(output_path: Path, db_path: Path | None = None) -> Path:
    rows = load_ranker_training_rows(db_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    return output_path
