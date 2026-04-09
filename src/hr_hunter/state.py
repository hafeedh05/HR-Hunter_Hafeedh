from __future__ import annotations

import json
import threading
import uuid
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

from hr_hunter.briefing import normalize_text
from hr_hunter.db import connect_database, resolve_database_target
from hr_hunter.identity import candidate_primary_key
from hr_hunter.models import CandidateProfile, SearchBrief, SearchRunReport
from hr_hunter.output import load_report


_INITIALIZED_STATE_TARGETS: set[str] = set()
_STATE_INIT_LOCK = threading.Lock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


DEFAULT_JOB_STALE_SECONDS = 0
STALE_JOB_FAILURE_REASON = (
    "This background job stopped before completion, likely because the app restarted or the job was interrupted. "
    "Please retry."
)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def _parse_iso_timestamp(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _scalar_row_value(row: Any, *, key: str = "count") -> int:
    if row is None:
        return 0
    if isinstance(row, dict):
        if key in row:
            return int(row[key] or 0)
        values = list(row.values())
        return int(values[0] or 0) if values else 0
    keys = getattr(row, "keys", None)
    if callable(keys):
        row_keys = list(keys())
        if key in row_keys:
            return int(row[key] or 0)
        if row_keys:
            return int(row[row_keys[0]] or 0)
    try:
        return int(row[0] or 0)
    except (KeyError, IndexError, TypeError):
        return 0


def _resolve_target(db_path: Path | str | None) -> Any:
    return resolve_database_target(
        db_path,
        env_var="HR_HUNTER_STATE_DB",
        default_path="output/state/hr_hunter_state.db",
    )


def _connect(db_path: Path | str | None) -> Any:
    return connect_database(_resolve_target(db_path))


def _run_summary_from_artifact(
    summary: Dict[str, Any],
    *,
    candidate_count: int,
    report_json_path: str,
) -> Dict[str, Any]:
    resolved_summary = dict(summary or {})
    counted = sum(
        int(resolved_summary.get(key, 0) or 0)
        for key in ("verified_count", "review_count", "reject_count")
    )
    needs_refresh = not resolved_summary or (candidate_count > 0 and (counted <= 0 or counted != candidate_count))
    if not needs_refresh:
        return resolved_summary
    artifact_path = Path(str(report_json_path or "")).expanduser()
    if not artifact_path.exists():
        return resolved_summary
    try:
        report = load_report(artifact_path)
    except Exception:
        return resolved_summary
    return dict(report.summary or resolved_summary)


def init_state_db(db_path: Path | str | None = None) -> Path | str:
    target = _resolve_target(db_path)
    cache_key = target.locator
    if cache_key in _INITIALIZED_STATE_TARGETS:
        return target.path if target.backend == "sqlite" else target.locator
    with _STATE_INIT_LOCK:
        if cache_key in _INITIALIZED_STATE_TARGETS:
            return target.path if target.backend == "sqlite" else target.locator
        with connect_database(target) as connection:
            connection.executescript(
                """
            CREATE TABLE IF NOT EXISTS mandates (
                id TEXT PRIMARY KEY,
                org_id TEXT NOT NULL,
                brief_id TEXT NOT NULL,
                role_title TEXT DEFAULT '',
                owner_id TEXT DEFAULT '',
                owner_name TEXT DEFAULT '',
                team_id TEXT DEFAULT '',
                status TEXT DEFAULT 'active',
                brief_json TEXT DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_mandates_org_brief
                ON mandates(org_id, brief_id);

            CREATE TABLE IF NOT EXISTS search_runs (
                id TEXT PRIMARY KEY,
                mandate_id TEXT NOT NULL,
                brief_id TEXT NOT NULL,
                org_id TEXT NOT NULL,
                status TEXT DEFAULT 'completed',
                execution_backend TEXT DEFAULT 'local_engine',
                provider_order_json TEXT DEFAULT '[]',
                summary_json TEXT DEFAULT '{}',
                report_json_path TEXT DEFAULT '',
                report_csv_path TEXT DEFAULT '',
                dry_run INTEGER DEFAULT 0,
                candidate_count INTEGER DEFAULT 0,
                accepted_count INTEGER DEFAULT 0,
                limit_requested INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_search_runs_mandate
                ON search_runs(mandate_id, created_at DESC);

            CREATE TABLE IF NOT EXISTS candidate_registry (
                id TEXT PRIMARY KEY,
                org_id TEXT NOT NULL,
                identity_key TEXT NOT NULL,
                full_name TEXT DEFAULT '',
                current_title TEXT DEFAULT '',
                current_company TEXT DEFAULT '',
                location_name TEXT DEFAULT '',
                linkedin_url TEXT DEFAULT '',
                source_url TEXT DEFAULT '',
                latest_candidate_json TEXT DEFAULT '{}',
                search_ids_json TEXT DEFAULT '[]',
                search_count INTEGER DEFAULT 0,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_candidate_registry_identity
                ON candidate_registry(org_id, identity_key);

            CREATE TABLE IF NOT EXISTS run_candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                mandate_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                rank_index INTEGER DEFAULT 0,
                score REAL DEFAULT 0,
                verification_status TEXT DEFAULT '',
                qualification_tier TEXT DEFAULT '',
                feature_json TEXT DEFAULT '{}',
                anchor_json TEXT DEFAULT '{}',
                source TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                UNIQUE(run_id, candidate_id)
            );

            CREATE INDEX IF NOT EXISTS idx_run_candidates_run
                ON run_candidates(run_id, rank_index ASC);

            CREATE TABLE IF NOT EXISTS review_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mandate_id TEXT NOT NULL,
                run_id TEXT DEFAULT '',
                candidate_id TEXT NOT NULL,
                reviewer_id TEXT NOT NULL,
                reviewer_name TEXT DEFAULT '',
                owner_id TEXT DEFAULT '',
                action TEXT NOT NULL,
                reason_code TEXT DEFAULT '',
                note TEXT DEFAULT '',
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_review_actions_mandate
                ON review_actions(mandate_id, created_at DESC);

            CREATE TABLE IF NOT EXISTS candidate_reviews (
                candidate_id TEXT NOT NULL,
                mandate_id TEXT NOT NULL,
                owner_id TEXT DEFAULT '',
                owner_name TEXT DEFAULT '',
                latest_action TEXT DEFAULT '',
                latest_reason_code TEXT DEFAULT '',
                latest_note TEXT DEFAULT '',
                latest_run_id TEXT DEFAULT '',
                updated_at TEXT NOT NULL,
                PRIMARY KEY(candidate_id, mandate_id)
            );

            CREATE TABLE IF NOT EXISTS model_versions (
                id TEXT PRIMARY KEY,
                model_type TEXT NOT NULL,
                model_version TEXT NOT NULL,
                model_dir TEXT DEFAULT '',
                metadata_json TEXT DEFAULT '{}',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS audit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                actor_id TEXT DEFAULT '',
                payload_json TEXT DEFAULT '{}',
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_audit_events_entity
                ON audit_events(entity_type, entity_id, created_at DESC);

            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                job_type TEXT NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT DEFAULT '{}',
                result_json TEXT DEFAULT '{}',
                error_text TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                started_at TEXT DEFAULT '',
                finished_at TEXT DEFAULT ''
            );
                """
            )
        _INITIALIZED_STATE_TARGETS.add(cache_key)
    return target.path if target.backend == "sqlite" else target.locator


def _mandate_id(org_id: str, brief: SearchBrief) -> str:
    return f"mandate:{org_id}:{brief.id}"


def _candidate_registry_row(
    connection: Any,
    *,
    org_id: str,
    candidate: CandidateProfile,
    run_id: str,
) -> str:
    candidate_id = candidate_primary_key(candidate)
    if not candidate_id:
        candidate_id = f"anon:{uuid.uuid4().hex}"
    row_id = f"{org_id}:{candidate_id}"
    current = connection.execute(
        """
        SELECT search_ids_json, search_count, first_seen_at
        FROM candidate_registry
        WHERE org_id = ? AND identity_key = ?
        """,
        (org_id, candidate_id),
    ).fetchone()
    search_ids: List[str] = []
    if current:
        try:
            search_ids = list(json.loads(current["search_ids_json"] or "[]"))
        except Exception:
            search_ids = []
    if run_id and run_id not in search_ids:
        search_ids.append(run_id)
    created_at = current["first_seen_at"] if current else _now()
    connection.execute(
        """
        INSERT INTO candidate_registry (
            id, org_id, identity_key, full_name, current_title, current_company,
            location_name, linkedin_url, source_url, latest_candidate_json, search_ids_json,
            search_count, first_seen_at, last_seen_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            full_name = excluded.full_name,
            current_title = excluded.current_title,
            current_company = excluded.current_company,
            location_name = excluded.location_name,
            linkedin_url = excluded.linkedin_url,
            source_url = excluded.source_url,
            latest_candidate_json = excluded.latest_candidate_json,
            search_ids_json = excluded.search_ids_json,
            search_count = excluded.search_count,
            last_seen_at = excluded.last_seen_at,
            updated_at = excluded.updated_at
        """,
        (
            row_id,
            org_id,
            candidate_id,
            candidate.full_name,
            candidate.current_title,
            candidate.current_company,
            candidate.location_name,
            candidate.linkedin_url or "",
            candidate.source_url or "",
            _json(asdict(candidate)),
            _json(search_ids),
            len(search_ids),
            created_at,
            _now(),
            created_at,
            _now(),
        ),
    )
    return candidate_id


def log_audit_event(
    event_type: str,
    entity_type: str,
    entity_id: str,
    *,
    actor_id: str = "",
    payload: Dict[str, Any] | None = None,
    db_path: Path | None = None,
) -> None:
    resolved = init_state_db(db_path)
    with _connect(resolved) as connection:
        connection.execute(
            """
            INSERT INTO audit_events (event_type, entity_type, entity_id, actor_id, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (event_type, entity_type, entity_id, actor_id, _json(payload or {}), _now()),
        )


def persist_search_run(
    brief: SearchBrief,
    report: SearchRunReport,
    *,
    provider_names: Sequence[str],
    limit_requested: int,
    json_report_path: Path | None = None,
    csv_report_path: Path | None = None,
    db_path: Path | None = None,
    org_id: str = "local",
    owner_id: str = "",
    owner_name: str = "",
    team_id: str = "",
    execution_backend: str = "local_engine",
    mandate_id_override: str = "",
) -> Dict[str, Any]:
    resolved = init_state_db(db_path)
    mandate_id = str(mandate_id_override or "").strip() or _mandate_id(org_id, brief)
    mandate_org_id = org_id if not str(mandate_id_override or "").strip() else f"{org_id}:project:{mandate_id}"
    accepted_count = len(
        [candidate for candidate in report.candidates if candidate.verification_status in {"verified", "review"}]
    )
    with _connect(resolved) as connection:
        connection.execute(
            """
            INSERT INTO mandates (
                id, org_id, brief_id, role_title, owner_id, owner_name, team_id,
                status, brief_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                role_title = excluded.role_title,
                owner_id = CASE WHEN excluded.owner_id != '' THEN excluded.owner_id ELSE mandates.owner_id END,
                owner_name = CASE WHEN excluded.owner_name != '' THEN excluded.owner_name ELSE mandates.owner_name END,
                team_id = CASE WHEN excluded.team_id != '' THEN excluded.team_id ELSE mandates.team_id END,
                brief_json = excluded.brief_json,
                updated_at = excluded.updated_at
            """,
            (
                mandate_id,
                mandate_org_id,
                brief.id,
                brief.role_title,
                owner_id,
                owner_name,
                team_id,
                "active",
                _json(asdict(brief)),
                _now(),
                _now(),
            ),
        )
        connection.execute(
            """
            INSERT INTO search_runs (
                id, mandate_id, brief_id, org_id, status, execution_backend, provider_order_json,
                summary_json, report_json_path, report_csv_path, dry_run, candidate_count,
                accepted_count, limit_requested, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                status = excluded.status,
                execution_backend = excluded.execution_backend,
                provider_order_json = excluded.provider_order_json,
                summary_json = excluded.summary_json,
                report_json_path = excluded.report_json_path,
                report_csv_path = excluded.report_csv_path,
                dry_run = excluded.dry_run,
                candidate_count = excluded.candidate_count,
                accepted_count = excluded.accepted_count,
                limit_requested = excluded.limit_requested,
                updated_at = excluded.updated_at
            """,
            (
                report.run_id,
                mandate_id,
                brief.id,
                mandate_org_id,
                "completed",
                execution_backend,
                _json(list(provider_names)),
                _json(report.summary),
                str(json_report_path or ""),
                str(csv_report_path or ""),
                int(bool(report.dry_run)),
                len(report.candidates),
                accepted_count,
                int(limit_requested),
                _now(),
                _now(),
            ),
        )
        for index, candidate in enumerate(report.candidates, start=1):
            candidate_id = _candidate_registry_row(connection, org_id=org_id, candidate=candidate, run_id=report.run_id)
            connection.execute(
                """
                INSERT INTO run_candidates (
                    run_id, mandate_id, candidate_id, rank_index, score, verification_status,
                    qualification_tier, feature_json, anchor_json, source, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, candidate_id) DO UPDATE SET
                    mandate_id = excluded.mandate_id,
                    rank_index = excluded.rank_index,
                    score = excluded.score,
                    verification_status = excluded.verification_status,
                    qualification_tier = excluded.qualification_tier,
                    feature_json = excluded.feature_json,
                    anchor_json = excluded.anchor_json,
                    source = excluded.source,
                    created_at = excluded.created_at
                """,
                (
                    report.run_id,
                    mandate_id,
                    candidate_id,
                    index,
                    float(candidate.score or 0.0),
                    candidate.verification_status,
                    candidate.qualification_tier,
                    _json(candidate.feature_scores),
                    _json(candidate.anchor_scores),
                    candidate.source,
                    _now(),
                ),
            )
        connection.execute(
            """
            INSERT INTO audit_events (event_type, entity_type, entity_id, actor_id, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "search_run_completed",
                "search_run",
                report.run_id,
                owner_id,
                _json(
                    {
                        "mandate_id": mandate_id,
                        "candidate_count": len(report.candidates),
                        "accepted_count": accepted_count,
                        "execution_backend": execution_backend,
                    }
                ),
                _now(),
            ),
        )
    return {
        "db_path": str(resolved),
        "mandate_id": mandate_id,
        "run_id": report.run_id,
        "candidate_count": len(report.candidates),
        "accepted_count": accepted_count,
    }


def attach_registry_metadata(
    candidates: Sequence[CandidateProfile],
    *,
    db_path: Path | None = None,
    org_id: str = "local",
) -> List[CandidateProfile]:
    resolved = init_state_db(db_path)
    with _connect(resolved) as connection:
        rows = connection.execute(
            """
            SELECT identity_key, search_ids_json, search_count, first_seen_at, last_seen_at
            FROM candidate_registry
            WHERE org_id = ?
            """,
            (org_id,),
        ).fetchall()
    index = {row["identity_key"]: row for row in rows}
    for candidate in candidates:
        candidate_id = candidate_primary_key(candidate)
        row = index.get(candidate_id)
        if not row:
            continue
        try:
            search_ids = list(json.loads(row["search_ids_json"] or "[]"))
        except Exception:
            search_ids = []
        registry_payload = {
            "search_count": int(row["search_count"] or len(search_ids)),
            "search_ids": search_ids,
            "first_seen_at": row["first_seen_at"],
            "last_seen_at": row["last_seen_at"],
        }
        candidate.raw = dict(candidate.raw or {})
        candidate.raw["registry"] = registry_payload
        candidate.search_strategies = list(
            dict.fromkeys([*candidate.search_strategies, f"registry_seen:{registry_payload['search_count']}"])
        )
    return list(candidates)


def review_candidate(
    *,
    mandate_id: str,
    run_id: str,
    candidate_id: str,
    reviewer_id: str,
    action: str,
    note: str = "",
    reason_code: str = "",
    reviewer_name: str = "",
    owner_id: str = "",
    owner_name: str = "",
    db_path: Path | None = None,
) -> Dict[str, Any]:
    resolved = init_state_db(db_path)
    with _connect(resolved) as connection:
        connection.execute(
            """
            INSERT INTO review_actions (
                mandate_id, run_id, candidate_id, reviewer_id, reviewer_name,
                owner_id, action, reason_code, note, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                mandate_id,
                run_id,
                candidate_id,
                reviewer_id,
                reviewer_name,
                owner_id,
                action,
                reason_code,
                note,
                _now(),
            ),
        )
        connection.execute(
            """
            INSERT INTO candidate_reviews (
                candidate_id, mandate_id, owner_id, owner_name, latest_action,
                latest_reason_code, latest_note, latest_run_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(candidate_id, mandate_id) DO UPDATE SET
                owner_id = excluded.owner_id,
                owner_name = excluded.owner_name,
                latest_action = excluded.latest_action,
                latest_reason_code = excluded.latest_reason_code,
                latest_note = excluded.latest_note,
                latest_run_id = excluded.latest_run_id,
                updated_at = excluded.updated_at
            """,
            (
                candidate_id,
                mandate_id,
                owner_id,
                owner_name,
                action,
                reason_code,
                note,
                run_id,
                _now(),
            ),
        )
        connection.execute(
            """
            INSERT INTO audit_events (event_type, entity_type, entity_id, actor_id, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "candidate_reviewed",
                "candidate_review",
                f"{mandate_id}:{candidate_id}",
                reviewer_id,
                _json(
                    {
                        "run_id": run_id,
                        "action": action,
                        "reason_code": reason_code,
                        "owner_id": owner_id,
                    }
                ),
                _now(),
            ),
        )
    return {
        "db_path": str(resolved),
        "mandate_id": mandate_id,
        "run_id": run_id,
        "candidate_id": candidate_id,
        "action": action,
    }


def record_model_version(
    *,
    model_type: str,
    model_version: str,
    model_dir: str,
    metadata: Dict[str, Any],
    db_path: Path | None = None,
) -> Dict[str, Any]:
    resolved = init_state_db(db_path)
    row_id = f"{model_type}:{model_version}"
    with _connect(resolved) as connection:
        connection.execute(
            """
            INSERT INTO model_versions (id, model_type, model_version, model_dir, metadata_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                model_type = excluded.model_type,
                model_version = excluded.model_version,
                model_dir = excluded.model_dir,
                metadata_json = excluded.metadata_json,
                created_at = excluded.created_at
            """,
            (row_id, model_type, model_version, model_dir, _json(metadata), _now()),
        )
    return {"db_path": str(resolved), "id": row_id, "model_version": model_version}


def list_run_history(
    *,
    db_path: Path | None = None,
    limit: int = 25,
    mandate_id: str = "",
) -> List[Dict[str, Any]]:
    resolved = init_state_db(db_path)
    params: List[Any] = []
    where_clause = ""
    if str(mandate_id or "").strip():
        where_clause = "WHERE sr.mandate_id = ?"
        params.append(str(mandate_id).strip())
    params.append(int(limit))
    with _connect(resolved) as connection:
        rows = connection.execute(
            f"""
            SELECT sr.id, sr.mandate_id, sr.brief_id, sr.execution_backend, sr.candidate_count,
                   sr.accepted_count, sr.report_json_path, sr.report_csv_path, sr.summary_json,
                   sr.created_at, m.role_title
            FROM search_runs sr
            JOIN mandates m ON m.id = sr.mandate_id
            {where_clause}
            ORDER BY sr.created_at DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    history: List[Dict[str, Any]] = []
    for row in rows:
        try:
            summary = json.loads(row["summary_json"] or "{}")
        except Exception:
            summary = {}
        summary = _run_summary_from_artifact(
            summary,
            candidate_count=int(row["candidate_count"] or 0),
            report_json_path=str(row["report_json_path"] or ""),
        )
        history.append(
            {
                "run_id": row["id"],
                "mandate_id": row["mandate_id"],
                "brief_id": row["brief_id"],
                "role_title": row["role_title"],
                "execution_backend": row["execution_backend"],
                "candidate_count": int(row["candidate_count"] or 0),
                "accepted_count": int(row["accepted_count"] or 0),
                "report_json_path": row["report_json_path"],
                "report_csv_path": row["report_csv_path"],
                "created_at": row["created_at"],
                "summary": summary,
            }
        )
    return history


def list_review_history(
    *,
    db_path: Path | None = None,
    limit: int = 25,
    mandate_id: str = "",
    candidate_id: str = "",
    org_id: str = "local",
) -> List[Dict[str, Any]]:
    resolved = init_state_db(db_path)
    clauses: List[str] = []
    params: List[Any] = [org_id]
    if mandate_id:
        clauses.append("ra.mandate_id = ?")
        params.append(mandate_id)
    if candidate_id:
        clauses.append("ra.candidate_id = ?")
        params.append(candidate_id)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = f"""
        SELECT
            ra.id,
            ra.mandate_id,
            ra.run_id,
            ra.candidate_id,
            ra.reviewer_id,
            ra.reviewer_name,
            ra.owner_id,
            cr.owner_name,
            ra.action,
            ra.reason_code,
            ra.note,
            ra.created_at,
            reg.full_name,
            reg.current_title,
            reg.current_company,
            reg.location_name,
            reg.linkedin_url
        FROM review_actions ra
        LEFT JOIN candidate_reviews cr
            ON cr.candidate_id = ra.candidate_id
           AND cr.mandate_id = ra.mandate_id
        LEFT JOIN candidate_registry reg
            ON reg.identity_key = ra.candidate_id
           AND reg.org_id = ?
        {where_clause}
        ORDER BY ra.created_at DESC
        LIMIT ?
    """
    params.append(int(limit))
    with _connect(resolved) as connection:
        rows = connection.execute(query, params).fetchall()
    return [
        {
            "review_id": row["id"],
            "mandate_id": row["mandate_id"],
            "run_id": row["run_id"],
            "candidate_id": row["candidate_id"],
            "full_name": row["full_name"] or "",
            "current_title": row["current_title"] or "",
            "current_company": row["current_company"] or "",
            "location_name": row["location_name"] or "",
            "linkedin_url": row["linkedin_url"] or "",
            "reviewer_id": row["reviewer_id"],
            "reviewer_name": row["reviewer_name"] or "",
            "owner_id": row["owner_id"] or "",
            "owner_name": row["owner_name"] or "",
            "action": row["action"],
            "reason_code": row["reason_code"] or "",
            "note": row["note"] or "",
            "created_at": row["created_at"],
        }
        for row in rows
    ]


def _registry_similarity(candidate: CandidateProfile, brief: SearchBrief, search_count: int) -> float:
    title_terms = {token for token in normalize_text(candidate.current_title).split() if len(token) > 2}
    brief_title_terms = {
        token
        for token in normalize_text(" ".join([brief.role_title, *brief.titles, *brief.title_keywords])).split()
        if len(token) > 2
    }
    title_overlap = len(title_terms.intersection(brief_title_terms))
    company_match = 1.0 if normalize_text(candidate.current_company) in {
        normalize_text(company) for company in brief.company_targets if normalize_text(company)
    } else 0.0
    location_terms = normalize_text(" ".join([candidate.location_name, candidate.summary]))
    location_match = any(normalize_text(target) and normalize_text(target) in location_terms for target in brief.location_targets)
    score = float(title_overlap * 3.0) + company_match * 2.5 + (1.5 if location_match else 0.0)
    score += min(float(search_count), 5.0) * 0.2
    return score


def search_registry_memory(
    brief: SearchBrief,
    *,
    limit: int = 10,
    db_path: Path | None = None,
    org_id: str = "local",
) -> List[CandidateProfile]:
    resolved = init_state_db(db_path)
    with _connect(resolved) as connection:
        rows = connection.execute(
            """
            SELECT identity_key, latest_candidate_json, search_count
            FROM candidate_registry
            WHERE org_id = ?
            ORDER BY last_seen_at DESC
            """,
            (org_id,),
        ).fetchall()
    from hr_hunter.output import build_candidate

    scored: List[tuple[float, CandidateProfile]] = []
    for row in rows:
        try:
            payload = json.loads(row["latest_candidate_json"] or "{}")
        except Exception:
            continue
        candidate = build_candidate(payload)
        candidate.source = "registry_memory"
        similarity = _registry_similarity(candidate, brief, int(row["search_count"] or 0))
        if similarity <= 0.0:
            continue
        candidate.raw = dict(candidate.raw or {})
        candidate.raw["registry"] = {
            "search_count": int(row["search_count"] or 0),
            "identity_key": row["identity_key"],
        }
        scored.append((similarity, candidate))
    scored.sort(key=lambda item: (-item[0], item[1].full_name.lower()))
    return [candidate for _, candidate in scored[: max(0, int(limit))]]


def find_similar_candidates(
    candidate: CandidateProfile,
    *,
    limit: int = 5,
    db_path: Path | None = None,
    org_id: str = "local",
) -> List[Dict[str, Any]]:
    resolved = init_state_db(db_path)
    base_terms = {token for token in normalize_text(candidate.current_title).split() if len(token) > 2}
    location_terms = normalize_text(candidate.location_name)
    with _connect(resolved) as connection:
        rows = connection.execute(
            """
            SELECT identity_key, full_name, current_title, current_company, location_name, linkedin_url, search_count
            FROM candidate_registry
            WHERE org_id = ?
            ORDER BY search_count DESC, last_seen_at DESC
            """,
            (org_id,),
        ).fetchall()
    similar: List[tuple[float, Dict[str, Any]]] = []
    base_id = candidate_primary_key(candidate)
    for row in rows:
        if row["identity_key"] == base_id:
            continue
        row_terms = {token for token in normalize_text(row["current_title"]).split() if len(token) > 2}
        overlap = len(base_terms.intersection(row_terms))
        same_company = normalize_text(row["current_company"]) == normalize_text(candidate.current_company)
        same_location = bool(location_terms) and location_terms == normalize_text(row["location_name"])
        score = float(overlap * 2.0) + (1.0 if same_company else 0.0) + (0.8 if same_location else 0.0)
        if score <= 0.0:
            continue
        similar.append(
            (
                score,
                {
                    "candidate_id": row["identity_key"],
                    "full_name": row["full_name"],
                    "current_title": row["current_title"],
                    "current_company": row["current_company"],
                    "location_name": row["location_name"],
                    "linkedin_url": row["linkedin_url"],
                    "search_count": int(row["search_count"] or 0),
                },
            )
        )
    similar.sort(key=lambda item: (-item[0], -item[1]["search_count"], item[1]["full_name"].lower()))
    return [payload for _, payload in similar[: max(0, int(limit))]]


def summarize_system_state(*, db_path: Path | None = None) -> Dict[str, Any]:
    resolved = init_state_db(db_path)
    with _connect(resolved) as connection:
        counts = {
            "mandates": _scalar_row_value(connection.execute("SELECT COUNT(*) AS count FROM mandates").fetchone()),
            "search_runs": _scalar_row_value(connection.execute("SELECT COUNT(*) AS count FROM search_runs").fetchone()),
            "candidate_registry": _scalar_row_value(
                connection.execute("SELECT COUNT(*) AS count FROM candidate_registry").fetchone()
            ),
            "review_actions": _scalar_row_value(connection.execute("SELECT COUNT(*) AS count FROM review_actions").fetchone()),
            "model_versions": _scalar_row_value(connection.execute("SELECT COUNT(*) AS count FROM model_versions").fetchone()),
            "jobs": _scalar_row_value(connection.execute("SELECT COUNT(*) AS count FROM jobs").fetchone()),
        }
        latest_run = connection.execute(
            "SELECT id, created_at, execution_backend, candidate_count FROM search_runs ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    return {
        "db_path": str(resolved),
        "counts": counts,
        "latest_run": dict(latest_run) if latest_run else None,
    }


def enqueue_job(job_type: str, payload: Dict[str, Any], *, db_path: Path | None = None) -> Dict[str, Any]:
    resolved = init_state_db(db_path)
    job_id = f"job_{uuid.uuid4().hex[:12]}"
    with _connect(resolved) as connection:
        connection.execute(
            """
            INSERT INTO jobs (id, job_type, status, payload_json, result_json, error_text, created_at, started_at, finished_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (job_id, job_type, "queued", _json(payload), "{}", "", _now(), "", ""),
        )
    return {"db_path": str(resolved), "job_id": job_id, "status": "queued"}


def start_job(job_id: str, *, db_path: Path | None = None) -> None:
    resolved = init_state_db(db_path)
    with _connect(resolved) as connection:
        connection.execute(
            "UPDATE jobs SET status = ?, started_at = ? WHERE id = ?",
            ("running", _now(), job_id),
        )


def complete_job(job_id: str, result: Dict[str, Any], *, db_path: Path | None = None) -> None:
    resolved = init_state_db(db_path)
    with _connect(resolved) as connection:
        connection.execute(
            "UPDATE jobs SET status = ?, result_json = ?, finished_at = ? WHERE id = ?",
            ("completed", _json(result), _now(), job_id),
        )


def fail_job(job_id: str, error_text: str, *, db_path: Path | None = None) -> None:
    resolved = init_state_db(db_path)
    with _connect(resolved) as connection:
        connection.execute(
            "UPDATE jobs SET status = ?, error_text = ?, finished_at = ? WHERE id = ?",
            ("failed", error_text, _now(), job_id),
        )


def load_job(job_id: str, *, db_path: Path | None = None) -> Dict[str, Any] | None:
    resolved = init_state_db(db_path)
    with _connect(resolved) as connection:
        row = connection.execute(
            "SELECT id, job_type, status, payload_json, result_json, error_text, created_at, started_at, finished_at FROM jobs WHERE id = ?",
            (job_id,),
        ).fetchone()
    if not row:
        return None
    return {
        "job_id": row["id"],
        "job_type": row["job_type"],
        "status": row["status"],
        "payload": json.loads(row["payload_json"] or "{}"),
        "result": json.loads(row["result_json"] or "{}"),
        "error": row["error_text"],
        "created_at": row["created_at"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
    }


def list_jobs(
    *,
    db_path: Path | None = None,
    limit: int = 25,
    job_type: str = "",
    status: str = "",
    project_id: str = "",
) -> List[Dict[str, Any]]:
    resolved = init_state_db(db_path)
    fetch_limit = max(int(limit) * 5, 50)
    with _connect(resolved) as connection:
        rows = connection.execute(
            """
            SELECT id, job_type, status, payload_json, result_json, error_text, created_at, started_at, finished_at
            FROM jobs
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (fetch_limit,),
        ).fetchall()
    results: List[Dict[str, Any]] = []
    expected_type = str(job_type or "").strip().lower()
    expected_status = str(status or "").strip().lower()
    expected_project = str(project_id or "").strip()
    for row in rows:
        job = {
            "job_id": row["id"],
            "job_type": row["job_type"],
            "status": row["status"],
            "payload": json.loads(row["payload_json"] or "{}"),
            "result": json.loads(row["result_json"] or "{}"),
            "error": row["error_text"],
            "created_at": row["created_at"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
        }
        if expected_type and str(job["job_type"]).strip().lower() != expected_type:
            continue
        if expected_status and str(job["status"]).strip().lower() != expected_status:
            continue
        if expected_project:
            job_project_id = str(job["payload"].get("project_id") or job["result"].get("project", {}).get("id") or "").strip()
            if job_project_id != expected_project:
                continue
        results.append(job)
        if len(results) >= max(1, int(limit)):
            break
    return results


def latest_project_job(
    project_id: str,
    *,
    db_path: Path | None = None,
    job_type: str = "search",
) -> Dict[str, Any] | None:
    jobs = list_jobs(db_path=db_path, limit=1, job_type=job_type, project_id=project_id)
    return jobs[0] if jobs else None


def stop_job(job_id: str, *, reason: str = "", db_path: Path | None = None) -> Dict[str, Any] | None:
    existing = load_job(job_id, db_path=db_path)
    if not existing:
        return None
    if str(existing.get("status", "")).lower() in {"completed", "failed"}:
        return existing
    error_text = str(reason or "").strip() or STALE_JOB_FAILURE_REASON
    resolved = init_state_db(db_path)
    with _connect(resolved) as connection:
        connection.execute(
            "UPDATE jobs SET status = ?, error_text = ?, finished_at = ? WHERE id = ?",
            ("failed", error_text, _now(), job_id),
        )
    return load_job(job_id, db_path=db_path)


def expire_stale_jobs(
    *,
    db_path: Path | None = None,
    max_age_seconds: int = DEFAULT_JOB_STALE_SECONDS,
    reason: str = STALE_JOB_FAILURE_REASON,
) -> List[str]:
    try:
        stale_seconds = int(max_age_seconds)
    except (TypeError, ValueError):
        stale_seconds = 0
    if stale_seconds <= 0:
        return []
    resolved = init_state_db(db_path)
    threshold = datetime.now(timezone.utc) - timedelta(seconds=max(60, stale_seconds))
    stale_job_ids: List[str] = []
    with _connect(resolved) as connection:
        rows = connection.execute(
            """
            SELECT id, status, created_at, started_at
            FROM jobs
            WHERE status IN ('queued', 'running')
            ORDER BY created_at ASC
            """
        ).fetchall()
        for row in rows:
            reference = _parse_iso_timestamp(str(row["started_at"] or "")) or _parse_iso_timestamp(str(row["created_at"] or ""))
            if not reference or reference > threshold:
                continue
            connection.execute(
                "UPDATE jobs SET status = ?, error_text = ?, finished_at = ? WHERE id = ? AND status IN ('queued', 'running')",
                ("failed", str(reason).strip() or STALE_JOB_FAILURE_REASON, _now(), row["id"]),
            )
            stale_job_ids.append(str(row["id"]))
    return stale_job_ids
