from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Iterable

from hr_hunter_transformer.models import CandidateEntity, PipelineResult, RawSearchHit, SearchBrief


class RunStorage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with closing(self._connect()) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    brief_json TEXT NOT NULL,
                    queries_json TEXT NOT NULL,
                    raw_hit_count INTEGER NOT NULL,
                    extracted_records INTEGER NOT NULL,
                    unique_candidates INTEGER NOT NULL,
                    verified_count INTEGER NOT NULL,
                    review_count INTEGER NOT NULL,
                    reject_count INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS raw_hits (
                    run_id TEXT NOT NULL,
                    query_text TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    snippet TEXT NOT NULL,
                    url TEXT NOT NULL,
                    source TEXT NOT NULL,
                    metadata_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS candidates (
                    run_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    full_name TEXT NOT NULL,
                    canonical_key TEXT NOT NULL,
                    current_title TEXT NOT NULL,
                    current_company TEXT NOT NULL,
                    current_location TEXT NOT NULL,
                    role_family TEXT NOT NULL,
                    score REAL NOT NULL,
                    semantic_similarity REAL NOT NULL,
                    verification_status TEXT NOT NULL,
                    notes_json TEXT NOT NULL,
                    evidence_json TEXT NOT NULL
                );
                """
            )
            connection.commit()

    def save_run(
        self,
        *,
        run_id: str,
        created_at: str,
        brief: SearchBrief,
        queries: list[str],
        hits: list[RawSearchHit],
        result: PipelineResult,
    ) -> None:
        brief_payload = {
            "role_title": brief.role_title,
            "titles": brief.titles,
            "countries": brief.countries,
            "cities": brief.cities,
            "company_targets": brief.company_targets,
            "required_keywords": brief.required_keywords,
            "preferred_keywords": brief.preferred_keywords,
            "industry_keywords": brief.industry_keywords,
            "target_count": brief.target_count,
        }
        with closing(self._connect()) as connection:
            connection.execute("DELETE FROM runs WHERE run_id = ?", (run_id,))
            connection.execute("DELETE FROM raw_hits WHERE run_id = ?", (run_id,))
            connection.execute("DELETE FROM candidates WHERE run_id = ?", (run_id,))
            connection.execute(
                """
                INSERT INTO runs (
                    run_id, created_at, brief_json, queries_json, raw_hit_count,
                    extracted_records, unique_candidates, verified_count, review_count, reject_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    created_at,
                    json.dumps(brief_payload),
                    json.dumps(queries),
                    result.metrics.raw_found,
                    result.metrics.extracted_records,
                    result.metrics.unique_candidates,
                    result.metrics.verified_count,
                    result.metrics.review_count,
                    result.metrics.reject_count,
                ),
            )
            connection.executemany(
                """
                INSERT INTO raw_hits (
                    run_id, query_text, ordinal, title, snippet, url, source, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        str(hit.metadata.get("query", "")),
                        index,
                        hit.title,
                        hit.snippet,
                        hit.url,
                        hit.source,
                        json.dumps(hit.metadata),
                    )
                    for index, hit in enumerate(hits)
                ],
            )
            connection.executemany(
                """
                INSERT INTO candidates (
                    run_id, ordinal, full_name, canonical_key, current_title, current_company,
                    current_location, role_family, score, semantic_similarity, verification_status,
                    notes_json, evidence_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        index,
                        candidate.full_name,
                        candidate.canonical_key,
                        candidate.current_title,
                        candidate.current_company,
                        candidate.current_location,
                        candidate.role_family,
                        candidate.score,
                        candidate.semantic_similarity,
                        candidate.verification_status,
                        json.dumps(candidate.notes),
                        json.dumps(
                            [
                                {
                                    "source_url": evidence.source_url,
                                    "source_domain": evidence.source_domain,
                                    "source_type": evidence.source_type,
                                    "page_title": evidence.page_title,
                                    "page_snippet": evidence.page_snippet,
                                    "current_title": evidence.current_title,
                                    "current_company": evidence.current_company,
                                    "current_location": evidence.current_location,
                                    "confidence": evidence.confidence,
                                }
                                for evidence in candidate.evidence
                            ]
                        ),
                    )
                    for index, candidate in enumerate(result.candidates)
                ],
            )
            connection.commit()

    def list_runs(self, *, limit: int = 10) -> list[sqlite3.Row]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT run_id, created_at, raw_hit_count, unique_candidates,
                       verified_count, review_count, reject_count
                FROM runs
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return rows

    def fetch_run(self, run_id: str) -> sqlite3.Row | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT *
                FROM runs
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
        return row

    def fetch_candidates(self, run_id: str) -> list[sqlite3.Row]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM candidates
                WHERE run_id = ?
                ORDER BY ordinal ASC
                """,
                (run_id,),
            ).fetchall()
        return rows

    def fetch_raw_hits(self, run_id: str, *, limit: int = 100) -> list[sqlite3.Row]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM raw_hits
                WHERE run_id = ?
                ORDER BY ordinal ASC
                LIMIT ?
                """,
                (run_id, limit),
            ).fetchall()
        return rows
