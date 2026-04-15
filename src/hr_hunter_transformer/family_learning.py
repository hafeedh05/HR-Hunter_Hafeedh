from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from hr_hunter.config import resolve_feedback_db_path
from hr_hunter.db import connect_database, resolve_database_target
from hr_hunter.feedback import NEGATIVE_ACTIONS, POSITIVE_ACTIONS
from hr_hunter_transformer.role_profiles import infer_role_family


@dataclass(frozen=True)
class FamilyLearningStats:
    family: str
    run_count: int = 0
    average_fill_rate: float = 0.0
    average_verified_rate: float = 0.0
    average_review_rate: float = 0.0
    average_reject_rate: float = 0.0
    feedback_count: int = 0
    positive_feedback_rate: float = 0.0
    negative_feedback_rate: float = 0.0


def _default_report_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "output" / "search"


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return max(0.0, float(numerator) / float(denominator))


def _infer_report_family(payload: dict[str, Any], report_path: Path) -> str:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    family = str(summary.get("role_family") or payload.get("role_family") or "").strip()
    if family:
        return family
    candidates = payload.get("candidates") if isinstance(payload.get("candidates"), list) else []
    family_counts: dict[str, int] = {}
    for candidate in candidates[:25]:
        if not isinstance(candidate, dict):
            continue
        candidate_family = str(candidate.get("matched_title_family") or "").strip()
        if candidate_family:
            family_counts[candidate_family] = family_counts.get(candidate_family, 0) + 1
    if family_counts:
        return max(family_counts.items(), key=lambda item: item[1])[0]
    hints = [
        str(payload.get("role_title") or "").strip(),
        str(summary.get("role_title") or "").strip(),
        report_path.stem.replace("-", " "),
    ]
    return infer_role_family(*hints)


def _report_family_stats(report_dir: Path) -> dict[str, dict[str, float]]:
    stats: dict[str, dict[str, float]] = {}
    if not report_dir.exists():
        return stats
    report_paths = sorted(report_dir.glob("*.json"), key=lambda path: path.stat().st_mtime, reverse=True)[:250]
    for report_path in report_paths:
        try:
            payload = json.loads(report_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        family = _infer_report_family(payload, report_path)
        if not family:
            continue
        returned = float(
            summary.get("candidate_count")
            or summary.get("returned_candidate_count")
            or payload.get("candidate_count")
            or len(payload.get("candidates") or [])
            or 0
        )
        requested = float(summary.get("requested_candidate_limit") or payload.get("requested_candidate_limit") or returned or 0)
        verified = float(summary.get("verified_count") or 0)
        review = float(summary.get("review_count") or 0)
        reject = float(summary.get("reject_count") or 0)
        bucket = stats.setdefault(
            family,
            {
                "run_count": 0.0,
                "fill_rate": 0.0,
                "verified_rate": 0.0,
                "review_rate": 0.0,
                "reject_rate": 0.0,
            },
        )
        bucket["run_count"] += 1.0
        bucket["fill_rate"] += min(1.25, _safe_ratio(returned, requested if requested > 0 else returned or 1))
        bucket["verified_rate"] += _safe_ratio(verified, returned if returned > 0 else 1)
        bucket["review_rate"] += _safe_ratio(review, returned if returned > 0 else 1)
        bucket["reject_rate"] += _safe_ratio(reject, returned if returned > 0 else 1)
    return stats


def _feedback_family_stats() -> dict[str, dict[str, float]]:
    target = resolve_database_target(
        resolve_feedback_db_path(),
        env_var="HR_HUNTER_FEEDBACK_DB",
        default_path="output/feedback/hr_hunter_feedback.db",
    )
    stats: dict[str, dict[str, float]] = {}
    try:
        with connect_database(target) as connection:
            rows = connection.execute(
                """
                SELECT b.role_title AS role_title, fe.action AS action
                FROM feedback_events fe
                JOIN briefs b ON b.id = fe.brief_id
                """
            ).fetchall()
    except Exception:
        return stats

    for row in rows:
        role_title = str(row["role_title"] or "").strip()
        family = infer_role_family(role_title)
        if not family or family == "other":
            continue
        action = str(row["action"] or "").strip().lower()
        bucket = stats.setdefault(family, {"count": 0.0, "positive": 0.0, "negative": 0.0})
        bucket["count"] += 1.0
        if action in POSITIVE_ACTIONS:
            bucket["positive"] += 1.0
        elif action in NEGATIVE_ACTIONS:
            bucket["negative"] += 1.0
    return stats


@lru_cache(maxsize=1)
def load_family_learning_stats(
    report_dir_str: str = "",
    feedback_db_marker: str = "",
) -> dict[str, FamilyLearningStats]:
    del feedback_db_marker  # cache-bust marker reserved for future use
    report_dir = Path(report_dir_str).expanduser().resolve() if report_dir_str else _default_report_dir()
    report_stats = _report_family_stats(report_dir)
    feedback_stats = _feedback_family_stats()
    families = set(report_stats) | set(feedback_stats)
    resolved: dict[str, FamilyLearningStats] = {}
    for family in families:
        report_bucket = report_stats.get(family, {})
        feedback_bucket = feedback_stats.get(family, {})
        run_count = int(report_bucket.get("run_count", 0.0) or 0.0)
        feedback_count = int(feedback_bucket.get("count", 0.0) or 0.0)
        divisor = float(run_count or 1)
        resolved[family] = FamilyLearningStats(
            family=family,
            run_count=run_count,
            average_fill_rate=round(float(report_bucket.get("fill_rate", 0.0) or 0.0) / divisor, 4),
            average_verified_rate=round(float(report_bucket.get("verified_rate", 0.0) or 0.0) / divisor, 4),
            average_review_rate=round(float(report_bucket.get("review_rate", 0.0) or 0.0) / divisor, 4),
            average_reject_rate=round(float(report_bucket.get("reject_rate", 0.0) or 0.0) / divisor, 4),
            feedback_count=feedback_count,
            positive_feedback_rate=round(
                _safe_ratio(float(feedback_bucket.get("positive", 0.0) or 0.0), float(feedback_count or 1)),
                4,
            ),
            negative_feedback_rate=round(
                _safe_ratio(float(feedback_bucket.get("negative", 0.0) or 0.0), float(feedback_count or 1)),
                4,
            ),
        )
    return resolved


def family_learning_stats(family: str) -> FamilyLearningStats | None:
    return load_family_learning_stats().get(str(family or "").strip())
