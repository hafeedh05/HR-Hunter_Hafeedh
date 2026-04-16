#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from typing import Any

from hr_hunter.workspace import (
    _connect,
    _delete_artifact,
    _now,
    delete_project_run,
    init_workspace_db,
    list_project_runs,
    list_projects,
)


def _admin_user() -> dict[str, Any]:
    return {
        "id": "user_admin_hrhunter",
        "full_name": "HR Hunter Admin",
        "team_id": "leadership",
        "is_admin": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Prune saved project runs while keeping the newest N per project.")
    parser.add_argument("--keep", type=int, default=2, help="Number of newest runs to keep per project.")
    parser.add_argument("--limit", type=int, default=100, help="Maximum projects/runs to inspect.")
    parser.add_argument("--fast", action="store_true", help="Use direct DB pruning instead of per-run registry rebuilds.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned deletions without mutating state.")
    args = parser.parse_args()

    keep = max(1, int(args.keep or 2))
    limit = max(1, int(args.limit or 100))
    if args.fast:
        return _fast_prune(keep=keep, dry_run=bool(args.dry_run))

    user = _admin_user()
    projects = list_projects(user, query="", limit=limit)
    summary: list[dict[str, Any]] = []
    for project in projects:
        project_id = str(project["id"])
        runs = list_project_runs(user, project_id=project_id, limit=limit)
        removed: list[str] = []
        errors: list[str] = []
        for run in runs[keep:]:
            run_id = str(run.get("run_id") or run.get("id") or "").strip()
            if not run_id:
                continue
            if args.dry_run:
                removed.append(run_id)
                continue
            try:
                delete_project_run(user, project_id=project_id, run_id=run_id)
                removed.append(run_id)
            except Exception as exc:  # pragma: no cover - operator script
                errors.append(f"{run_id}: {exc}")
        remaining = list_project_runs(user, project_id=project_id, limit=limit)
        summary.append(
            {
                "project_id": project_id,
                "name": project.get("name"),
                "before": len(runs),
                "deleted": len(removed),
                "after": len(remaining),
                "kept_run_ids": [run.get("run_id") or run.get("id") for run in remaining[:keep]],
                "errors": errors,
            }
        )
    print(json.dumps({"keep": keep, "dry_run": bool(args.dry_run), "projects": summary}, indent=2))
    return 0


def _fast_prune(*, keep: int, dry_run: bool) -> int:
    init_workspace_db()
    artifacts: list[str] = []
    summary: list[dict[str, Any]] = []
    with _connect(None) as connection:
        rows = connection.execute(
            """
            SELECT id, mandate_id, report_json_path, report_csv_path, created_at
            FROM (
                SELECT
                    id,
                    mandate_id,
                    report_json_path,
                    report_csv_path,
                    created_at,
                    ROW_NUMBER() OVER (PARTITION BY mandate_id ORDER BY created_at DESC, id DESC) AS rn
                FROM search_runs
            ) ranked
            WHERE rn > ?
            ORDER BY mandate_id, created_at DESC
            """,
            (keep,),
        ).fetchall()
        by_project: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            by_project.setdefault(str(row["mandate_id"]), []).append(dict(row))
        for project_id, run_rows in by_project.items():
            deleted_ids = [str(row["id"]) for row in run_rows]
            summary.append({"project_id": project_id, "deleted": len(deleted_ids), "run_ids": deleted_ids})
            if dry_run:
                continue
            for row in run_rows:
                run_id = str(row["id"])
                connection.execute("DELETE FROM run_candidates WHERE mandate_id = ? AND run_id = ?", (project_id, run_id))
                connection.execute("DELETE FROM review_actions WHERE mandate_id = ? AND run_id = ?", (project_id, run_id))
                connection.execute("DELETE FROM audit_events WHERE entity_type = 'search_run' AND entity_id = ?", (run_id,))
                connection.execute("DELETE FROM search_runs WHERE mandate_id = ? AND id = ?", (project_id, run_id))
                artifacts.extend([str(row.get("report_json_path") or ""), str(row.get("report_csv_path") or "")])
            latest = connection.execute(
                """
                SELECT id, created_at
                FROM search_runs
                WHERE mandate_id = ?
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (project_id,),
            ).fetchone()
            latest_run_id = str(latest["id"] or "").strip() if latest else ""
            latest_run_at = str(latest["created_at"] or "").strip() if latest else ""
            connection.execute(
                """
                UPDATE projects
                SET latest_run_id = ?, latest_run_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (latest_run_id, latest_run_at, _now(), project_id),
            )
    if not dry_run:
        for path in artifacts:
            _delete_artifact(path)
    print(json.dumps({"keep": keep, "dry_run": dry_run, "fast": True, "projects": summary}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
