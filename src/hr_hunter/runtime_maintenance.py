from __future__ import annotations

import json
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from hr_hunter.config import resolve_output_dir
from hr_hunter.db import connect_database, describe_database_target, resolve_database_target


DEFAULT_KEEP_RELEASES = 8
DEFAULT_KEEP_BACKUPS = 30
DEFAULT_BACKUP_MIN_AGE_DAYS = 14
DEFAULT_ARTIFACT_MAX_AGE_DAYS = 45


@dataclass(slots=True)
class RuntimeMaintenanceConfig:
    workspace_root: Path
    keep_releases: int = DEFAULT_KEEP_RELEASES
    keep_backups: int = DEFAULT_KEEP_BACKUPS
    backup_min_age_days: int = DEFAULT_BACKUP_MIN_AGE_DAYS
    artifact_max_age_days: int = DEFAULT_ARTIFACT_MAX_AGE_DAYS
    dry_run: bool = False


def _sorted_dirs(path: Path) -> list[Path]:
    if not path.exists():
        return []
    return sorted([entry for entry in path.iterdir() if entry.is_dir()], key=lambda entry: entry.name)


def _path_mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


def _path_size_bytes(path: Path) -> int:
    try:
        if path.is_symlink():
            return 0
        if path.is_file():
            return int(path.stat().st_size)
        total = 0
        for root, dirs, files in os.walk(path):
            total += 0
            for name in files:
                candidate = Path(root) / name
                if candidate.is_symlink():
                    continue
                try:
                    total += int(candidate.stat().st_size)
                except OSError:
                    continue
        return total
    except OSError:
        return 0


def _delete_path(path: Path, *, dry_run: bool) -> bool:
    if not path.exists() and not path.is_symlink():
        return False
    if dry_run:
        return True
    if path.is_symlink() or path.is_file():
        path.unlink(missing_ok=True)
        return True
    shutil.rmtree(path, ignore_errors=True)
    return True


def resolve_current_release_path(workspace_root: Path) -> Path | None:
    current_path = workspace_root / "current"
    if not current_path.exists() and not current_path.is_symlink():
        return None
    try:
        return current_path.resolve()
    except OSError:
        return None


def select_release_prune_paths(
    releases_dir: Path,
    *,
    current_release: Path | None,
    keep_releases: int,
) -> list[Path]:
    keep_releases = max(1, int(keep_releases or 1))
    releases = _sorted_dirs(releases_dir)
    keep: set[Path] = set(releases[-keep_releases:])
    if current_release is not None:
        keep.add(current_release)
    return [path for path in releases if path not in keep]


def select_backup_prune_paths(
    backups_dir: Path,
    *,
    keep_backups: int,
    backup_min_age_days: int,
    now_ts: float | None = None,
) -> list[Path]:
    keep_backups = max(0, int(keep_backups or 0))
    min_age_seconds = max(0, int(backup_min_age_days or 0)) * 86400
    now_ts = float(now_ts or time.time())
    backups = _sorted_dirs(backups_dir)
    keep: set[Path] = set(backups[-keep_backups:]) if keep_backups else set()
    prune: list[Path] = []
    for path in backups:
        age_seconds = max(0.0, now_ts - _path_mtime(path))
        if path in keep or age_seconds < min_age_seconds:
            continue
        prune.append(path)
    return prune


def _resolve_workspace_db_target(explicit: str | Path | None = None) -> Any:
    return resolve_database_target(
        explicit,
        env_var="HR_HUNTER_STATE_DB",
        default_path="output/state/hr_hunter_state.db",
    )


def collect_referenced_artifact_paths(
    *,
    db_locator: str | Path | None = None,
) -> dict[str, Any]:
    target = _resolve_workspace_db_target(db_locator)
    referenced: set[Path] = set()
    run_count = 0
    try:
        with connect_database(target) as connection:
            rows = connection.execute(
                """
                SELECT report_json_path, report_csv_path
                FROM search_runs
                """
            ).fetchall()
    except Exception:
        rows = []
    for row in rows:
        run_count += 1
        for key in ("report_json_path", "report_csv_path"):
            raw_path = str(row.get(key, "") if isinstance(row, dict) else row[key] or "").strip()
            if not raw_path:
                continue
            referenced.add(Path(raw_path).expanduser().resolve())
    return {
        "referenced_paths": referenced,
        "run_count": run_count,
        "storage": describe_database_target(target),
    }


def _artifact_is_referenced(path: Path, referenced_paths: set[Path]) -> bool:
    resolved = path.expanduser().resolve()
    if resolved in referenced_paths:
        return True
    if resolved.name.endswith("-client.csv"):
        base_csv = resolved.with_name(f"{resolved.name[:-11]}.csv")
        if base_csv in referenced_paths:
            return True
    return False


def select_orphan_artifact_paths(
    output_dir: Path,
    *,
    referenced_paths: set[Path],
    artifact_max_age_days: int,
    now_ts: float | None = None,
) -> list[Path]:
    if not output_dir.exists():
        return []
    max_age_seconds = max(0, int(artifact_max_age_days or 0)) * 86400
    now_ts = float(now_ts or time.time())
    candidates: list[Path] = []
    for suffix in ("*.json", "*.csv"):
        candidates.extend(output_dir.glob(suffix))
    prune: list[Path] = []
    for path in sorted(set(candidates)):
        age_seconds = max(0.0, now_ts - _path_mtime(path))
        if age_seconds < max_age_seconds:
            continue
        if _artifact_is_referenced(path, referenced_paths):
            continue
        prune.append(path)
    return prune


def _cleanup_summary(paths: Iterable[Path], *, dry_run: bool) -> dict[str, Any]:
    deleted: list[str] = []
    bytes_estimate = 0
    for path in paths:
        bytes_estimate += _path_size_bytes(path)
        if _delete_path(path, dry_run=dry_run):
            deleted.append(str(path))
    return {
        "deleted_count": len(deleted),
        "bytes_estimate": int(bytes_estimate),
        "paths": deleted,
    }


def run_runtime_maintenance(config: RuntimeMaintenanceConfig) -> dict[str, Any]:
    workspace_root = config.workspace_root.expanduser().resolve()
    current_release = resolve_current_release_path(workspace_root)
    releases_dir = workspace_root / "releases"
    backups_dir = workspace_root / "backups"
    output_dir = resolve_output_dir()
    referenced = collect_referenced_artifact_paths()
    release_prune = select_release_prune_paths(
        releases_dir,
        current_release=current_release,
        keep_releases=config.keep_releases,
    )
    backup_prune = select_backup_prune_paths(
        backups_dir,
        keep_backups=config.keep_backups,
        backup_min_age_days=config.backup_min_age_days,
    )
    artifact_prune = select_orphan_artifact_paths(
        output_dir,
        referenced_paths=referenced["referenced_paths"],
        artifact_max_age_days=config.artifact_max_age_days,
    )
    release_summary = _cleanup_summary(release_prune, dry_run=config.dry_run)
    backup_summary = _cleanup_summary(backup_prune, dry_run=config.dry_run)
    artifact_summary = _cleanup_summary(artifact_prune, dry_run=config.dry_run)
    return {
        "workspace_root": str(workspace_root),
        "current_release": str(current_release) if current_release else "",
        "output_dir": str(output_dir),
        "dry_run": bool(config.dry_run),
        "db_storage": referenced["storage"],
        "referenced_run_count": int(referenced["run_count"]),
        "release_cleanup": release_summary,
        "backup_cleanup": backup_summary,
        "artifact_cleanup": artifact_summary,
        "total_bytes_estimate": int(
            release_summary["bytes_estimate"]
            + backup_summary["bytes_estimate"]
            + artifact_summary["bytes_estimate"]
        ),
    }


def runtime_maintenance_json(config: RuntimeMaintenanceConfig) -> str:
    return json.dumps(run_runtime_maintenance(config), indent=2, sort_keys=True)
