from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import tarfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class RuntimeRestoreDrillConfig:
    workspace_root: Path
    archive_uri: str = ""
    temp_root: Path | None = None
    dry_run: bool = False


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _latest_local_archive(backups_dir: Path) -> Path:
    candidates = sorted(backups_dir.glob("*.tar.gz"))
    if not candidates:
        raise FileNotFoundError(f"No backup archives found in {backups_dir}")
    return candidates[-1]


def _copy_archive_source(source: str, destination: Path, *, dry_run: bool) -> dict[str, Any]:
    normalized = str(source or "").strip()
    if normalized.startswith("gs://"):
        command = ["gcloud", "storage", "cp", normalized, str(destination)]
        if dry_run:
            return {"source": normalized, "command": command, "downloaded": False, "planned": True}
        destination.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(command, check=True, capture_output=True, text=True)
        return {"source": normalized, "command": command, "downloaded": True, "planned": True}
    source_path = Path(normalized).expanduser().resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"Archive source not found: {source_path}")
    if dry_run:
        return {"source": str(source_path), "command": [], "downloaded": False, "planned": True}
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, destination)
    return {"source": str(source_path), "command": [], "downloaded": True, "planned": True}


def _extract_archive(archive_path: Path, destination: Path, *, dry_run: bool) -> bool:
    if dry_run:
        return False
    destination.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path, "r:gz") as handle:
        handle.extractall(destination)
    return True


def _find_backup_dir(extracted_root: Path) -> Path:
    for candidate in sorted(extracted_root.iterdir()):
        if candidate.is_dir() and (candidate / "metadata.json").exists():
            return candidate
    raise FileNotFoundError("Extracted backup metadata.json not found.")


def _validate_feedback_sqlite(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "checked": False, "ok": False, "reason": "missing"}
    with sqlite3.connect(path) as connection:
        row = connection.execute("PRAGMA integrity_check").fetchone()
    return {
        "path": str(path),
        "checked": True,
        "ok": bool(row and str(row[0]).lower() == "ok"),
        "result": str(row[0]) if row else "",
    }


def _validate_pg_dump(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "checked": False, "ok": False, "reason": "missing"}
    if shutil.which("pg_restore") is None:
        return {"path": str(path), "checked": False, "ok": False, "reason": "pg_restore_missing"}
    command = ["pg_restore", "-l", str(path)]
    subprocess.run(command, check=True, capture_output=True, text=True)
    return {"path": str(path), "checked": True, "ok": True, "command": command}


def _resolve_db_snapshot_path(metadata: dict[str, Any], backup_dir: Path) -> Path:
    db_snapshot = metadata.get("db_snapshot") or {}
    raw_path = str(db_snapshot.get("path", "") or "").strip()
    if raw_path:
        path = Path(raw_path)
        return path if path.is_absolute() else backup_dir / path
    backend = str((metadata.get("workspace_storage") or {}).get("backend", "") or "").strip().lower()
    if backend == "postgres":
        fallback = backup_dir / "state" / "workspace.pg.dump"
        if fallback.exists():
            return fallback
    for candidate in sorted((backup_dir / "state").glob("*")):
        if candidate.is_file() and candidate.suffix.lower() in {".db", ".sqlite", ".dump"}:
            return candidate
    return Path()


def _resolve_feedback_snapshot_path(metadata: dict[str, Any], backup_dir: Path) -> Path:
    raw_path = str(metadata.get("feedback_snapshot", "") or "").strip()
    if raw_path:
        path = Path(raw_path)
        return path if path.is_absolute() else backup_dir / path
    for candidate in sorted((backup_dir / "feedback").glob("*")):
        if candidate.is_file() and candidate.suffix.lower() in {".db", ".sqlite"}:
            return candidate
    return Path()


def run_runtime_restore_drill(config: RuntimeRestoreDrillConfig) -> dict[str, Any]:
    workspace_root = config.workspace_root.expanduser().resolve()
    backups_dir = workspace_root / "backups"
    selected_source = str(config.archive_uri or "").strip() or str(_latest_local_archive(backups_dir))
    temp_root = (config.temp_root or (workspace_root / "shared" / "monitoring" / "restore-drills")).expanduser().resolve()
    drill_dir = temp_root / f"{_timestamp()}-restore-drill"
    archive_path = drill_dir / "archive.tar.gz"
    extracted_root = drill_dir / "extracted"

    if not config.dry_run:
        drill_dir.mkdir(parents=True, exist_ok=True)
    source_result = _copy_archive_source(selected_source, archive_path, dry_run=config.dry_run)
    extracted = _extract_archive(archive_path, extracted_root, dry_run=config.dry_run)
    metadata: dict[str, Any] = {}
    backup_dir = Path()
    db_check: dict[str, Any] = {}
    feedback_check: dict[str, Any] = {}
    if not config.dry_run:
        backup_dir = _find_backup_dir(extracted_root)
        metadata = json.loads((backup_dir / "metadata.json").read_text(encoding="utf-8"))
        db_path = _resolve_db_snapshot_path(metadata, backup_dir)
        if str((metadata.get("workspace_storage") or {}).get("backend", "")) == "postgres":
            db_check = _validate_pg_dump(db_path)
        else:
            db_check = _validate_feedback_sqlite(db_path)
        feedback_snapshot = _resolve_feedback_snapshot_path(metadata, backup_dir)
        feedback_check = _validate_feedback_sqlite(feedback_snapshot) if str(feedback_snapshot) else {
            "path": "",
            "checked": False,
            "ok": True,
            "reason": "not_present",
        }
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "workspace_root": str(workspace_root),
        "selected_source": selected_source,
        "drill_dir": str(drill_dir),
        "archive_path": str(archive_path),
        "archive_source": source_result,
        "archive_extracted": bool(extracted),
        "backup_dir": str(backup_dir) if str(backup_dir) else "",
        "metadata": metadata,
        "db_check": db_check,
        "feedback_check": feedback_check,
        "dry_run": bool(config.dry_run),
        "ok": bool(config.dry_run or (db_check.get("ok") and feedback_check.get("ok"))),
    }
