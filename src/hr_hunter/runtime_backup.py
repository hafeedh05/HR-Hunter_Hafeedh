from __future__ import annotations

import json
import shutil
import subprocess
import tarfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hr_hunter.config import resolve_feedback_db_path
from hr_hunter.db import describe_database_target, resolve_database_target
from hr_hunter.runtime_maintenance import (
    collect_referenced_artifact_paths,
    resolve_current_release_path,
)


DEFAULT_SERVICE_FILE = Path("/etc/systemd/system/hr-hunter.service")
DEFAULT_PROXY_FILE = Path("/etc/caddy/Caddyfile")
DEFAULT_ENV_FILE = Path("/srv/hr-hunter/shared/env/hr-hunter.env")


@dataclass(slots=True)
class RuntimeBackupConfig:
    workspace_root: Path
    label: str = "runtimebackup"
    backup_root: Path | None = None
    service_file: Path = DEFAULT_SERVICE_FILE
    proxy_file: Path = DEFAULT_PROXY_FILE
    env_file: Path = DEFAULT_ENV_FILE
    bucket_uri: str = ""
    dry_run: bool = False


def _resolve_workspace_db_target(explicit: str | Path | None = None) -> Any:
    return resolve_database_target(
        explicit,
        env_var="HR_HUNTER_STATE_DB",
        default_path="output/state/hr_hunter_state.db",
    )


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _slugify(value: str) -> str:
    cleaned = "".join(char.lower() if char.isalnum() else "-" for char in str(value or "").strip())
    parts = [part for part in cleaned.split("-") if part]
    return "-".join(parts[:6]) or "runtimebackup"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _copy_if_exists(source: Path, destination: Path, *, dry_run: bool) -> bool:
    if not source.exists():
        return False
    if dry_run:
        return True
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    return True


def _redact_env_text(raw_text: str) -> str:
    redacted_lines: list[str] = []
    sensitive_tokens = ("KEY", "SECRET", "TOKEN", "PASSWORD", "DATABASE_URL", "CONNECTION", "CREDENTIAL")
    for line in raw_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            redacted_lines.append(line)
            continue
        key, _value = line.split("=", 1)
        marker = "<redacted>" if any(token in key.upper() for token in sensitive_tokens) else "<set>"
        redacted_lines.append(f"{key}={marker}")
    return "\n".join(redacted_lines) + ("\n" if raw_text.endswith("\n") else "")


def _write_env_snapshot(source: Path, destination: Path, *, dry_run: bool) -> bool:
    if not source.exists():
        return False
    if dry_run:
        return True
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(_redact_env_text(source.read_text(encoding="utf-8")), encoding="utf-8")
    return True


def _pg_dump(locator: str, destination: Path, *, dry_run: bool) -> dict[str, Any]:
    command = ["pg_dump", locator, "-Fc", "-f", str(destination)]
    if dry_run:
        return {"command": command, "ran": False}
    destination.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(command, check=True, capture_output=True, text=True)
    return {"command": command, "ran": True}


def _copy_sqlite(path: Path, destination: Path, *, dry_run: bool) -> bool:
    if not path.exists():
        return False
    if dry_run:
        return True
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, destination)
    return True


def _create_archive(source_dir: Path, archive_path: Path, *, dry_run: bool) -> bool:
    if dry_run:
        return True
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path, "w:gz") as handle:
        handle.add(source_dir, arcname=source_dir.name)
    return True


def _upload_archive(archive_path: Path, bucket_uri: str, *, dry_run: bool) -> dict[str, Any]:
    normalized = str(bucket_uri or "").strip().rstrip("/")
    if not normalized:
        return {"uploaded": False, "planned": False, "bucket_uri": "", "archive_uri": ""}
    archive_uri = f"{normalized}/{archive_path.name}"
    command = ["gcloud", "storage", "cp", str(archive_path), archive_uri]
    if dry_run:
        return {
            "uploaded": False,
            "planned": True,
            "bucket_uri": normalized,
            "archive_uri": archive_uri,
            "command": command,
            "ran": False,
        }
    subprocess.run(command, check=True, capture_output=True, text=True)
    return {
        "uploaded": True,
        "planned": True,
        "bucket_uri": normalized,
        "archive_uri": archive_uri,
        "command": command,
        "ran": True,
    }


def run_runtime_backup(config: RuntimeBackupConfig) -> dict[str, Any]:
    workspace_root = config.workspace_root.expanduser().resolve()
    backup_root = (config.backup_root or (workspace_root / "backups")).expanduser().resolve()
    backup_id = f"{_timestamp()}-{_slugify(config.label)}"
    backup_dir = backup_root / backup_id
    archive_path = backup_root / f"{backup_id}.tar.gz"
    current_release = resolve_current_release_path(workspace_root)
    workspace_target = _resolve_workspace_db_target()
    workspace_storage = describe_database_target(workspace_target)
    feedback_path = resolve_feedback_db_path()
    artifact_manifest = collect_referenced_artifact_paths()

    copied_files: list[str] = []
    if not config.dry_run:
        backup_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "backup_id": backup_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "workspace_root": str(workspace_root),
        "current_release": str(current_release) if current_release else "",
        "workspace_storage": workspace_storage,
        "feedback_db": str(feedback_path),
        "bucket_uri": str(config.bucket_uri or "").strip(),
        "referenced_run_count": int(artifact_manifest["run_count"]),
    }
    if not config.dry_run:
        _write_json(backup_dir / "metadata.json", metadata)
        _write_json(
            backup_dir / "artifacts" / "referenced-artifacts.json",
            {
                "storage": artifact_manifest["storage"],
                "run_count": int(artifact_manifest["run_count"]),
                "paths": sorted(str(path) for path in artifact_manifest["referenced_paths"]),
            },
        )

    if _copy_if_exists(config.service_file, backup_dir / "config" / config.service_file.name, dry_run=config.dry_run):
        copied_files.append(str(config.service_file))
    if _copy_if_exists(config.proxy_file, backup_dir / "config" / config.proxy_file.name, dry_run=config.dry_run):
        copied_files.append(str(config.proxy_file))
    if _write_env_snapshot(config.env_file, backup_dir / "config" / f"{config.env_file.name}.redacted", dry_run=config.dry_run):
        copied_files.append(f"{config.env_file} (redacted)")

    db_snapshot: dict[str, Any] = {"backend": workspace_storage["backend"], "path": "", "command": []}
    if workspace_target.backend == "postgres":
        dump_path = backup_dir / "state" / "workspace.pg.dump"
        pg_dump_result = _pg_dump(workspace_target.locator, dump_path, dry_run=config.dry_run)
        db_snapshot["path"] = str(dump_path)
        db_snapshot["command"] = list(pg_dump_result["command"])
    elif workspace_target.path is not None:
        sqlite_target = backup_dir / "state" / workspace_target.path.name
        _copy_sqlite(workspace_target.path, sqlite_target, dry_run=config.dry_run)
        db_snapshot["path"] = str(sqlite_target)

    feedback_snapshot = ""
    if feedback_path.exists():
        feedback_target = backup_dir / "feedback" / feedback_path.name
        _copy_sqlite(feedback_path, feedback_target, dry_run=config.dry_run)
        feedback_snapshot = str(feedback_target)

    archive_created = _create_archive(backup_dir, archive_path, dry_run=config.dry_run)
    upload_result = _upload_archive(archive_path, config.bucket_uri, dry_run=config.dry_run)

    result = {
        "backup_id": backup_id,
        "backup_dir": str(backup_dir),
        "archive_path": str(archive_path),
        "archive_created": bool(archive_created and not config.dry_run),
        "archive_planned": bool(archive_created),
        "workspace_root": str(workspace_root),
        "current_release": str(current_release) if current_release else "",
        "workspace_storage": workspace_storage,
        "db_snapshot": db_snapshot,
        "feedback_snapshot": feedback_snapshot,
        "copied_files": copied_files,
        "upload": upload_result,
        "dry_run": bool(config.dry_run),
    }
    if not config.dry_run:
        _write_json(backup_dir / "metadata.json", {**metadata, **result})
    return result
