from __future__ import annotations

import json
import shutil
import subprocess
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hr_hunter.config import resolve_feedback_db_path, resolve_output_dir
from hr_hunter.db import describe_database_target, resolve_database_target
from hr_hunter.runtime_maintenance import resolve_current_release_path


@dataclass(slots=True)
class RuntimeHealthcheckConfig:
    workspace_root: Path
    service_name: str = "hr-hunter"
    health_url: str = "http://127.0.0.1:8765/healthz"
    max_disk_percent: int = 90
    snapshot_dir: Path | None = None
    dry_run: bool = False


def _resolve_workspace_db_target(explicit: str | Path | None = None) -> Any:
    return resolve_database_target(
        explicit,
        env_var="HR_HUNTER_STATE_DB",
        default_path="output/state/hr_hunter_state.db",
    )


def _systemctl_state(service_name: str) -> dict[str, str]:
    try:
        active = subprocess.run(
            ["systemctl", "is-active", service_name],
            check=False,
            capture_output=True,
            text=True,
        )
        enabled = subprocess.run(
            ["systemctl", "is-enabled", service_name],
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception as exc:
        return {
            "active": "unknown",
            "enabled": "unknown",
            "error": str(exc),
        }
    return {
        "active": active.stdout.strip() or active.stderr.strip() or "unknown",
        "enabled": enabled.stdout.strip() or enabled.stderr.strip() or "unknown",
        "error": "",
    }


def _fetch_health(url: str) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(url, timeout=8) as response:
            raw_body = response.read().decode("utf-8", errors="replace")
            return {
                "ok": 200 <= int(response.status) < 300,
                "status_code": int(response.status),
                "body": raw_body.strip(),
                "error": "",
            }
    except urllib.error.URLError as exc:
        return {
            "ok": False,
            "status_code": 0,
            "body": "",
            "error": str(exc),
        }


def run_runtime_healthcheck(config: RuntimeHealthcheckConfig) -> dict[str, Any]:
    workspace_root = config.workspace_root.expanduser().resolve()
    usage = shutil.disk_usage(workspace_root)
    used_percent = round((usage.used / usage.total) * 100, 2) if usage.total else 0.0
    current_release = resolve_current_release_path(workspace_root)
    service_state = _systemctl_state(config.service_name)
    health = _fetch_health(config.health_url)
    workspace_storage = describe_database_target(_resolve_workspace_db_target())
    warnings: list[str] = []
    if used_percent >= float(config.max_disk_percent):
        warnings.append("disk_pressure")
    if service_state["active"] != "active":
        warnings.append("service_not_active")
    if not bool(health["ok"]):
        warnings.append("health_endpoint_failed")

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "workspace_root": str(workspace_root),
        "current_release": str(current_release) if current_release else "",
        "service": {
            "name": config.service_name,
            **service_state,
        },
        "health": health,
        "disk": {
            "total_bytes": int(usage.total),
            "used_bytes": int(usage.used),
            "free_bytes": int(usage.free),
            "used_percent": float(used_percent),
            "threshold_percent": int(config.max_disk_percent),
        },
        "paths": {
            "output_dir": str(resolve_output_dir()),
            "feedback_db": str(resolve_feedback_db_path()),
        },
        "workspace_storage": workspace_storage,
        "warnings": warnings,
        "healthy": len(warnings) == 0,
        "dry_run": bool(config.dry_run),
    }
    if config.snapshot_dir is not None and not config.dry_run:
        snapshot_dir = config.snapshot_dir.expanduser().resolve()
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        latest_path = snapshot_dir / "latest.json"
        history_path = snapshot_dir / f"{timestamp}.json"
        payload = json.dumps(result, indent=2, sort_keys=True)
        latest_path.write_text(payload, encoding="utf-8")
        history_path.write_text(payload, encoding="utf-8")
        result["snapshot_latest"] = str(latest_path)
        result["snapshot_path"] = str(history_path)
    return result
