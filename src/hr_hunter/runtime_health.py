from __future__ import annotations

import json
import os
import shutil
import subprocess
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hr_hunter.config import resolve_feedback_db_path, resolve_output_dir
from hr_hunter.config import env_flag
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


def _load_previous_snapshot(snapshot_dir: Path | None) -> dict[str, Any]:
    if snapshot_dir is None:
        return {}
    latest_path = snapshot_dir.expanduser().resolve() / "latest.json"
    if not latest_path.exists():
        return {}
    try:
        return json.loads(latest_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _alert_webhook_url() -> str:
    return str(os.getenv("HR_HUNTER_ALERT_WEBHOOK_URL", "") or "").strip()


def _alert_headers() -> dict[str, str]:
    raw = str(os.getenv("HR_HUNTER_ALERT_WEBHOOK_HEADERS_JSON", "") or "").strip()
    if not raw:
        return {"Content-Type": "application/json"}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {"Content-Type": "application/json"}
    if not isinstance(payload, dict):
        return {"Content-Type": "application/json"}
    headers = {"Content-Type": "application/json"}
    for key, value in payload.items():
        if str(key).strip():
            headers[str(key).strip()] = str(value)
    return headers


def _post_alert(url: str, payload: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        response_body = response.read().decode("utf-8", errors="replace")
        return {
            "status_code": int(response.status),
            "body": response_body[:2000],
        }


def _maybe_send_alert(result: dict[str, Any], previous: dict[str, Any]) -> dict[str, Any]:
    webhook_url = _alert_webhook_url()
    alert: dict[str, Any] = {
        "configured": bool(webhook_url),
        "attempted": False,
        "delivered": False,
        "event_type": "",
        "reason": "",
    }
    previously_healthy = bool(previous.get("healthy", True)) if previous else True
    currently_healthy = bool(result.get("healthy", False))
    send_recovery = env_flag("HR_HUNTER_ALERT_ON_HEALTHY_RECOVERY", default=True)
    if currently_healthy and (previously_healthy or not send_recovery):
        alert["reason"] = "healthy_no_alert"
        return alert
    if currently_healthy and not previously_healthy:
        alert["event_type"] = "runtime_recovered"
    elif not currently_healthy:
        alert["event_type"] = "runtime_unhealthy"
    else:
        alert["reason"] = "no_state_change"
        return alert
    if not webhook_url:
        alert["reason"] = "webhook_not_configured"
        return alert
    payload = {
        "event_type": alert["event_type"],
        "service": result.get("service", {}),
        "health": result.get("health", {}),
        "disk": result.get("disk", {}),
        "warnings": list(result.get("warnings", [])),
        "current_release": result.get("current_release", ""),
        "generated_at": result.get("generated_at", ""),
    }
    alert["attempted"] = True
    try:
        response = _post_alert(webhook_url, payload, _alert_headers())
    except Exception as exc:
        alert["reason"] = str(exc)
        return alert
    alert["delivered"] = True
    alert["status_code"] = int(response.get("status_code", 0) or 0)
    alert["reason"] = "sent"
    return alert


def run_runtime_healthcheck(config: RuntimeHealthcheckConfig) -> dict[str, Any]:
    workspace_root = config.workspace_root.expanduser().resolve()
    previous_snapshot = _load_previous_snapshot(config.snapshot_dir)
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
    result["alert"] = _maybe_send_alert(result, previous_snapshot) if not config.dry_run else {
        "configured": bool(_alert_webhook_url()),
        "attempted": False,
        "delivered": False,
        "event_type": "",
        "reason": "dry_run",
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
