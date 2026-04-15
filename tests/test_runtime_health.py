from __future__ import annotations

import json
from collections import namedtuple
from pathlib import Path

from hr_hunter.runtime_health import RuntimeHealthcheckConfig, run_runtime_healthcheck


def test_runtime_healthcheck_records_snapshot_and_marks_healthy(tmp_path: Path, monkeypatch) -> None:
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir()
    snapshot_dir = tmp_path / "snapshots"
    usage = namedtuple("usage", "total used free")(1_000_000, 100_000, 900_000)

    monkeypatch.setattr(
        "hr_hunter.runtime_health.shutil.disk_usage",
        lambda _path: usage,
    )
    monkeypatch.setattr(
        "hr_hunter.runtime_health._systemctl_state",
        lambda _service: {"active": "active", "enabled": "enabled", "error": ""},
    )
    monkeypatch.setattr(
        "hr_hunter.runtime_health._fetch_health",
        lambda _url: {"ok": True, "status_code": 200, "body": '{"status":"ok"}', "error": ""},
    )

    result = run_runtime_healthcheck(
        RuntimeHealthcheckConfig(
            workspace_root=workspace_root,
            snapshot_dir=snapshot_dir,
        )
    )

    assert result["healthy"] is True
    assert Path(result["snapshot_latest"]).exists()
    assert Path(result["snapshot_path"]).exists()
    assert result["warnings"] == []


def test_runtime_healthcheck_flags_disk_pressure(tmp_path: Path, monkeypatch) -> None:
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir()
    usage = namedtuple("usage", "total used free")(1_000_000, 950_000, 50_000)

    monkeypatch.setattr(
        "hr_hunter.runtime_health.shutil.disk_usage",
        lambda _path: usage,
    )
    monkeypatch.setattr(
        "hr_hunter.runtime_health._systemctl_state",
        lambda _service: {"active": "active", "enabled": "enabled", "error": ""},
    )
    monkeypatch.setattr(
        "hr_hunter.runtime_health._fetch_health",
        lambda _url: {"ok": True, "status_code": 200, "body": '{"status":"ok"}', "error": ""},
    )

    result = run_runtime_healthcheck(
        RuntimeHealthcheckConfig(
            workspace_root=workspace_root,
            max_disk_percent=90,
        )
    )

    assert result["healthy"] is False
    assert "disk_pressure" in result["warnings"]


def test_runtime_healthcheck_sends_alert_when_transitioning_to_unhealthy(tmp_path: Path, monkeypatch) -> None:
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir()
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    (snapshot_dir / "latest.json").write_text(json.dumps({"healthy": True}), encoding="utf-8")
    usage = namedtuple("usage", "total used free")(1_000_000, 910_000, 90_000)

    monkeypatch.setattr(
        "hr_hunter.runtime_health.shutil.disk_usage",
        lambda _path: usage,
    )
    monkeypatch.setattr(
        "hr_hunter.runtime_health._systemctl_state",
        lambda _service: {"active": "active", "enabled": "enabled", "error": ""},
    )
    monkeypatch.setattr(
        "hr_hunter.runtime_health._fetch_health",
        lambda _url: {"ok": True, "status_code": 200, "body": '{"status":"ok"}', "error": ""},
    )
    monkeypatch.setenv("HR_HUNTER_ALERT_WEBHOOK_URL", "https://alerts.example/webhook")
    delivered: list[dict[str, object]] = []

    def _fake_post(url: str, payload: dict[str, object], headers: dict[str, str]) -> dict[str, object]:
        delivered.append({"url": url, "payload": payload, "headers": headers})
        return {"status_code": 200, "body": "ok"}

    monkeypatch.setattr("hr_hunter.runtime_health._post_alert", _fake_post)

    result = run_runtime_healthcheck(
        RuntimeHealthcheckConfig(
            workspace_root=workspace_root,
            snapshot_dir=snapshot_dir,
            max_disk_percent=90,
        )
    )

    assert result["healthy"] is False
    assert result["alert"]["configured"] is True
    assert result["alert"]["attempted"] is True
    assert result["alert"]["delivered"] is True
    assert result["alert"]["event_type"] == "runtime_unhealthy"
    assert delivered[0]["payload"]["event_type"] == "runtime_unhealthy"


def test_runtime_healthcheck_recovery_alert_requires_previous_failure(tmp_path: Path, monkeypatch) -> None:
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir()
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    (snapshot_dir / "latest.json").write_text(json.dumps({"healthy": False}), encoding="utf-8")
    usage = namedtuple("usage", "total used free")(1_000_000, 100_000, 900_000)

    monkeypatch.setattr(
        "hr_hunter.runtime_health.shutil.disk_usage",
        lambda _path: usage,
    )
    monkeypatch.setattr(
        "hr_hunter.runtime_health._systemctl_state",
        lambda _service: {"active": "active", "enabled": "enabled", "error": ""},
    )
    monkeypatch.setattr(
        "hr_hunter.runtime_health._fetch_health",
        lambda _url: {"ok": True, "status_code": 200, "body": '{"status":"ok"}', "error": ""},
    )
    monkeypatch.setenv("HR_HUNTER_ALERT_WEBHOOK_URL", "https://alerts.example/webhook")
    deliveries: list[str] = []
    monkeypatch.setattr(
        "hr_hunter.runtime_health._post_alert",
        lambda _url, payload, _headers: deliveries.append(str(payload.get("event_type"))) or {"status_code": 200, "body": "ok"},
    )

    result = run_runtime_healthcheck(
        RuntimeHealthcheckConfig(
            workspace_root=workspace_root,
            snapshot_dir=snapshot_dir,
        )
    )

    assert result["healthy"] is True
    assert result["alert"]["event_type"] == "runtime_recovered"
    assert deliveries == ["runtime_recovered"]
