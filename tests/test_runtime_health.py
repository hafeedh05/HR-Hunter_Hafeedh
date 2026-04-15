from __future__ import annotations

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
