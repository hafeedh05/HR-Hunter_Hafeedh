from __future__ import annotations

import sqlite3
from pathlib import Path

from hr_hunter.runtime_backup import RuntimeBackupConfig, run_runtime_backup


def test_runtime_backup_copies_sqlite_feedback_and_configs(tmp_path: Path, monkeypatch) -> None:
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir()
    (workspace_root / "backups").mkdir()
    releases_dir = workspace_root / "releases"
    releases_dir.mkdir()
    release = releases_dir / "20260415T010000Z-abcd123"
    release.mkdir()
    (workspace_root / "current").symlink_to(release)

    state_db = tmp_path / "state.db"
    feedback_db = tmp_path / "feedback.db"
    for path in (state_db, feedback_db):
        connection = sqlite3.connect(path)
        connection.execute("CREATE TABLE test (id TEXT PRIMARY KEY)")
        connection.commit()
        connection.close()

    service_file = tmp_path / "hr-hunter.service"
    proxy_file = tmp_path / "Caddyfile"
    env_file = tmp_path / "hr-hunter.env"
    service_file.write_text("[Service]\n", encoding="utf-8")
    proxy_file.write_text(":8765\n", encoding="utf-8")
    env_file.write_text("SCRAPINGBEE_API_KEY=abc123\nREGION=us-central1\n", encoding="utf-8")

    monkeypatch.setenv("HR_HUNTER_STATE_DB", str(state_db))
    monkeypatch.delenv("HR_HUNTER_DATABASE_URL", raising=False)
    monkeypatch.setenv("HR_HUNTER_FEEDBACK_DB", str(feedback_db))
    monkeypatch.setenv("HR_HUNTER_OUTPUT_DIR", str(tmp_path / "output" / "search"))
    monkeypatch.setattr(
        "hr_hunter.runtime_backup.collect_referenced_artifact_paths",
        lambda: {
            "referenced_paths": set(),
            "run_count": 0,
            "storage": {"backend": "sqlite", "display_locator": str(state_db), "credentials_redacted": False},
        },
    )

    result = run_runtime_backup(
        RuntimeBackupConfig(
            workspace_root=workspace_root,
            service_file=service_file,
            proxy_file=proxy_file,
            env_file=env_file,
        )
    )

    backup_dir = Path(result["backup_dir"])
    assert backup_dir.exists()
    assert (backup_dir / "config" / "hr-hunter.service").exists()
    assert (backup_dir / "config" / "Caddyfile").exists()
    redacted_env = (backup_dir / "config" / "hr-hunter.env.redacted").read_text(encoding="utf-8")
    assert "<redacted>" in redacted_env
    assert "<set>" in redacted_env
    assert (backup_dir / "state" / "state.db").exists()
    assert (backup_dir / "feedback" / "feedback.db").exists()
    assert Path(result["archive_path"]).exists()


def test_runtime_backup_uses_pg_dump_and_gcloud_for_postgres_upload(tmp_path: Path, monkeypatch) -> None:
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir()
    (workspace_root / "backups").mkdir()
    monkeypatch.setenv("HR_HUNTER_OUTPUT_DIR", str(tmp_path / "output" / "search"))

    calls: list[list[str]] = []

    def _fake_run(command, check, capture_output, text):  # type: ignore[no-untyped-def]
        calls.append(list(command))
        if command[0] == "pg_dump":
            dump_path = Path(command[-1])
            dump_path.parent.mkdir(parents=True, exist_ok=True)
            dump_path.write_text("pgdump", encoding="utf-8")
        return None

    monkeypatch.setenv("HR_HUNTER_DATABASE_URL", "postgresql://user:pass@example.com/hr_hunter")
    monkeypatch.setenv("HR_HUNTER_FEEDBACK_DB", str(tmp_path / "missing-feedback.db"))
    monkeypatch.setattr("subprocess.run", _fake_run)
    monkeypatch.setattr(
        "hr_hunter.runtime_backup.collect_referenced_artifact_paths",
        lambda: {
            "referenced_paths": set(),
            "run_count": 0,
            "storage": {"backend": "postgres", "display_locator": "postgresql://<redacted>/hr_hunter", "credentials_redacted": True},
        },
    )

    result = run_runtime_backup(
        RuntimeBackupConfig(
            workspace_root=workspace_root,
            bucket_uri="gs://hr-hunter-prod-backups/test",
        )
    )

    assert result["workspace_storage"]["backend"] == "postgres"
    assert calls[0][0] == "pg_dump"
    assert calls[1][:3] == ["gcloud", "storage", "cp"]
    assert result["upload"]["archive_uri"].startswith("gs://hr-hunter-prod-backups/test/")
