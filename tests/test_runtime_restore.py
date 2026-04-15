from __future__ import annotations

import json
import sqlite3
import tarfile
from pathlib import Path

from hr_hunter.runtime_restore import RuntimeRestoreDrillConfig, run_runtime_restore_drill


def test_runtime_restore_drill_validates_sqlite_backup_archive(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    backups_dir = workspace_root / "backups"
    backups_dir.mkdir(parents=True)

    source_backup = tmp_path / "source-backup"
    backup_dir = source_backup / "20260415T010000Z-runtimebackup"
    state_dir = backup_dir / "state"
    feedback_dir = backup_dir / "feedback"
    state_dir.mkdir(parents=True)
    feedback_dir.mkdir(parents=True)

    state_db = state_dir / "workspace.db"
    feedback_db = feedback_dir / "feedback.db"
    for path in (state_db, feedback_db):
        connection = sqlite3.connect(path)
        connection.execute("CREATE TABLE sample (id TEXT PRIMARY KEY)")
        connection.commit()
        connection.close()

    metadata = {
        "workspace_storage": {"backend": "sqlite"},
        "db_snapshot": {"path": "state/workspace.db"},
        "feedback_snapshot": "feedback/feedback.db",
    }
    (backup_dir / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")

    archive_path = backups_dir / "20260415T010000Z-runtimebackup.tar.gz"
    with tarfile.open(archive_path, "w:gz") as handle:
        handle.add(backup_dir, arcname=backup_dir.name)

    result = run_runtime_restore_drill(
        RuntimeRestoreDrillConfig(
            workspace_root=workspace_root,
        )
    )

    assert result["ok"] is True
    assert result["archive_extracted"] is True
    assert result["db_check"]["ok"] is True
    assert result["feedback_check"]["ok"] is True
    assert Path(result["backup_dir"]).exists()


def test_runtime_restore_drill_dry_run_reports_selected_archive(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    backups_dir = workspace_root / "backups"
    backups_dir.mkdir(parents=True)
    archive_path = backups_dir / "20260415T010000Z-runtimebackup.tar.gz"
    archive_path.write_text("placeholder", encoding="utf-8")

    result = run_runtime_restore_drill(
        RuntimeRestoreDrillConfig(
            workspace_root=workspace_root,
            dry_run=True,
        )
    )

    assert result["dry_run"] is True
    assert result["selected_source"] == str(archive_path)
    assert result["archive_source"]["planned"] is True
    assert result["ok"] is True


def test_runtime_restore_drill_falls_back_to_standard_backup_paths_when_metadata_is_sparse(tmp_path: Path, monkeypatch) -> None:
    workspace_root = tmp_path / "workspace"
    backups_dir = workspace_root / "backups"
    backups_dir.mkdir(parents=True)

    source_backup = tmp_path / "source-backup"
    backup_dir = source_backup / "20260415T010000Z-runtimebackup"
    state_dir = backup_dir / "state"
    feedback_dir = backup_dir / "feedback"
    state_dir.mkdir(parents=True)
    feedback_dir.mkdir(parents=True)
    (state_dir / "workspace.pg.dump").write_text("pgdump", encoding="utf-8")

    feedback_db = feedback_dir / "feedback.db"
    connection = sqlite3.connect(feedback_db)
    connection.execute("CREATE TABLE sample (id TEXT PRIMARY KEY)")
    connection.commit()
    connection.close()

    metadata = {
        "workspace_storage": {"backend": "postgres"},
        "feedback_db": "/srv/hr-hunter/shared/output/feedback/hr_hunter_feedback.db",
    }
    (backup_dir / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")

    archive_path = backups_dir / "20260415T010000Z-runtimebackup.tar.gz"
    with tarfile.open(archive_path, "w:gz") as handle:
        handle.add(backup_dir, arcname=backup_dir.name)

    commands: list[list[str]] = []

    def _fake_run(command, check, capture_output, text):  # type: ignore[no-untyped-def]
        commands.append(list(command))
        return None

    monkeypatch.setattr("subprocess.run", _fake_run)
    monkeypatch.setattr("shutil.which", lambda name: "/usr/bin/pg_restore" if name == "pg_restore" else None)

    result = run_runtime_restore_drill(RuntimeRestoreDrillConfig(workspace_root=workspace_root))

    assert result["ok"] is True
    assert result["db_check"]["ok"] is True
    assert result["feedback_check"]["ok"] is True
    assert commands[0][:2] == ["pg_restore", "-l"]
