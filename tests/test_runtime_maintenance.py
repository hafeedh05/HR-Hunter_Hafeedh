from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path

from hr_hunter.runtime_maintenance import (
    RuntimeMaintenanceConfig,
    collect_referenced_artifact_paths,
    run_runtime_maintenance,
    select_backup_prune_paths,
    select_orphan_artifact_paths,
    select_release_prune_paths,
)


def test_select_release_prune_paths_keeps_current_plus_latest_window(tmp_path: Path) -> None:
    releases_dir = tmp_path / "releases"
    releases_dir.mkdir()
    release_a = releases_dir / "20260413T010000Z-a"
    release_b = releases_dir / "20260413T020000Z-b"
    release_c = releases_dir / "20260413T030000Z-c"
    release_d = releases_dir / "20260413T040000Z-d"
    for release in [release_a, release_b, release_c, release_d]:
        release.mkdir()

    pruned = select_release_prune_paths(
        releases_dir,
        current_release=release_b,
        keep_releases=2,
    )

    assert pruned == [release_a]


def test_select_backup_prune_paths_honors_age_and_keep_count(tmp_path: Path) -> None:
    backups_dir = tmp_path / "backups"
    backups_dir.mkdir()
    backup_a = backups_dir / "20260401T010000Z-a"
    backup_b = backups_dir / "20260402T010000Z-b"
    backup_c = backups_dir / "20260403T010000Z-c"
    for backup in [backup_a, backup_b, backup_c]:
        backup.mkdir()

    now_ts = 1_800_000_000.0
    old_ts = now_ts - (30 * 86400)
    recent_ts = now_ts - (2 * 86400)
    os.utime(backup_a, (old_ts, old_ts))
    os.utime(backup_b, (old_ts, old_ts))
    os.utime(backup_c, (recent_ts, recent_ts))

    pruned = select_backup_prune_paths(
        backups_dir,
        keep_backups=1,
        backup_min_age_days=14,
        now_ts=now_ts,
    )

    assert pruned == [backup_a, backup_b]


def test_collect_referenced_artifact_paths_reads_search_run_artifacts(tmp_path: Path) -> None:
    db_path = tmp_path / "workspace.db"
    json_path = tmp_path / "run.json"
    csv_path = tmp_path / "run.csv"
    json_path.write_text("{}", encoding="utf-8")
    csv_path.write_text("name\nCandidate\n", encoding="utf-8")

    connection = sqlite3.connect(db_path)
    connection.execute(
        """
        CREATE TABLE search_runs (
            id TEXT PRIMARY KEY,
            report_json_path TEXT,
            report_csv_path TEXT
        )
        """
    )
    connection.execute(
        "INSERT INTO search_runs (id, report_json_path, report_csv_path) VALUES (?, ?, ?)",
        ("run-1", str(json_path), str(csv_path)),
    )
    connection.commit()
    connection.close()

    result = collect_referenced_artifact_paths(db_locator=db_path)

    assert json_path.resolve() in result["referenced_paths"]
    assert csv_path.resolve() in result["referenced_paths"]
    assert result["run_count"] == 1
    assert result["storage"]["backend"] == "sqlite"


def test_select_orphan_artifact_paths_keeps_referenced_client_csv_and_recent_files(tmp_path: Path) -> None:
    output_dir = tmp_path / "output" / "search"
    output_dir.mkdir(parents=True)
    referenced_csv = output_dir / "kept.csv"
    referenced_client_csv = output_dir / "kept-client.csv"
    orphan_old_json = output_dir / "orphan.json"
    orphan_recent_csv = output_dir / "recent.csv"
    for path in [referenced_csv, referenced_client_csv, orphan_old_json, orphan_recent_csv]:
        path.write_text("artifact", encoding="utf-8")

    now_ts = 1_800_000_000.0
    old_ts = now_ts - (60 * 86400)
    recent_ts = now_ts - (2 * 86400)
    os.utime(referenced_csv, (old_ts, old_ts))
    os.utime(referenced_client_csv, (old_ts, old_ts))
    os.utime(orphan_old_json, (old_ts, old_ts))
    os.utime(orphan_recent_csv, (recent_ts, recent_ts))

    pruned = select_orphan_artifact_paths(
        output_dir,
        referenced_paths={referenced_csv.resolve()},
        artifact_max_age_days=45,
        now_ts=now_ts,
    )

    assert pruned == [orphan_old_json]


def test_run_runtime_maintenance_deletes_old_release_backup_and_orphan_artifact(tmp_path: Path, monkeypatch) -> None:
    workspace_root = tmp_path / "workspace"
    releases_dir = workspace_root / "releases"
    backups_dir = workspace_root / "backups"
    releases_dir.mkdir(parents=True)
    backups_dir.mkdir(parents=True)

    release_old = releases_dir / "20260410T010000Z-old"
    release_keep = releases_dir / "20260411T010000Z-keep"
    release_current = releases_dir / "20260412T010000Z-current"
    for release in [release_old, release_keep, release_current]:
        release.mkdir()
        (release / "marker.txt").write_text("release", encoding="utf-8")

    current_link = workspace_root / "current"
    current_link.symlink_to(release_current)

    backup_old = backups_dir / "20260401T010000Z-old"
    backup_keep = backups_dir / "20260414T010000Z-keep"
    for backup in [backup_old, backup_keep]:
        backup.mkdir()
        (backup / "snapshot.json").write_text("{}", encoding="utf-8")

    now_ts = time.time()
    old_ts = now_ts - (60 * 86400)
    recent_ts = now_ts - (2 * 86400)
    os.utime(backup_old, (old_ts, old_ts))
    os.utime(backup_keep, (recent_ts, recent_ts))

    output_dir = tmp_path / "shared-output" / "search"
    output_dir.mkdir(parents=True)
    kept_json = output_dir / "kept.json"
    kept_csv = output_dir / "kept.csv"
    orphan_json = output_dir / "orphan.json"
    for path in [kept_json, kept_csv, orphan_json]:
        path.write_text("artifact", encoding="utf-8")
        os.utime(path, (old_ts, old_ts))

    db_path = tmp_path / "workspace.db"
    connection = sqlite3.connect(db_path)
    connection.execute(
        """
        CREATE TABLE search_runs (
            id TEXT PRIMARY KEY,
            report_json_path TEXT,
            report_csv_path TEXT
        )
        """
    )
    connection.execute(
        "INSERT INTO search_runs (id, report_json_path, report_csv_path) VALUES (?, ?, ?)",
        ("run-1", str(kept_json), str(kept_csv)),
    )
    connection.commit()
    connection.close()

    monkeypatch.setenv("HR_HUNTER_OUTPUT_DIR", str(output_dir))
    monkeypatch.setenv("HR_HUNTER_STATE_DB", str(db_path))
    monkeypatch.delenv("HR_HUNTER_DATABASE_URL", raising=False)

    result = run_runtime_maintenance(
        RuntimeMaintenanceConfig(
            workspace_root=workspace_root,
            keep_releases=2,
            keep_backups=1,
            backup_min_age_days=14,
            artifact_max_age_days=45,
            dry_run=False,
        )
    )

    assert not release_old.exists()
    assert release_keep.exists()
    assert release_current.exists()
    assert not backup_old.exists()
    assert backup_keep.exists()
    assert not orphan_json.exists()
    assert kept_json.exists()
    assert kept_csv.exists()
    assert result["release_cleanup"]["deleted_count"] == 1
    assert result["backup_cleanup"]["deleted_count"] == 1
    assert result["artifact_cleanup"]["deleted_count"] == 1
