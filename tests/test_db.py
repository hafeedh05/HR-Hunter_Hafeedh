from __future__ import annotations

from pathlib import Path

from hr_hunter.config import resolve_database_locator, resolve_secret_manager_project, resolve_secret_manager_secret_name
from hr_hunter.db import iter_sql_statements, resolve_database_target, translate_sql


def test_resolve_database_locator_prefers_shared_url(monkeypatch):
    monkeypatch.setenv("HR_HUNTER_DATABASE_URL", "postgresql://user:pass@db.example.com/hrhunter")
    monkeypatch.delenv("HR_HUNTER_STATE_DB", raising=False)

    resolved = resolve_database_locator(None, env_var="HR_HUNTER_STATE_DB", default_path="output/state/hr_hunter_state.db")

    assert resolved == "postgresql://user:pass@db.example.com/hrhunter"


def test_resolve_database_target_preserves_sqlite_paths(tmp_path, monkeypatch):
    monkeypatch.delenv("HR_HUNTER_DATABASE_URL", raising=False)

    target = resolve_database_target(tmp_path / "state.db", env_var="HR_HUNTER_STATE_DB", default_path="output/state.db")

    assert target.backend == "sqlite"
    assert target.path == (tmp_path / "state.db").resolve()


def test_translate_sql_converts_sqlite_autoincrement_for_postgres():
    sqlite_ddl = "CREATE TABLE demo (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);"

    translated = translate_sql(sqlite_ddl, "postgres", is_schema=True)

    assert "BIGSERIAL PRIMARY KEY" in translated
    assert "AUTOINCREMENT" not in translated


def test_iter_sql_statements_splits_multiline_scripts():
    statements = list(
        iter_sql_statements(
            """
            CREATE TABLE one (
                id INTEGER PRIMARY KEY
            );
            CREATE INDEX idx_one_id
                ON one(id);
            """.strip()
        )
    )

    assert len(statements) == 2
    assert statements[0].startswith("CREATE TABLE one")
    assert statements[1].startswith("CREATE INDEX idx_one_id")


def test_resolve_secret_manager_secret_name_defaults_when_enabled(monkeypatch):
    monkeypatch.setenv("HR_HUNTER_USE_SECRET_MANAGER", "true")
    monkeypatch.delenv("SCRAPINGBEE_API_KEY_SECRET_NAME", raising=False)

    assert resolve_secret_manager_secret_name("SCRAPINGBEE_API_KEY") == "scrapingbee-api-key"


def test_resolve_secret_manager_project_prefers_hr_hunter_project(monkeypatch):
    monkeypatch.setenv("HR_HUNTER_GCP_PROJECT", "azadea-bi")
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "other-project")

    assert resolve_secret_manager_project() == "azadea-bi"
