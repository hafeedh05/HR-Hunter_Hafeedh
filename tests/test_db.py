from __future__ import annotations

from pathlib import Path

import hr_hunter.db as db_module
from hr_hunter.config import resolve_database_locator, resolve_secret_manager_project, resolve_secret_manager_secret_name
from hr_hunter.db import DatabaseTarget, connect_database, iter_sql_statements, resolve_database_target, translate_sql


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


def test_connect_database_reuses_postgres_connection_per_thread(monkeypatch):
    class FakeCursor:
        def execute(self, sql, params):
            self.sql = sql
            self.params = params

        def fetchall(self):
            return []

        def close(self):
            self.closed = True

    class FakeConnection:
        def __init__(self):
            self.closed = False
            self.broken = False
            self.commit_calls = 0
            self.rollback_calls = 0
            self.close_calls = 0

        def cursor(self):
            return FakeCursor()

        def commit(self):
            self.commit_calls += 1

        def rollback(self):
            self.rollback_calls += 1

        def close(self):
            self.close_calls += 1
            self.closed = True

    class FakePsycopg:
        class IntegrityError(Exception):
            pass

        connect = None

    created_connections: list[FakeConnection] = []

    def fake_connect(locator, row_factory=None):
        connection = FakeConnection()
        created_connections.append(connection)
        return connection

    monkeypatch.setattr(db_module, "psycopg", FakePsycopg)
    monkeypatch.setattr(db_module, "dict_row", object())
    monkeypatch.setattr(FakePsycopg, "connect", staticmethod(fake_connect))
    monkeypatch.setattr(db_module, "_POSTGRES_CONNECTION_CACHE", __import__("threading").local())

    target = DatabaseTarget(backend="postgres", locator="postgresql://db.example.com/hr_hunter")

    with connect_database(target) as connection:
        connection.execute("SELECT 1", ())
    with connect_database(target) as connection:
        connection.execute("SELECT 1", ())

    assert len(created_connections) == 1
    assert created_connections[0].commit_calls == 2
    assert created_connections[0].rollback_calls == 0
    assert created_connections[0].close_calls == 0
