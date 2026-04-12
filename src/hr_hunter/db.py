from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence
from urllib.parse import urlsplit

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - postgres is optional in local dev
    psycopg = None
    dict_row = None

from hr_hunter.config import resolve_database_locator


POSTGRES_SCHEMES = ("postgres://", "postgresql://")


class DbIntegrityError(Exception):
    """Raised when a write violates a DB constraint."""


@dataclass(frozen=True)
class DatabaseTarget:
    backend: str
    locator: str
    path: Path | None = None


def is_postgres_locator(value: str | None) -> bool:
    normalized = str(value or "").strip().lower()
    return normalized.startswith(POSTGRES_SCHEMES)


def redact_database_locator(locator: str | None) -> str:
    normalized = str(locator or "").strip()
    if not is_postgres_locator(normalized):
        return normalized
    parts = urlsplit(normalized)
    scheme = parts.scheme or "postgresql"
    database_path = str(parts.path or "").strip()
    if database_path and not database_path.startswith("/"):
        database_path = f"/{database_path}"
    return f"{scheme}://<redacted>{database_path}"


def resolve_database_target(
    explicit: str | Path | None,
    *,
    env_var: str,
    default_path: str,
) -> DatabaseTarget:
    locator = resolve_database_locator(explicit, env_var=env_var, default_path=default_path)
    if isinstance(locator, Path):
        return DatabaseTarget(backend="sqlite", locator=str(locator), path=locator)
    return DatabaseTarget(backend="postgres", locator=str(locator))


def describe_database_target(target: DatabaseTarget) -> dict[str, Any]:
    if target.backend == "sqlite":
        return {
            "backend": "sqlite",
            "display_locator": str(target.path) if target.path is not None else target.locator,
            "credentials_redacted": False,
        }
    return {
        "backend": "postgres",
        "display_locator": redact_database_locator(target.locator),
        "credentials_redacted": True,
    }


def connect_database(target: DatabaseTarget) -> "ConnectionWrapper":
    if target.backend == "sqlite":
        assert target.path is not None
        target.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(str(target.path))
        connection.row_factory = sqlite3.Row
        return ConnectionWrapper(connection=connection, backend="sqlite")

    if psycopg is None:
        raise RuntimeError(
            "Postgres support requires psycopg. Install hr-hunter with the database dependency available."
        )
    connection = psycopg.connect(target.locator, row_factory=dict_row)
    return ConnectionWrapper(connection=connection, backend="postgres")


def _translate_placeholders(sql: str) -> str:
    return sql.replace("?", "%s")


def _translate_schema_sql(statement: str) -> str:
    translated = re.sub(
        r"\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b",
        "BIGSERIAL PRIMARY KEY",
        statement,
        flags=re.IGNORECASE,
    )
    return translated


def translate_sql(sql: str, backend: str, *, is_schema: bool = False) -> str:
    translated = sql
    if backend == "postgres":
        translated = _translate_placeholders(translated)
        if is_schema:
            translated = _translate_schema_sql(translated)
    return translated


def iter_sql_statements(script: str) -> Iterator[str]:
    buffer: list[str] = []
    for line in script.splitlines():
        buffer.append(line)
        if line.strip().endswith(";"):
            statement = "\n".join(buffer).strip()
            if statement:
                yield statement
            buffer = []
    trailing = "\n".join(buffer).strip()
    if trailing:
        yield trailing


class CursorWrapper:
    def __init__(self, cursor: Any, *, backend: str) -> None:
        self._cursor = cursor
        self._backend = backend

    def fetchone(self) -> Any:
        row = self._cursor.fetchone()
        self.close()
        return row

    def fetchall(self) -> list[Any]:
        rows = list(self._cursor.fetchall())
        self.close()
        return rows

    def close(self) -> None:
        close = getattr(self._cursor, "close", None)
        if callable(close):
            close()


class ConnectionWrapper:
    def __init__(self, connection: Any, *, backend: str) -> None:
        self._connection = connection
        self.backend = backend

    def __enter__(self) -> "ConnectionWrapper":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc is None:
            self._connection.commit()
        else:
            self._connection.rollback()
        self._connection.close()

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> CursorWrapper:
        adapted_sql = translate_sql(sql, self.backend)
        parameters: Sequence[Any] = params or ()
        try:
            if self.backend == "sqlite":
                cursor = self._connection.execute(adapted_sql, parameters)
            else:
                cursor = self._connection.cursor()
                cursor.execute(adapted_sql, parameters)
        except sqlite3.IntegrityError as exc:
            raise DbIntegrityError(str(exc)) from exc
        except Exception as exc:
            if psycopg is not None and isinstance(exc, psycopg.IntegrityError):
                raise DbIntegrityError(str(exc)) from exc
            raise
        return CursorWrapper(cursor, backend=self.backend)

    def executescript(self, script: str) -> None:
        if self.backend == "sqlite":
            self._connection.executescript(script)
            return
        for statement in iter_sql_statements(script):
            self.execute(translate_sql(statement, self.backend, is_schema=True))

    def close(self) -> None:
        self._connection.close()
