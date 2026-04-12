from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import threading
import uuid
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence
from urllib.parse import quote

from hr_hunter.config import env_flag
from hr_hunter.db import DbIntegrityError, connect_database, describe_database_target, resolve_database_target
from hr_hunter.output import load_report
from hr_hunter.state import _run_summary_from_artifact, init_state_db, list_jobs, stop_job


DEFAULT_ADMIN_EMAIL = "admin.hrhunter@hyve"
DEFAULT_ADMIN_PASSWORD = "password123"
DEFAULT_ADMIN_NAME = "HR Hunter Admin"
DEFAULT_TOTP_ISSUER = "HR Hunter"
TOTP_DIGITS = 6
TOTP_PERIOD_SECONDS = 30
TOTP_VALID_WINDOW = 2
FIXED_SECRET_TOTP_VALID_WINDOW = 10
SESSION_TTL_DAYS = 30
PROJECT_STATUS_OPTIONS = [
    {"id": "active", "label": "Active"},
    {"id": "on_hold", "label": "On Hold"},
    {"id": "closed", "label": "Closed"},
]
_INITIALIZED_WORKSPACE_TARGETS: set[str] = set()
_WORKSPACE_INIT_LOCK = threading.Lock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_target(db_path: Path | str | None) -> Any:
    return resolve_database_target(
        db_path,
        env_var="HR_HUNTER_STATE_DB",
        default_path="output/state/hr_hunter_state.db",
    )


def _connect(db_path: Path | str | None) -> Any:
    return connect_database(_resolve_target(db_path))


def _storage_metadata(db_path: Path | str | None) -> Dict[str, Any]:
    return describe_database_target(_resolve_target(db_path))


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _project_status(value: str | None) -> str:
    normalized = str(value or "").strip().lower().replace(" ", "_")
    allowed = {item["id"] for item in PROJECT_STATUS_OPTIONS}
    return normalized if normalized in allowed else "active"


def _hash_password(password: str, salt_hex: str | None = None) -> tuple[str, str]:
    salt = bytes.fromhex(salt_hex) if salt_hex else secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 240_000)
    return digest.hex(), salt.hex()


def _generate_totp_secret() -> str:
    return base64.b32encode(secrets.token_bytes(20)).decode("ascii").rstrip("=")


def _normalize_totp_secret(secret: str) -> str:
    return str(secret or "").strip().replace(" ", "").upper()


def _normalize_otp_code(code: str) -> str:
    return "".join(character for character in str(code or "") if character.isdigit())


def _fixed_totp_secret() -> str:
    return _normalize_totp_secret(str(os.getenv("HR_HUNTER_FIXED_TOTP_SECRET", "")))


def _fixed_totp_email() -> str:
    configured = str(os.getenv("HR_HUNTER_LOGIN_EMAIL", "")).strip().lower()
    return configured or DEFAULT_ADMIN_EMAIL


def _configured_totp_account_name(email: str) -> str:
    configured = str(os.getenv("HR_HUNTER_TOTP_ACCOUNT_NAME", "")).strip()
    if configured:
        return configured
    return str(email or "").strip().lower()


def _totp_secret_bytes(secret: str) -> bytes:
    normalized = _normalize_totp_secret(secret)
    if not normalized:
        raise ValueError("TOTP secret is required.")
    padding = "=" * ((8 - (len(normalized) % 8)) % 8)
    return base64.b32decode(normalized + padding, casefold=True)


def generate_totp_code(secret: str, *, at_time: datetime | None = None) -> str:
    timestamp = at_time or datetime.now(timezone.utc)
    counter = int(timestamp.timestamp() // TOTP_PERIOD_SECONDS)
    digest = hmac.new(
        _totp_secret_bytes(secret),
        counter.to_bytes(8, "big"),
        hashlib.sha1,
    ).digest()
    offset = digest[-1] & 0x0F
    binary = int.from_bytes(digest[offset : offset + 4], "big") & 0x7FFFFFFF
    return str(binary % (10**TOTP_DIGITS)).zfill(TOTP_DIGITS)


def _verify_totp_code(secret: str, code: str, *, at_time: datetime | None = None) -> bool:
    normalized_code = _normalize_otp_code(code)
    if len(normalized_code) != TOTP_DIGITS:
        return False
    timestamp = at_time or datetime.now(timezone.utc)
    fixed_secret = _fixed_totp_secret()
    valid_window = (
        FIXED_SECRET_TOTP_VALID_WINDOW
        if fixed_secret and secrets.compare_digest(_normalize_totp_secret(secret), fixed_secret)
        else TOTP_VALID_WINDOW
    )
    for offset in range(-valid_window, valid_window + 1):
        candidate_time = timestamp + timedelta(seconds=offset * TOTP_PERIOD_SECONDS)
        if secrets.compare_digest(generate_totp_code(secret, at_time=candidate_time), normalized_code):
            return True
    return False


def _build_totp_provisioning_uri(*, email: str, secret: str) -> str:
    account_name = _configured_totp_account_name(email)
    label = quote(f"{DEFAULT_TOTP_ISSUER}:{account_name}")
    issuer = quote(DEFAULT_TOTP_ISSUER)
    return (
        f"otpauth://totp/{label}"
        f"?secret={_normalize_totp_secret(secret)}"
        f"&issuer={issuer}"
        f"&algorithm=SHA1"
        f"&digits={TOTP_DIGITS}"
        f"&period={TOTP_PERIOD_SECONDS}"
    )


def _serialize_totp_setup(row: Any) -> Dict[str, Any]:
    secret = _normalize_totp_secret(row["totp_secret"])
    account_name = _configured_totp_account_name(str(row["email"]))
    return {
        "issuer": DEFAULT_TOTP_ISSUER,
        "account_name": account_name,
        "secret": secret,
        "period_seconds": TOTP_PERIOD_SECONDS,
        "digits": TOTP_DIGITS,
        "provisioning_uri": _build_totp_provisioning_uri(email=account_name, secret=secret),
    }


def _serialize_user(row: Any) -> Dict[str, Any]:
    return {
        "id": row["id"],
        "email": row["email"],
        "full_name": row["full_name"],
        "role": row["role"],
        "team_id": row["team_id"] or "",
        "is_active": bool(row["is_active"]),
        "is_admin": row["role"] == "admin",
        "auth_mode": "totp",
        "totp_enabled": bool(row["totp_enabled"]) if "totp_enabled" in set(row.keys()) else True,
        "has_totp_secret": bool(str(row["totp_secret"] or "").strip()) if "totp_secret" in set(row.keys()) else False,
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _sqlite_column_names(connection: Any, table_name: str) -> set[str]:
    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row["name"]) for row in rows}


def _postgres_column_names(connection: Any, table_name: str) -> set[str]:
    rows = connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = ?
        """,
        (table_name,),
    ).fetchall()
    return {str(row["column_name"]) for row in rows}


def _table_column_names(connection: Any, table_name: str) -> set[str]:
    if connection.backend == "sqlite":
        return _sqlite_column_names(connection, table_name)
    return _postgres_column_names(connection, table_name)


def _ensure_user_columns(connection: Any) -> None:
    columns = _table_column_names(connection, "users")
    if "totp_secret" not in columns:
        connection.execute("ALTER TABLE users ADD COLUMN totp_secret TEXT DEFAULT ''")
    if "totp_enabled" not in columns:
        connection.execute("ALTER TABLE users ADD COLUMN totp_enabled INTEGER NOT NULL DEFAULT 1")


def _load_user_row(
    connection: Any,
    *,
    user_id: str | None = None,
    email: str | None = None,
) -> Any:
    if user_id:
        return connection.execute("SELECT * FROM users WHERE id = ? LIMIT 1", (str(user_id).strip(),)).fetchone()
    if email:
        return connection.execute(
            "SELECT * FROM users WHERE lower(email) = lower(?) LIMIT 1",
            (str(email).strip(),),
        ).fetchone()
    raise ValueError("A user id or email is required.")


def _ensure_user_totp_secret(
    connection: Any,
    *,
    user_id: str | None = None,
    email: str | None = None,
    rotate: bool = False,
) -> Any:
    row = _load_user_row(connection, user_id=user_id, email=email)
    if not row:
        raise ValueError("Recruiter account not found.")
    existing_secret = _normalize_totp_secret(row["totp_secret"]) if "totp_secret" in set(row.keys()) else ""
    fixed_secret = _fixed_totp_secret()
    user_email = str(row["email"]).strip().lower()
    fixed_email = _fixed_totp_email()

    # Explicit rotate must always rotate to a new key.
    if rotate:
        connection.execute(
            """
            UPDATE users
            SET totp_secret = ?, totp_enabled = 1, updated_at = ?
            WHERE id = ?
            """,
            (_generate_totp_secret(), _now(), row["id"]),
        )
        row = _load_user_row(connection, user_id=row["id"])
        return row

    # Fixed secret is a bootstrap/default only; it should not override an already-rotated key.
    if fixed_secret and user_email == fixed_email:
        force_fixed = env_flag("HR_HUNTER_FIXED_TOTP_FORCE") or _code_only_login_enabled()
        if force_fixed and existing_secret and existing_secret != fixed_secret:
            connection.execute(
                """
                UPDATE users
                SET totp_secret = ?, totp_enabled = 1, updated_at = ?
                WHERE id = ?
                """,
                (fixed_secret, _now(), row["id"]),
            )
            row = _load_user_row(connection, user_id=row["id"])
        elif (not existing_secret) or (not bool(row["totp_enabled"])):
            connection.execute(
                """
                UPDATE users
                SET totp_secret = ?, totp_enabled = 1, updated_at = ?
                WHERE id = ?
                """,
                (fixed_secret, _now(), row["id"]),
            )
            row = _load_user_row(connection, user_id=row["id"])
        return row
    if not existing_secret:
        connection.execute(
            """
            UPDATE users
            SET totp_secret = ?, totp_enabled = 1, updated_at = ?
            WHERE id = ?
            """,
            (_generate_totp_secret(), _now(), row["id"]),
        )
        row = _load_user_row(connection, user_id=row["id"])
    return row


def init_workspace_db(db_path: Path | str | None = None) -> Path | str:
    target = _resolve_target(db_path)
    resolved = target.path if target.backend == "sqlite" else target.locator
    cache_key = target.locator
    if cache_key in _INITIALIZED_WORKSPACE_TARGETS:
        return resolved
    with _WORKSPACE_INIT_LOCK:
        if cache_key in _INITIALIZED_WORKSPACE_TARGETS:
            return resolved
        init_state_db(resolved)
        with connect_database(target) as connection:
            connection.executescript(
                """
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                password_salt TEXT NOT NULL,
                full_name TEXT DEFAULT '',
                role TEXT NOT NULL DEFAULT 'recruiter',
                team_id TEXT DEFAULT '',
                is_active INTEGER NOT NULL DEFAULT 1,
                created_by TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS user_sessions (
                id TEXT PRIMARY KEY,
                session_token TEXT NOT NULL UNIQUE,
                user_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS projects (
                id TEXT PRIMARY KEY,
                org_id TEXT NOT NULL DEFAULT 'local',
                name TEXT NOT NULL,
                client_name TEXT DEFAULT '',
                role_title TEXT DEFAULT '',
                target_geography TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'active',
                notes TEXT DEFAULT '',
                created_by TEXT NOT NULL,
                owner_id TEXT DEFAULT '',
                latest_brief_json TEXT DEFAULT '{}',
                latest_run_id TEXT DEFAULT '',
                latest_run_at TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS project_members (
                project_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                assigned_by TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                PRIMARY KEY(project_id, user_id)
            );

            CREATE INDEX IF NOT EXISTS idx_projects_updated_at
                ON projects(updated_at DESC);

            CREATE INDEX IF NOT EXISTS idx_project_members_user
                ON project_members(user_id, project_id);
                """
            )
            _ensure_user_columns(connection)
            user_rows = connection.execute("SELECT id FROM users").fetchall()
            for row in user_rows:
                _ensure_user_totp_secret(connection, user_id=row["id"])
        seed_default_admin_account(resolved)
        _INITIALIZED_WORKSPACE_TARGETS.add(cache_key)
    return resolved


def seed_default_admin_account(db_path: Path | None = None) -> Dict[str, Any]:
    target = _resolve_target(db_path)
    resolved = target.path if target.backend == "sqlite" else target.locator
    init_workspace_db(resolved) if False else None
    with _connect(resolved) as connection:
        _ensure_user_columns(connection)
        current = connection.execute(
            """
            SELECT *
            FROM users
            WHERE lower(email) = lower(?)
            """,
            (DEFAULT_ADMIN_EMAIL,),
        ).fetchone()
        if current:
            return _serialize_user(_ensure_user_totp_secret(connection, user_id=current["id"]))
        password_hash, password_salt = _hash_password(DEFAULT_ADMIN_PASSWORD)
        user_id = "user_admin_hrhunter"
        created_at = _now()
        connection.execute(
            """
            INSERT INTO users (
                id, email, password_hash, password_salt, full_name, role,
                team_id, is_active, created_by, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                DEFAULT_ADMIN_EMAIL,
                password_hash,
                password_salt,
                DEFAULT_ADMIN_NAME,
                "admin",
                "leadership",
                1,
                "system",
                created_at,
                created_at,
            ),
        )
        seeded = _ensure_user_totp_secret(connection, user_id=user_id)
    return _serialize_user(seeded)


def _session_expiry() -> str:
    return (datetime.now(timezone.utc) + timedelta(days=SESSION_TTL_DAYS)).isoformat()


def _code_only_login_enabled() -> bool:
    return env_flag("HR_HUNTER_CODE_ONLY_LOGIN")


def _resolve_auth_email(email: str) -> str:
    normalized_email = str(email or "").strip().lower()
    if normalized_email:
        return normalized_email
    if _code_only_login_enabled():
        configured = str(os.getenv("HR_HUNTER_LOGIN_EMAIL", "")).strip().lower()
        return configured or DEFAULT_ADMIN_EMAIL
    return ""


def _invalid_login_message(email: str) -> str:
    if _code_only_login_enabled() and not str(email or "").strip():
        return "Invalid authenticator code. Check the current code in your authenticator app and make sure the device time is synced."
    return "Invalid email or verification code."


def create_session_for_user(user_id: str, *, db_path: Path | None = None) -> Dict[str, Any]:
    resolved = init_workspace_db(db_path)
    session_token = secrets.token_urlsafe(32)
    row_id = f"session_{uuid.uuid4().hex[:16]}"
    created_at = _now()
    expires_at = _session_expiry()
    with _connect(resolved) as connection:
        connection.execute(
            """
            INSERT INTO user_sessions (id, session_token, user_id, created_at, last_seen_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (row_id, session_token, user_id, created_at, created_at, expires_at),
        )
    return {"session_token": session_token, "expires_at": expires_at}


def authenticate_user(email: str, otp_code: str, *, db_path: Path | None = None) -> Dict[str, Any]:
    resolved = init_workspace_db(db_path)
    effective_email = _resolve_auth_email(email)
    if not effective_email:
        raise ValueError("Email is required.")
    invalid_login_message = _invalid_login_message(email)
    with _connect(resolved) as connection:
        row = _load_user_row(connection, email=effective_email)
        if row:
            row = _ensure_user_totp_secret(connection, user_id=row["id"])
    if not row or not bool(row["is_active"]):
        raise ValueError(invalid_login_message)
    if not bool(row["totp_enabled"]) or not _verify_totp_code(str(row["totp_secret"] or ""), otp_code):
        raise ValueError(invalid_login_message)
    session = create_session_for_user(str(row["id"]), db_path=resolved)
    return {
        "db_path": _storage_metadata(resolved)["display_locator"],
        "storage": _storage_metadata(resolved),
        "session_token": session["session_token"],
        "expires_at": session["expires_at"],
        "user": _serialize_user(row),
    }


def resolve_session_user(session_token: str, *, db_path: Path | None = None) -> Dict[str, Any]:
    resolved = init_workspace_db(db_path)
    token = str(session_token or "").strip()
    if not token:
        raise ValueError("Session token is required.")
    with _connect(resolved) as connection:
        row = connection.execute(
            """
            SELECT s.session_token, s.expires_at, u.*
            FROM user_sessions s
            JOIN users u ON u.id = s.user_id
            WHERE s.session_token = ?
            """,
            (token,),
        ).fetchone()
        if not row:
            raise ValueError("Your session is not valid. Please sign in again.")
        expires_at = datetime.fromisoformat(str(row["expires_at"]))
        if expires_at <= datetime.now(timezone.utc):
            connection.execute("DELETE FROM user_sessions WHERE session_token = ?", (token,))
            raise ValueError("Your session has expired. Please sign in again.")
        connection.execute(
            "UPDATE user_sessions SET last_seen_at = ? WHERE session_token = ?",
            (_now(), token),
        )
    return _serialize_user(row)


def revoke_session(session_token: str, *, db_path: Path | None = None) -> None:
    resolved = init_workspace_db(db_path)
    with _connect(resolved) as connection:
        connection.execute("DELETE FROM user_sessions WHERE session_token = ?", (str(session_token or "").strip(),))


def list_users(*, db_path: Path | None = None, query: str = "", include_inactive: bool = False) -> List[Dict[str, Any]]:
    resolved = init_workspace_db(db_path)
    params: List[Any] = []
    clauses: List[str] = []
    if not include_inactive:
        clauses.append("is_active = 1")
    search = str(query or "").strip().lower()
    if search:
        clauses.append("(lower(email) LIKE ? OR lower(full_name) LIKE ? OR lower(team_id) LIKE ?)")
        like = f"%{search}%"
        params.extend([like, like, like])
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with _connect(resolved) as connection:
        rows = connection.execute(
            f"""
            SELECT id, email, full_name, role, team_id, is_active, totp_enabled, totp_secret, created_at, updated_at
            FROM users
            {where_clause}
            ORDER BY CASE WHEN role = 'admin' THEN 0 ELSE 1 END, lower(full_name), lower(email)
            """,
            params,
        ).fetchall()
    return [_serialize_user(row) for row in rows]


def create_user_account(
    *,
    email: str,
    password: str = "",
    full_name: str,
    team_id: str = "",
    role: str = "recruiter",
    created_by: str = "",
    db_path: Path | None = None,
) -> Dict[str, Any]:
    resolved = init_workspace_db(db_path)
    normalized_role = "admin" if str(role).strip().lower() == "admin" else "recruiter"
    normalized_email = str(email or "").strip().lower()
    if not normalized_email or "@" not in normalized_email:
        raise ValueError("A valid recruiter email is required.")
    if not str(full_name or "").strip():
        raise ValueError("Recruiter full name is required.")
    password_hash, password_salt = _hash_password(str(password or secrets.token_urlsafe(24)))
    row_id = f"user_{uuid.uuid4().hex[:12]}"
    created_at = _now()
    try:
        with _connect(resolved) as connection:
            _ensure_user_columns(connection)
            connection.execute(
                """
                INSERT INTO users (
                    id, email, password_hash, password_salt, full_name, role, totp_secret, totp_enabled,
                    team_id, is_active, created_by, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row_id,
                    normalized_email,
                    password_hash,
                    password_salt,
                    str(full_name).strip(),
                    normalized_role,
                    "",
                    1,
                    str(team_id or "").strip(),
                    1,
                    created_by,
                    created_at,
                    created_at,
                ),
            )
            row = _ensure_user_totp_secret(connection, user_id=row_id)
    except DbIntegrityError as exc:
        raise ValueError("A recruiter account with that email already exists.") from exc
    return {
        **_serialize_user(row),
        "totp": _serialize_totp_setup(row),
    }


def get_user_totp_setup(
    *,
    user_id: str | None = None,
    email: str | None = None,
    rotate: bool = False,
    db_path: Path | None = None,
) -> Dict[str, Any]:
    resolved = init_workspace_db(db_path)
    with _connect(resolved) as connection:
        _ensure_user_columns(connection)
        row = _ensure_user_totp_secret(connection, user_id=user_id, email=email, rotate=rotate)
    return {
        "user": _serialize_user(row),
        "totp": _serialize_totp_setup(row),
    }


def _project_access_clause(user: Dict[str, Any]) -> tuple[str, List[Any]]:
    if user.get("is_admin"):
        return "", []
    return (
        """
        WHERE (
            lower(p.created_by) = lower(?)
            OR lower(p.owner_id) = lower(?)
            OR EXISTS (
                SELECT 1
                FROM project_members pm
                WHERE pm.project_id = p.id AND lower(pm.user_id) = lower(?)
            )
        )
        """,
        [user["id"], user["id"], user["id"]],
    )


def _load_project_members(
    connection: Any,
    project_ids: Sequence[str],
) -> Dict[str, List[Dict[str, Any]]]:
    if not project_ids:
        return {}
    placeholders = ", ".join("?" for _ in project_ids)
    rows = connection.execute(
        f"""
        SELECT pm.project_id, u.id, u.email, u.full_name, u.role, u.team_id, u.is_active
        FROM project_members pm
        JOIN users u ON u.id = pm.user_id
        WHERE pm.project_id IN ({placeholders})
        ORDER BY lower(u.full_name), lower(u.email)
        """,
        list(project_ids),
    ).fetchall()
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(row["project_id"], []).append(
            {
                "id": row["id"],
                "email": row["email"],
                "full_name": row["full_name"],
                "role": row["role"],
                "team_id": row["team_id"] or "",
                "is_active": bool(row["is_active"]),
            }
        )
    return grouped


def _validated_member_ids(
    connection: Any,
    member_ids: Iterable[str],
) -> List[str]:
    normalized = sorted({str(value).strip() for value in member_ids if str(value).strip()})
    if not normalized:
        return []
    placeholders = ", ".join("?" for _ in normalized)
    rows = connection.execute(
        f"""
        SELECT id
        FROM users
        WHERE id IN ({placeholders}) AND is_active = 1
        """,
        normalized,
    ).fetchall()
    found_ids = {str(row["id"]) for row in rows}
    missing_ids = [member_id for member_id in normalized if member_id not in found_ids]
    if missing_ids:
        raise ValueError("One or more assigned recruiters could not be found.")
    return normalized


def _project_summary_from_row(row: Any, members: List[Dict[str, Any]]) -> Dict[str, Any]:
    brief_payload = {}
    try:
        brief_payload = json.loads(row["latest_brief_json"] or "{}")
    except Exception:
        brief_payload = {}
    column_names = set(row.keys())
    latest_run_summary = {}
    latest_run_candidate_count = int(row["latest_run_candidate_count"] or 0) if "latest_run_candidate_count" in column_names else 0
    if "latest_run_summary_json" in column_names:
        try:
            latest_run_summary = json.loads(row["latest_run_summary_json"] or "{}")
        except Exception:
            latest_run_summary = {}
        latest_run_summary = _run_summary_from_artifact(
            latest_run_summary,
            candidate_count=latest_run_candidate_count,
            report_json_path=str(row["latest_run_report_json_path"] or ""),
        )
    return {
        "id": row["id"],
        "name": row["name"],
        "client_name": row["client_name"] or "",
        "role_title": row["role_title"] or "",
        "target_geography": row["target_geography"] or "",
        "status": row["status"],
        "notes": row["notes"] or "",
        "created_by": row["created_by"],
        "owner_id": row["owner_id"] or "",
        "latest_brief_json": brief_payload,
        "latest_run_id": row["latest_run_id"] or "",
        "latest_run_at": row["latest_run_at"] or "",
        "latest_run_status": (row["latest_run_status"] or "") if "latest_run_status" in column_names else "",
        "latest_run_execution_backend": (row["latest_run_execution_backend"] or "") if "latest_run_execution_backend" in column_names else "",
        "latest_run_accepted_count": int(row["latest_run_accepted_count"] or 0) if "latest_run_accepted_count" in column_names else 0,
        "latest_run_summary": latest_run_summary,
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "assigned_recruiters": members,
        "run_count": int(row["run_count"] or 0) if "run_count" in column_names else 0,
        "latest_run_candidate_count": latest_run_candidate_count,
    }


def list_projects(
    user: Dict[str, Any],
    *,
    db_path: Path | None = None,
    query: str = "",
    limit: int = 50,
) -> List[Dict[str, Any]]:
    resolved = init_workspace_db(db_path)
    access_clause, access_params = _project_access_clause(user)
    params: List[Any] = [*access_params]
    search = str(query or "").strip().lower()
    filters: List[str] = []
    if search:
        filters.append("(lower(p.name) LIKE ? OR lower(p.client_name) LIKE ? OR lower(p.role_title) LIKE ? OR lower(p.target_geography) LIKE ?)")
        like = f"%{search}%"
        params.extend([like, like, like, like])
    filter_clause = ""
    if filters:
        if access_clause:
            filter_clause = " AND " + " AND ".join(filters)
        else:
            filter_clause = " WHERE " + " AND ".join(filters)
    params.append(max(1, int(limit)))
    with _connect(resolved) as connection:
        rows = connection.execute(
            f"""
            SELECT
                p.*,
                COALESCE(run_counts.run_count, 0) AS run_count,
                COALESCE(sr_latest.candidate_count, 0) AS latest_run_candidate_count,
                COALESCE(sr_latest.accepted_count, 0) AS latest_run_accepted_count,
                COALESCE(sr_latest.summary_json, '{{}}') AS latest_run_summary_json,
                COALESCE(sr_latest.report_json_path, '') AS latest_run_report_json_path,
                COALESCE(sr_latest.status, '') AS latest_run_status,
                COALESCE(sr_latest.execution_backend, '') AS latest_run_execution_backend
            FROM projects p
            LEFT JOIN (
                SELECT mandate_id, COUNT(*) AS run_count
                FROM search_runs
                GROUP BY mandate_id
            ) run_counts ON run_counts.mandate_id = p.id
            LEFT JOIN search_runs sr_latest
                ON sr_latest.mandate_id = p.id AND sr_latest.id = p.latest_run_id
            {access_clause}
            {filter_clause}
            ORDER BY
                CASE p.status WHEN 'active' THEN 0 WHEN 'on_hold' THEN 1 ELSE 2 END,
                COALESCE(NULLIF(p.latest_run_at, ''), p.updated_at) DESC,
                lower(p.name) ASC
            LIMIT ?
            """,
            params,
        ).fetchall()
        members_by_project = _load_project_members(connection, [row["id"] for row in rows])
    return [_project_summary_from_row(row, members_by_project.get(row["id"], [])) for row in rows]


def _project_row_for_user(
    connection: Any,
    user: Dict[str, Any],
    project_id: str,
) -> Any:
    access_clause, access_params = _project_access_clause(user)
    if access_clause:
        query = (
            """
            SELECT p.*
            FROM projects p
            """
            + access_clause
            + " AND p.id = ? LIMIT 1"
        )
        params = [*access_params, project_id]
    else:
        query = "SELECT p.* FROM projects p WHERE p.id = ? LIMIT 1"
        params = [project_id]
    return connection.execute(query, params).fetchone()


def get_project(
    user: Dict[str, Any],
    project_id: str,
    *,
    db_path: Path | None = None,
) -> Dict[str, Any]:
    resolved = init_workspace_db(db_path)
    with _connect(resolved) as connection:
        row = _project_row_for_user(connection, user, project_id)
        if not row:
            raise ValueError("Project not found or you do not have access to it.")
        members = _load_project_members(connection, [project_id]).get(project_id, [])
        run_stats = connection.execute(
            """
            SELECT COUNT(*) AS run_count
            FROM search_runs
            WHERE mandate_id = ?
            """,
            (project_id,),
        ).fetchone()
        latest_run_stats = connection.execute(
            """
            SELECT
                COALESCE(candidate_count, 0) AS candidate_count,
                COALESCE(accepted_count, 0) AS accepted_count,
                COALESCE(summary_json, '{}') AS summary_json,
                COALESCE(report_json_path, '') AS report_json_path,
                COALESCE(status, '') AS status,
                COALESCE(execution_backend, '') AS execution_backend
            FROM search_runs
            WHERE mandate_id = ? AND id = ?
            LIMIT 1
            """,
            (project_id, str(row["latest_run_id"] or "").strip()),
        ).fetchone()
    project = _project_summary_from_row(row, members)
    project["run_count"] = int(run_stats["run_count"] or 0)
    project["latest_run_candidate_count"] = int((latest_run_stats["candidate_count"] if latest_run_stats else 0) or 0)
    project["latest_run_accepted_count"] = int((latest_run_stats["accepted_count"] if latest_run_stats else 0) or 0)
    project["latest_run_status"] = str((latest_run_stats["status"] if latest_run_stats else "") or "")
    project["latest_run_execution_backend"] = str((latest_run_stats["execution_backend"] if latest_run_stats else "") or "")
    if latest_run_stats:
        try:
            latest_run_summary = json.loads(latest_run_stats["summary_json"] or "{}")
        except Exception:
            latest_run_summary = {}
        project["latest_run_summary"] = _run_summary_from_artifact(
            latest_run_summary,
            candidate_count=project["latest_run_candidate_count"],
            report_json_path=str(latest_run_stats["report_json_path"] or ""),
        )
    else:
        project["latest_run_summary"] = {}
    return project


def create_project(
    user: Dict[str, Any],
    *,
    name: str,
    client_name: str = "",
    role_title: str = "",
    target_geography: str = "",
    status: str = "active",
    notes: str = "",
    brief_json: Dict[str, Any] | None = None,
    assigned_user_ids: Sequence[str] | None = None,
    db_path: Path | None = None,
) -> Dict[str, Any]:
    resolved = init_workspace_db(db_path)
    project_name = str(name or "").strip()
    if not project_name:
        raise ValueError("Project name is required.")
    project_id = f"project_{uuid.uuid4().hex[:12]}"
    created_at = _now()
    with _connect(resolved) as connection:
        members = set(_validated_member_ids(connection, assigned_user_ids or []))
        members.add(user["id"])
        connection.execute(
            """
            INSERT INTO projects (
                id, org_id, name, client_name, role_title, target_geography, status, notes,
                created_by, owner_id, latest_brief_json, latest_run_id, latest_run_at, created_at, updated_at
            ) VALUES (?, 'local', ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '', ?, ?)
            """,
            (
                project_id,
                project_name,
                str(client_name or "").strip(),
                str(role_title or "").strip(),
                str(target_geography or "").strip(),
                _project_status(status),
                str(notes or "").strip(),
                user["id"],
                user["id"],
                _json(brief_json or {}),
                created_at,
                created_at,
            ),
        )
        for member_id in sorted(members):
                connection.execute(
                    """
                    INSERT INTO project_members (project_id, user_id, assigned_by, created_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(project_id, user_id) DO NOTHING
                    """,
                    (project_id, member_id, user["id"], created_at),
                )
    return get_project(user if user.get("is_admin") else user, project_id, db_path=resolved)


def update_project(
    user: Dict[str, Any],
    *,
    project_id: str,
    name: str,
    client_name: str = "",
    role_title: str = "",
    target_geography: str = "",
    status: str = "active",
    notes: str = "",
    brief_json: Dict[str, Any] | None = None,
    assigned_user_ids: Sequence[str] | None = None,
    db_path: Path | None = None,
) -> Dict[str, Any]:
    resolved = init_workspace_db(db_path)
    project_name = str(name or "").strip()
    if not project_name:
        raise ValueError("Project name is required.")
    with _connect(resolved) as connection:
        row = _project_row_for_user(connection, user, project_id)
        if not row:
            raise ValueError("Project not found or you do not have access to it.")
        connection.execute(
            """
            UPDATE projects
            SET name = ?, client_name = ?, role_title = ?, target_geography = ?, status = ?, notes = ?,
                latest_brief_json = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                project_name,
                str(client_name or "").strip(),
                str(role_title or "").strip(),
                str(target_geography or "").strip(),
                _project_status(status),
                str(notes or "").strip(),
                _json(brief_json or {}),
                _now(),
                project_id,
            ),
        )
        if assigned_user_ids is not None:
            members = set(_validated_member_ids(connection, assigned_user_ids))
            members.add(user["id"])
            connection.execute("DELETE FROM project_members WHERE project_id = ?", (project_id,))
            for member_id in sorted(members):
                connection.execute(
                    """
                    INSERT INTO project_members (project_id, user_id, assigned_by, created_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(project_id, user_id) DO NOTHING
                    """,
                    (project_id, member_id, user["id"], _now()),
                )
    return get_project(user, project_id, db_path=resolved)


def save_project_brief(
    user: Dict[str, Any],
    *,
    project_id: str,
    brief_json: Dict[str, Any],
    role_title: str = "",
    target_geography: str = "",
    db_path: Path | None = None,
) -> Dict[str, Any]:
    resolved = init_workspace_db(db_path)
    with _connect(resolved) as connection:
        row = _project_row_for_user(connection, user, project_id)
        if not row:
            raise ValueError("Project not found or you do not have access to it.")
        connection.execute(
            """
            UPDATE projects
            SET latest_brief_json = ?, role_title = CASE WHEN ? != '' THEN ? ELSE role_title END,
                target_geography = CASE WHEN ? != '' THEN ? ELSE target_geography END,
                updated_at = ?
            WHERE id = ?
            """,
            (
                _json(brief_json or {}),
                str(role_title or "").strip(),
                str(role_title or "").strip(),
                str(target_geography or "").strip(),
                str(target_geography or "").strip(),
                _now(),
                project_id,
            ),
        )
    return get_project(user, project_id, db_path=resolved)


def attach_project_run(
    user: Dict[str, Any],
    *,
    project_id: str,
    run_id: str,
    brief_json: Dict[str, Any] | None = None,
    db_path: Path | None = None,
) -> Dict[str, Any]:
    resolved = init_workspace_db(db_path)
    with _connect(resolved) as connection:
        row = _project_row_for_user(connection, user, project_id)
        if not row:
            raise ValueError("Project not found or you do not have access to it.")
        connection.execute(
            """
            UPDATE projects
            SET latest_run_id = ?, latest_run_at = ?, latest_brief_json = ?, updated_at = ?
            WHERE id = ?
            """,
            (run_id, _now(), _json(brief_json or {}), _now(), project_id),
        )
    return get_project(user, project_id, db_path=resolved)


def _delete_artifact(path_value: str) -> None:
    target = Path(str(path_value or "").strip()).expanduser()
    if not str(path_value or "").strip():
        return
    try:
        if target.exists() and target.is_file():
            target.unlink()
    except OSError:
        return


def _refresh_project_latest_run(connection: Any, project_id: str) -> None:
    latest = connection.execute(
        """
        SELECT id, created_at
        FROM search_runs
        WHERE mandate_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (project_id,),
    ).fetchone()
    latest_run_id = str(latest["id"] or "").strip() if latest else ""
    latest_run_at = str(latest["created_at"] or "").strip() if latest else ""
    connection.execute(
        """
        UPDATE projects
        SET latest_run_id = ?, latest_run_at = ?, updated_at = ?
        WHERE id = ?
        """,
        (latest_run_id, latest_run_at, _now(), project_id),
    )


def _rebuild_candidate_review_summary(
    connection: Any,
    *,
    project_id: str,
    candidate_id: str,
) -> None:
    latest = connection.execute(
        """
        SELECT owner_id, action, reason_code, note, run_id, created_at
        FROM review_actions
        WHERE mandate_id = ? AND candidate_id = ?
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (project_id, candidate_id),
    ).fetchone()
    if not latest:
        connection.execute(
            "DELETE FROM candidate_reviews WHERE candidate_id = ? AND mandate_id = ?",
            (candidate_id, project_id),
        )
        return
    connection.execute(
        """
        INSERT INTO candidate_reviews (
            candidate_id, mandate_id, owner_id, owner_name, latest_action,
            latest_reason_code, latest_note, latest_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(candidate_id, mandate_id) DO UPDATE SET
            owner_id = excluded.owner_id,
            latest_action = excluded.latest_action,
            latest_reason_code = excluded.latest_reason_code,
            latest_note = excluded.latest_note,
            latest_run_id = excluded.latest_run_id,
            updated_at = excluded.updated_at
        """,
        (
            candidate_id,
            project_id,
            latest["owner_id"] or "",
            "",
            latest["action"] or "",
            latest["reason_code"] or "",
            latest["note"] or "",
            latest["run_id"] or "",
            _now(),
        ),
    )


def delete_project_run(
    user: Dict[str, Any],
    *,
    project_id: str,
    run_id: str,
    db_path: Path | None = None,
) -> Dict[str, Any]:
    if not user.get("is_admin"):
        raise ValueError("Admin access is required to delete saved runs.")
    resolved = init_workspace_db(db_path)
    run_summary: Dict[str, Any] = {}
    artifact_paths: List[str] = []
    with _connect(resolved) as connection:
        project_row = _project_row_for_user(connection, user, project_id)
        if not project_row:
            raise ValueError("Project not found or you do not have access to it.")
        run_row = connection.execute(
            """
            SELECT id, org_id, candidate_count, accepted_count, created_at, report_json_path, report_csv_path
            FROM search_runs
            WHERE mandate_id = ? AND id = ?
            LIMIT 1
            """,
            (project_id, run_id),
        ).fetchone()
        if not run_row:
            raise ValueError("Saved run not found for this project.")

        run_summary = {
            "run_id": run_row["id"],
            "project_id": project_id,
            "project_name": project_row["name"],
            "candidate_count": int(run_row["candidate_count"] or 0),
            "accepted_count": int(run_row["accepted_count"] or 0),
            "created_at": run_row["created_at"],
        }
        artifact_paths = [
            str(run_row["report_json_path"] or "").strip(),
            str(run_row["report_csv_path"] or "").strip(),
        ]

        candidate_rows = connection.execute(
            """
            SELECT DISTINCT candidate_id
            FROM run_candidates
            WHERE mandate_id = ? AND run_id = ?
            """,
            (project_id, run_id),
        ).fetchall()
        reviewed_rows = connection.execute(
            """
            SELECT DISTINCT candidate_id
            FROM review_actions
            WHERE mandate_id = ? AND run_id = ?
            """,
            (project_id, run_id),
        ).fetchall()
        affected_candidate_ids = sorted(
            {
                str(row["candidate_id"])
                for row in [*candidate_rows, *reviewed_rows]
                if str(row["candidate_id"] or "").strip()
            }
        )

        connection.execute("DELETE FROM run_candidates WHERE mandate_id = ? AND run_id = ?", (project_id, run_id))
        connection.execute("DELETE FROM review_actions WHERE mandate_id = ? AND run_id = ?", (project_id, run_id))
        connection.execute("DELETE FROM audit_events WHERE entity_type = 'search_run' AND entity_id = ?", (run_id,))
        connection.execute("DELETE FROM search_runs WHERE mandate_id = ? AND id = ?", (project_id, run_id))

        org_id = str(run_row["org_id"] or "").strip() or "local"
        for candidate_id in affected_candidate_ids:
            registry_row = connection.execute(
                """
                SELECT id, search_ids_json
                FROM candidate_registry
                WHERE org_id = ? AND identity_key = ?
                LIMIT 1
                """,
                (org_id, candidate_id),
            ).fetchone()
            if registry_row:
                try:
                    search_ids = list(json.loads(registry_row["search_ids_json"] or "[]"))
                except Exception:
                    search_ids = []
                remaining_ids = [item for item in search_ids if item != run_id]
                if remaining_ids:
                    connection.execute(
                        """
                        UPDATE candidate_registry
                        SET search_ids_json = ?, search_count = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (_json(remaining_ids), len(remaining_ids), _now(), registry_row["id"]),
                    )
                else:
                    connection.execute("DELETE FROM candidate_registry WHERE id = ?", (registry_row["id"],))
            _rebuild_candidate_review_summary(connection, project_id=project_id, candidate_id=candidate_id)

        _refresh_project_latest_run(connection, project_id)

        connection.execute(
            """
            INSERT INTO audit_events (event_type, entity_type, entity_id, actor_id, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "search_run_deleted",
                "project",
                project_id,
                user["id"],
                _json({"run_id": run_id, "candidate_count": run_summary["candidate_count"]}),
                _now(),
            ),
        )

    for artifact_path in artifact_paths:
        _delete_artifact(artifact_path)

    return run_summary


def delete_project(
    user: Dict[str, Any],
    *,
    project_id: str,
    db_path: Path | None = None,
) -> Dict[str, Any]:
    stopped_job_ids: List[str] = []
    try:
        active_jobs = list_jobs(db_path=db_path, limit=500, project_id=project_id)
    except Exception:
        active_jobs = []
    for job in active_jobs:
        if str(job.get("status", "")).strip().lower() not in {"queued", "running"}:
            continue
        stopped = stop_job(
            str(job.get("job_id", "")).strip(),
            reason="Stopped automatically because the project was deleted.",
            db_path=db_path,
        )
        if stopped:
            stopped_job_ids.append(str(stopped.get("job_id", "")).strip())

    resolved = init_workspace_db(db_path)
    with _connect(resolved) as connection:
        row = _project_row_for_user(connection, user, project_id)
        if not row:
            raise ValueError("Project not found or you do not have access to it.")
        summary = {
            "id": row["id"],
            "name": row["name"],
            "role_title": row["role_title"] or "",
            "stopped_job_ids": stopped_job_ids,
        }
        connection.execute("DELETE FROM project_members WHERE project_id = ?", (project_id,))
        connection.execute("DELETE FROM run_candidates WHERE mandate_id = ?", (project_id,))
        connection.execute("DELETE FROM review_actions WHERE mandate_id = ?", (project_id,))
        connection.execute("DELETE FROM candidate_reviews WHERE mandate_id = ?", (project_id,))
        connection.execute("DELETE FROM search_runs WHERE mandate_id = ?", (project_id,))
        connection.execute("DELETE FROM mandates WHERE id = ?", (project_id,))
        connection.execute("DELETE FROM projects WHERE id = ?", (project_id,))
    return summary


def list_project_runs(
    user: Dict[str, Any],
    *,
    project_id: str,
    db_path: Path | None = None,
    limit: int = 25,
) -> List[Dict[str, Any]]:
    resolved = init_workspace_db(db_path)
    with _connect(resolved) as connection:
        project_row = _project_row_for_user(connection, user, project_id)
        if not project_row:
            raise ValueError("Project not found or you do not have access to it.")
        rows = connection.execute(
            """
            SELECT id, mandate_id, brief_id, execution_backend, candidate_count, accepted_count,
                   report_json_path, report_csv_path, summary_json, created_at
            FROM search_runs
            WHERE mandate_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (project_id, max(1, int(limit))),
        ).fetchall()
    results: List[Dict[str, Any]] = []
    for row in rows:
        try:
            summary = json.loads(row["summary_json"] or "{}")
        except Exception:
            summary = {}
        summary = _run_summary_from_artifact(
            summary,
            candidate_count=int(row["candidate_count"] or 0),
            report_json_path=str(row["report_json_path"] or ""),
        )
        results.append(
            {
                "run_id": row["id"],
                "mandate_id": row["mandate_id"],
                "brief_id": row["brief_id"],
                "execution_backend": row["execution_backend"],
                "candidate_count": int(row["candidate_count"] or 0),
                "accepted_count": int(row["accepted_count"] or 0),
                "report_json_path": row["report_json_path"] or "",
                "report_csv_path": row["report_csv_path"] or "",
                "summary": summary,
                "created_at": row["created_at"],
            }
        )
    return results


def get_project_run_report(
    user: Dict[str, Any],
    *,
    project_id: str,
    run_id: str = "",
    db_path: Path | None = None,
) -> Dict[str, Any]:
    resolved = init_workspace_db(db_path)
    with _connect(resolved) as connection:
        project_row = _project_row_for_user(connection, user, project_id)
        if not project_row:
            raise ValueError("Project not found or you do not have access to it.")
        row = None
        if run_id:
            row = connection.execute(
                """
                SELECT id, report_json_path, report_csv_path, execution_backend, created_at
                FROM search_runs
                WHERE mandate_id = ? AND id = ?
                LIMIT 1
                """,
                (project_id, run_id),
            ).fetchone()
        else:
            latest_run_id = str(project_row["latest_run_id"] or "").strip()
            if latest_run_id:
                row = connection.execute(
                    """
                    SELECT id, report_json_path, report_csv_path, execution_backend, created_at
                    FROM search_runs
                    WHERE mandate_id = ? AND id = ?
                    LIMIT 1
                    """,
                    (project_id, latest_run_id),
                ).fetchone()
            if not row:
                row = connection.execute(
                    """
                    SELECT id, report_json_path, report_csv_path, execution_backend, created_at
                    FROM search_runs
                    WHERE mandate_id = ?
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (project_id,),
                ).fetchone()
        if not row:
            return {}
        report_path = Path(str(row["report_json_path"])).expanduser()
    if not report_path.exists():
        return {}
    report = load_report(report_path)
    return {
        "run_id": report.run_id,
        "brief_id": report.brief_id,
        "summary": report.summary,
        "candidates": [asdict(candidate) for candidate in report.candidates],
        "provider_results": [asdict(result) for result in report.provider_results],
        "report_paths": {
            "json": str(report_path.resolve()),
            "csv": str(Path(str(row["report_csv_path"])).expanduser().resolve()) if str(row["report_csv_path"] or "").strip() else "",
        },
        "execution_backend": row["execution_backend"],
        "generated_at": report.generated_at,
    }
