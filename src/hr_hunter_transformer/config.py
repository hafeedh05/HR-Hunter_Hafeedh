from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable


WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SECRET_ENV_FILES = (
    WORKSPACE_ROOT / ".env",
    WORKSPACE_ROOT.parent / "HR Hunter Clone" / ".env",
    WORKSPACE_ROOT.parent / "HR Hunter By Team" / ".env",
    WORKSPACE_ROOT.parent / "Original HR Hunter Files" / ".env",
    Path("/etc/reap/reap.env"),
    Path.home() / "reap-bot" / ".env",
    Path.home() / ".env",
)


def parse_env_lines(lines: Iterable[str]) -> dict[str, str]:
    loaded: dict[str, str] = {}
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            loaded[key] = value
    return loaded


def load_env_values(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    return parse_env_lines(path.read_text(encoding="utf-8").splitlines())


def resolve_secret(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value:
        return value
    for path in DEFAULT_SECRET_ENV_FILES:
        values = load_env_values(path)
        if name in values:
            os.environ.setdefault(name, values[name])
            return values[name]
    try:
        from hr_hunter.config import resolve_secret as resolve_main_app_secret
    except Exception:
        resolve_main_app_secret = None
    if resolve_main_app_secret is not None:
        shared_value = resolve_main_app_secret(name)
        if shared_value:
            os.environ.setdefault(name, shared_value)
            return shared_value
    return default


def resolve_output_dir(explicit: str | None = None) -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()
    configured = os.getenv("HR_HUNTER_TRANSFORMER_OUTPUT_DIR")
    if configured:
        return Path(configured).expanduser().resolve()
    return (WORKSPACE_ROOT / "output").resolve()


def resolve_storage_db_path(explicit: str | None = None) -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()
    configured = os.getenv("HR_HUNTER_TRANSFORMER_DB")
    if configured:
        return Path(configured).expanduser().resolve()
    return resolve_output_dir() / "hr_hunter_transformer.db"
