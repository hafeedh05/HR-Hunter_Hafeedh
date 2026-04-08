from __future__ import annotations

import os
from pathlib import Path
from functools import lru_cache
from typing import Any, Dict, Iterable, Optional

import yaml

try:
    from google.cloud import secretmanager
except ImportError:  # pragma: no cover - optional in local dev
    secretmanager = None


DEFAULT_SECRET_ENV_FILES = (
    "/etc/reap/reap.env",
    "~/reap-bot/.env",
    "~/.env",
)
TRUE_ENV_VALUES = {"1", "true", "yes", "on"}


def parse_env_lines(lines: Iterable[str]) -> Dict[str, str]:
    loaded: Dict[str, str] = {}
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


def load_env_values(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    return parse_env_lines(path.read_text(encoding="utf-8").splitlines())


def load_env_file(path: Path) -> None:
    for key, value in load_env_values(path).items():
        os.environ.setdefault(key, value)


def env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in TRUE_ENV_VALUES


def iter_secret_env_files() -> Iterable[Path]:
    configured = os.getenv("HR_HUNTER_SECRET_ENV_FILES", "")
    seen = set()
    for raw_path in [*DEFAULT_SECRET_ENV_FILES, *configured.split(os.pathsep)]:
        if not raw_path:
            continue
        path = Path(raw_path).expanduser()
        normalized = str(path)
        if normalized in seen:
            continue
        seen.add(normalized)
        yield path


def _default_secret_name(name: str) -> str:
    return str(name or "").strip().lower().replace("_", "-")


@lru_cache(maxsize=64)
def _load_secret_manager_value(project_id: str, secret_name: str, version: str) -> Optional[str]:
    if secretmanager is None:
        return None
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/{version}"
    response = client.access_secret_version(request={"name": secret_path})
    payload = response.payload.data.decode("utf-8")
    return payload


def resolve_secret_manager_secret_name(name: str) -> Optional[str]:
    explicit = os.getenv(f"{name}_SECRET_NAME")
    if explicit:
        return explicit.strip()
    if os.getenv("HR_HUNTER_USE_SECRET_MANAGER", "").strip().lower() not in {"1", "true", "yes", "on"}:
        return None
    return _default_secret_name(name)


def resolve_secret_manager_project() -> Optional[str]:
    return (
        os.getenv("HR_HUNTER_GCP_PROJECT")
        or os.getenv("GOOGLE_CLOUD_PROJECT")
        or os.getenv("GCP_PROJECT")
        or None
    )


def resolve_secret(name: str, default: Optional[str] = None) -> Optional[str]:
    if os.getenv(name):
        return os.getenv(name)

    for path in iter_secret_env_files():
        values = load_env_values(path)
        if name in values:
            os.environ.setdefault(name, values[name])
            return values[name]

    project_id = resolve_secret_manager_project()
    secret_name = resolve_secret_manager_secret_name(name)
    secret_version = os.getenv(f"{name}_SECRET_VERSION", "latest").strip() or "latest"
    if project_id and secret_name:
        try:
            value = _load_secret_manager_value(project_id, secret_name, secret_version)
        except Exception:
            value = None
        if value is not None:
            os.environ.setdefault(name, value)
            return value

    return default


def load_yaml_file(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}
    if not isinstance(loaded, dict):
        raise ValueError("Brief file must contain a YAML object at the top level.")
    return loaded


def resolve_output_dir(explicit: Optional[str] = None) -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()
    configured = os.getenv("HR_HUNTER_OUTPUT_DIR")
    if configured:
        return Path(configured).expanduser().resolve()
    return Path("output/search").resolve()


def resolve_database_locator(
    explicit: Optional[str | Path] = None,
    *,
    env_var: str,
    default_path: str,
) -> str | Path:
    if explicit:
        raw_value = str(explicit).strip()
        if "://" in raw_value:
            return raw_value
        return Path(explicit).expanduser().resolve()

    shared_url = os.getenv("HR_HUNTER_DATABASE_URL")
    if shared_url:
        return shared_url.strip()

    configured = os.getenv(env_var)
    if configured:
        configured = configured.strip()
        if "://" in configured:
            return configured
        return Path(configured).expanduser().resolve()

    return Path(default_path).resolve()


def resolve_feedback_db_path(explicit: Optional[str] = None) -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()
    configured = os.getenv("HR_HUNTER_FEEDBACK_DB")
    if configured:
        return Path(configured).expanduser().resolve()
    return Path("output/feedback/hr_hunter_feedback.db").resolve()


def resolve_ranker_model_dir(explicit: Optional[str] = None) -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()
    configured = os.getenv("HR_HUNTER_RANKER_MODEL_DIR")
    if configured:
        return Path(configured).expanduser().resolve()
    return Path("output/models/ranker/latest").resolve()


def resolve_state_db_path(explicit: Optional[str] = None) -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()
    configured = os.getenv("HR_HUNTER_STATE_DB")
    if configured:
        return Path(configured).expanduser().resolve()
    return Path("output/state/hr_hunter_state.db").resolve()
