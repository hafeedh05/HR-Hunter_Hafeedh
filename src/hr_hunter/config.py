from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import yaml


DEFAULT_SECRET_ENV_FILES = (
    "/etc/reap/reap.env",
    "~/reap-bot/.env",
    "~/.env",
)


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


def resolve_secret(name: str, default: Optional[str] = None) -> Optional[str]:
    if os.getenv(name):
        return os.getenv(name)

    for path in iter_secret_env_files():
        values = load_env_values(path)
        if name in values:
            os.environ.setdefault(name, values[name])
            return values[name]

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
