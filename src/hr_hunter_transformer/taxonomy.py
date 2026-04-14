from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


@lru_cache(maxsize=1)
def load_taxonomy() -> dict[str, dict[str, tuple[str, ...]]]:
    path = Path(__file__).with_name("taxonomy_data.yaml")
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    taxonomy: dict[str, dict[str, tuple[str, ...]]] = {}
    if not isinstance(payload, dict):
        return taxonomy
    for family, subfamilies in payload.items():
        if not isinstance(subfamilies, dict):
            continue
        normalized_subfamilies: dict[str, tuple[str, ...]] = {}
        for subfamily, aliases in subfamilies.items():
            if not isinstance(aliases, list):
                continue
            normalized_subfamilies[str(subfamily)] = tuple(str(alias).strip() for alias in aliases if str(alias).strip())
        if normalized_subfamilies:
            taxonomy[str(family)] = normalized_subfamilies
    return taxonomy


def family_hints_map() -> dict[str, tuple[str, ...]]:
    hints: dict[str, tuple[str, ...]] = {}
    for family, subfamilies in load_taxonomy().items():
        values: list[str] = []
        for aliases in subfamilies.values():
            values.extend(list(aliases))
        hints[family] = tuple(values)
    return hints


def resolve_subfamily(role_family: str, title_value: str, normalize: Any) -> str:
    title = normalize(title_value)
    taxonomy = load_taxonomy()
    subfamilies = taxonomy.get(role_family, {})
    for subfamily, aliases in subfamilies.items():
        if any(normalize(alias) in title for alias in aliases):
            return subfamily
    return next(iter(subfamilies.keys()), "general")

