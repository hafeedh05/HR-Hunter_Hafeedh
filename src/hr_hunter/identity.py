from __future__ import annotations

import re
from typing import Set
from urllib.parse import urlparse

from hr_hunter.models import CandidateProfile


NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def normalize_identity_text(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    return " ".join(NON_ALNUM_RE.sub(" ", raw).split())


def canonicalize_profile_url(url: str | None) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""

    parsed = urlparse(raw)
    if parsed.netloc:
        host = parsed.netloc.lower()
        path = re.sub(r"/+", "/", parsed.path or "").rstrip("/").lower()
        return f"{host}{path}"

    fallback = raw.lower().split("?", 1)[0].split("#", 1)[0].rstrip("/")
    return re.sub(r"/+", "/", fallback)


def candidate_identity_keys(candidate: CandidateProfile) -> Set[str]:
    keys: Set[str] = set()

    for url in {candidate.linkedin_url, candidate.source_url}:
        canonical_url = canonicalize_profile_url(url)
        if canonical_url:
            keys.add(f"url:{canonical_url}")

    name = normalize_identity_text(candidate.full_name)
    company = normalize_identity_text(candidate.current_company)
    title = normalize_identity_text(candidate.current_title)
    location = normalize_identity_text(candidate.location_name)

    if name and company:
        keys.add(f"person:{name}|{company}")
    if name and company and title:
        keys.add(f"role:{name}|{company}|{title}")
    elif name and title and location:
        keys.add(f"role:{name}|{title}|{location}")
    elif name and location:
        keys.add(f"person:{name}|{location}")

    if not keys and name:
        keys.add(f"name:{name}")

    return keys


def candidate_primary_key(candidate: CandidateProfile) -> str:
    keys = candidate_identity_keys(candidate)
    for prefix in ("url:", "person:", "role:", "name:"):
        for key in sorted(keys):
            if key.startswith(prefix):
                return key
    return ""
