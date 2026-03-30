from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List

from hr_hunter.briefing import build_search_brief
from hr_hunter.config import load_yaml_file
from hr_hunter.engine import SearchEngine

try:
    from fastapi import FastAPI, HTTPException
except ImportError as exc:  # pragma: no cover - optional dependency
    FastAPI = None
    HTTPException = RuntimeError
    FASTAPI_IMPORT_ERROR = exc
else:
    FASTAPI_IMPORT_ERROR = None


def create_app() -> "FastAPI":
    if FastAPI is None:
        raise RuntimeError(
            "FastAPI is not installed. Run `uv sync --extra api` to enable the API surface."
        ) from FASTAPI_IMPORT_ERROR

    app = FastAPI(title="HR Hunter", version="0.1.0")
    engine = SearchEngine()

    @app.get("/healthz")
    async def healthz() -> Dict[str, str]:
        return {"status": "ok"}

    @app.post("/search")
    async def search(payload: Dict[str, Any]) -> Dict[str, Any]:
        brief_config = payload.get("brief")
        brief_path = payload.get("brief_path")
        providers = payload.get("providers", ["pdl", "scrapingbee_google"])
        limit = int(payload.get("limit", 100))
        dry_run = bool(payload.get("dry_run", False))

        if brief_path:
            brief_config = load_yaml_file(Path(brief_path))
        if not isinstance(brief_config, dict):
            raise HTTPException(status_code=400, detail="Provide `brief` or `brief_path`.")

        brief = build_search_brief(brief_config)
        report = await engine.run(brief, list(providers), limit=limit, dry_run=dry_run)
        return {
            "summary": report.summary,
            "candidates": [asdict(candidate) for candidate in report.candidates],
            "provider_results": [asdict(result) for result in report.provider_results],
        }

    return app
