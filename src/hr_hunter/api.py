from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from hr_hunter.briefing import build_search_brief
from hr_hunter.config import (
    load_env_file,
    load_yaml_file,
    resolve_feedback_db_path,
    resolve_output_dir,
    resolve_ranker_model_dir,
)
from hr_hunter.engine import SearchEngine
from hr_hunter.feedback import export_training_rows, init_feedback_db, load_ranker_training_rows, log_feedback
from hr_hunter.output import (
    collect_seen_candidate_keys,
    collect_seen_provider_queries,
    write_report,
)
from hr_hunter.recruiter_app import (
    build_app_bootstrap,
    build_ui_brief_payload,
    extract_job_description_breakdown,
    safe_artifact_path,
)
from hr_hunter.ranker import train_learned_ranker

try:
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import FileResponse
    from fastapi.staticfiles import StaticFiles
except ImportError as exc:  # pragma: no cover - optional dependency
    FastAPI = None
    HTTPException = RuntimeError
    FileResponse = None
    StaticFiles = None
    FASTAPI_IMPORT_ERROR = exc
else:
    FASTAPI_IMPORT_ERROR = None


def _write_app_request(kind: str, payload: Dict[str, Any]) -> Dict[str, str]:
    output_root = resolve_output_dir() / "inbox"
    output_root.mkdir(parents=True, exist_ok=True)
    output_path = output_root / f"{kind}.jsonl"
    record = {
        "kind": kind,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }
    with output_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    return {"saved_to": str(output_path)}


def create_app() -> "FastAPI":
    if FastAPI is None:
        raise RuntimeError(
            "FastAPI is not installed. Run `uv sync --extra api` to enable the API surface."
        ) from FASTAPI_IMPORT_ERROR

    load_env_file(Path(".env"))
    app = FastAPI(title="HR Hunter", version="0.1.0")
    engine = SearchEngine()
    workspace_root = Path(__file__).resolve().parents[2]
    ui_dir = workspace_root / "UI"
    if ui_dir.exists():
        app.mount("/assets", StaticFiles(directory=ui_dir), name="assets")

    @app.get("/")
    async def home() -> FileResponse:
        if not ui_dir.exists():
            raise HTTPException(status_code=404, detail="UI assets are not installed.")
        return FileResponse(ui_dir / "index.html")

    @app.get("/healthz")
    async def healthz() -> Dict[str, str]:
        return {"status": "ok"}

    @app.get("/app-config")
    async def app_config() -> Dict[str, Any]:
        bootstrap = build_app_bootstrap()
        bootstrap["paths"] = {
            "workspace_root": str(workspace_root),
            "output_dir": str(resolve_output_dir()),
            "feedback_db": str(resolve_feedback_db_path()),
            "model_dir": str(resolve_ranker_model_dir()),
        }
        return bootstrap

    @app.post("/search")
    async def search(payload: Dict[str, Any]) -> Dict[str, Any]:
        brief_config = payload.get("brief")
        brief_path = payload.get("brief_path")
        providers = payload.get("providers", ["scrapingbee_google"])
        limit = int(payload.get("limit", 100))
        dry_run = bool(payload.get("dry_run", False))
        exclude_report_paths = payload.get("exclude_report_paths", [])
        exclude_history_dirs = payload.get("exclude_history_dirs", [])

        if brief_path:
            brief_config = load_yaml_file(Path(brief_path))
        if not isinstance(brief_config, dict):
            raise HTTPException(status_code=400, detail="Provide `brief` or `brief_path`.")

        brief = build_search_brief(brief_config)
        exclusion_sources = [Path(value) for value in [*exclude_report_paths, *exclude_history_dirs]]
        exclude_candidate_keys = collect_seen_candidate_keys(
            exclusion_sources
        )
        exclude_provider_queries = collect_seen_provider_queries(
            exclusion_sources
        )
        report = await engine.run(
            brief,
            list(providers),
            limit=limit,
            dry_run=dry_run,
            exclude_candidate_keys=exclude_candidate_keys,
            exclude_provider_queries=exclude_provider_queries,
        )
        return {
            "summary": report.summary,
            "candidates": [asdict(candidate) for candidate in report.candidates],
            "provider_results": [asdict(result) for result in report.provider_results],
        }

    @app.post("/app/jd-breakdown")
    async def app_jd_breakdown(payload: Dict[str, Any]) -> Dict[str, Any]:
        return extract_job_description_breakdown(
            str(payload.get("job_description", "")),
            role_title=str(payload.get("role_title", "")),
        )

    @app.post("/app/search")
    async def app_search(payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            ui_payload = build_ui_brief_payload(payload)
            brief = build_search_brief(ui_payload["brief_config"])
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        if not brief.role_title.strip():
            raise HTTPException(status_code=400, detail="Role title is required.")

        exclude_report_paths = [Path(value) for value in payload.get("exclude_report_paths", [])]
        exclude_history_dirs = [Path(value) for value in payload.get("exclude_history_dirs", [])]
        exclusion_sources = [*exclude_report_paths, *exclude_history_dirs]
        exclude_candidate_keys = collect_seen_candidate_keys(exclusion_sources)
        exclude_provider_queries = collect_seen_provider_queries(exclusion_sources)

        report = await engine.run(
            brief,
            list(ui_payload["providers"]),
            limit=int(ui_payload["limit"]),
            dry_run=bool(payload.get("dry_run", False)),
            exclude_candidate_keys=exclude_candidate_keys,
            exclude_provider_queries=exclude_provider_queries,
        )
        output_dir = Path(ui_payload["output_dir"])
        json_path, csv_path = write_report(
            report,
            output_dir,
            csv_candidate_limit=int(ui_payload["csv_export_limit"]),
        )
        return {
            "summary": report.summary,
            "candidates": [asdict(candidate) for candidate in report.candidates],
            "provider_results": [asdict(result) for result in report.provider_results],
            "report_paths": {
                "json": str(json_path),
                "csv": str(csv_path),
            },
            "csv_export_limit": int(ui_payload["csv_export_limit"]),
            "feedback_db": ui_payload["feedback_db"],
            "model_dir": ui_payload["model_dir"],
            "brief": ui_payload["brief_config"],
            "jd_breakdown": ui_payload["job_description_breakdown"],
        }

    @app.get("/app/artifact")
    async def app_artifact(path: str) -> FileResponse:
        try:
            artifact_path = safe_artifact_path(path, workspace_root=workspace_root)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if not artifact_path.exists() or not artifact_path.is_file():
            raise HTTPException(status_code=404, detail="Artifact not found.")
        return FileResponse(artifact_path)

    @app.post("/feedback")
    async def feedback(payload: Dict[str, Any]) -> Dict[str, Any]:
        brief = None
        brief_config = payload.get("brief")
        brief_path = payload.get("brief_path")
        if brief_path:
            brief_config = load_yaml_file(Path(brief_path))
        if isinstance(brief_config, dict):
            brief = build_search_brief(brief_config)

        try:
            return log_feedback(
                report_path=Path(str(payload.get("report_path", ""))).expanduser().resolve(),
                candidate_ref=str(payload.get("candidate_ref", "")),
                recruiter_id=str(payload.get("recruiter_id", "")),
                action=str(payload.get("action", "")),
                reason_code=str(payload.get("reason_code", "")),
                note=str(payload.get("note", "")),
                recruiter_name=str(payload.get("recruiter_name", "")),
                team_id=str(payload.get("team_id", "")),
                db_path=Path(str(payload["feedback_db"])).expanduser().resolve() if payload.get("feedback_db") else None,
                brief=brief,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/app/feedback")
    async def app_feedback(payload: Dict[str, Any]) -> Dict[str, Any]:
        brief_config = payload.get("brief")
        brief = build_search_brief(brief_config) if isinstance(brief_config, dict) else None
        try:
            result = log_feedback(
                report_path=Path(str(payload.get("report_path", ""))).expanduser().resolve(),
                candidate_ref=str(payload.get("candidate_ref", "")),
                recruiter_id=str(payload.get("recruiter_id", "")),
                action=str(payload.get("action", "")),
                reason_code=str(payload.get("reason_code", "")),
                note=str(payload.get("note", "")),
                recruiter_name=str(payload.get("recruiter_name", "")),
                team_id=str(payload.get("team_id", "")),
                db_path=Path(str(payload.get("feedback_db", resolve_feedback_db_path()))).expanduser().resolve(),
                brief=brief,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        result["feedback_db"] = str(Path(str(payload.get("feedback_db", resolve_feedback_db_path()))).expanduser().resolve())
        return result

    @app.post("/feedback/export")
    async def feedback_export(payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            db_path = Path(str(payload["feedback_db"])).expanduser().resolve() if payload.get("feedback_db") else None
            output_path = export_training_rows(
                Path(str(payload.get("output_path", ""))).expanduser().resolve(),
                db_path=db_path,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "output_path": str(output_path),
            "row_count": len(load_ranker_training_rows(db_path)),
        }

    @app.post("/train-ranker")
    async def train_ranker(payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            db_path = Path(str(payload["feedback_db"])).expanduser().resolve() if payload.get("feedback_db") else None
            init_feedback_db(db_path)
            training_rows = load_ranker_training_rows(db_path)
            return train_learned_ranker(
                training_rows,
                model_dir=Path(str(payload["model_dir"])).expanduser().resolve() if payload.get("model_dir") else None,
                n_estimators=int(payload.get("n_estimators", 80)),
                learning_rate=float(payload.get("learning_rate", 0.1)),
                num_leaves=int(payload.get("num_leaves", 31)),
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/app/train-ranker")
    async def app_train_ranker(payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            db_path = Path(str(payload.get("feedback_db", resolve_feedback_db_path()))).expanduser().resolve()
            model_dir = Path(str(payload.get("model_dir", resolve_ranker_model_dir()))).expanduser().resolve()
            init_feedback_db(db_path)
            training_rows = load_ranker_training_rows(db_path)
            return train_learned_ranker(
                training_rows,
                model_dir=model_dir,
                n_estimators=int(payload.get("n_estimators", 80)),
                learning_rate=float(payload.get("learning_rate", 0.1)),
                num_leaves=int(payload.get("num_leaves", 31)),
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/app/support-request")
    async def app_support_request(payload: Dict[str, Any]) -> Dict[str, str]:
        try:
            return _write_app_request("support_requests", payload)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/app/feature-request")
    async def app_feature_request(payload: Dict[str, Any]) -> Dict[str, str]:
        try:
            return _write_app_request("feature_requests", payload)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    return app
