from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from hr_hunter_transformer.config import resolve_storage_db_path
from hr_hunter_transformer.models import SearchBrief
from hr_hunter_transformer.pipeline import CandidateIntelligencePipeline
from hr_hunter_transformer.scrapingbee_adapter import ScrapingBeeSearchConfig, ScrapingBeeTransformerRetriever
from hr_hunter_transformer.storage import RunStorage


UI_DIR = Path(__file__).resolve().parents[2] / "ui"


class SearchRequest(BaseModel):
    role_title: str
    titles: list[str] = Field(default_factory=list)
    countries: list[str] = Field(default_factory=list)
    cities: list[str] = Field(default_factory=list)
    company_targets: list[str] = Field(default_factory=list)
    required_keywords: list[str] = Field(default_factory=list)
    preferred_keywords: list[str] = Field(default_factory=list)
    industry_keywords: list[str] = Field(default_factory=list)
    target_count: int = 300
    max_queries: int = 60
    pages_per_query: int = 2
    parallel_requests: int = 6
    use_transformer: bool = True


@dataclass
class JobState:
    job_id: str
    status: str = "queued"
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    started_at: str | None = None
    completed_at: str | None = None
    detail: str = "Queued"
    role_title: str = ""
    target_count: int = 300
    run_id: str | None = None
    metrics: dict[str, int] = field(default_factory=dict)
    error: str | None = None


app = FastAPI(title="HR Hunter Transformer UI")
app.mount("/assets", StaticFiles(directory=UI_DIR), name="assets")
storage = RunStorage(resolve_storage_db_path())
jobs: dict[str, JobState] = {}


def _serialize_row(row) -> dict:
    return dict(row) if row is not None else {}


def _serialize_run(run_id: str) -> dict:
    row = storage.fetch_run(run_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Run not found.")
    candidates = []
    for candidate in storage.fetch_candidates(run_id):
        candidates.append(
            {
                **dict(candidate),
                "notes_json": json.loads(candidate["notes_json"]),
                "evidence_json": json.loads(candidate["evidence_json"]),
            }
        )
    raw_hits = [dict(hit) for hit in storage.fetch_raw_hits(run_id, limit=60)]
    return {
        "run": dict(row),
        "candidates": candidates,
        "raw_hits": raw_hits,
    }


async def _execute_job(job: JobState, payload: SearchRequest) -> None:
    job.status = "running"
    job.started_at = datetime.now(timezone.utc).isoformat()
    job.detail = "Fetching public profile results from ScrapingBee."
    brief = SearchBrief(
        role_title=payload.role_title,
        titles=payload.titles,
        countries=payload.countries,
        cities=payload.cities,
        company_targets=payload.company_targets,
        required_keywords=payload.required_keywords,
        preferred_keywords=payload.preferred_keywords,
        industry_keywords=payload.industry_keywords,
        target_count=payload.target_count or 300,
    )
    retriever = ScrapingBeeTransformerRetriever(
        ScrapingBeeSearchConfig(
            max_queries=payload.max_queries,
            pages_per_query=payload.pages_per_query,
            parallel_requests=payload.parallel_requests,
        )
    )
    pipeline = CandidateIntelligencePipeline(use_transformer=payload.use_transformer)
    try:
        queries, hits = await retriever.search_async(brief)
        job.detail = "Ranking and verifying candidates with transformer scoring."
        result = pipeline.run(brief, hits)
        run_id = f"transformer-{uuid4().hex[:12]}"
        storage.save_run(
            run_id=run_id,
            created_at=datetime.now(timezone.utc).isoformat(),
            brief=brief,
            queries=queries,
            hits=hits,
            result=result,
        )
        job.status = "completed"
        job.completed_at = datetime.now(timezone.utc).isoformat()
        job.detail = "Run completed."
        job.run_id = run_id
        job.metrics = {
            "raw_found": result.metrics.raw_found,
            "unique_candidates": result.metrics.unique_candidates,
            "verified_count": result.metrics.verified_count,
            "review_count": result.metrics.review_count,
            "reject_count": result.metrics.reject_count,
        }
    except Exception as exc:
        job.status = "failed"
        job.completed_at = datetime.now(timezone.utc).isoformat()
        job.detail = "Run failed."
        job.error = str(exc)


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(UI_DIR / "index.html")


@app.get("/api/runs")
async def list_runs(limit: int = 12) -> dict:
    rows = [_serialize_row(row) for row in storage.list_runs(limit=limit)]
    return {"runs": rows}


@app.get("/api/runs/{run_id}")
async def get_run(run_id: str) -> dict:
    return _serialize_run(run_id)


@app.post("/api/search")
async def create_search(payload: SearchRequest) -> dict:
    job_id = f"job-{uuid4().hex[:12]}"
    job = JobState(
        job_id=job_id,
        role_title=payload.role_title,
        target_count=payload.target_count or 300,
    )
    jobs[job_id] = job
    asyncio.create_task(_execute_job(job, payload))
    return {"job_id": job_id}


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str) -> dict:
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found.")
    return asdict(job)
