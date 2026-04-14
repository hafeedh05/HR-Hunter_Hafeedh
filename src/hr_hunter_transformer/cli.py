from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from hr_hunter_transformer.config import resolve_storage_db_path
from hr_hunter_transformer.models import SearchBrief
from hr_hunter_transformer.pipeline import CandidateIntelligencePipeline
from hr_hunter_transformer.scrapingbee_adapter import ScrapingBeeSearchConfig, ScrapingBeeTransformerRetriever
from hr_hunter_transformer.storage import RunStorage


def _split_csv(values: list[str] | None) -> list[str]:
    items: list[str] = []
    for value in values or []:
        items.extend(part.strip() for part in value.split(","))
    return [item for item in items if item]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run HR Hunter Transformer with ScrapingBee retrieval.")
    parser.add_argument("--role-title", required=True)
    parser.add_argument("--title", action="append", dest="titles")
    parser.add_argument("--country", action="append", dest="countries")
    parser.add_argument("--city", action="append", dest="cities")
    parser.add_argument("--company", action="append", dest="companies")
    parser.add_argument("--required", action="append", dest="required_keywords")
    parser.add_argument("--preferred", action="append", dest="preferred_keywords")
    parser.add_argument("--industry", action="append", dest="industry_keywords")
    parser.add_argument("--target-count", type=int, default=300)
    parser.add_argument("--max-queries", type=int, default=60)
    parser.add_argument("--pages-per-query", type=int, default=1)
    parser.add_argument("--parallel-requests", type=int, default=8)
    parser.add_argument("--db-path")
    parser.add_argument("--brief-json")
    parser.add_argument("--use-transformer", action="store_true", default=True)
    parser.add_argument("--no-transformer", action="store_false", dest="use_transformer")
    return parser


def brief_from_args(args: argparse.Namespace) -> SearchBrief:
    if args.brief_json:
        payload = json.loads(Path(args.brief_json).read_text(encoding="utf-8"))
        return SearchBrief(
            role_title=payload["role_title"],
            titles=payload.get("titles", []),
            countries=payload.get("countries", []),
            cities=payload.get("cities", []),
            company_targets=payload.get("company_targets", []),
            required_keywords=payload.get("required_keywords", []),
            preferred_keywords=payload.get("preferred_keywords", []),
            industry_keywords=payload.get("industry_keywords", []),
            target_count=int(payload.get("target_count", 300) or 300),
        )
    return SearchBrief(
        role_title=args.role_title,
        titles=_split_csv(args.titles),
        countries=_split_csv(args.countries),
        cities=_split_csv(args.cities),
        company_targets=_split_csv(args.companies),
        required_keywords=_split_csv(args.required_keywords),
        preferred_keywords=_split_csv(args.preferred_keywords),
        industry_keywords=_split_csv(args.industry_keywords),
        target_count=int(args.target_count or 300),
    )


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    brief = brief_from_args(args)
    retriever = ScrapingBeeTransformerRetriever(
        ScrapingBeeSearchConfig(
            pages_per_query=args.pages_per_query,
            parallel_requests=args.parallel_requests,
            max_queries=args.max_queries,
        )
    )
    pipeline = CandidateIntelligencePipeline(use_transformer=args.use_transformer)
    queries, hits = retriever.search(brief)
    result = pipeline.run(brief, hits)

    storage = RunStorage(resolve_storage_db_path(args.db_path))
    run_id = f"transformer-{uuid4().hex[:12]}"
    created_at = datetime.now(timezone.utc).isoformat()
    storage.save_run(
        run_id=run_id,
        created_at=created_at,
        brief=brief,
        queries=queries,
        hits=hits,
        result=result,
    )
    print(
        json.dumps(
            {
                "run_id": run_id,
                "queries": len(queries),
                "raw_found": result.metrics.raw_found,
                "extracted_records": result.metrics.extracted_records,
                "unique_candidates": result.metrics.unique_candidates,
                "verified": result.metrics.verified_count,
                "review": result.metrics.review_count,
                "reject": result.metrics.reject_count,
                "db_path": str(storage.db_path),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
