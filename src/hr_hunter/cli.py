from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import List

from hr_hunter.api import create_app
from hr_hunter.briefing import build_search_brief
from hr_hunter.config import load_env_file, load_yaml_file, resolve_output_dir
from hr_hunter.engine import SearchEngine
from hr_hunter.output import (
    collect_seen_candidate_keys,
    collect_seen_provider_queries,
    load_report,
    write_report,
)
from hr_hunter.verifier import PublicEvidenceVerifier, refresh_report_summary


def parse_provider_names(raw: str) -> List[str]:
    providers = [value.strip() for value in raw.split(",") if value.strip()]
    return providers or ["pdl", "scrapingbee_google"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Executive search assistant.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    search_parser = subparsers.add_parser("search", help="Run a search brief.")
    search_parser.add_argument("--brief", required=True, help="Path to YAML brief.")
    search_parser.add_argument(
        "--providers",
        default="pdl,scrapingbee_google",
        help="Comma-separated provider order.",
    )
    search_parser.add_argument("--limit", type=int, default=100, help="Maximum profiles to keep.")
    search_parser.add_argument(
        "--verify-top",
        type=int,
        default=0,
        help="Run public-web evidence verification on the top N candidates after search.",
    )
    search_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compile queries without making provider requests.",
    )
    search_parser.add_argument(
        "--output-dir",
        help="Override the output directory for JSON and CSV artifacts.",
    )
    search_parser.add_argument(
        "--exclude-report",
        action="append",
        default=[],
        help="JSON report path to use as an exclusion source. Repeatable.",
    )
    search_parser.add_argument(
        "--exclude-history-dir",
        action="append",
        default=[],
        help="Directory of prior JSON reports to exclude from future runs. Repeatable.",
    )

    verify_parser = subparsers.add_parser("verify", help="Verify an existing report with public-web evidence.")
    verify_parser.add_argument("--brief", required=True, help="Path to YAML brief.")
    verify_parser.add_argument("--report", required=True, help="Path to the JSON report to verify.")
    verify_parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum candidates to verify from the report.",
    )
    verify_parser.add_argument(
        "--output-dir",
        help="Override the output directory for JSON and CSV artifacts.",
    )

    serve_parser = subparsers.add_parser("serve", help="Serve the optional FastAPI app.")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8000)

    return parser


async def run_search(args: argparse.Namespace) -> int:
    load_env_file(Path(".env"))

    brief_config = load_yaml_file(Path(args.brief))
    brief = build_search_brief(brief_config)

    exclusion_sources = [Path(value) for value in [*args.exclude_report, *args.exclude_history_dir] if value]
    exclude_candidate_keys = collect_seen_candidate_keys(exclusion_sources)
    exclude_provider_queries = collect_seen_provider_queries(exclusion_sources)

    engine = SearchEngine()
    report = await engine.run(
        brief,
        provider_names=parse_provider_names(args.providers),
        limit=args.limit,
        dry_run=bool(args.dry_run),
        exclude_candidate_keys=exclude_candidate_keys,
        exclude_provider_queries=exclude_provider_queries,
    )

    verification_stats = None
    if args.verify_top and not args.dry_run:
        verifier = PublicEvidenceVerifier(brief.provider_settings.get("scrapingbee_google", {}))
        selected_candidates = report.candidates[: max(0, args.verify_top)]
        verification_stats = await verifier.verify_candidates(selected_candidates, brief, limit=args.verify_top)
        refresh_report_summary(report, verification_stats)

    output_dir = resolve_output_dir(args.output_dir)
    json_path, csv_path = write_report(report, output_dir)

    result = {
        "run_id": report.run_id,
        "dry_run": report.dry_run,
        "summary": report.summary,
        "json_report": str(json_path),
        "csv_report": str(csv_path),
    }
    if verification_stats:
        result["verification_stats"] = verification_stats
    print(json.dumps(result, indent=2))
    return 0


async def run_verify(args: argparse.Namespace) -> int:
    load_env_file(Path(".env"))

    brief_config = load_yaml_file(Path(args.brief))
    brief = build_search_brief(brief_config)
    report = load_report(Path(args.report))

    verifier = PublicEvidenceVerifier(brief.provider_settings.get("scrapingbee_google", {}))
    verification_stats = await verifier.verify_candidates(report.candidates, brief, limit=args.limit)
    refresh_report_summary(report, verification_stats)
    report.run_id = f"{report.run_id}-verified"

    output_dir = resolve_output_dir(args.output_dir or str(Path(args.report).expanduser().resolve().parent))
    json_path, csv_path = write_report(report, output_dir)

    result = {
        "run_id": report.run_id,
        "summary": report.summary,
        "verification_stats": verification_stats,
        "json_report": str(json_path),
        "csv_report": str(csv_path),
    }
    print(json.dumps(result, indent=2))
    return 0


def run_server(args: argparse.Namespace) -> int:
    try:
        import uvicorn
    except ImportError as exc:
        raise RuntimeError(
            "uvicorn is not installed. Run `uv sync --extra api` to enable `serve`."
        ) from exc

    app = create_app()
    uvicorn.run(app, host=args.host, port=args.port)
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "search":
        return asyncio.run(run_search(args))
    if args.command == "verify":
        return asyncio.run(run_verify(args))
    if args.command == "serve":
        return run_server(args)
    raise RuntimeError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
