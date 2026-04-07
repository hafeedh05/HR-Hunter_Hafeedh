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
from hr_hunter.feedback import export_training_rows, init_feedback_db, load_ranker_training_rows, log_feedback
from hr_hunter.orchestrator import load_search_matrix, run_search_matrix
from hr_hunter.output import (
    collect_seen_candidate_keys,
    collect_seen_provider_queries,
    load_report,
    write_report,
)
from hr_hunter.ranker import train_learned_ranker
from hr_hunter.sheets import append_report_to_sheet
from hr_hunter.state import persist_search_run
from hr_hunter.verifier import PublicEvidenceVerifier, refresh_report_summary


def parse_provider_names(raw: str) -> List[str]:
    providers = [value.strip() for value in raw.split(",") if value.strip()]
    return providers or ["scrapingbee_google"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Executive search assistant.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    search_parser = subparsers.add_parser("search", help="Run a search brief.")
    search_parser.add_argument("--brief", required=True, help="Path to YAML brief.")
    search_parser.add_argument(
        "--providers",
        default="scrapingbee_google",
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

    matrix_parser = subparsers.add_parser("matrix-search", help="Run multiple search briefs as one deduped search matrix.")
    matrix_parser.add_argument("--matrix", required=True, help="Path to the matrix YAML manifest.")
    matrix_parser.add_argument(
        "--limit",
        type=int,
        default=120,
        help="Maximum final merged profiles to keep.",
    )
    matrix_parser.add_argument(
        "--verify-top",
        type=int,
        default=0,
        help="Run public-web verification on the top N merged candidates after the matrix run.",
    )
    matrix_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compile matrix queries without making provider requests.",
    )
    matrix_parser.add_argument(
        "--output-dir",
        help="Override the output directory for JSON and CSV artifacts.",
    )
    matrix_parser.add_argument(
        "--exclude-report",
        action="append",
        default=[],
        help="JSON report path to use as an exclusion source. Repeatable.",
    )
    matrix_parser.add_argument(
        "--exclude-history-dir",
        action="append",
        default=[],
        help="Directory of prior JSON reports to exclude from future runs. Repeatable.",
    )
    matrix_parser.add_argument(
        "--spreadsheet-id",
        help="Optional Google Sheets spreadsheet id to append net-new candidates into after the run.",
    )
    matrix_parser.add_argument(
        "--worksheet",
        default="Candidates",
        help="Worksheet/tab name to append into when --spreadsheet-id is set.",
    )
    matrix_parser.add_argument(
        "--credentials-file",
        help="Optional Google service account credentials file for automatic sheet sync.",
    )
    matrix_parser.add_argument(
        "--sheet-history-report",
        action="append",
        default=[],
        help="Additional report paths to treat as already synced history during automatic sheet sync.",
    )
    matrix_parser.add_argument(
        "--sheet-history-dir",
        action="append",
        default=[],
        help="Additional report directories to treat as already synced history during automatic sheet sync.",
    )
    matrix_parser.add_argument(
        "--append-csv",
        help="Optional path to also write an append-only CSV payload of the net-new rows during automatic sheet sync.",
    )

    sheet_parser = subparsers.add_parser("sheet-sync", help="Append net-new candidates from a report into a Google Sheet.")
    sheet_parser.add_argument("--report", required=True, help="Path to the JSON report to sync.")
    sheet_parser.add_argument("--spreadsheet-id", required=True, help="Target Google Sheets spreadsheet id.")
    sheet_parser.add_argument("--worksheet", default="Candidates", help="Worksheet/tab name to append into.")
    sheet_parser.add_argument(
        "--history-report",
        action="append",
        default=[],
        help="JSON report path to treat as already synced history. Repeatable.",
    )
    sheet_parser.add_argument(
        "--history-dir",
        action="append",
        default=[],
        help="Directory of JSON reports to treat as already synced history. Repeatable.",
    )
    sheet_parser.add_argument(
        "--credentials-file",
        help="Optional Google service account credentials file. Falls back to GOOGLE_SERVICE_ACCOUNT_JSON.",
    )
    sheet_parser.add_argument(
        "--append-csv",
        help="Optional path to also write an append-only CSV payload of the net-new rows.",
    )

    feedback_parser = subparsers.add_parser("feedback-log", help="Persist recruiter feedback for a report candidate.")
    feedback_parser.add_argument("--report", required=True, help="Path to the JSON report.")
    feedback_parser.add_argument("--candidate", required=True, help="Candidate id, URL, or name from the report.")
    feedback_parser.add_argument("--recruiter-id", required=True, help="Stable recruiter identifier.")
    feedback_parser.add_argument("--action", required=True, help="Feedback action to save.")
    feedback_parser.add_argument("--reason-code", default="", help="Optional structured reason code.")
    feedback_parser.add_argument("--note", default="", help="Optional free-text note.")
    feedback_parser.add_argument("--recruiter-name", default="", help="Optional recruiter display name.")
    feedback_parser.add_argument("--team-id", default="", help="Optional recruiter team id.")
    feedback_parser.add_argument("--feedback-db", help="Override the feedback SQLite database path.")
    feedback_parser.add_argument("--brief", help="Optional brief YAML path for richer feature snapshots.")

    feedback_export_parser = subparsers.add_parser("feedback-export", help="Export ranker training rows from feedback.")
    feedback_export_parser.add_argument("--output", required=True, help="JSON path for exported training rows.")
    feedback_export_parser.add_argument("--feedback-db", help="Override the feedback SQLite database path.")

    train_ranker_parser = subparsers.add_parser("train-ranker", help="Train a LightGBM LambdaRank model from feedback.")
    train_ranker_parser.add_argument("--feedback-db", help="Override the feedback SQLite database path.")
    train_ranker_parser.add_argument("--model-dir", help="Directory to write the trained model artifacts into.")
    train_ranker_parser.add_argument("--n-estimators", type=int, default=80, help="LightGBM tree count.")
    train_ranker_parser.add_argument("--learning-rate", type=float, default=0.1, help="LightGBM learning rate.")
    train_ranker_parser.add_argument("--num-leaves", type=int, default=31, help="LightGBM num_leaves setting.")

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
    persist_search_run(
        brief,
        report,
        provider_names=parse_provider_names(args.providers),
        limit_requested=args.limit,
        json_report_path=json_path,
        csv_report_path=csv_path,
    )

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
    persist_search_run(
        brief,
        report,
        provider_names=[result.provider_name for result in report.provider_results],
        limit_requested=args.limit,
        json_report_path=json_path,
        csv_report_path=csv_path,
        execution_backend="local_verify",
    )

    result = {
        "run_id": report.run_id,
        "summary": report.summary,
        "verification_stats": verification_stats,
        "json_report": str(json_path),
        "csv_report": str(csv_path),
    }
    print(json.dumps(result, indent=2))
    return 0


async def run_matrix_search(args: argparse.Namespace) -> int:
    load_env_file(Path(".env"))

    matrix = load_search_matrix(Path(args.matrix))
    report = await run_search_matrix(
        matrix,
        dry_run=bool(args.dry_run),
        limit=args.limit,
        verify_top=args.verify_top,
        extra_exclude_reports=[Path(value) for value in args.exclude_report if value],
        extra_exclude_history_dirs=[Path(value) for value in args.exclude_history_dir if value],
    )

    output_dir = resolve_output_dir(args.output_dir)
    json_path, csv_path = write_report(report, output_dir)
    primary_brief = build_search_brief(load_yaml_file(matrix.primary_brief_path))
    persist_search_run(
        primary_brief,
        report,
        provider_names=[provider for strategy in matrix.strategies for provider in strategy.providers],
        limit_requested=args.limit,
        json_report_path=json_path,
        csv_report_path=csv_path,
        execution_backend="matrix_local_engine",
    )
    sheet_sync = None
    if args.spreadsheet_id:
        history_paths = [
            Path(value)
            for value in [
                *args.exclude_report,
                *args.exclude_history_dir,
                *args.sheet_history_report,
                *args.sheet_history_dir,
            ]
            if value
        ]
        sheet_sync = append_report_to_sheet(
            report_path=json_path,
            spreadsheet_id=args.spreadsheet_id,
            worksheet_name=args.worksheet,
            history_paths=history_paths,
            credentials_file=args.credentials_file,
            append_csv_path=Path(args.append_csv).expanduser().resolve() if args.append_csv else None,
        )

    result = {
        "run_id": report.run_id,
        "dry_run": report.dry_run,
        "summary": report.summary,
        "json_report": str(json_path),
        "csv_report": str(csv_path),
    }
    if sheet_sync:
        result["sheet_sync"] = sheet_sync
    print(json.dumps(result, indent=2))
    return 0


def run_sheet_sync(args: argparse.Namespace) -> int:
    load_env_file(Path(".env"))

    result = append_report_to_sheet(
        report_path=Path(args.report).expanduser().resolve(),
        spreadsheet_id=args.spreadsheet_id,
        worksheet_name=args.worksheet,
        history_paths=[Path(value) for value in [*args.history_report, *args.history_dir] if value],
        credentials_file=args.credentials_file,
        append_csv_path=Path(args.append_csv).expanduser().resolve() if args.append_csv else None,
    )
    print(json.dumps(result, indent=2))
    return 0


def run_feedback_log(args: argparse.Namespace) -> int:
    load_env_file(Path(".env"))

    brief = None
    if args.brief:
        brief = build_search_brief(load_yaml_file(Path(args.brief)))

    result = log_feedback(
        report_path=Path(args.report).expanduser().resolve(),
        candidate_ref=args.candidate,
        recruiter_id=args.recruiter_id,
        action=args.action,
        reason_code=args.reason_code,
        note=args.note,
        recruiter_name=args.recruiter_name,
        team_id=args.team_id,
        db_path=Path(args.feedback_db).expanduser().resolve() if args.feedback_db else None,
        brief=brief,
    )
    print(json.dumps(result, indent=2))
    return 0


def run_feedback_export(args: argparse.Namespace) -> int:
    load_env_file(Path(".env"))

    output_path = export_training_rows(
        Path(args.output).expanduser().resolve(),
        db_path=Path(args.feedback_db).expanduser().resolve() if args.feedback_db else None,
    )
    print(
        json.dumps(
            {
                "output_path": str(output_path),
                "row_count": len(load_ranker_training_rows(Path(args.feedback_db).expanduser().resolve() if args.feedback_db else None)),
            },
            indent=2,
        )
    )
    return 0


def run_train_ranker(args: argparse.Namespace) -> int:
    load_env_file(Path(".env"))

    db_path = Path(args.feedback_db).expanduser().resolve() if args.feedback_db else None
    init_feedback_db(db_path)
    training_rows = load_ranker_training_rows(db_path)
    result = train_learned_ranker(
        training_rows,
        model_dir=Path(args.model_dir).expanduser().resolve() if args.model_dir else None,
        n_estimators=args.n_estimators,
        learning_rate=args.learning_rate,
        num_leaves=args.num_leaves,
    )
    print(json.dumps(result, indent=2))
    return 0


def run_server(args: argparse.Namespace) -> int:
    try:
        import uvicorn
    except ImportError as exc:
        raise RuntimeError(
            "uvicorn is not installed. Run `uv sync --extra api` to enable `serve`."
        ) from exc

    load_env_file(Path(".env"))
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
    if args.command == "matrix-search":
        return asyncio.run(run_matrix_search(args))
    if args.command == "sheet-sync":
        return run_sheet_sync(args)
    if args.command == "feedback-log":
        return run_feedback_log(args)
    if args.command == "feedback-export":
        return run_feedback_export(args)
    if args.command == "train-ranker":
        return run_train_ranker(args)
    if args.command == "serve":
        return run_server(args)
    raise RuntimeError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
