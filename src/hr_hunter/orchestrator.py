from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set

from hr_hunter.briefing import build_search_brief, unique_preserving_order
from hr_hunter.config import load_yaml_file
from hr_hunter.engine import SearchEngine, dedupe_candidates
from hr_hunter.identity import candidate_identity_keys
from hr_hunter.models import CandidateProfile, ProviderRunResult, SearchBrief, SearchRunReport
from hr_hunter.output import (
    build_reporting_summary,
    collect_seen_candidate_keys,
    collect_seen_provider_queries,
    hydrate_candidate_reporting,
)
from hr_hunter.scoring import score_candidate, sort_candidates
from hr_hunter.verifier import PublicEvidenceVerifier, refresh_report_summary


@dataclass
class SearchMatrixStrategy:
    id: str
    label: str
    brief_path: Path
    providers: List[str]
    limit: int
    verify_top: int = 0
    filters: Dict[str, object] = field(default_factory=dict)


@dataclass
class SearchMatrixSpec:
    id: str
    role_title: str
    primary_brief_path: Path
    default_providers: List[str]
    default_limit: int
    strategies: List[SearchMatrixStrategy]
    exclude_report_paths: List[Path]
    exclude_history_dirs: List[Path]


def _resolve_matrix_path(base_dir: Path, raw_path: str) -> Path:
    candidate = Path(raw_path).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    return (base_dir / candidate).resolve()


def _parse_provider_list(raw_value: object, default: Sequence[str]) -> List[str]:
    if raw_value is None:
        return list(default)
    if isinstance(raw_value, str):
        providers = [value.strip() for value in raw_value.split(",") if value.strip()]
        return providers or list(default)
    if isinstance(raw_value, list):
        providers = [str(value).strip() for value in raw_value if str(value).strip()]
        return providers or list(default)
    return list(default)


def load_search_matrix(path: Path) -> SearchMatrixSpec:
    config = load_yaml_file(path)
    base_dir = path.expanduser().resolve().parent
    strategies_config = config.get("strategies", [])
    if not isinstance(strategies_config, list) or not strategies_config:
        raise ValueError("Matrix config must define a non-empty 'strategies' list.")

    default_providers = _parse_provider_list(config.get("providers"), ["scrapingbee_google"])
    default_limit = int(config.get("limit", 100))
    primary_brief_raw = str(config.get("primary_brief") or strategies_config[0].get("brief", "")).strip()
    if not primary_brief_raw:
        raise ValueError("Matrix config must define 'primary_brief' or a first strategy with 'brief'.")

    strategies: List[SearchMatrixStrategy] = []
    for index, strategy_config in enumerate(strategies_config, start=1):
        if not isinstance(strategy_config, dict):
            raise ValueError("Each matrix strategy must be a YAML object.")
        raw_brief_path = str(strategy_config.get("brief", "")).strip()
        if not raw_brief_path:
            raise ValueError("Each matrix strategy must define 'brief'.")
        strategy_id = str(strategy_config.get("id", f"strategy-{index}")).strip()
        strategy_label = str(strategy_config.get("label", strategy_id)).strip()
        strategies.append(
            SearchMatrixStrategy(
                id=strategy_id,
                label=strategy_label,
                brief_path=_resolve_matrix_path(base_dir, raw_brief_path),
                providers=_parse_provider_list(strategy_config.get("providers"), default_providers),
                limit=int(strategy_config.get("limit", default_limit)),
                verify_top=int(strategy_config.get("verify_top", 0) or 0),
                filters=dict(strategy_config.get("filters", {})),
            )
        )

    exclude_report_paths = [
        _resolve_matrix_path(base_dir, str(raw_path))
        for raw_path in config.get("exclude_reports", [])
        if str(raw_path).strip()
    ]
    exclude_history_dirs = [
        _resolve_matrix_path(base_dir, str(raw_path))
        for raw_path in config.get("exclude_history_dirs", [])
        if str(raw_path).strip()
    ]

    return SearchMatrixSpec(
        id=str(config.get("id", path.stem)).strip() or path.stem,
        role_title=str(config.get("role_title", "")).strip(),
        primary_brief_path=_resolve_matrix_path(base_dir, primary_brief_raw),
        default_providers=default_providers,
        default_limit=default_limit,
        strategies=strategies,
        exclude_report_paths=exclude_report_paths,
        exclude_history_dirs=exclude_history_dirs,
    )


def _stamp_strategy_provenance(
    report: SearchRunReport,
    strategy: SearchMatrixStrategy,
    brief: SearchBrief,
) -> None:
    for candidate in report.candidates:
        raw = dict(candidate.raw or {})
        strategy_ids = unique_preserving_order([*raw.get("strategy_ids", []), strategy.id])
        strategy_labels = unique_preserving_order([*raw.get("strategy_labels", []), strategy.label])
        raw.update(
            {
                "strategy_id": strategy.id,
                "strategy_label": strategy.label,
                "strategy_ids": strategy_ids,
                "strategy_labels": strategy_labels,
                "source_brief_id": brief.id,
                "source_run_id": report.run_id,
            }
        )
        candidate.raw = raw
        candidate.search_strategies = unique_preserving_order([*candidate.search_strategies, strategy.id])
        candidate.verification_notes = unique_preserving_order(
            [*candidate.verification_notes, f"strategy:{strategy.id}"]
        )


def _extend_seen_candidate_keys(seen_keys: Set[str], report: SearchRunReport) -> None:
    for candidate in report.candidates:
        seen_keys.update(candidate_identity_keys(candidate))


def _candidate_passes_strategy_filters(candidate: CandidateProfile, filters: Dict[str, object]) -> bool:
    candidate = hydrate_candidate_reporting(candidate)
    accepted_statuses = [str(value).strip() for value in filters.get("accepted_statuses", []) if str(value).strip()]
    if accepted_statuses and candidate.verification_status not in accepted_statuses:
        return False
    if bool(filters.get("require_current_target_company")) and not candidate.current_target_company_match:
        return False
    if bool(filters.get("require_any_target_company_signal")) and not (
        candidate.current_target_company_match or candidate.target_company_history_match
    ):
        return False
    if bool(filters.get("require_target_company_history")) and not candidate.target_company_history_match:
        return False
    if bool(filters.get("require_current_title_match")) and not candidate.current_title_match:
        return False
    if bool(filters.get("require_current_location_confirmed")) and not candidate.current_location_confirmed:
        return False
    if bool(filters.get("require_current_employment_confirmed")) and not candidate.current_employment_confirmed:
        return False
    if bool(filters.get("require_location_aligned")) and not candidate.location_aligned:
        return False
    min_score = float(filters.get("min_score", 0.0) or 0.0)
    if candidate.score < min_score:
        return False
    min_function_fit = float(filters.get("min_current_function_fit", 0.0) or 0.0)
    if candidate.current_function_fit < min_function_fit:
        return False
    min_fmcg_fit = float(filters.get("min_current_fmcg_fit", 0.0) or 0.0)
    if candidate.current_fmcg_fit < min_fmcg_fit:
        return False
    return True


def _extend_seen_provider_queries(
    seen_queries: Dict[str, Set[str]],
    provider_results: Iterable[ProviderRunResult],
) -> None:
    for result in provider_results:
        provider_seen = seen_queries.setdefault(result.provider_name, set())
        diagnostics_queries = result.diagnostics.get("queries", [])
        if not isinstance(diagnostics_queries, list):
            continue
        for item in diagnostics_queries:
            if not isinstance(item, dict):
                continue
            search_query = str(item.get("search", "")).strip()
            if search_query:
                provider_seen.add(search_query)


async def run_search_matrix(
    spec: SearchMatrixSpec,
    *,
    dry_run: bool,
    limit: int,
    verify_top: int,
    extra_exclude_reports: Iterable[Path] = (),
    extra_exclude_history_dirs: Iterable[Path] = (),
) -> SearchRunReport:
    primary_brief = build_search_brief(load_yaml_file(spec.primary_brief_path))
    exclusion_sources = [
        *spec.exclude_report_paths,
        *spec.exclude_history_dirs,
        *[Path(path).expanduser().resolve() for path in extra_exclude_reports],
        *[Path(path).expanduser().resolve() for path in extra_exclude_history_dirs],
    ]
    seen_candidate_keys = collect_seen_candidate_keys(exclusion_sources)
    seen_provider_queries = collect_seen_provider_queries(exclusion_sources)
    initial_seen_count = len(seen_candidate_keys)

    engine = SearchEngine()
    strategy_reports: List[SearchRunReport] = []
    provider_results: List[ProviderRunResult] = []
    merged_candidates = []

    for strategy in spec.strategies:
        brief = build_search_brief(load_yaml_file(strategy.brief_path))
        strategy_report = await engine.run(
            brief,
            provider_names=strategy.providers,
            limit=strategy.limit,
            dry_run=dry_run,
            exclude_candidate_keys=seen_candidate_keys,
            exclude_provider_queries=seen_provider_queries,
        )
        if strategy.verify_top and not dry_run:
            strategy_verifier = PublicEvidenceVerifier(brief.provider_settings.get("scrapingbee_google", {}))
            strategy_verification_stats = await strategy_verifier.verify_candidates(
                strategy_report.candidates,
                brief,
                limit=strategy.verify_top,
            )
            refresh_report_summary(strategy_report, strategy_verification_stats)
        _stamp_strategy_provenance(strategy_report, strategy, brief)
        strategy_report.candidates = [
            candidate
            for candidate in strategy_report.candidates
            if _candidate_passes_strategy_filters(candidate, strategy.filters)
        ]
        strategy_report.summary = build_reporting_summary(
            strategy_report.candidates,
            {
                **strategy_report.summary,
                "strategy_id": strategy.id,
                "strategy_label": strategy.label,
                "strategy_filter_count": len(strategy_report.candidates),
            },
        )
        strategy_reports.append(strategy_report)
        provider_results.extend(strategy_report.provider_results)
        merged_candidates.extend(strategy_report.candidates)
        _extend_seen_candidate_keys(seen_candidate_keys, strategy_report)
        _extend_seen_provider_queries(seen_provider_queries, strategy_report.provider_results)

    rescored_candidates = []
    for candidate in dedupe_candidates(merged_candidates):
        rescored_candidates.append(score_candidate(candidate, primary_brief))
    final_candidates = sort_candidates(rescored_candidates)[:limit]

    base_summary = {
        "role_title": spec.role_title or primary_brief.role_title,
        "dry_run": dry_run,
        "provider_order": unique_preserving_order(
            [provider_name for strategy in spec.strategies for provider_name in strategy.providers]
        ),
        "strategy_count": len(spec.strategies),
        "primary_brief_id": primary_brief.id,
        "excluded_seen_count": initial_seen_count,
        "strategy_runs": [
            {
                "strategy_id": strategy.id,
                "strategy_label": strategy.label,
                "brief_id": report.brief_id,
                "run_id": report.run_id,
                "candidate_count": len(report.candidates),
                "verified_count": report.summary.get("verified_count", 0),
                "review_count": report.summary.get("review_count", 0),
                "reject_count": report.summary.get("reject_count", 0),
            }
            for strategy, report in zip(spec.strategies, strategy_reports)
        ],
    }

    matrix_report = SearchRunReport(
        run_id=f"{spec.id}-{uuid.uuid4().hex[:8]}",
        brief_id=spec.id,
        dry_run=dry_run,
        generated_at=datetime.now(timezone.utc).isoformat(),
        provider_results=provider_results,
        candidates=final_candidates,
        summary=build_reporting_summary(final_candidates, base_summary),
    )

    if verify_top and not dry_run:
        verifier = PublicEvidenceVerifier(primary_brief.provider_settings.get("scrapingbee_google", {}))
        verification_stats = await verifier.verify_candidates(matrix_report.candidates, primary_brief, limit=verify_top)
        refresh_report_summary(matrix_report, verification_stats)
        matrix_report.summary["strategy_runs"] = base_summary["strategy_runs"]
        matrix_report.summary["strategy_count"] = base_summary["strategy_count"]
        matrix_report.summary["primary_brief_id"] = base_summary["primary_brief_id"]
        matrix_report.summary["excluded_seen_count"] = base_summary["excluded_seen_count"]

    return matrix_report
