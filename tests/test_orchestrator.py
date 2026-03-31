import asyncio
from pathlib import Path

from hr_hunter.orchestrator import load_search_matrix, run_search_matrix


def _write_yaml(path: Path, content: str) -> Path:
    path.write_text(content.strip() + "\n", encoding="utf-8")
    return path


def test_matrix_search_allows_later_strategy_when_earlier_strategy_filters_out_candidate(tmp_path: Path) -> None:
    brief_path = _write_yaml(
        tmp_path / "brief.yaml",
        """
id: matrix-brief
role_title: Brand Manager
titles:
  - Brand Manager
company_targets:
  - Unilever
geography:
  location_name: Drogheda
  country: Ireland
  center_latitude: 53.7179
  center_longitude: -6.3561
  radius_miles: 60
industry_keywords:
  - FMCG
provider_settings:
  mock: {}
""",
    )
    matrix_path = _write_yaml(
        tmp_path / "matrix.yaml",
        f"""
id: matrix-test
role_title: Brand Manager
primary_brief: {brief_path.name}
providers:
  - mock
limit: 10
strategies:
  - id: filtered-out
    label: Filtered out first
    brief: {brief_path.name}
    providers:
      - mock
    limit: 5
    filters:
      require_current_location_confirmed: true
  - id: fallback
    label: Fallback strategy
    brief: {brief_path.name}
    providers:
      - mock
    limit: 5
    filters:
      min_score: 0
""",
    )

    spec = load_search_matrix(matrix_path)
    report = asyncio.run(run_search_matrix(spec, dry_run=False, limit=10, verify_top=0))

    assert len(report.candidates) == 1
    candidate = report.candidates[0]
    assert candidate.full_name == "Jane Operator"
    assert candidate.search_strategies == ["fallback"]
    assert report.summary["strategy_runs"][0]["candidate_count"] == 0
    assert report.summary["strategy_runs"][1]["candidate_count"] == 1
