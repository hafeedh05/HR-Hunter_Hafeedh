import json
from pathlib import Path

from hr_hunter.briefing import build_search_brief
from hr_hunter.feedback import export_training_rows, load_ranker_training_rows, log_feedback
from hr_hunter.models import CandidateProfile, SearchRunReport
from hr_hunter.output import write_report
from hr_hunter.scoring import score_candidate


def _build_feedback_brief():
    return build_search_brief(
        {
            "id": "feedback-brief",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "company_targets": ["Unilever"],
            "geography": {"location_name": "Dublin", "country": "Ireland"},
            "required_keywords": ["brand", "commercial"],
            "anchors": {
                "title": "critical",
                "company": "critical",
                "location": "important",
            },
        }
    )


def test_log_feedback_persists_training_rows_and_pairs(tmp_path: Path) -> None:
    brief = _build_feedback_brief()
    candidates = [
        score_candidate(
            CandidateProfile(
                full_name="Alice Match",
                current_title="Senior Brand Manager",
                current_company="Unilever",
                location_name="Dublin, Ireland",
                summary="Brand and commercial leader in consumer goods.",
                linkedin_url="https://www.linkedin.com/in/alice-match",
            ),
            brief,
        ),
        score_candidate(
            CandidateProfile(
                full_name="Bob Reject",
                current_title="Engineering Manager",
                current_company="Platform Labs",
                location_name="London, United Kingdom",
                summary="Software platform leader.",
                linkedin_url="https://www.linkedin.com/in/bob-reject",
            ),
            brief,
        ),
    ]
    report = SearchRunReport(
        run_id="feedback-report",
        brief_id=brief.id,
        dry_run=False,
        generated_at="2026-04-03T00:00:00+00:00",
        provider_results=[],
        candidates=candidates,
        summary={"role_title": brief.role_title, "anchor_weights": brief.anchor_weights},
    )
    report_path, _ = write_report(report, tmp_path)
    db_path = tmp_path / "feedback.db"

    first = log_feedback(
        report_path=report_path,
        candidate_ref="Alice Match",
        recruiter_id="rec-1",
        action="shortlist",
        db_path=db_path,
        brief=brief,
    )
    second = log_feedback(
        report_path=report_path,
        candidate_ref="Bob Reject",
        recruiter_id="rec-1",
        action="reject",
        db_path=db_path,
        brief=brief,
    )

    assert first["training_pair_count"] == 0
    assert second["training_pair_count"] == 1

    rows = load_ranker_training_rows(db_path)
    assert len(rows) == 2
    by_name = {row["candidate_name"]: row for row in rows}
    assert by_name["Alice Match"]["label"] == 3
    assert by_name["Bob Reject"]["label"] == 0
    assert by_name["Alice Match"]["feature_json"]["title_similarity"] > 0.0
    assert by_name["Alice Match"]["feature_json"]["heuristic_score"] == candidates[0].score
    assert by_name["Alice Match"]["feature_json"]["anchor_title_similarity"] == brief.anchor_weights["title_similarity"]

    export_path = export_training_rows(tmp_path / "training_rows.json", db_path)
    exported_rows = json.loads(export_path.read_text(encoding="utf-8"))
    assert len(exported_rows) == 2
    assert exported_rows[0]["query_id"] == "feedback-brief:rec-1"
