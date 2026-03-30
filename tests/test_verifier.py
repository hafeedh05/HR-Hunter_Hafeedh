from pathlib import Path

from hr_hunter.briefing import build_search_brief
from hr_hunter.config import resolve_secret
from hr_hunter.models import CandidateProfile, EvidenceRecord, SearchRunReport
from hr_hunter.output import load_report, write_report
from hr_hunter.verifier import PublicEvidenceVerifier, refresh_report_summary


def test_resolve_secret_reads_custom_env_file(tmp_path: Path, monkeypatch) -> None:
    secret_file = tmp_path / "runtime.env"
    secret_file.write_text(
        "SCRAPINGBEE_API_KEY=sb_test_123\nSMTP_PASSWORD=<weird-value>\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("SCRAPINGBEE_API_KEY", raising=False)
    monkeypatch.setenv("HR_HUNTER_SECRET_ENV_FILES", str(secret_file))

    assert resolve_secret("SCRAPINGBEE_API_KEY") == "sb_test_123"


def test_apply_evidence_promotes_review_candidate_to_verified() -> None:
    verifier = PublicEvidenceVerifier()
    candidate = CandidateProfile(
        full_name="Jane Search",
        current_title="Senior Product Manager",
        current_company="Procter & Gamble",
        verification_status="review",
        score=60.0,
    )
    evidence = [
        EvidenceRecord(
            source_url="https://example.com/profile",
            source_domain="example.com",
            name_match=True,
            company_match="Procter & Gamble",
            title_matches=["Senior Product Manager"],
            confidence=0.82,
        ),
        EvidenceRecord(
            source_url="https://another.example.com/bio",
            source_domain="another.example.com",
            name_match=True,
            company_match="Procter & Gamble",
            title_matches=["Senior Product Manager"],
            location_match=True,
            confidence=0.76,
        ),
    ]

    updated = verifier.apply_evidence(candidate, evidence)

    assert updated.evidence_verdict == "corroborated"
    assert updated.verification_status == "verified"
    assert updated.score >= 68.0


def test_report_roundtrip_preserves_evidence_fields(tmp_path: Path) -> None:
    candidate = CandidateProfile(
        full_name="Jane Search",
        current_title="Senior Product Manager",
        current_company="Procter & Gamble",
        verification_status="verified",
        score=72.0,
        evidence_confidence=0.84,
        evidence_verdict="corroborated",
        evidence_records=[
            EvidenceRecord(
                source_url="https://example.com/profile",
                source_domain="example.com",
                title="Jane Search - Senior Product Manager",
                confidence=0.84,
            )
        ],
    )
    report = SearchRunReport(
        run_id="roundtrip",
        brief_id="brief",
        dry_run=False,
        generated_at="2026-03-30T00:00:00+00:00",
        provider_results=[],
        candidates=[candidate],
        summary={"candidate_count": 1},
    )
    refresh_report_summary(report, {"requests_used": 4})

    json_path, _ = write_report(report, tmp_path)
    loaded = load_report(json_path)

    assert loaded.candidates[0].evidence_verdict == "corroborated"
    assert loaded.candidates[0].evidence_confidence == 0.84
    assert loaded.candidates[0].evidence_records[0].source_domain == "example.com"
