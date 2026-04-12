from pathlib import Path

from hr_hunter.briefing import build_search_brief
from hr_hunter.config import resolve_secret
from hr_hunter.models import CandidateProfile, EvidenceRecord, SearchRunReport
from hr_hunter.output import load_report, write_report
from hr_hunter.scoring import score_candidate
from hr_hunter.verifier import PublicEvidenceVerifier, refresh_report_summary


def test_resolve_secret_reads_custom_env_file(tmp_path: Path, monkeypatch) -> None:
    secret_file = tmp_path / "runtime.env"
    secret_file.write_text(
        "HR_HUNTER_TEST_SECRET=sb_test_123\nSMTP_PASSWORD=<weird-value>\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("HR_HUNTER_TEST_SECRET", raising=False)
    monkeypatch.setenv("HR_HUNTER_SECRET_ENV_FILES", str(secret_file))

    assert resolve_secret("HR_HUNTER_TEST_SECRET") == "sb_test_123"


def test_apply_evidence_promotes_review_candidate_to_verified() -> None:
    verifier = PublicEvidenceVerifier()
    brief = build_search_brief(
        {
            "id": "verify-test",
            "role_title": "Senior Product Manager",
            "titles": ["Senior Product Manager"],
            "company_targets": ["Procter & Gamble"],
            "geography": {
                "location_name": "Drogheda",
                "country": "Ireland",
                "center_latitude": 53.7179,
                "center_longitude": -6.3561,
                "radius_miles": 60,
            },
            "industry_keywords": ["consumer goods"],
        }
    )
    candidate = score_candidate(
        CandidateProfile(
            full_name="Jane Search",
            current_title="Senior Product Manager",
            current_company="Procter & Gamble",
            location_name="Drogheda, Ireland",
            location_geo="53.7179,-6.3561",
            verification_status="review",
            score=60.0,
        ),
        brief,
    )
    evidence = [
        EvidenceRecord(
            source_url="https://example.com/profile",
            source_domain="example.com",
            name_match=True,
            company_match="Procter & Gamble",
            title_matches=["Senior Product Manager"],
            location_match=True,
            location_match_text="Drogheda, Ireland",
            precise_location_match=True,
            profile_signal=True,
            current_employment_signal=True,
            confidence=0.82,
        ),
        EvidenceRecord(
            source_url="https://another.example.com/bio",
            source_domain="another.example.com",
            name_match=True,
            company_match="Procter & Gamble",
            title_matches=["Senior Product Manager"],
            location_match=True,
            location_match_text="Drogheda, Ireland",
            precise_location_match=True,
            profile_signal=True,
            current_employment_signal=True,
            confidence=0.76,
        ),
    ]

    updated = verifier.apply_evidence(candidate, brief, evidence)

    assert updated.evidence_verdict == "corroborated"
    assert updated.verification_status == "verified"
    assert updated.score >= 68.0
    assert updated.current_employment_confirmed is True
    assert updated.precise_location_confirmed is True


def test_apply_evidence_caps_verified_without_current_role_proof() -> None:
    verifier = PublicEvidenceVerifier()
    brief = build_search_brief(
        {
            "id": "verify-cap-test",
            "role_title": "Senior Product Manager",
            "titles": ["Senior Product Manager"],
            "company_targets": ["Procter & Gamble"],
            "geography": {
                "location_name": "Drogheda",
                "country": "Ireland",
                "center_latitude": 53.7179,
                "center_longitude": -6.3561,
                "radius_miles": 60,
            },
        }
    )
    candidate = score_candidate(
        CandidateProfile(
            full_name="Jane Search",
            current_title="Senior Product Manager",
            current_company="Procter & Gamble",
            location_name="Drogheda, Ireland",
            location_geo="53.7179,-6.3561",
            linkedin_url="https://www.linkedin.com/in/jane-search",
            summary="Senior product manager in consumer goods.",
        ),
        brief,
    )
    evidence = [
        EvidenceRecord(
            source_url="https://example.com/article",
            source_domain="example.com",
            name_match=True,
            company_match="Procter & Gamble",
            title_matches=["Senior Product Manager"],
            location_match=True,
            profile_signal=False,
            current_employment_signal=False,
            confidence=0.74,
        )
    ]

    updated = verifier.apply_evidence(candidate, brief, evidence)

    assert updated.current_employment_confirmed is False
    assert updated.verification_status != "verified"


def test_apply_evidence_accepts_strong_profile_page_as_current_role_proof() -> None:
    verifier = PublicEvidenceVerifier()
    brief = build_search_brief(
        {
            "id": "verify-strong-profile-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "company_targets": ["Unilever"],
            "geography": {
                "location_name": "Drogheda",
                "country": "Ireland",
                "center_latitude": 53.7179,
                "center_longitude": -6.3561,
                "radius_miles": 60,
            },
            "industry_keywords": ["FMCG"],
        }
    )
    candidate = score_candidate(
        CandidateProfile(
            full_name="Jane Search",
            current_title="Senior Brand Manager",
            current_company="Unilever",
            location_name="Dublin, Ireland",
            summary="Senior FMCG brand leader.",
        ),
        brief,
    )
    evidence = [
        EvidenceRecord(
            source_url="https://example.com/people/jane-search",
            source_domain="example.com",
            name_match=True,
            company_match="Unilever",
            title_matches=["Brand Manager"],
            location_match=True,
            profile_signal=True,
            current_employment_signal=False,
            confidence=0.81,
        )
    ]

    updated = verifier.apply_evidence(candidate, brief, evidence)

    assert updated.current_employment_confirmed is True
    assert updated.verification_status == "verified"


def test_apply_evidence_hard_verifies_exact_exec_match_below_default_threshold() -> None:
    verifier = PublicEvidenceVerifier()
    brief = build_search_brief(
        {
            "id": "verify-hard-gate-test",
            "role_title": "Chief Executive Officer",
            "titles": ["Chief Executive Officer", "Managing Director", "President"],
            "company_targets": ["Crate & Barrel"],
            "allow_adjacent_titles": False,
            "geography": {
                "location_name": "Dubai",
                "country": "United Arab Emirates",
                "location_hints": ["Dubai", "United Arab Emirates"],
            },
        }
    )
    candidate = score_candidate(
        CandidateProfile(
            full_name="Jorge Example",
            current_title="Chief Executive Officer",
            current_company="Crate & Barrel",
            location_name="Dubai, United Arab Emirates",
            verification_status="review",
            score=66.0,
        ),
        brief,
    )
    candidate.score = 66.0
    evidence = [
        EvidenceRecord(
            source_url="https://example.com/leadership/jorge-example",
            source_domain="example.com",
            name_match=True,
            company_match="Crate & Barrel",
            title_matches=["Chief Executive Officer"],
            location_match=True,
            location_match_text="Dubai, United Arab Emirates",
            precise_location_match=True,
            profile_signal=True,
            current_employment_signal=True,
            confidence=0.82,
        )
    ]

    updated = verifier.apply_evidence(candidate, brief, evidence)

    assert updated.current_target_company_match is True
    assert updated.current_title_match is True
    assert updated.current_employment_confirmed is True
    assert updated.precise_location_confirmed is True
    assert updated.verification_status == "verified"
    assert updated.score >= 72.0


def test_build_queries_adds_location_probe_for_imprecise_location() -> None:
    verifier = PublicEvidenceVerifier({"queries_per_candidate": 2, "location_probe_queries": 1})
    brief = build_search_brief(
        {
            "id": "verify-location-probe-query-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "company_targets": ["Unilever"],
            "geography": {
                "location_name": "Drogheda",
                "country": "Ireland",
                "location_hints": ["Dublin", "County Louth", "County Meath"],
            },
        }
    )
    candidate = CandidateProfile(
        full_name="Jane Search",
        current_title="Senior Brand Manager",
        current_company="Unilever",
        location_name="Ireland",
        location_precision_bucket="country_only",
    )

    queries = verifier.build_queries(candidate, brief)

    assert len(queries) == 3
    assert any('"based in"' in query for query in queries)
    assert any('"Dublin"' in query for query in queries)


def test_build_queries_adds_site_targeted_location_probe_when_configured() -> None:
    verifier = PublicEvidenceVerifier(
        {
            "queries_per_candidate": 2,
            "location_probe_queries": 2,
            "location_include_site_terms": ["site:shelflife.ie", "site:retailnews.ie"],
            "location_source_terms": ["appointed", "speaker"],
        }
    )
    brief = build_search_brief(
        {
            "id": "verify-site-targeted-location-probe-query-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "company_targets": ["Unilever"],
            "geography": {
                "location_name": "Drogheda",
                "country": "Ireland",
                "location_hints": ["Dublin", "County Louth", "County Meath"],
            },
        }
    )
    candidate = CandidateProfile(
        full_name="Jane Search",
        current_title="Senior Brand Manager",
        current_company="Unilever",
        location_name="Ireland",
        location_precision_bucket="country_only",
    )

    queries = verifier.build_queries(candidate, brief)

    assert any("site:shelflife.ie" in query for query in queries)
    assert any('"appointed"' in query for query in queries)


def test_build_company_location_queries_use_location_sites() -> None:
    verifier = PublicEvidenceVerifier(
        {
            "company_location_probe_queries": 1,
            "location_include_site_terms": ["site:knorr.com/ie", "site:jnj.com"],
            "company_location_source_terms": ["office", "contact"],
        }
    )
    brief = build_search_brief(
        {
            "id": "verify-company-location-query-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "company_targets": ["Unilever"],
            "company_aliases": {"Unilever": ["Knorr"]},
            "geography": {
                "location_name": "Drogheda",
                "country": "Ireland",
                "location_hints": ["Dublin", "County Meath"],
            },
        }
    )
    candidate = CandidateProfile(
        full_name="Jane Search",
        current_title="Senior Brand Manager",
        current_company="Unilever",
        location_name="Ireland",
        location_precision_bucket="country_only",
    )

    queries = verifier.build_company_location_queries(candidate, brief)

    assert len(queries) == 1
    assert "site:knorr.com/ie" in queries[0]
    assert '"office"' in queries[0]


def test_build_record_prefers_precise_location_over_country_only() -> None:
    verifier = PublicEvidenceVerifier()
    brief = build_search_brief(
        {
            "id": "verify-build-record-location-priority-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "company_targets": ["Unilever"],
            "geography": {
                "location_name": "Drogheda",
                "country": "Ireland",
                "location_hints": ["Dublin", "County Louth"],
            },
        }
    )
    candidate = CandidateProfile(
        full_name="Jane Search",
        current_title="Senior Brand Manager",
        current_company="Unilever",
        location_name="Ireland",
    )

    record = verifier.build_record(
        candidate,
        brief,
        '"Jane Search" "Unilever"',
        {
            "title": "Jane Search - Senior Brand Manager at Unilever",
            "description": "Senior FMCG leader based in Dublin, Ireland.",
            "url": "https://example.com/people/jane-search",
        },
    )

    assert record.location_match is True
    assert record.location_match_text == "Dublin"
    assert record.precise_location_match is True


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
                location_match=True,
                location_match_text="Dublin",
                precise_location_match=True,
                profile_signal=True,
                current_employment_signal=True,
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
    assert loaded.candidates[0].evidence_records[0].current_employment_signal is True
    assert loaded.candidates[0].evidence_records[0].location_match_text == "Dublin"
    assert loaded.candidates[0].evidence_records[0].precise_location_match is True


def test_apply_evidence_caps_historical_only_public_match() -> None:
    verifier = PublicEvidenceVerifier()
    brief = build_search_brief(
        {
            "id": "verify-historical-only-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "company_targets": ["Unilever"],
            "geography": {
                "location_name": "Drogheda",
                "country": "Ireland",
                "center_latitude": 53.7179,
                "center_longitude": -6.3561,
                "radius_miles": 60,
            },
            "industry_keywords": ["FMCG"],
        }
    )
    candidate = score_candidate(
        CandidateProfile(
            full_name="Historical FMCG",
            current_title="Senior Brand Manager",
            current_company="Unilever",
            location_name="Drogheda, Ireland",
            location_geo="53.7179,-6.3561",
            summary="Senior FMCG brand leader.",
        ),
        brief,
    )
    evidence = [
        EvidenceRecord(
            source_url="https://example.com/article",
            source_domain="example.com",
            name_match=True,
            company_match="Unilever",
            title_matches=["Brand Manager"],
            location_match=True,
            profile_signal=False,
            current_employment_signal=False,
            confidence=0.82,
        )
    ]

    updated = verifier.apply_evidence(candidate, brief, evidence)

    assert updated.current_employment_confirmed is False
    assert updated.verification_status != "verified"
    assert "historical_only_public_evidence" in getattr(updated, "cap_reasons")


def test_apply_evidence_rejects_before_joining_profile_as_current_role_proof() -> None:
    verifier = PublicEvidenceVerifier()
    brief = build_search_brief(
        {
            "id": "verify-before-joining-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "company_targets": ["Procter & Gamble"],
            "geography": {
                "location_name": "Drogheda",
                "country": "Ireland",
                "center_latitude": 53.7179,
                "center_longitude": -6.3561,
                "radius_miles": 60,
            },
            "industry_keywords": ["FMCG"],
        }
    )
    candidate = score_candidate(
        CandidateProfile(
            full_name="Jonathan Gordon",
            current_title="Brand Manager",
            current_company="Procter & Gamble",
            location_name="Ireland",
            summary="Brand leader in Ireland.",
        ),
        brief,
    )
    evidence = [
        EvidenceRecord(
            source_url="https://www.mckinsey.com/our-people/jonathan-gordon",
            source_domain="mckinsey.com",
            title="Jonathan Gordon",
            snippet="Before joining McKinsey, Jonathan worked as a brand manager at Procter & Gamble. Originally from Ireland.",
            name_match=True,
            company_match="Procter & Gamble",
            title_matches=["Brand Manager"],
            location_match=True,
            profile_signal=True,
            current_employment_signal=True,
            confidence=0.91,
        )
    ]

    updated = verifier.apply_evidence(candidate, brief, evidence)

    assert updated.current_employment_confirmed is False
    assert updated.current_company_confirmed is False
    assert updated.current_title_confirmed is False
    assert updated.verification_status != "verified"
    assert "historical_only_public_evidence" in getattr(updated, "cap_reasons")


def test_apply_evidence_caps_stale_current_role_proof() -> None:
    verifier = PublicEvidenceVerifier()
    brief = build_search_brief(
        {
            "id": "verify-stale-proof-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "company_targets": ["Unilever"],
            "geography": {
                "location_name": "Drogheda",
                "country": "Ireland",
                "center_latitude": 53.7179,
                "center_longitude": -6.3561,
                "radius_miles": 60,
            },
            "industry_keywords": ["FMCG"],
        }
    )
    candidate = score_candidate(
        CandidateProfile(
            full_name="Stale FMCG",
            current_title="Senior Brand Manager",
            current_company="Unilever",
            location_name="Drogheda, Ireland",
            location_geo="53.7179,-6.3561",
            summary="Senior FMCG brand leader.",
        ),
        brief,
    )
    evidence = [
        EvidenceRecord(
            source_url="https://example.com/people/stale-fmcg",
            source_domain="example.com",
            name_match=True,
            company_match="Unilever",
            title_matches=["Brand Manager"],
            location_match=True,
            profile_signal=True,
            current_employment_signal=True,
            recency_year=2022,
            confidence=0.84,
        )
    ]

    updated = verifier.apply_evidence(candidate, brief, evidence)

    assert updated.stale_data_risk is True
    assert updated.current_employment_confirmed is False
    assert updated.verification_status != "verified"
    assert getattr(updated, "evidence_freshness_year") == 2022


def test_apply_evidence_caps_country_only_ireland_from_verified() -> None:
    verifier = PublicEvidenceVerifier()
    brief = build_search_brief(
        {
            "id": "verify-country-only-ireland-cap-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "company_targets": ["Procter & Gamble"],
            "geography": {
                "location_name": "Drogheda",
                "country": "Ireland",
                "location_hints": ["Ireland", "Dublin"],
                "center_latitude": 53.7179,
                "center_longitude": -6.3561,
                "radius_miles": 60,
            },
            "industry_keywords": ["FMCG"],
        }
    )
    candidate = score_candidate(
        CandidateProfile(
            full_name="Country Only FMCG",
            current_title="Brand Director",
            current_company="Procter & Gamble",
            location_name="Ireland",
            summary="Brand Director at Procter & Gamble Ireland with FMCG leadership experience.",
        ),
        brief,
    )
    evidence = [
        EvidenceRecord(
            source_url="https://example.com/people/country-only-fmcg",
            source_domain="example.com",
            name_match=True,
            company_match="Procter & Gamble",
            title_matches=["Brand Manager"],
            location_match=True,
            profile_signal=True,
            current_employment_signal=True,
            confidence=0.88,
        )
    ]

    updated = verifier.apply_evidence(candidate, brief, evidence)

    assert updated.location_precision_bucket == "country_only"
    assert updated.verification_status != "verified"
    assert "precise location" in getattr(updated, "cap_reasons")


def test_apply_evidence_promotes_precise_location_probe_result() -> None:
    verifier = PublicEvidenceVerifier()
    brief = build_search_brief(
        {
            "id": "verify-precise-location-promotion-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "company_targets": ["Procter & Gamble"],
            "geography": {
                "location_name": "Drogheda",
                "country": "Ireland",
                "location_hints": ["Ireland", "Dublin", "County Louth"],
                "center_latitude": 53.7179,
                "center_longitude": -6.3561,
                "radius_miles": 120,
            },
            "industry_keywords": ["FMCG"],
        }
    )
    candidate = score_candidate(
        CandidateProfile(
            full_name="Precise Location FMCG",
            current_title="Brand Manager",
            current_company="Procter & Gamble",
            location_name="Ireland",
            summary="Brand manager at Procter & Gamble Ireland.",
        ),
        brief,
    )
    evidence = [
        EvidenceRecord(
            source_url="https://example.com/people/precise-location-fmcg",
            source_domain="example.com",
            name_match=True,
            company_match="Procter & Gamble",
            title_matches=["Brand Manager"],
            location_match=True,
            location_match_text="Dublin",
            precise_location_match=True,
            profile_signal=True,
            current_employment_signal=True,
            confidence=0.92,
        )
    ]

    updated = verifier.apply_evidence(candidate, brief, evidence)

    assert updated.precise_location_confirmed is True
    assert updated.location_name == "Dublin, Ireland"
    assert updated.location_precision_bucket == "named_target_location"
    assert updated.verification_status == "verified"


def test_apply_evidence_refines_country_only_ireland_with_company_office_location() -> None:
    verifier = PublicEvidenceVerifier()
    brief = build_search_brief(
        {
            "id": "verify-company-office-location-refinement-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "company_targets": ["Unilever"],
            "company_aliases": {"Unilever": ["Knorr"]},
            "geography": {
                "location_name": "Drogheda",
                "country": "Ireland",
                "location_hints": ["Ireland", "Dublin", "Citywest"],
                "center_latitude": 53.7179,
                "center_longitude": -6.3561,
                "radius_miles": 120,
            },
            "industry_keywords": ["FMCG"],
        }
    )
    candidate = score_candidate(
        CandidateProfile(
            full_name="Ireland Only FMCG",
            current_title="Senior Brand Manager",
            current_company="Unilever",
            location_name="Ireland",
            summary="Senior brand manager at Unilever Ireland.",
        ),
        brief,
    )
    evidence = [
        EvidenceRecord(
            source_url="https://example.com/people/ireland-only-fmcg",
            source_domain="example.com",
            name_match=True,
            company_match="Unilever",
            title_matches=["Brand Manager"],
            location_match=True,
            location_match_text="Ireland",
            precise_location_match=False,
            profile_signal=True,
            current_employment_signal=True,
            confidence=0.88,
        ),
        EvidenceRecord(
            source_url="https://www.knorr.com/ie/contact-us.html",
            source_domain="knorr.com",
            source_type="company_location",
            company_match="Unilever",
            location_match=True,
            location_match_text="Citywest",
            precise_location_match=True,
            confidence=0.72,
        ),
    ]

    updated = verifier.apply_evidence(candidate, brief, evidence)

    assert updated.precise_location_confirmed is True
    assert updated.location_name == "Citywest, Ireland"
    assert updated.location_precision_bucket == "named_target_location"
    assert "precise location inferred from company office/contact evidence" in updated.verification_notes
