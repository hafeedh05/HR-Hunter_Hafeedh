from hr_hunter.briefing import build_search_brief
from hr_hunter.models import CandidateProfile
from hr_hunter.scoring import score_candidate, status_from_score


def test_status_from_score_respects_explicit_user_bands() -> None:
    assert status_from_score(100.0) == "verified"
    assert status_from_score(70.0) == "verified"
    assert status_from_score(69.99) == "review"
    assert status_from_score(50.0) == "review"
    assert status_from_score(49.99) == "reject"


def test_score_candidate_marks_high_fit_as_verified() -> None:
    brief = build_search_brief(
        {
            "id": "score-test",
            "role_title": "Global Product Manager",
            "titles": ["Global Product Manager", "Senior Product Manager"],
            "title_keywords": ["product manager", "product"],
            "company_targets": ["Procter & Gamble"],
            "company_aliases": {"Procter & Gamble": ["P&G"]},
            "geography": {
                "location_name": "Drogheda",
                "country": "Ireland",
                "center_latitude": 53.7179,
                "center_longitude": -6.3561,
                "radius_miles": 60,
            },
            "required_keywords": ["product strategy", "commercial"],
            "preferred_keywords": ["CPG"],
            "minimum_years_experience": 10,
        }
    )

    candidate = CandidateProfile(
        full_name="Jane Search",
        current_title="Global Product Manager",
        current_company="Procter & Gamble",
        location_name="Drogheda, Ireland",
        location_geo="53.7179,-6.3561",
        linkedin_url="https://www.linkedin.com/in/jane-search",
        summary="Product strategy and commercial leadership in CPG.",
        experience=[
            {
                "company": {"name": "Procter & Gamble"},
                "start_date": "2010-01-01",
            }
        ],
    )

    scored = score_candidate(candidate, brief)

    assert scored.verification_status == "verified"
    assert scored.score >= 70
    assert "Procter & Gamble" in scored.matched_companies


def test_score_candidate_caps_former_target_company_to_review() -> None:
    brief = build_search_brief(
        {
            "id": "score-former-company-test",
            "role_title": "Global Product Manager",
            "titles": ["Global Product Manager"],
            "title_keywords": ["product manager"],
            "company_targets": ["Procter & Gamble"],
            "geography": {
                "location_name": "Drogheda",
                "country": "Ireland",
                "center_latitude": 53.7179,
                "center_longitude": -6.3561,
                "radius_miles": 60,
            },
            "industry_keywords": ["consumer goods", "CPG"],
            "minimum_years_experience": 8,
        }
    )

    candidate = CandidateProfile(
        full_name="Jane Former FMCG",
        current_title="Global Product Manager",
        current_company="Acme SaaS",
        location_name="Drogheda, Ireland",
        location_geo="53.7179,-6.3561",
        linkedin_url="https://www.linkedin.com/in/jane-former-fmcg",
        summary="Consumer goods portfolio leader with former P&G experience.",
        experience=[
            {
                "company": {"name": "Procter & Gamble"},
                "start_date": "2012-01-01",
                "end_date": "2020-01-01",
            }
        ],
    )

    scored = score_candidate(candidate, brief)

    assert scored.current_target_company_match is False
    assert scored.target_company_history_match is True
    assert scored.verification_status != "verified"


def test_score_candidate_matches_target_company_subdivision_name() -> None:
    brief = build_search_brief(
        {
            "id": "score-company-alias-test",
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
            "industry_keywords": ["consumer goods", "FMCG"],
        }
    )

    candidate = CandidateProfile(
        full_name="Jane Division FMCG",
        current_title="Senior Brand Manager",
        current_company="Unilever Ireland Limited",
        location_name="Drogheda, Ireland",
        location_geo="53.7179,-6.3561",
        summary="FMCG brand leadership across consumer goods categories.",
    )

    scored = score_candidate(candidate, brief)

    assert scored.current_target_company_match is True
    assert "Unilever" in scored.matched_companies


def test_score_candidate_demotes_generic_tech_product_manager() -> None:
    brief = build_search_brief(
        {
            "id": "score-tech-demotion-test",
            "role_title": "Brand / Product Lead",
            "titles": ["Brand Manager", "Category Manager", "Product Marketing Director"],
            "title_keywords": ["product marketing", "brand manager", "category manager"],
            "company_targets": ["Procter & Gamble"],
            "geography": {
                "location_name": "Drogheda",
                "country": "Ireland",
                "center_latitude": 53.7179,
                "center_longitude": -6.3561,
                "radius_miles": 60,
            },
            "industry_keywords": ["consumer goods", "FMCG"],
            "exclude_title_keywords": ["software", "saas", "platform", "technical product manager"],
        }
    )

    candidate = CandidateProfile(
        full_name="Tech PM",
        current_title="Senior Product Manager",
        current_company="Cloud Platform Co",
        location_name="Drogheda, Ireland",
        location_geo="53.7179,-6.3561",
        summary="SaaS platform roadmap owner for software products and engineering teams.",
    )

    scored = score_candidate(candidate, brief)

    assert scored.current_target_company_match is False
    assert scored.verification_status != "verified"
    assert scored.score < 50.0


def test_score_candidate_accepts_adjacent_fmcg_title_family() -> None:
    brief = build_search_brief(
        {
            "id": "score-adjacent-title-test",
            "role_title": "Brand / Category Lead",
            "titles": ["Brand Manager", "Category Manager", "Product Marketing Director"],
            "company_targets": ["Unilever"],
            "geography": {
                "location_name": "Drogheda",
                "country": "Ireland",
                "center_latitude": 53.7179,
                "center_longitude": -6.3561,
                "radius_miles": 120,
            },
            "industry_keywords": ["consumer goods", "FMCG"],
            "required_keywords": ["brand", "category", "commercial"],
            "minimum_years_experience": 8,
        }
    )

    candidate = CandidateProfile(
        full_name="Adjacent FMCG Lead",
        current_title="Shopper Marketing Manager",
        current_company="Unilever Ireland",
        location_name="Dublin, Ireland",
        location_geo="53.3498,-6.2603",
        summary="FMCG category and brand growth across Ireland retail accounts.",
        experience=[
            {
                "company": {"name": "Unilever"},
                "title": "Category Development Manager",
                "start_date": "2014-01-01",
            }
        ],
    )

    scored = score_candidate(candidate, brief)

    assert scored.current_target_company_match is True
    assert scored.current_title_match is True
    assert scored.score >= 70.0


def test_score_candidate_demotes_low_seniority_specialist_role() -> None:
    brief = build_search_brief(
        {
            "id": "score-low-seniority-test",
            "role_title": "Brand / Category Lead",
            "titles": ["Brand Manager", "Category Manager"],
            "company_targets": ["Procter & Gamble"],
            "geography": {
                "location_name": "Drogheda",
                "country": "Ireland",
                "center_latitude": 53.7179,
                "center_longitude": -6.3561,
                "radius_miles": 120,
            },
            "industry_keywords": ["consumer goods", "FMCG"],
            "minimum_years_experience": 8,
        }
    )

    candidate = CandidateProfile(
        full_name="Junior FMCG Marketer",
        current_title="Marketing Specialist",
        current_company="Procter & Gamble",
        location_name="Dublin, Ireland",
        location_geo="53.3498,-6.2603",
        summary="Consumer goods marketing specialist supporting campaigns in Ireland.",
    )

    scored = score_candidate(candidate, brief)

    assert scored.current_target_company_match is True
    assert scored.score < 70.0
    assert scored.verification_status != "verified"


def test_score_candidate_boosts_explicit_company_interest_signal() -> None:
    brief = build_search_brief(
        {
            "id": "score-company-interest-test",
            "role_title": "Senior Data Analyst",
            "titles": ["Senior Data Analyst", "Data Analyst"],
            "geography": {
                "location_name": "Dubai",
                "country": "United Arab Emirates",
            },
            "required_keywords": ["sql", "python"],
            "hiring_company_name": "OpenAI",
            "hiring_company_aliases": ["Open AI"],
            "anchors": {
                "title": "critical",
                "skills": "critical",
                "company_interest": "important",
            },
        }
    )

    candidate = CandidateProfile(
        full_name="Interested Candidate",
        current_title="Senior Data Analyst",
        current_company="Analytics Co",
        location_name="Dubai, United Arab Emirates",
        summary="Senior data analyst interested in joining OpenAI and building analytics products.",
    )

    scored = score_candidate(candidate, brief)

    assert scored.company_interest_score >= 0.95
    assert scored.feature_scores["company_interest"] >= 0.95
    assert "ranker_bonus: explicit_company_interest" in scored.verification_notes


def test_score_candidate_caps_when_company_interest_is_required_but_missing() -> None:
    brief = build_search_brief(
        {
            "id": "score-company-interest-required-test",
            "role_title": "Senior Data Analyst",
            "titles": ["Senior Data Analyst"],
            "geography": {
                "location_name": "Dubai",
                "country": "United Arab Emirates",
            },
            "hiring_company_name": "OpenAI",
            "candidate_interest_required": True,
            "anchors": {
                "title": "critical",
                "company_interest": "critical",
            },
        }
    )

    candidate = CandidateProfile(
        full_name="No Interest Signal",
        current_title="Senior Data Analyst",
        current_company="Analytics Co",
        location_name="Dubai, United Arab Emirates",
        summary="Senior data analyst focused on dashboarding and experimentation.",
    )

    scored = score_candidate(candidate, brief)

    assert scored.company_interest_score == 0.0
    assert "company_interest_required" in scored.cap_reasons
    assert scored.score <= 69.0
    assert scored.verification_status != "verified"


def test_score_candidate_caps_off_function_current_role_even_with_relevant_history() -> None:
    brief = build_search_brief(
        {
            "id": "score-off-function-cap-test",
            "role_title": "Brand / Category Lead",
            "titles": ["Brand Manager", "Category Manager"],
            "company_targets": ["Unilever"],
            "geography": {
                "location_name": "Drogheda",
                "country": "Ireland",
                "center_latitude": 53.7179,
                "center_longitude": -6.3561,
                "radius_miles": 120,
            },
            "industry_keywords": ["consumer goods", "FMCG"],
            "required_keywords": ["brand", "category", "commercial"],
            "minimum_years_experience": 8,
        }
    )

    candidate = CandidateProfile(
        full_name="Off Function FMCG",
        current_title="Customer Development Manager",
        current_company="Unilever",
        location_name="Dublin, Ireland",
        location_geo="53.3498,-6.2603",
        summary="Consumer goods leader with historic category experience.",
        experience=[
            {
                "company": {"name": "Unilever"},
                "title": "Category Development Manager",
                "start_date": "2012-01-01",
            }
        ],
    )

    scored = score_candidate(candidate, brief)

    assert scored.verification_status != "verified"
    assert getattr(scored, "current_function_fit") == 0.0
    assert "customer development" in getattr(scored, "disqualifier_reasons")


def test_score_candidate_country_only_ireland_signal_caps_verified() -> None:
    brief = build_search_brief(
        {
            "id": "score-country-only-location-test",
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
            "industry_keywords": ["consumer goods", "FMCG"],
        }
    )

    candidate = CandidateProfile(
        full_name="Country Only FMCG",
        current_title="Senior Brand Manager",
        current_company="Unilever",
        location_name="Ireland",
        summary="FMCG brand leader based in Ireland.",
    )

    scored = score_candidate(candidate, brief)

    assert getattr(scored, "location_precision_bucket") == "country_only"
    assert scored.verification_status != "verified"


def test_score_candidate_outside_search_area_is_rejected() -> None:
    brief = build_search_brief(
        {
            "id": "score-outside-area-test",
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
            "industry_keywords": ["consumer goods", "FMCG"],
        }
    )

    candidate = CandidateProfile(
        full_name="Far Away FMCG",
        current_title="Senior Brand Manager",
        current_company="Unilever",
        location_name="London, United Kingdom",
        location_geo="51.5072,-0.1276",
        summary="FMCG brand leader currently based in London.",
    )

    scored = score_candidate(candidate, brief)

    assert getattr(scored, "location_precision_bucket") == "outside_target_area"
    assert scored.location_aligned is False
    assert scored.score < 70.0
