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


def test_score_candidate_does_not_treat_peer_source_company_as_hard_company_match() -> None:
    brief = build_search_brief(
        {
            "id": "score-peer-company-separation-test",
            "role_title": "Chief Executive Officer",
            "titles": ["Chief Executive Officer", "Managing Director"],
            "company_targets": ["Marina Home Interiors"],
            "peer_company_targets": ["The One"],
            "geography": {
                "location_name": "Dubai",
                "country": "United Arab Emirates",
                "location_hints": ["Dubai", "United Arab Emirates"],
            },
            "industry_keywords": ["premium retail", "home furnishings"],
        }
    )

    candidate = CandidateProfile(
        full_name="Peer Source Executive",
        current_title="Chief Executive Officer",
        current_company="The One",
        location_name="Dubai, United Arab Emirates",
        summary="Premium home retail executive with GCC scope.",
    )

    scored = score_candidate(candidate, brief)

    assert scored.current_target_company_match is False
    assert "Marina Home Interiors" not in scored.matched_companies


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


def test_score_candidate_requires_adjacent_title_opt_in_for_exec_briefs() -> None:
    brief = build_search_brief(
        {
            "id": "score-exec-adjacent-opt-in-test",
            "role_title": "Chief Executive Officer",
            "titles": ["Chief Executive Officer", "Managing Director", "President"],
            "company_targets": ["The One"],
            "allow_adjacent_titles": False,
            "geography": {
                "location_name": "Dubai",
                "country": "United Arab Emirates",
                "location_hints": ["Dubai", "United Arab Emirates"],
            },
        }
    )

    candidate = CandidateProfile(
        full_name="Adjacent Exec",
        current_title="Chief Operating Officer",
        current_company="The One",
        location_name="Dubai, United Arab Emirates",
        summary="Retail operator leading GCC expansion.",
    )

    scored = score_candidate(candidate, brief)

    assert scored.current_target_company_match is True
    assert scored.current_title_match is False
    assert scored.verification_status != "verified"


def test_score_candidate_avoids_generic_single_token_company_alias_false_positive() -> None:
    brief = build_search_brief(
        {
            "id": "score-generic-company-alias-test",
            "role_title": "Chief Executive Officer",
            "titles": ["Chief Executive Officer"],
            "company_targets": ["The One"],
            "geography": {
                "location_name": "Dubai",
                "country": "United Arab Emirates",
                "location_hints": ["Dubai", "United Arab Emirates"],
            },
        }
    )

    candidate = CandidateProfile(
        full_name="False Positive Company",
        current_title="Chief Executive Officer",
        current_company="One Global Holding, Casheer",
        location_name="Kuwait City, Kuwait",
        summary="Regional operator across payments and consumer platforms.",
    )

    scored = score_candidate(candidate, brief)

    assert scored.current_target_company_match is False
    assert "The One" not in scored.matched_companies


def test_score_candidate_treats_ceo_acronym_as_exact_exec_match() -> None:
    brief = build_search_brief(
        {
            "id": "score-ceo-acronym-match-test",
            "role_title": "Chief Executive Officer",
            "titles": ["Chief Executive Officer", "Managing Director", "President"],
            "company_targets": ["Al Huzaifa"],
            "allow_adjacent_titles": False,
            "geography": {
                "location_name": "Dubai",
                "country": "United Arab Emirates",
                "location_hints": ["Dubai", "United Arab Emirates"],
            },
        }
    )

    candidate = CandidateProfile(
        full_name="Shiraz Jamaji",
        current_title="CEO",
        current_company="Al Huzaifa Furniture Industry",
        location_name="Dubai, United Arab Emirates",
        summary="Retail operator leading premium furniture growth in the GCC.",
    )

    scored = score_candidate(candidate, brief)

    assert scored.current_target_company_match is True
    assert scored.current_title_match is True
    assert scored.current_function_fit >= 0.72


def test_score_candidate_demotes_company_page_style_exec_result() -> None:
    brief = build_search_brief(
        {
            "id": "score-company-page-style-exec-test",
            "role_title": "Chief Executive Officer",
            "titles": ["Chief Executive Officer", "Managing Director", "President"],
            "company_targets": ["Marina Home Interiors"],
            "allow_adjacent_titles": False,
            "geography": {
                "location_name": "Dubai",
                "country": "United Arab Emirates",
                "location_hints": ["Dubai", "United Arab Emirates"],
            },
        }
    )

    candidate = CandidateProfile(
        full_name="Marina Home Interiors",
        current_title="Marina Home Interiors Retail Chain",
        current_company="Marina Home Interiors",
        location_name="Dubai, United Arab Emirates",
        summary="Premium home furnishings retail chain.",
    )

    scored = score_candidate(candidate, brief)

    assert scored.current_title_match is False
    assert scored.parser_confidence < 0.25
    assert "parser_confidence_too_low" in scored.cap_reasons


def test_score_candidate_prefers_priority_geo_matches() -> None:
    brief = build_search_brief(
        {
            "id": "score-priority-geo-test",
            "role_title": "Chief Executive Officer",
            "titles": ["Chief Executive Officer", "Managing Director"],
            "company_targets": ["Marina Home Interiors"],
            "geography": {
                "location_name": "",
                "country": "",
                "radius_miles": 0,
                "location_hints": [
                    "Dubai",
                    "Abu Dhabi",
                    "Riyadh",
                    "Jeddah",
                    "London",
                    "Paris",
                ],
            },
            "location_targets": [
                "Dubai",
                "Abu Dhabi",
                "Riyadh",
                "Jeddah",
                "London",
                "Paris",
            ],
            "industry_keywords": ["home furnishings", "premium retail"],
            "minimum_years_experience": 12,
        }
    )

    priority_candidate = CandidateProfile(
        full_name="Priority Geo",
        current_title="Chief Executive Officer",
        current_company="Marina Home Interiors",
        location_name="Dubai, United Arab Emirates",
        summary="Premium retail and home furnishings operator.",
        experience=[{"company": {"name": "Marina Home Interiors"}, "start_date": "2010-01-01"}],
    )
    secondary_candidate = CandidateProfile(
        full_name="Secondary Geo",
        current_title="Chief Executive Officer",
        current_company="Marina Home Interiors",
        location_name="Paris, France",
        summary="Premium retail and home furnishings operator.",
        experience=[{"company": {"name": "Marina Home Interiors"}, "start_date": "2010-01-01"}],
    )

    scored_priority = score_candidate(priority_candidate, brief)
    scored_secondary = score_candidate(secondary_candidate, brief)

    assert scored_priority.location_match_score > scored_secondary.location_match_score
    assert scored_priority.location_precision_bucket == "priority_target_location"
    assert scored_secondary.location_precision_bucket in {"named_target_location", "secondary_target_location"}


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


def test_score_candidate_can_require_not_currently_employed() -> None:
    brief = build_search_brief(
        {
            "id": "score-employment-status-test",
            "role_title": "Senior Data Analyst",
            "titles": ["Senior Data Analyst", "Data Analyst"],
            "geography": {"location_name": "Dubai", "country": "United Arab Emirates"},
            "employment_status_mode": "not_currently_employed",
            "required_keywords": ["sql", "python"],
        }
    )

    employed_candidate = CandidateProfile(
        full_name="Currently Employed Analyst",
        current_title="Senior Data Analyst",
        current_company="noon",
        location_name="Dubai, United Arab Emirates",
        summary="SQL and Python analytics lead.",
    )
    not_currently_employed_candidate = CandidateProfile(
        full_name="Available Analyst",
        current_title="Senior Data Analyst",
        current_company="",
        location_name="Dubai, United Arab Emirates",
        summary="Open to work senior data analyst with strong SQL and Python background.",
    )

    employed_scored = score_candidate(employed_candidate, brief)
    available_scored = score_candidate(not_currently_employed_candidate, brief)

    assert employed_scored.feature_scores["employment_status"] == 0.0
    assert "employment_status_required" in employed_scored.cap_reasons
    assert employed_scored.verification_status == "reject"
    assert available_scored.feature_scores["employment_status"] > 0.8
    assert available_scored.score > employed_scored.score


def test_score_candidate_keeps_digital_marketing_role_in_scope_for_marketing_brief() -> None:
    brief = build_search_brief(
        {
            "id": "score-digital-marketing-fit-test",
            "role_title": "Digital Marketing Manager",
            "titles": ["Digital Marketing Manager"],
            "geography": {"location_name": "Dubai", "country": "United Arab Emirates"},
            "required_keywords": ["Google Ads", "Meta Ads", "GA4"],
            "preferred_keywords": ["Lead Generation"],
            "industry_keywords": ["consumer", "ecommerce"],
            "minimum_years_experience": 4,
            "maximum_years_experience": 10,
        }
    )

    candidate = CandidateProfile(
        full_name="Benefit Beauty Growth",
        current_title="Digital Marketing Manager",
        current_company="Benefit Cosmetics",
        location_name="Dubai, United Arab Emirates",
        summary=(
            "Digital marketing manager leading Facebook Ads, Google Analytics 4, paid social, lead generation, "
            "and cross-functional collaboration for a beauty ecommerce business."
        ),
        experience=[{"start_date": "2018-01-01"}],
    )

    scored = score_candidate(candidate, brief)

    assert scored.current_function_fit >= 0.72
    assert "digital" not in getattr(scored, "disqualifier_reasons")
    assert scored.skill_overlap_score >= 0.75
    assert scored.industry_fit_score >= 0.75
    assert scored.verification_status in {"review", "verified"}


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


def test_score_candidate_country_only_market_counts_as_aligned_for_country_brief() -> None:
    brief = build_search_brief(
        {
            "id": "score-country-market-aligned-test",
            "role_title": "Data Analyst",
            "titles": ["Data Analyst"],
            "geography": {
                "country": "United Arab Emirates",
            },
            "required_keywords": ["SQL", "Python"],
        }
    )

    candidate = CandidateProfile(
        full_name="Country Match Analyst",
        current_title="Data Analyst",
        current_company="noon",
        location_name="United Arab Emirates",
        summary="SQL and Python data analyst supporting dashboards and reporting.",
    )

    scored = score_candidate(candidate, brief)

    assert scored.location_precision_bucket == "country_only"
    assert scored.location_aligned is True
    assert scored.score >= 50.0


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
