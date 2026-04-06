from hr_hunter.briefing import build_search_brief
from hr_hunter.recruiter_app import build_app_bootstrap, build_ui_brief_payload, extract_job_description_breakdown


def test_extract_job_description_breakdown_detects_keywords_and_years():
    breakdown = extract_job_description_breakdown(
        """
        We are hiring a Senior Data Analyst with 5-8 years of experience.
        Strong SQL and Python are required.
        Preferred experience with Power BI, experimentation, and ecommerce.
        """,
        role_title="Senior Data Analyst",
    )

    assert breakdown["years"]["min"] == 5
    assert breakdown["years"]["max"] == 8
    assert "sql" in breakdown["required_keywords"]
    assert "python" in breakdown["required_keywords"]
    assert "power bi" in breakdown["preferred_keywords"]
    assert "ecommerce" in breakdown["industry_keywords"]


def test_build_ui_brief_payload_maps_company_mode_and_location_targets():
    payload = build_ui_brief_payload(
        {
            "role_title": "Senior Data Analyst",
            "titles": ["Senior Data Analyst", "Data Analyst"],
            "countries": ["United Arab Emirates"],
            "continents": ["Middle East"],
            "cities": ["Dubai"],
            "company_targets": ["Careem", "talabat"],
            "company_match_mode": "past_only",
            "years_mode": "plus_minus",
            "years_value": 6,
            "years_tolerance": 2,
            "must_have_keywords": ["SQL", "Python"],
            "anchors": {
                "title": "critical",
                "company": "important",
                "location": "important",
            },
            "job_description": "Need 5-8 years of SQL and Python experience in ecommerce.",
        }
    )

    brief = build_search_brief(payload["brief_config"])
    assert brief.company_match_mode == "past_only"
    assert brief.minimum_years_experience == 4
    assert brief.maximum_years_experience == 8
    assert "Dubai" in brief.location_targets
    assert "United Arab Emirates" in brief.location_targets
    assert brief.geography.country == "United Arab Emirates"
    assert brief.anchor_weights["company_match"] > 0
    assert brief.hiring_company_name == ""
    assert brief.candidate_interest_required is False


def test_build_app_bootstrap_exposes_supported_ui_options():
    bootstrap = build_app_bootstrap()
    defaults = bootstrap["defaults"]
    preset = bootstrap["presets"]["senior_data_analyst_uae"]

    assert "company_interest" not in [anchor["id"] for anchor in bootstrap["anchors"]]
    assert "pdl" not in bootstrap["providers"]
    assert bootstrap["themes"]
    assert defaults["company_match_mode"] == "both"
    assert defaults["theme"] == "bright"
    assert preset["company_match_mode"] == "both"
