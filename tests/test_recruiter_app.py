from hr_hunter.briefing import build_search_brief
from hr_hunter.recruiter_app import (
    build_app_bootstrap,
    build_ui_brief_payload,
    compute_internal_fetch_limit,
    compute_top_up_fetch_limit,
    extract_job_description_breakdown,
)


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
    assert breakdown["summary"].startswith("Target role: Senior Data Analyst.")
    assert breakdown["key_experience_points"]
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
            "employment_status_mode": "not_currently_employed",
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
    assert brief.employment_status_mode == "not_currently_employed"
    assert brief.minimum_years_experience == 4
    assert brief.maximum_years_experience == 8
    assert "Dubai" in brief.location_targets
    assert "United Arab Emirates" in brief.location_targets
    assert brief.geography.country == "United Arab Emirates"
    assert brief.anchor_weights["company_match"] > 0
    assert brief.provider_settings["registry_memory"]["enabled"] is True
    assert brief.provider_settings["retrieval"]["include_history_slices"] is True


def test_build_app_bootstrap_exposes_supported_ui_options():
    bootstrap = build_app_bootstrap()
    defaults = bootstrap["defaults"]
    preset = bootstrap["presets"]["supply_chain_manager_uae"]

    assert "company_interest" not in [anchor["id"] for anchor in bootstrap["anchors"]]
    assert "pdl" not in bootstrap["providers"]
    assert bootstrap["themes"]
    assert bootstrap["employment_status_options"]
    assert defaults["company_match_mode"] == "both"
    assert defaults["employment_status_mode"] == "any"
    assert defaults["theme"] == "bright"
    assert preset["company_match_mode"] == "past_only"
    assert preset["employment_status_mode"] == "not_currently_employed"
    assert preset["jd_breakdown"]["key_experience_points"]


def test_internal_fetch_limit_overfetches_large_candidate_targets():
    assert compute_internal_fetch_limit(20) == 20
    assert compute_internal_fetch_limit(100) >= 400
    assert compute_internal_fetch_limit(200) >= 800


def test_top_up_fetch_limit_expands_large_candidate_targets():
    assert compute_top_up_fetch_limit(100, 400) == 600
    assert compute_top_up_fetch_limit(200, 800) == 1200


def test_build_ui_brief_payload_uses_internal_fetch_budget_for_retrieval():
    payload = build_ui_brief_payload(
        {
            "role_title": "Senior Product Manager",
            "titles": ["Senior Product Manager"],
            "countries": ["United Arab Emirates"],
            "company_targets": ["Careem", "talabat"],
            "limit": 100,
            "csv_export_limit": 100,
            "job_description": "Need product strategy, experimentation, analytics, and stakeholder management.",
        }
    )

    internal_fetch_limit = payload["internal_fetch_limit"]

    assert internal_fetch_limit > 100
    assert payload["brief_config"]["provider_settings"]["retrieval"]["results_per_slice"] == internal_fetch_limit
    assert payload["brief_config"]["provider_settings"]["registry_memory"]["limit"] == internal_fetch_limit
    assert payload["brief_config"]["provider_settings"]["reranker"]["top_n"] == internal_fetch_limit


def test_build_ui_brief_payload_respects_internal_fetch_override():
    payload = build_ui_brief_payload(
        {
            "role_title": "Senior Product Manager",
            "titles": ["Senior Product Manager"],
            "countries": ["United Arab Emirates"],
            "limit": 100,
            "internal_fetch_limit_override": 600,
            "job_description": "Need product strategy and experimentation experience.",
        }
    )

    assert payload["internal_fetch_limit"] == 600
    assert payload["brief_config"]["provider_settings"]["retrieval"]["results_per_slice"] == 600
    assert payload["brief_config"]["provider_settings"]["registry_memory"]["limit"] == 600
    assert payload["brief_config"]["provider_settings"]["reranker"]["top_n"] == 600
