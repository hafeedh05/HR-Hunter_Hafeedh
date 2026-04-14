from pathlib import Path

from hr_hunter.briefing import build_search_brief
from hr_hunter.recruiter_app import (
    DEFAULT_UI_RERANKER_MODEL,
    assess_ui_brief_quality,
    build_app_bootstrap,
    build_ui_brief_payload,
    compute_internal_fetch_limit,
    compute_top_up_fetch_limit,
    ensure_structured_jd_breakdown,
    extract_job_description_breakdown,
    resolve_job_description_source,
    safe_artifact_path,
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
    assert "power bi" in breakdown["preferred_keywords"] or "power bi" in breakdown["required_keywords"]
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
    assert brief.provider_settings["registry_memory"]["enabled"] is False
    assert brief.provider_settings["retrieval"]["include_history_slices"] is True


def test_safe_artifact_path_allows_configured_shared_output(monkeypatch, tmp_path: Path):
    workspace_root = tmp_path / "release"
    (workspace_root / "output").mkdir(parents=True)
    shared_output = tmp_path / "shared-output"
    shared_output.mkdir(parents=True)
    artifact_path = shared_output / "run.csv"
    artifact_path.write_text("name\nCandidate\n", encoding="utf-8")

    monkeypatch.setattr("hr_hunter.recruiter_app.resolve_output_dir", lambda *_args, **_kwargs: shared_output)

    assert safe_artifact_path(str(artifact_path), workspace_root=workspace_root) == artifact_path.resolve()


def test_assess_ui_brief_quality_recommends_follow_up_questions_for_ambiguous_brief():
    payload = build_ui_brief_payload(
        {
            "role_title": "Regional Marketing Manager",
            "titles": ["Regional Marketing Manager"],
            "countries": ["United Arab Emirates", "Saudi Arabia", "Qatar"],
            "must_have_keywords": ["Performance Marketing", "Paid Media"],
            "job_description": "Need someone who can lead regional digital acquisition, media mix, and multi-market campaign planning.",
            "limit": 80,
        }
    )

    quality = assess_ui_brief_quality(payload["brief_config"])
    question_ids = {question["id"] for question in quality["follow_up_questions"]}

    assert quality["ok"] is True
    assert quality["needs_clarification"] is True
    assert quality["search_profile"] == "balanced"
    assert "prioritize_first_location" in question_ids
    assert "allow_adjacent_titles" in question_ids
    assert "expand_search_when_thin" in question_ids


def test_assess_ui_brief_quality_asks_explicit_ceo_title_scope_question():
    payload = build_ui_brief_payload(
        {
            "role_title": "Chief Executive Officer (CEO)",
            "titles": ["Chief Executive Officer"],
            "countries": ["United Arab Emirates", "Saudi Arabia"],
            "company_targets": ["Marina Home Interiors"],
            "must_have_keywords": ["P&L", "Retail Operations"],
            "job_description": "Need a real GCC retail chief executive with full P&L ownership and store network leadership.",
            "limit": 120,
        }
    )

    quality = assess_ui_brief_quality(payload["brief_config"])
    title_scope_question = next(
        question for question in quality["follow_up_questions"] if question["id"] == "allow_adjacent_titles"
    )

    assert title_scope_question["label"] == "Title Scope"
    assert "Managing Director" in title_scope_question["prompt"]
    assert "President" in title_scope_question["prompt"]
    assert title_scope_question["recommended_answer"] is False


def test_assess_ui_brief_quality_asks_exact_company_scope_question():
    payload = build_ui_brief_payload(
        {
            "role_title": "Chief Executive Officer (CEO)",
            "titles": ["Chief Executive Officer"],
            "countries": ["United Arab Emirates"],
            "company_targets": ["Marina Home Interiors", "The One"],
            "must_have_keywords": ["P&L", "Retail Operations"],
            "job_description": "Need a premium retail chief executive from a named target company with GCC exposure.",
            "limit": 120,
        }
    )

    quality = assess_ui_brief_quality(payload["brief_config"])
    company_scope_question = next(
        question for question in quality["follow_up_questions"] if question["id"] == "exact_company_scope"
    )

    assert company_scope_question["label"] == "Company Scope"
    assert "Marina Home Interiors" in company_scope_question["prompt"]
    assert company_scope_question["recommended_answer"] is True


def test_assess_ui_brief_quality_asks_market_scope_question_for_multimarket_brief():
    payload = build_ui_brief_payload(
        {
            "role_title": "Regional Marketing Manager",
            "titles": ["Regional Marketing Manager"],
            "countries": ["United Arab Emirates", "Saudi Arabia"],
            "cities": ["Dubai", "Riyadh"],
            "must_have_keywords": ["Performance Marketing", "Paid Media"],
            "job_description": "Need a regional marketing leader who can run multi-market growth programs across GCC cities.",
            "limit": 80,
        }
    )

    quality = assess_ui_brief_quality(payload["brief_config"])
    market_scope_question = next(
        question for question in quality["follow_up_questions"] if question["id"] == "strict_market_scope"
    )

    assert market_scope_question["label"] == "Market Scope"
    assert "Dubai" in market_scope_question["prompt"]
    assert market_scope_question["recommended_answer"] is False


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
    assert defaults["reranker_model_name"] == DEFAULT_UI_RERANKER_MODEL
    assert preset["company_match_mode"] == "both"
    assert preset["employment_status_mode"] == "any"
    assert preset["jd_breakdown"]["key_experience_points"]


def test_build_app_bootstrap_marina_preset_uses_peer_companies_for_sourcing() -> None:
    preset = build_app_bootstrap()["presets"]["ceo_marina_home_emea"]

    assert preset["company_targets"] == []
    assert "Marina Home Interiors" not in preset["peer_company_targets"]
    assert "The One" in preset["peer_company_targets"]


def test_build_app_bootstrap_supply_chain_preset_is_distinct_from_ceo_demo() -> None:
    bootstrap = build_app_bootstrap()
    preset = bootstrap["presets"]["supply_chain_manager_uae"]

    assert preset["role_title"] == "Supply Chain Manager"
    assert preset["project_name"] != bootstrap["presets"]["ceo_marina_home_emea"]["project_name"]
    assert preset["company_targets"] == []
    assert "Marina Home Interiors" not in preset["job_description"]
    assert "The One" not in preset["peer_company_targets"]
    assert "Amazon" in preset["peer_company_targets"]
    assert "S&OP" in preset["must_have_keywords"]
    assert preset["brief_clarifications"]["strict_market_scope"] is True
    assert preset["jd_breakdown"]["titles"][0] == "Supply Chain Manager"
    assert preset["jd_breakdown"]["search_tuning"]["search_profile"] == "focused"
    assert preset["jd_breakdown"]["search_tuning"]["reranker_model_name"] == DEFAULT_UI_RERANKER_MODEL


def test_build_app_bootstrap_data_analyst_preset_is_available() -> None:
    preset = build_app_bootstrap()["presets"]["data_analyst_uae"]

    assert preset["role_title"] == "Data Analyst"
    assert preset["max_profiles"] == 100
    assert "Careem" in preset["peer_company_targets"]
    assert preset["jd_breakdown"]["search_tuning"]["search_profile"] == "focused"
    assert preset["jd_breakdown"]["search_tuning"]["reranker_model_name"] == DEFAULT_UI_RERANKER_MODEL


def test_build_app_bootstrap_can_enable_code_only_login(monkeypatch):
    monkeypatch.setenv("HR_HUNTER_CODE_ONLY_LOGIN", "true")

    bootstrap = build_app_bootstrap()

    assert bootstrap["auth"]["email_required"] is False
    assert bootstrap["auth"]["code_only_login_enabled"] is True


def test_internal_fetch_limit_overfetches_large_candidate_targets():
    assert compute_internal_fetch_limit(20) == 20
    assert compute_internal_fetch_limit(100) >= 280
    assert compute_internal_fetch_limit(200) >= 540


def test_top_up_fetch_limit_expands_large_candidate_targets():
    assert compute_top_up_fetch_limit(100, 400) == 700
    assert compute_top_up_fetch_limit(200, 800) == 1400


def test_build_ui_brief_payload_defaults_blank_limit_to_300():
    payload = build_ui_brief_payload(
        {
            "role_title": "AI Engineer",
            "titles": ["AI Engineer"],
            "countries": ["United Arab Emirates"],
            "limit": "",
        }
    )

    assert payload["limit"] == 300
    assert payload["brief_config"]["max_profiles"] == 300


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
    assert payload["brief_config"]["provider_settings"]["reranker"]["top_n"] <= 500
    assert payload["brief_config"]["provider_settings"]["reranker"]["top_n"] >= 200


def test_build_ui_brief_payload_supports_keyword_tracks_and_search_tuning_from_breakdown():
    payload = build_ui_brief_payload(
        {
            "role_title": "Chief Executive Officer (CEO)",
            "titles": ["Chief Executive Officer", "Managing Director"],
            "countries": ["United Arab Emirates", "Saudi Arabia"],
            "company_targets": ["Marina Home Interiors", "The One"],
            "limit": 300,
            "job_description": "Need a premium retail executive with real P&L and multi-country leadership experience.",
            "jd_breakdown": {
                "summary": "Target role: CEO.",
                "titles": ["Chief Executive Officer"],
                "required_keywords": ["P&L ownership"],
                "preferred_keywords": ["premium retail"],
                "industry_keywords": ["home furnishings"],
                "key_experience_points": ["Multi-country retail leadership."],
                "years": {"mode": "at_least", "value": 12, "min": 12, "max": None, "tolerance": 0},
                "keyword_tracks": {
                    "portfolio_keywords": ["omnichannel"],
                    "commercial_keywords": ["profitability"],
                    "leadership_keywords": ["board governance"],
                    "scope_keywords": ["regional"],
                },
                "search_tuning": {
                    "internal_fetch_limit_override": 900,
                    "provider_parallel_requests": 18,
                    "scrapingbee_max_queries": 520,
                    "max_geo_groups": 10,
                    "company_chunk_size": 4,
                    "company_slice_location_group_limit": 6,
                    "include_discovery_slices": False,
                    "verification_top_n": 120,
                    "verification_parallel_candidates": 8,
                    "query_family_budgets": {
                        "org_chart_profile_pages": 24,
                        "profile_like_public_pages": 18,
                    },
                },
            },
        }
    )

    brief = payload["brief_config"]
    assert payload["internal_fetch_limit"] == 900
    assert brief["portfolio_keywords"] == ["omnichannel"]
    assert brief["commercial_keywords"] == ["profitability"]
    assert brief["leadership_keywords"] == ["board governance"]
    assert brief["scope_keywords"] == ["regional"]
    assert brief["provider_settings"]["retrieval"]["company_chunk_size"] == 4
    assert brief["provider_settings"]["retrieval"]["max_geo_groups"] == 10
    assert brief["provider_settings"]["retrieval"]["include_discovery_slices"] is False
    assert brief["provider_settings"]["scrapingbee_google"]["parallel_requests"] == 18
    assert brief["provider_settings"]["scrapingbee_google"]["max_queries"] == 520
    assert brief["provider_settings"]["scrapingbee_google"]["company_slice_location_group_limit"] == 6
    assert brief["provider_settings"]["scrapingbee_google"]["query_family_budgets"]["org_chart_profile_pages"] == 24
    assert brief["provider_settings"]["verification"]["top_n"] == 120
    assert brief["provider_settings"]["verification"]["parallel_candidates"] == 8


def test_build_ui_brief_payload_supports_top_level_search_tuning_overrides() -> None:
    payload = build_ui_brief_payload(
        {
            "role_title": "Chief Executive Officer (CEO)",
            "titles": ["Chief Executive Officer", "Managing Director", "President"],
            "countries": ["United Arab Emirates", "Saudi Arabia"],
            "company_targets": ["Marina Home Interiors", "The One"],
            "limit": 300,
            "job_description": "Need a premium retail executive with exact-market focus.",
            "search_tuning": {
                "provider_parallel_requests": 20,
                "scrapingbee_max_queries": 48,
                "company_chunk_size": 4,
                "company_slice_location_group_limit": 1,
                "max_company_terms_per_query": 6,
                "stagnation_query_window": 10,
                "stagnation_min_results": 260,
            },
        }
    )

    brief = payload["brief_config"]

    assert brief["provider_settings"]["scrapingbee_google"]["parallel_requests"] == 20
    assert brief["provider_settings"]["scrapingbee_google"]["max_queries"] == 48
    assert brief["provider_settings"]["scrapingbee_google"]["company_slice_location_group_limit"] == 1
    assert brief["provider_settings"]["scrapingbee_google"]["stagnation_query_window"] == 10
    assert brief["provider_settings"]["retrieval"]["company_chunk_size"] == 4


def test_build_ui_brief_payload_preserves_saved_ui_meta_search_tuning() -> None:
    payload = build_ui_brief_payload(
        {
            "role_title": "Data Analyst",
            "titles": ["Data Analyst"],
            "countries": ["United Arab Emirates"],
            "cities": ["Dubai"],
            "limit": 100,
            "job_description": "Need a UAE data analyst with SQL, Python, dashboards, and ecommerce exposure.",
            "jd_breakdown": {
                "summary": "Target role: Data Analyst.",
                "titles": ["Data Analyst"],
                "required_keywords": ["sql", "python"],
                "preferred_keywords": ["dashboarding"],
                "industry_keywords": ["ecommerce"],
                "keyword_tracks": {
                    "scope_keywords": ["dubai", "uae"],
                },
            },
            "ui_meta": {
                "search_tuning": {
                    "search_profile": "focused",
                    "provider_parallel_requests": 18,
                    "scrapingbee_max_queries": 48,
                    "stagnation_query_window": 10,
                    "include_history_slices": False,
                },
                "search_profile": "focused",
                "scope_first_enabled": True,
                "in_scope_target": 45,
                "verification_scope_target": 30,
                "brief_clarifications": {
                    "allow_adjacent_titles": False,
                    "strict_market_scope": True,
                },
            },
        }
    )

    brief = payload["brief_config"]

    assert brief["jd_breakdown"]["search_tuning"]["scrapingbee_max_queries"] == 48
    assert brief["jd_breakdown"]["search_tuning"]["provider_parallel_requests"] == 18
    assert brief["provider_settings"]["scrapingbee_google"]["max_queries"] == 48
    assert brief["provider_settings"]["scrapingbee_google"]["parallel_requests"] == 18
    assert brief.get("scope_first_enabled", False) is False
    assert brief.get("in_scope_target", 0) == 0
    assert brief.get("verification_scope_target", 0) == 0
    assert brief["brief_search_profile"] == "focused"
    assert brief["brief_clarifications"]["allow_adjacent_titles"] is False


def test_build_ui_brief_payload_preserves_zero_verification_probe_overrides():
    payload = build_ui_brief_payload(
        {
            "role_title": "Chief Executive Officer (CEO)",
            "titles": ["Chief Executive Officer", "Managing Director"],
            "countries": ["United Arab Emirates", "Saudi Arabia"],
            "company_targets": ["Marina Home Interiors", "The One"],
            "limit": 300,
            "job_description": "Need a premium retail executive with real P&L and GCC leadership experience.",
            "jd_breakdown": {
                "summary": "Target role: CEO.",
                "titles": ["Chief Executive Officer"],
                "required_keywords": ["P&L ownership"],
                "preferred_keywords": ["premium retail"],
                "industry_keywords": ["home furnishings"],
                "key_experience_points": ["GCC retail leadership."],
                "years": {"mode": "at_least", "value": 9, "min": 9, "max": None, "tolerance": 0},
                "search_tuning": {
                    "verification_parallel_candidates": 24,
                    "verification_location_probe_queries": 0,
                    "verification_company_location_probe_queries": 0,
                },
            },
        }
    )

    verification = payload["brief_config"]["provider_settings"]["verification"]

    assert verification["parallel_candidates"] == 24
    assert verification["location_probe_queries"] == 0
    assert verification["company_location_probe_queries"] == 0


def test_build_ui_brief_payload_accepts_explicit_search_profile_and_reranker_model_from_search_tuning():
    payload = build_ui_brief_payload(
        {
            "role_title": "Supply Chain Manager",
            "titles": ["Supply Chain Manager", "Demand Planning Manager"],
            "countries": ["United Arab Emirates"],
            "cities": ["Dubai"],
            "must_have_keywords": ["S&OP", "Demand Planning"],
            "job_description": "Need a UAE supply chain manager with planning, logistics, and ERP ownership.",
            "limit": 300,
            "jd_breakdown": {
                "summary": "Target role: Supply Chain Manager.",
                "titles": ["Supply Chain Manager"],
                "required_keywords": ["s&op", "demand planning"],
                "preferred_keywords": ["retail"],
                "industry_keywords": ["retail", "ecommerce"],
                "key_experience_points": ["UAE retail logistics leadership."],
                "search_tuning": {
                    "search_profile": "focused",
                    "reranker_model_name": DEFAULT_UI_RERANKER_MODEL,
                    "provider_parallel_requests": 24,
                    "scrapingbee_max_queries": 54,
                },
            },
        }
    )

    brief = payload["brief_config"]

    assert brief["brief_search_profile"] == "focused"
    assert brief["provider_settings"]["scrapingbee_google"]["parallel_requests"] == 24
    assert brief["provider_settings"]["scrapingbee_google"]["max_queries"] == 54
    assert brief["provider_settings"]["reranker"]["model_name"] == DEFAULT_UI_RERANKER_MODEL
    assert brief["ui_meta"]["reranker_model_name"] == DEFAULT_UI_RERANKER_MODEL


def test_build_ui_brief_payload_applies_brief_clarifications_and_focused_tuning():
    payload = build_ui_brief_payload(
        {
            "role_title": "Data Analyst",
            "titles": ["Data Analyst"],
            "countries": ["United Arab Emirates", "Saudi Arabia"],
            "must_have_keywords": ["SQL", "Python"],
            "job_description": "Need a hands-on analyst with SQL, Python, dashboards, and stakeholder reporting.",
            "limit": 50,
            "brief_clarifications": {
                "prioritize_first_location": True,
                "allow_adjacent_titles": False,
                "expand_search_when_thin": False,
            },
        }
    )

    brief = payload["brief_config"]
    search_brief = build_search_brief(brief)

    assert payload["internal_fetch_limit"] == 100
    assert brief["brief_search_profile"] == "focused"
    assert brief["geography"]["country"] == "United Arab Emirates"
    assert brief["expand_title_keywords"] is False
    assert brief["provider_settings"]["retrieval"]["include_broad_slice"] is False
    assert brief["provider_settings"]["retrieval"]["include_discovery_slices"] is False
    assert brief["provider_settings"]["scrapingbee_google"]["include_country_only_queries"] is False
    assert brief["provider_settings"]["scrapingbee_google"]["max_queries"] == 100
    assert search_brief.title_keywords == []


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
    assert payload["brief_config"]["provider_settings"]["reranker"]["top_n"] == 220


def test_build_ui_brief_payload_treats_peer_companies_as_real_scope_detail() -> None:
    payload = build_ui_brief_payload(
        {
            "role_title": "Regional Sales Manager",
            "titles": ["Regional Sales Manager"],
            "countries": ["United Arab Emirates"],
            "peer_company_targets": ["Careem", "talabat"],
            "limit": 80,
        }
    )

    brief = payload["brief_config"]

    assert brief["brief_search_profile"] == "balanced"


def test_build_ui_brief_payload_uses_broader_recommended_defaults_for_common_50_candidate_searches():
    payload = build_ui_brief_payload(
        {
            "role_title": "Data Analyst",
            "titles": ["Data Analyst"],
            "countries": ["United Arab Emirates"],
            "must_have_keywords": ["SQL", "Python"],
            "job_description": "Need SQL, Python, BI reporting, and stakeholder support.",
            "limit": 50,
        }
    )

    brief = payload["brief_config"]

    assert brief["brief_search_profile"] == "focused"
    assert brief["brief_clarifications"]["allow_adjacent_titles"] is True
    assert brief["brief_clarifications"]["expand_search_when_thin"] is True
    assert payload["internal_fetch_limit"] == 100
    assert brief["provider_settings"]["scrapingbee_google"]["max_queries"] == 100
    assert brief["provider_settings"]["reranker"]["top_n"] == 100
    assert brief["provider_settings"]["verification"]["top_n"] == 50
    assert brief["provider_settings"]["retrieval"]["include_broad_slice"] is True
    assert brief["provider_settings"]["retrieval"]["include_discovery_slices"] is True
    assert brief["provider_settings"]["retrieval"]["include_history_slices"] is False
    assert brief["provider_settings"]["scrapingbee_google"]["query_family_budgets"]["profile_like_public_pages"] == 18
    assert brief["provider_settings"]["scrapingbee_google"]["query_family_budgets"]["trade_directory_pages"] == 8
    assert brief["provider_settings"]["scrapingbee_google"]["query_family_budgets"]["industry_association_pages"] == 6
    assert brief["provider_settings"]["scrapingbee_google"]["query_family_budgets"]["award_industry_pages"] == 0


def test_assess_ui_brief_quality_counts_peer_company_targets_as_brief_detail() -> None:
    payload = build_ui_brief_payload(
        {
            "role_title": "Regional Sales Manager",
            "titles": ["Regional Sales Manager"],
            "countries": ["United Arab Emirates"],
            "peer_company_targets": ["Careem", "talabat"],
            "limit": 80,
        }
    )

    quality = assess_ui_brief_quality(payload["brief_config"])

    assert quality["ok"] is True
    assert quality["score"] >= 5
    assert quality["issues"] == []


def test_build_ui_brief_payload_keeps_common_100_candidate_searches_in_focused_precision_mode():
    payload = build_ui_brief_payload(
        {
            "role_title": "Digital Marketing Manager",
            "titles": ["Digital Marketing Manager"],
            "countries": ["United Arab Emirates"],
            "must_have_keywords": ["Google Ads", "Meta Ads", "GA4"],
            "preferred_keywords": ["Lead Generation"],
            "industry_keywords": ["ecommerce"],
            "job_description": "Need a Dubai-first digital marketing manager with strong paid media ownership.",
            "limit": 100,
        }
    )

    brief = payload["brief_config"]

    assert brief["brief_search_profile"] == "focused"
    assert payload["internal_fetch_limit"] == 240
    assert brief["provider_settings"]["scrapingbee_google"]["max_queries"] == 240
    assert brief["provider_settings"]["scrapingbee_google"]["parallel_requests"] == 10
    assert brief["provider_settings"]["scrapingbee_google"]["query_family_budgets"]["profile_like_public_pages"] == 18
    assert brief["provider_settings"]["scrapingbee_google"]["query_family_budgets"]["trade_directory_pages"] == 8
    assert brief["provider_settings"]["scrapingbee_google"]["query_family_budgets"]["org_chart_profile_pages"] == 1


def test_build_ui_brief_payload_does_not_auto_broaden_when_titles_are_already_explicit():
    payload = build_ui_brief_payload(
        {
            "role_title": "Chief Executive Officer (CEO)",
            "titles": ["Chief Executive Officer", "Managing Director", "President"],
            "countries": ["United Arab Emirates"],
            "company_targets": ["Marina Home Interiors"],
            "must_have_keywords": ["P&L", "Retail Operations"],
            "job_description": "Need a premium retail chief executive with GCC experience.",
            "limit": 150,
        }
    )

    brief = payload["brief_config"]
    quality = assess_ui_brief_quality(brief)
    question_ids = {question["id"] for question in quality["follow_up_questions"]}

    assert "allow_adjacent_titles" not in question_ids
    assert brief["brief_clarifications"]["allow_adjacent_titles"] is False
    assert brief["expand_title_keywords"] is False
    assert brief["provider_settings"]["retrieval"]["include_broad_slice"] is False


def test_build_ui_brief_payload_applies_exact_company_and_market_scope_clarifications():
    payload = build_ui_brief_payload(
        {
            "role_title": "Chief Executive Officer (CEO)",
            "titles": ["Chief Executive Officer"],
            "countries": ["United Arab Emirates", "Saudi Arabia"],
            "cities": ["Dubai", "Riyadh"],
            "company_targets": ["Marina Home Interiors", "The One"],
            "must_have_keywords": ["P&L", "Retail Operations"],
            "job_description": "Need a premium retail chief executive with GCC scope and current target-company alignment.",
            "limit": 150,
            "brief_clarifications": {
                "allow_adjacent_titles": False,
                "exact_company_scope": True,
                "strict_market_scope": True,
                "expand_search_when_thin": True,
            },
        }
    )

    brief = payload["brief_config"]

    assert brief["brief_clarifications"]["exact_company_scope"] is True
    assert brief["brief_clarifications"]["strict_market_scope"] is True
    assert brief["company_match_mode"] == "current_only"
    assert brief["provider_settings"]["retrieval"]["include_history_slices"] is False
    assert brief["provider_settings"]["retrieval"]["include_discovery_slices"] is False
    assert brief["provider_settings"]["retrieval"]["geo_fanout_enabled"] is False
    assert brief["provider_settings"]["scrapingbee_google"]["include_country_only_queries"] is False


def test_build_ui_brief_payload_keeps_exec_scope_first_searches_tight_before_top_up():
    payload = build_ui_brief_payload(
        {
            "role_title": "Chief Executive Officer (CEO)",
            "titles": ["Chief Executive Officer", "Managing Director", "President"],
            "countries": ["United Arab Emirates", "Saudi Arabia"],
            "cities": ["Dubai", "Riyadh"],
            "peer_company_targets": ["The One", "Al Huzaifa", "BoConcept"],
            "must_have_keywords": ["P&L", "Retail Operations", "Omnichannel"],
            "job_description": "Need a premium home retail chief executive with GCC scope and real store network ownership.",
            "limit": 300,
        }
    )

    brief = payload["brief_config"]

    assert brief.get("scope_first_enabled", False) is False
    assert brief["provider_settings"]["retrieval"]["include_history_slices"] is True
    assert brief["provider_settings"]["retrieval"]["include_discovery_slices"] is True
    assert brief["provider_settings"]["retrieval"]["geo_fanout_enabled"] is False
    assert brief["provider_settings"]["scrapingbee_google"]["include_country_only_queries"] is False
    assert brief["provider_settings"]["scrapingbee_google"]["query_family_budgets"] == {}


def test_build_ui_brief_payload_top_up_round_auto_broadens_focused_searches():
    payload = build_ui_brief_payload(
        {
            "role_title": "Data Analyst",
            "titles": ["Data Analyst"],
            "cities": ["Dubai"],
            "countries": ["United Arab Emirates"],
            "must_have_keywords": ["SQL", "Python"],
            "job_description": "Need SQL, Python, BI reporting, dashboards, and stakeholder support.",
            "limit": 50,
            "top_up_round": 2,
        }
    )

    brief = payload["brief_config"]

    assert brief["top_up_round"] == 2
    assert brief["top_up_strategy"]["auto_broadened"] is True
    assert brief["expand_title_keywords"] is True
    assert brief["provider_settings"]["retrieval"]["include_broad_slice"] is True
    assert brief["provider_settings"]["retrieval"]["include_discovery_slices"] is True
    assert brief["provider_settings"]["retrieval"]["geo_fanout_enabled"] is True
    assert brief["provider_settings"]["retrieval"]["max_geo_groups"] >= 4
    assert brief["provider_settings"]["scrapingbee_google"]["include_country_only_queries"] is True
    assert brief["provider_settings"]["scrapingbee_google"]["max_queries"] >= 200
    assert brief["provider_settings"]["scrapingbee_google"]["query_family_budgets"]["profile_like_public_pages"] == 14
    assert brief["provider_settings"]["scrapingbee_google"]["query_family_budgets"]["trade_directory_pages"] == 6
    assert brief["provider_settings"]["scrapingbee_google"]["query_family_budgets"]["team_leadership_pages"] == 2
    assert brief["provider_settings"]["scrapingbee_google"]["query_family_budgets"]["speaker_bio_pages"] == 0
    assert brief["provider_settings"]["scrapingbee_google"]["query_family_budgets"]["award_industry_pages"] == 0


def test_build_ui_brief_payload_top_up_round_respects_explicit_opt_outs():
    payload = build_ui_brief_payload(
        {
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "cities": ["Dubai"],
            "countries": ["United Arab Emirates"],
            "must_have_keywords": ["Brand Strategy", "Campaigns"],
            "job_description": "Need brand planning, campaign execution, and consumer marketing leadership.",
            "limit": 50,
            "top_up_round": 3,
            "geo_fanout_enabled": False,
            "brief_clarifications": {
                "allow_adjacent_titles": False,
                "expand_search_when_thin": False,
            },
        }
    )

    brief = payload["brief_config"]

    assert brief["top_up_round"] == 3
    assert brief["top_up_strategy"]["auto_broadened"] is False
    assert brief["expand_title_keywords"] is False
    assert brief["provider_settings"]["retrieval"]["include_broad_slice"] is False
    assert brief["provider_settings"]["retrieval"]["include_discovery_slices"] is False
    assert brief["provider_settings"]["retrieval"]["geo_fanout_enabled"] is False
    assert brief["provider_settings"]["scrapingbee_google"]["include_country_only_queries"] is False
    assert brief["provider_settings"]["scrapingbee_google"]["max_queries"] >= 200


def test_build_ui_brief_payload_technical_top_up_overrides_strict_market_cage() -> None:
    payload = build_ui_brief_payload(
        {
            "role_title": "AI Engineer",
            "titles": ["AI Engineer", "Machine Learning Engineer"],
            "cities": ["Dubai", "Abu Dhabi"],
            "countries": ["United Arab Emirates", "Saudi Arabia"],
            "must_have_keywords": ["Python", "LLM", "RAG"],
            "nice_to_have_keywords": ["PyTorch", "Transformers", "MLOps"],
            "job_description": "Need production AI engineers shipping LLM systems across GCC markets.",
            "limit": 300,
            "top_up_round": 2,
            "brief_clarifications": {
                "allow_adjacent_titles": False,
                "strict_market_scope": True,
                "expand_search_when_thin": False,
            },
        }
    )

    brief = payload["brief_config"]

    assert brief["top_up_round"] == 2
    assert brief["top_up_strategy"]["auto_broadened"] is True
    assert brief["brief_clarifications"]["strict_market_scope"] is True
    assert brief["provider_settings"]["retrieval"]["include_discovery_slices"] is True
    assert brief["provider_settings"]["retrieval"]["geo_fanout_enabled"] is True
    assert brief["provider_settings"]["scrapingbee_google"]["include_country_only_queries"] is True


def test_build_ui_brief_payload_high_volume_technical_search_verifies_full_target() -> None:
    payload = build_ui_brief_payload(
        {
            "role_title": "AI Engineer",
            "titles": ["AI Engineer", "Machine Learning Engineer"],
            "cities": ["Dubai", "Abu Dhabi"],
            "countries": ["United Arab Emirates", "Saudi Arabia"],
            "must_have_keywords": ["Python", "LLM", "RAG"],
            "nice_to_have_keywords": ["PyTorch", "Transformers", "MLOps"],
            "job_description": "Need production AI engineers shipping LLM systems across GCC markets.",
            "limit": 300,
        }
    )

    brief = payload["brief_config"]

    assert brief["provider_settings"]["verification"]["top_n"] == 300
    assert brief["provider_settings"]["verification"]["parallel_candidates"] >= 10


def test_build_ui_brief_payload_preserves_explicit_clarifications_without_follow_up_question():
    payload = build_ui_brief_payload(
        {
            "role_title": "Chief Executive Officer (CEO)",
            "titles": [
                "Chief Executive Officer",
                "Managing Director",
                "President",
                "General Manager",
            ],
            "countries": ["United Arab Emirates", "Saudi Arabia", "United Kingdom"],
            "cities": ["Dubai", "Riyadh", "London"],
            "must_have_keywords": ["P&L", "Retail Operations", "Omnichannel"],
            "job_description": "Need a premium retail chief executive with multi-country leadership and store network ownership.",
            "limit": 300,
            "brief_clarifications": {
                "prioritize_first_location": True,
                "allow_adjacent_titles": False,
                "expand_search_when_thin": True,
            },
        }
    )

    brief = payload["brief_config"]

    assert brief["brief_clarifications"]["prioritize_first_location"] is True
    assert brief["brief_clarifications"]["allow_adjacent_titles"] is False
    assert brief["brief_clarifications"]["expand_search_when_thin"] is True
    assert brief["expand_title_keywords"] is False
    assert brief["provider_settings"]["retrieval"]["include_broad_slice"] is False


def test_resolve_job_description_source_prefers_uploaded_text_and_keeps_notes():
    source = resolve_job_description_source(
        typed_text="Focus on retail and GCC exposure.",
        uploaded_text="We are hiring a Supply Chain Manager.\nStrong SAP and S&OP experience required.",
        uploaded_file_name="supply-chain-manager.pdf",
    )

    assert source["source"] == "uploaded_file"
    assert source["file_name"] == "supply-chain-manager.pdf"
    assert source["primary_text"].startswith("We are hiring a Supply Chain Manager")
    assert "Recruiter Notes" in source["combined_text"]
    assert "Focus on retail and GCC exposure." in source["combined_text"]


def test_build_ui_brief_payload_persists_uploaded_jd_metadata():
    payload = build_ui_brief_payload(
        {
            "role_title": "",
            "titles": [],
            "countries": ["United Arab Emirates"],
            "job_description": "Please prioritize GCC retail exposure.",
            "uploaded_job_description_name": "supply-chain-manager.pdf",
            "uploaded_job_description_text": "We are hiring a Supply Chain Manager with 6-9 years of experience in SAP, S&OP, and demand planning.",
        }
    )

    assert payload["brief_config"]["document_text"].startswith("We are hiring a Supply Chain Manager")
    assert "Recruiter Notes" in payload["brief_config"]["document_text"]
    assert payload["brief_config"]["ui_meta"]["uploaded_job_description_name"] == "supply-chain-manager.pdf"
    assert payload["brief_config"]["ui_meta"]["uploaded_job_description_text"].startswith("We are hiring a Supply Chain Manager")
    assert payload["brief_config"]["role_title"] == "Supply Chain Manager"


def test_ensure_structured_jd_breakdown_repairs_echoed_remote_response():
    text = (
        "We are hiring a Finance Manager in Dubai. "
        "The role requires 6-9 years of experience, strong budgeting, forecasting, FP&A, and stakeholder management. "
        "Preferred background in retail and GCC markets. Recruiter Notes: Focus on UAE retail exposure."
    )

    repaired = ensure_structured_jd_breakdown(
        {
            "summary": text,
            "titles": [],
            "required_keywords": [],
            "preferred_keywords": [],
            "industry_keywords": [],
            "key_experience_points": [],
            "years": {"mode": "range", "value": None, "min": None, "max": None, "tolerance": 0},
        },
        job_description=text,
        role_title="",
    )

    assert repaired["summary"].startswith("Target role:")
    assert repaired["titles"]
    assert repaired["titles"][0] == "Finance Manager"
    assert "forecasting" in repaired["required_keywords"]
    assert repaired["years"]["min"] == 6
    assert repaired["years"]["max"] == 9


def test_ensure_structured_jd_breakdown_enriches_sparse_key_points():
    text = """
    Position Summary
    We are seeking a Chief Executive Officer to lead regional expansion.
    Key Responsibilities
    - Strategic Leadership: Define company growth strategy and execution priorities.
    - Operational Excellence: Improve profitability, process rigor, and decision cadence.
    - Financial Stewardship: Own P&L outcomes, budgeting, and long-range planning.
    - Team Leadership: Build senior leadership capability and succession depth.
    - Stakeholder Engagement: Partner with board members and key external partners.
    Qualifications and Experience
    - Proven CEO or equivalent executive leadership track record.
    - Strong strategic planning and operational management expertise.
    - Demonstrated innovation mindset and adaptability.
    - Financial planning and governance experience.
    """

    repaired = ensure_structured_jd_breakdown(
        {
            "summary": "Target role: CEO.",
            "titles": ["CEO"],
            "required_keywords": ["innovation"],
            "preferred_keywords": [],
            "industry_keywords": ["retail"],
            "key_experience_points": ["Demonstrated innovation mindset."],
            "years": {"mode": "range", "value": None, "min": None, "max": None, "tolerance": 0},
        },
        job_description=text,
        role_title="CEO",
    )

    assert len(repaired["key_experience_points"]) >= 8
    lowered_points = " ".join(repaired["key_experience_points"]).lower()
    assert "key responsibilities" not in lowered_points
    assert "qualifications and experience" not in lowered_points


def test_extract_job_description_breakdown_handles_dense_multi_geo_prose():
    text = (
        "We are hiring a Supply Chain Manager for a multi-country EMEA mandate across GCC and Europe. "
        "The role leads planning, inventory, and logistics operations across cross-border distribution networks "
        "covering the UAE, Saudi Arabia, Qatar, Oman, Kuwait, Bahrain, and key European markets including the UK, "
        "Germany, Netherlands, France, Spain, Italy, and Poland. "
        "Required experience includes demand forecasting, inventory optimization, supplier and warehouse coordination, "
        "and S&OP execution with cross-functional teams. "
        "Ideal candidates have worked in retail, ecommerce, 3PL, aviation cargo, or consumer distribution environments, "
        "use ERP platforms such as SAP, and can improve service levels while reducing stockouts and fulfillment latency "
        "across multi-country operations."
    )

    breakdown = extract_job_description_breakdown(text, role_title="Supply Chain Manager")

    assert breakdown["titles"]
    assert "Supply Chain Manager" in breakdown["titles"][0]
    assert len(breakdown["key_experience_points"]) >= 8
    lowered_points = " ".join(breakdown["key_experience_points"]).lower()
    assert "demand forecasting" in lowered_points
    assert "inventory optimization" in lowered_points
    assert "s&op" in lowered_points
    assert "cross-border" in lowered_points
