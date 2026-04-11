from hr_hunter.briefing import build_search_brief
from hr_hunter.recruiter_app import (
    assess_ui_brief_quality,
    build_app_bootstrap,
    build_ui_brief_payload,
    compute_internal_fetch_limit,
    compute_top_up_fetch_limit,
    ensure_structured_jd_breakdown,
    extract_job_description_breakdown,
    resolve_job_description_source,
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
    assert brief.provider_settings["registry_memory"]["enabled"] is True
    assert brief.provider_settings["retrieval"]["include_history_slices"] is True


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
    assert preset["company_match_mode"] == "both"
    assert preset["employment_status_mode"] == "any"
    assert preset["jd_breakdown"]["key_experience_points"]


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
    assert brief["provider_settings"]["scrapingbee_google"]["query_family_budgets"]["profile_like_public_pages"] == 8
    assert brief["provider_settings"]["scrapingbee_google"]["query_family_budgets"]["team_leadership_pages"] == 6


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
    assert brief["provider_settings"]["scrapingbee_google"]["query_family_budgets"]["profile_like_public_pages"] >= 14
    assert brief["provider_settings"]["scrapingbee_google"]["query_family_budgets"]["team_leadership_pages"] >= 10


def test_build_ui_brief_payload_top_up_round_respects_explicit_opt_outs():
    payload = build_ui_brief_payload(
        {
            "role_title": "Data Analyst",
            "titles": ["Data Analyst"],
            "cities": ["Dubai"],
            "countries": ["United Arab Emirates"],
            "must_have_keywords": ["SQL", "Python"],
            "job_description": "Need SQL, Python, BI reporting, dashboards, and stakeholder support.",
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
