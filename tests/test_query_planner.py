from hr_hunter.briefing import build_search_brief
from hr_hunter.query_planner import build_search_slices


def test_query_planner_creates_strict_and_broad_slices() -> None:
    brief = build_search_brief(
        {
            "id": "planner-test",
            "role_title": "Role",
            "titles": ["Global Product Manager"],
            "company_targets": [
                "A",
                "B",
                "C",
                "D",
                "E",
                "F",
            ],
            "geography": {"location_name": "Drogheda", "country": "Ireland"},
            "provider_settings": {"retrieval": {"company_chunk_size": 5, "results_per_slice": 40}},
        }
    )

    slices = build_search_slices(brief)

    assert len(slices) == 4
    assert slices[0].search_mode == "strict"
    assert slices[1].search_mode == "broad"
    assert slices[0].companies == ["A", "B", "C", "D", "E"]
    assert slices[2].companies == ["F"]


def test_query_planner_adds_precision_keywords_to_broad_slices() -> None:
    brief = build_search_brief(
        {
            "id": "planner-keywords-test",
            "role_title": "Role",
            "titles": ["Brand Manager"],
            "company_targets": ["Unilever"],
            "geography": {"location_name": "Drogheda", "country": "Ireland"},
            "industry_keywords": ["FMCG", "consumer goods"],
            "required_keywords": ["product portfolio"],
            "portfolio_keywords": ["category"],
            "commercial_keywords": ["commercial"],
            "provider_settings": {"retrieval": {"include_discovery_slices": False}},
        }
    )

    slices = build_search_slices(brief)
    broad_slice = next(slice_config for slice_config in slices if slice_config.search_mode == "broad")

    assert "FMCG" in broad_slice.query_keywords
    assert "product portfolio" in broad_slice.query_keywords
    assert "commercial" in broad_slice.query_keywords


def test_query_planner_chunks_discovery_keywords() -> None:
    brief = build_search_brief(
        {
            "id": "planner-discovery-test",
            "role_title": "Role",
            "titles": ["Brand Manager"],
            "company_targets": ["A"],
            "geography": {"location_name": "Drogheda", "country": "Ireland"},
            "required_keywords": ["portfolio", "commercial", "consumer goods", "brand", "category"],
            "preferred_keywords": ["innovation", "retail", "FMCG"],
            "provider_settings": {
                "retrieval": {
                    "company_chunk_size": 5,
                    "results_per_slice": 20,
                    "discovery_keyword_chunk_size": 3,
                    "market_keyword_chunk_size": 2,
                }
            },
        }
    )

    slices = build_search_slices(brief)
    discovery_slices = [slice_config for slice_config in slices if slice_config.search_mode == "discovery"]
    market_slices = [slice_config for slice_config in slices if slice_config.search_mode == "market"]

    assert len(discovery_slices) >= 2
    assert len(market_slices) >= 2
    assert all(slice_config.query_keywords for slice_config in discovery_slices)
    assert all(slice_config.query_keywords for slice_config in market_slices)


def test_query_planner_discovery_slices_stay_fmcg_adjacent() -> None:
    brief = build_search_brief(
        {
            "id": "planner-adjacent-test",
            "role_title": "Senior Product Manager",
            "titles": ["Senior Product Manager"],
            "company_targets": ["Unilever"],
            "geography": {"location_name": "Drogheda", "country": "Ireland"},
            "industry_keywords": ["FMCG", "consumer goods"],
            "required_keywords": ["brand", "category", "portfolio"],
            "commercial_keywords": ["commercial"],
            "leadership_keywords": ["leadership"],
            "scope_keywords": ["global", "international"],
        }
    )

    slices = build_search_slices(brief)
    discovery_like = [
        slice_config for slice_config in slices if slice_config.search_mode in {"discovery", "market"}
    ]

    assert discovery_like
    assert all("leadership" not in slice_config.query_keywords for slice_config in discovery_like)
    assert all("global" not in slice_config.query_keywords for slice_config in discovery_like)
    assert any("FMCG" in slice_config.query_keywords for slice_config in discovery_like)
    assert any("brand" in slice_config.query_keywords for slice_config in discovery_like)


def test_query_planner_can_add_history_slices() -> None:
    brief = build_search_brief(
        {
            "id": "planner-history-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "company_targets": ["Unilever", "Procter & Gamble"],
            "geography": {"location_name": "Drogheda", "country": "Ireland"},
            "provider_settings": {
                "retrieval": {
                    "include_strict_slice": False,
                    "include_broad_slice": False,
                    "include_discovery_slices": False,
                    "include_history_slices": True,
                    "history_query_terms": ["formerly", "previously"],
                }
            },
        }
    )

    slices = build_search_slices(brief)
    history_slices = [slice_config for slice_config in slices if slice_config.search_mode == "history"]

    assert len(history_slices) == 1
    assert history_slices[0].companies == ["Unilever", "Procter & Gamble"]
    assert history_slices[0].query_keywords == ["formerly", "previously"]


def test_query_planner_keeps_exact_title_slices_without_company_targets() -> None:
    brief = build_search_brief(
        {
            "id": "planner-no-company-test",
            "role_title": "Data Analyst",
            "titles": ["Data Analyst"],
            "company_targets": [],
            "geography": {"location_name": "Dubai", "country": "United Arab Emirates"},
            "provider_settings": {
                "retrieval": {
                    "include_strict_slice": True,
                    "include_broad_slice": False,
                    "include_discovery_slices": False,
                    "include_history_slices": False,
                }
            },
        }
    )

    slices = build_search_slices(brief)

    assert len(slices) == 1
    assert slices[0].search_mode == "strict"
    assert slices[0].companies == []


def test_query_planner_skips_history_slices_without_company_targets() -> None:
    brief = build_search_brief(
        {
            "id": "planner-no-company-history-test",
            "role_title": "Data Analyst",
            "titles": ["Data Analyst"],
            "company_targets": [],
            "geography": {"location_name": "Dubai", "country": "United Arab Emirates"},
            "provider_settings": {
                "retrieval": {
                    "include_strict_slice": True,
                    "include_broad_slice": False,
                    "include_discovery_slices": False,
                    "include_history_slices": True,
                    "history_query_terms": ["formerly", "previously"],
                }
            },
        }
    )

    slices = build_search_slices(brief)

    assert [slice_config.search_mode for slice_config in slices] == ["strict"]
