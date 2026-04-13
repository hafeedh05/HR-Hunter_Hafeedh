import asyncio
import json

from hr_hunter.briefing import build_search_brief
from hr_hunter.identity import canonical_query_fingerprint
from hr_hunter.providers.scrapingbee import ScrapingBeeGoogleProvider
from hr_hunter.query_planner import build_search_slices


class _FakeSearchResponse:
    status_code = 200
    text = ""

    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class _FakeScrapingBeeClient:
    def __init__(self, payload):
        self._payload = payload
        self.api_key = "test-key"

    def is_configured(self):
        return True

    async def search(self, client, search_query, *, page, country_code, language, light_request):
        return _FakeSearchResponse(self._payload)


def test_candidate_parser_skips_non_person_team_pages() -> None:
    provider = ScrapingBeeGoogleProvider({})
    brief = build_search_brief(
        {
            "id": "scrapingbee-person-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "company_targets": ["Unilever"],
            "geography": {"location_name": "Drogheda", "country": "Ireland"},
        }
    )

    candidate = provider._candidate_from_result(
        {
            "title": "Our Leadership Team - Unilever",
            "description": "Meet the leadership team in Ireland.",
            "url": "https://example.com/our-team/",
        },
        brief,
    )

    assert candidate is None


def test_candidate_parser_keeps_matched_irish_location_hint() -> None:
    provider = ScrapingBeeGoogleProvider({})
    brief = build_search_brief(
        {
            "id": "scrapingbee-location-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "company_targets": ["Unilever"],
            "geography": {
                "location_name": "Drogheda, Ireland",
                "country": "Ireland",
                "location_hints": ["Dublin", "Galway"],
            },
        }
    )

    candidate = provider._candidate_from_result(
        {
            "title": "Jane Search - Senior Brand Manager - Unilever",
            "description": "Senior FMCG leader based in Dublin, Ireland.",
            "url": "https://example.com/people/jane-search",
        },
        brief,
    )

    assert candidate is not None
    assert candidate.location_name == "Dublin, Ireland"
    assert candidate.current_company == "Unilever"


def test_candidate_parser_skips_contact_directory_pages() -> None:
    provider = ScrapingBeeGoogleProvider({})
    brief = build_search_brief(
        {
            "id": "scrapingbee-contact-page-test",
            "role_title": "Senior Data Analyst",
            "titles": ["Senior Data Analyst"],
            "company_targets": ["noon"],
            "geography": {"location_name": "Dubai", "country": "United Arab Emirates"},
        }
    )

    candidate = provider._candidate_from_result(
        {
            "title": "Sneha Beniwal's email & phone | Noon's Data Analyst contact info",
            "description": "Sneha Beniwal works as a Data Analyst at Noon. Reveal for Free.",
            "url": "https://www.datanyze.com/people/Sneha-Beniwal/6888991245",
        },
        brief,
    )

    assert candidate is None


def test_candidate_parser_cleans_bidi_marks_and_infers_title_from_description() -> None:
    provider = ScrapingBeeGoogleProvider({})
    brief = build_search_brief(
        {
            "id": "scrapingbee-bidi-cleanup-test",
            "role_title": "Senior Data Analyst",
            "titles": ["Senior Data Analyst"],
            "title_keywords": ["data analyst"],
            "company_targets": ["dubizzle"],
            "company_aliases": {"dubizzle": ["Dubizzle Group"]},
            "geography": {"location_name": "Dubai", "country": "United Arab Emirates"},
        }
    )

    candidate = provider._candidate_from_result(
        {
            "title": "Ali Karim\u200f - \u200fdubizzle",
            "description": "Senior Data Analyst at dubizzle. Dubai, United Arab Emirates.",
            "url": "https://ae.linkedin.com/in/ali-karim",
        },
        brief,
    )

    assert candidate is not None
    assert candidate.full_name == "Ali Karim"
    assert candidate.current_title == "Senior Data Analyst"
    assert candidate.current_company == "dubizzle"


def test_candidate_parser_clears_skill_fragment_misparsed_as_company() -> None:
    provider = ScrapingBeeGoogleProvider({})
    brief = build_search_brief(
        {
            "id": "scrapingbee-skill-company-test",
            "role_title": "Digital Marketing Manager",
            "titles": ["Digital Marketing Manager"],
            "required_keywords": ["Google Ads", "Meta Ads", "GA4"],
            "preferred_keywords": ["Paid Social", "Performance Marketing"],
            "geography": {"location_name": "Dubai", "country": "United Arab Emirates"},
        }
    )

    candidate = provider._candidate_from_result(
        {
            "title": "Farah Example - Digital Marketing Manager at Google Ads",
            "description": "Digital Marketing Manager based in Dubai, United Arab Emirates.",
            "url": "https://example.com/people/farah-example",
        },
        brief,
    )

    assert candidate is not None
    assert candidate.current_title == "Digital Marketing Manager"
    assert candidate.current_company == ""


def test_candidate_parser_infers_company_from_public_description() -> None:
    provider = ScrapingBeeGoogleProvider({})
    brief = build_search_brief(
        {
            "id": "scrapingbee-description-company-test",
            "role_title": "Supply Chain Manager",
            "titles": ["Supply Chain Manager", "Distribution Manager"],
            "geography": {"location_name": "Dubai", "country": "United Arab Emirates"},
        }
    )

    candidate = provider._candidate_from_result(
        {
            "title": "Muhammed Riaz | Supply Chain Manager",
            "description": "Supply Chain Manager at Truebell in Dubai, United Arab Emirates.",
            "url": "https://example.com/people/muhammed-riaz",
        },
        brief,
    )

    assert candidate is not None
    assert candidate.current_title == "Supply Chain Manager"
    assert candidate.current_company == "Truebell"


def test_candidate_parser_infers_company_from_corporate_people_domain() -> None:
    provider = ScrapingBeeGoogleProvider({})
    brief = build_search_brief(
        {
            "id": "scrapingbee-domain-company-test",
            "role_title": "Supply Chain Manager",
            "titles": ["Supply Chain Manager"],
            "geography": {"location_name": "Dubai", "country": "United Arab Emirates"},
        }
    )

    candidate = provider._candidate_from_result(
        {
            "title": "Jane Search - Supply Chain Manager",
            "description": "Dubai, United Arab Emirates.",
            "url": "https://www.truebell.ae/team/jane-search",
        },
        brief,
    )

    assert candidate is not None
    assert candidate.current_company == "Truebell"


def test_publication_profile_domains_do_not_infer_company_from_host() -> None:
    provider = ScrapingBeeGoogleProvider({})
    assert provider._extract_org_from_url("https://www.gulfbusiness.com/profile/jane-search") == ""


def test_candidate_parser_does_not_infer_current_role_from_historical_snippet() -> None:
    provider = ScrapingBeeGoogleProvider({})
    brief = build_search_brief(
        {
            "id": "scrapingbee-historical-role-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "company_targets": ["Procter & Gamble"],
            "company_aliases": {"Procter & Gamble": ["P&G"]},
            "geography": {"location_name": "Drogheda", "country": "Ireland"},
        }
    )

    candidate = provider._candidate_from_result(
        {
            "title": "Jonathan Gordon",
            "description": "Before joining McKinsey, Jonathan worked as a brand manager at Procter & Gamble. Originally from Ireland.",
            "url": "https://www.mckinsey.com/our-people/jonathan-gordon",
        },
        brief,
    )

    assert candidate is not None
    assert candidate.current_company == ""
    assert candidate.current_title == "Jonathan Gordon"


def test_scrapingbee_builds_public_query_families() -> None:
    provider = ScrapingBeeGoogleProvider({})
    brief = build_search_brief(
        {
            "id": "scrapingbee-families-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "company_targets": ["Unilever"],
            "industry_keywords": ["FMCG"],
            "required_keywords": ["brand", "category"],
            "geography": {"location_name": "Drogheda", "country": "Ireland"},
        }
    )
    strict_slice = next(
        slice_config for slice_config in build_search_slices(brief) if slice_config.search_mode == "strict"
    )

    plans = provider._build_query_plans(brief, strict_slice)
    families = {plan["family"] for plan in plans}

    assert families == {
        "team_leadership_pages",
        "appointment_news_pages",
        "speaker_bio_pages",
        "award_industry_pages",
        "industry_association_pages",
        "trade_directory_pages",
        "org_chart_profile_pages",
        "profile_like_public_pages",
    }


def test_scrapingbee_dry_run_skips_queries_by_fingerprint() -> None:
    provider = ScrapingBeeGoogleProvider({})
    brief = build_search_brief(
        {
            "id": "scrapingbee-fingerprint-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "company_targets": ["Unilever"],
            "industry_keywords": ["FMCG"],
            "required_keywords": ["brand"],
            "geography": {"location_name": "Drogheda", "country": "Ireland"},
        }
    )
    strict_slice = next(
        slice_config for slice_config in build_search_slices(brief) if slice_config.search_mode == "strict"
    )
    first_plan = provider._build_query_plans(brief, strict_slice)[0]
    exclude_query = first_plan["search"].replace(") (", ")   (")

    result = asyncio.run(
        provider.run(
            brief,
            [strict_slice],
            limit=10,
            dry_run=True,
            exclude_queries={exclude_query},
        )
    )

    skipped = next(item for item in result.diagnostics["queries"] if item["fingerprint"] == first_plan["fingerprint"])
    assert canonical_query_fingerprint(exclude_query) == first_plan["fingerprint"]
    assert skipped["skipped"] is True
    assert skipped["skip_reason"] == "exclude_query"


def test_scrapingbee_dry_run_enforces_family_and_run_budgets() -> None:
    provider = ScrapingBeeGoogleProvider(
        {
            "max_queries": 3,
            "query_family_budgets": {
                "team_leadership_pages": 1,
            },
        }
    )
    brief = build_search_brief(
        {
            "id": "scrapingbee-budget-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "company_targets": ["Unilever"],
            "industry_keywords": ["FMCG"],
            "required_keywords": ["brand", "category"],
            "geography": {"location_name": "Drogheda", "country": "Ireland"},
        }
    )
    strict_slice = next(
        slice_config for slice_config in build_search_slices(brief) if slice_config.search_mode == "strict"
    )

    result = asyncio.run(provider.run(brief, [strict_slice], limit=10, dry_run=True))
    query_budget = result.diagnostics["query_budget"]

    assert result.diagnostics["query_budget_exhausted"] is True
    assert query_budget["executed_per_family"]["team_leadership_pages"] == 1
    assert query_budget["skipped_per_family"]["team_leadership_pages"] >= 1
    assert "team_leadership_pages" in query_budget["family_budget_exhausted"]


def test_scrapingbee_zero_family_budget_disables_family() -> None:
    provider = ScrapingBeeGoogleProvider(
        {
            "query_family_budgets": {
                "speaker_bio_pages": 0,
                "profile_like_public_pages": 1,
            },
        }
    )
    brief = build_search_brief(
        {
            "id": "scrapingbee-zero-budget-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "company_targets": ["Unilever"],
            "industry_keywords": ["FMCG"],
            "required_keywords": ["brand", "category"],
            "geography": {"location_name": "Drogheda", "country": "Ireland"},
        }
    )
    strict_slice = next(
        slice_config for slice_config in build_search_slices(brief) if slice_config.search_mode == "strict"
    )

    result = asyncio.run(provider.run(brief, [strict_slice], limit=10, dry_run=True))
    query_budget = result.diagnostics["query_budget"]

    assert "speaker_bio_pages" in query_budget["family_budget_exhausted"]
    assert query_budget["executed_per_family"].get("speaker_bio_pages", 0) == 0
    assert query_budget["skipped_per_family"]["speaker_bio_pages"] >= 1
    assert query_budget["executed_per_family"]["profile_like_public_pages"] == 1


def test_scrapingbee_builds_companyless_strict_query_plans() -> None:
    provider = ScrapingBeeGoogleProvider({})
    brief = build_search_brief(
        {
            "id": "scrapingbee-companyless-strict-test",
            "role_title": "Data Analyst",
            "titles": ["Data Analyst"],
            "company_targets": [],
            "required_keywords": ["SQL", "Python"],
            "geography": {"location_name": "Dubai", "country": "United Arab Emirates"},
            "provider_settings": {
                "retrieval": {
                    "include_broad_slice": False,
                    "include_discovery_slices": False,
                    "include_history_slices": False,
                }
            },
        }
    )
    strict_slice = next(
        slice_config for slice_config in build_search_slices(brief) if slice_config.search_mode == "strict"
    )

    plans = provider._build_query_plans(brief, strict_slice)

    assert plans
    assert all('"Data Analyst"' in plan["search"] for plan in plans)
    assert all('"Dubai"' in plan["search"] or '"United Arab Emirates"' in plan["search"] for plan in plans)


def test_scrapingbee_early_stop_keeps_query_completion_telemetry() -> None:
    provider = ScrapingBeeGoogleProvider({"parallel_requests": 1, "pages_per_query": 1})
    provider.client = _FakeScrapingBeeClient(
        {
            "organic_results": [
                {
                    "title": "Jane Doe - Data Analyst",
                    "description": "Data Analyst at Careem in Dubai, United Arab Emirates. SQL and Python.",
                    "url": "https://www.linkedin.com/in/jane-doe",
                }
            ]
        }
    )
    brief = build_search_brief(
        {
            "id": "scrapingbee-telemetry-test",
            "role_title": "Data Analyst",
            "titles": ["Data Analyst"],
            "company_targets": [],
            "required_keywords": ["SQL", "Python"],
            "geography": {"location_name": "Dubai", "country": "United Arab Emirates"},
            "provider_settings": {
                "retrieval": {
                    "include_broad_slice": False,
                    "include_discovery_slices": False,
                    "include_history_slices": False,
                }
            },
        }
    )
    strict_slice = next(
        slice_config for slice_config in build_search_slices(brief) if slice_config.search_mode == "strict"
    )
    events = []

    result = asyncio.run(
        provider.run(
            brief,
            [strict_slice],
            limit=1,
            dry_run=False,
            progress_callback=events.append,
        )
    )

    assert result.diagnostics["query_budget"]["query_page_completed"] >= 1
    assert any(int(event.get("queries_completed", 0) or 0) >= 1 for event in events)


def test_scrapingbee_builds_history_query_plans() -> None:
    provider = ScrapingBeeGoogleProvider({})
    brief = build_search_brief(
        {
            "id": "scrapingbee-history-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "title_keywords": ["Trade Marketing Manager"],
            "company_targets": ["Unilever"],
            "geography": {"location_name": "Drogheda", "country": "Ireland"},
        }
    )
    history_slice = build_search_slices(
        build_search_brief(
            {
                "id": "scrapingbee-history-test",
                "role_title": "Brand Manager",
                "titles": ["Brand Manager"],
                "title_keywords": ["Trade Marketing Manager"],
                "company_targets": ["Unilever"],
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
    )[0]

    plans = provider._build_query_plans(brief, history_slice)

    assert plans
    assert any('"formerly"' in plan["search"] for plan in plans)
    assert any('"Unilever"' in plan["search"] for plan in plans)


def test_scrapingbee_stops_waiting_on_hung_queries_once_limit_is_met(monkeypatch) -> None:
    provider = ScrapingBeeGoogleProvider(
        {
            "parallel_requests": 2,
            "max_queries": 2,
        }
    )
    provider.client.api_key = "test-key"
    brief = build_search_brief(
        {
            "id": "scrapingbee-stop-test",
            "role_title": "Chief Executive Officer",
            "titles": ["Chief Executive Officer", "CEO"],
            "company_targets": ["Marina Home Interiors"],
            "geography": {"location_name": "Dubai", "country": "United Arab Emirates"},
        }
    )
    strict_slice = next(
        slice_config for slice_config in build_search_slices(brief) if slice_config.search_mode == "strict"
    )
    second_call_started = asyncio.Event()
    second_call_cancelled = False
    call_count = 0

    class FakeResponse:
        def __init__(self, payload):
            self.status_code = 200
            self._payload = payload
            self.text = json.dumps(payload)

        def json(self):
            return self._payload

    async def fake_search(*args, **kwargs):
        nonlocal call_count, second_call_cancelled
        call_count += 1
        if call_count == 1:
            await asyncio.wait_for(second_call_started.wait(), timeout=0.2)
            return FakeResponse(
                {
                    "organic_results": [
                        {
                            "title": "Jane Search - Chief Executive Officer - Marina Home Interiors",
                            "description": "Chief Executive Officer at Marina Home Interiors in Dubai, United Arab Emirates.",
                            "url": "https://www.linkedin.com/in/jane-search",
                        }
                    ]
                }
            )
        second_call_started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            second_call_cancelled = True
            raise
        return FakeResponse({"organic_results": []})

    monkeypatch.setattr(provider.client, "search", fake_search)

    result = asyncio.run(
        asyncio.wait_for(
            provider.run(brief, [strict_slice], limit=1, dry_run=False),
            timeout=1.5,
        )
    )

    assert call_count == 2
    assert second_call_cancelled is True
    assert result.candidate_count == 1
