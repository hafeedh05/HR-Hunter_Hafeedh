from hr_hunter.briefing import build_search_brief
from hr_hunter.providers.scrapingbee import ScrapingBeeGoogleProvider


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
