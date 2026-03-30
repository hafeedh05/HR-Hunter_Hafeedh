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
            "provider_settings": {"pdl": {"company_chunk_size": 5, "results_per_slice": 40}},
        }
    )

    slices = build_search_slices(brief)

    assert len(slices) == 4
    assert slices[0].search_mode == "strict"
    assert slices[1].search_mode == "broad"
    assert slices[0].companies == ["A", "B", "C", "D", "E"]
    assert slices[2].companies == ["F"]
