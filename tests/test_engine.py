import asyncio

from hr_hunter.briefing import build_search_brief
from hr_hunter.engine import PROVIDER_REGISTRY, SearchEngine, provider_candidate_limit
from hr_hunter.models import CandidateProfile, ProviderRunResult


def _candidate(index: int) -> CandidateProfile:
    return CandidateProfile(
        full_name=f"Candidate {index}",
        current_title="Data Analyst",
        current_company=f"Company {index}",
        location_name="Dubai, United Arab Emirates",
        linkedin_url=f"https://www.linkedin.com/in/candidate-{index}",
        summary="SQL Python dashboarding ecommerce analytics",
    )


def test_provider_candidate_limit_shrinks_after_pool_is_already_populated() -> None:
    brief = build_search_brief(
        {
            "id": "engine-limit-test",
            "role_title": "Data Analyst",
            "titles": ["Data Analyst"],
            "geography": {"location_name": "Dubai", "country": "United Arab Emirates"},
            "required_keywords": ["sql", "python"],
        }
    )

    assert provider_candidate_limit(
        brief=brief,
        requested_limit=100,
        current_pool_size=0,
    ) == 100
    assert provider_candidate_limit(
        brief=brief,
        requested_limit=100,
        current_pool_size=90,
    ) == 40


def test_search_engine_caps_external_provider_after_registry_memory(monkeypatch) -> None:
    recorded: dict[str, int] = {}

    class RecordingProvider:
        def __init__(self, _settings):
            pass

        async def run(
            self,
            brief,
            slices,
            limit,
            dry_run,
            exclude_queries=None,
            progress_callback=None,
        ):
            recorded["limit"] = limit
            return ProviderRunResult(
                provider_name="recording_provider",
                executed=True,
                dry_run=dry_run,
                candidate_count=0,
                candidates=[],
            )

    monkeypatch.setattr("hr_hunter.engine.search_registry_memory", lambda brief, limit=0: [_candidate(i) for i in range(90)])
    monkeypatch.setitem(PROVIDER_REGISTRY, "recording_provider", RecordingProvider)

    brief = build_search_brief(
        {
            "id": "engine-provider-test",
            "role_title": "Data Analyst",
            "titles": ["Data Analyst"],
            "geography": {"location_name": "Dubai", "country": "United Arab Emirates"},
            "required_keywords": ["sql", "python"],
            "provider_settings": {
                "registry_memory": {"enabled": True, "limit": 100},
                "recording_provider": {},
            },
        }
    )

    report = asyncio.run(
        SearchEngine().run(
            brief=brief,
            provider_names=["recording_provider"],
            limit=100,
            dry_run=False,
        )
    )

    assert recorded["limit"] == 40
    assert report.provider_results[0].provider_name == "registry_memory"
    assert report.provider_results[1].provider_name == "recording_provider"
