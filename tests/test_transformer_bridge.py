import sys
from types import ModuleType, SimpleNamespace

from hr_hunter.briefing import build_search_brief
from hr_hunter.transformer_bridge import (
    _TRANSFORMER_PIPELINE_CACHE,
    _TRANSFORMER_RUNTIME_META,
    _candidate_from_transformer_entity,
    get_transformer_pipeline,
    transformer_runtime_status,
    warm_transformer_runtime,
)


def test_candidate_from_transformer_entity_matches_current_candidate_profile_shape() -> None:
    brief = build_search_brief(
        {
            "role_title": "Supply Chain Manager",
            "titles": ["Supply Chain Manager"],
            "geography": {"country": "United Arab Emirates", "location_name": "Dubai"},
        }
    )
    evidence = SimpleNamespace(
        source_url="https://www.linkedin.com/in/jane-doe",
        source_domain="linkedin.com",
        page_title="Jane Doe | Supply Chain Manager",
        page_snippet="Supply Chain Manager at ExampleCo in Dubai",
        source_type="profile",
        current_company="ExampleCo",
        company_match=True,
        current_title="Supply Chain Manager",
        location_match=True,
        current_location="Dubai, United Arab Emirates",
        current_role_signal=True,
        confidence=0.91,
        supporting_keywords=["s&op", "erp"],
    )
    entity = SimpleNamespace(
        evidence=[evidence],
        full_name="Jane Doe",
        current_title="Supply Chain Manager",
        current_company="ExampleCo",
        current_location="Dubai, United Arab Emirates",
        notes=[],
        diagnostics=[],
        verification_status="verified",
        current_company_confirmed=True,
        current_location_confirmed=True,
        current_title_confirmed=True,
        current_role_proof_count=2,
        company_match=True,
        title_match=True,
        location_match=True,
        role_family="operations",
        source_domains=["linkedin.com"],
        semantic_similarity=0.84,
        score=0.88,
        semantic_fit=0.82,
        title_match_score=0.94,
        skill_match_score=0.76,
        company_match_score=0.71,
        location_match_score=0.92,
        seniority_match_score=0.67,
        currentness_score=0.9,
        source_trust_score=0.95,
        verification_confidence=0.89,
    )

    candidate = _candidate_from_transformer_entity(entity, brief)

    assert candidate.full_name == "Jane Doe"
    assert candidate.current_company == "ExampleCo"
    assert candidate.location_precision_bucket == "named_target_location"
    assert candidate.verification_status == "verified"


def test_transformer_pipeline_cache_reuses_pipeline_instance(monkeypatch) -> None:
    _TRANSFORMER_PIPELINE_CACHE.clear()
    _TRANSFORMER_RUNTIME_META.update(
        {
            "warm_requested": False,
            "warm_completed": False,
            "last_error": "",
            "last_model_name": "",
            "initialization_seconds": 0.0,
        }
    )

    pipeline_module = ModuleType("hr_hunter_transformer.pipeline")

    class FakePipeline:
        created = 0

        def __init__(self, *, use_transformer: bool, transformer_model_name: str = "") -> None:
            FakePipeline.created += 1
            self.use_transformer = use_transformer
            self.transformer_model_name = transformer_model_name

        def usage_summary(self) -> dict[str, int | str | bool]:
            return {"encoder_type": "fake", "model_name": self.transformer_model_name}

    pipeline_module.CandidateIntelligencePipeline = FakePipeline
    monkeypatch.setattr("hr_hunter.transformer_bridge._ensure_transformer_import_path", lambda: None)
    monkeypatch.setitem(sys.modules, "hr_hunter_transformer.pipeline", pipeline_module)

    first = get_transformer_pipeline(use_transformer=True, transformer_model_name="mini-model")
    second = get_transformer_pipeline(use_transformer=True, transformer_model_name="mini-model")
    status = warm_transformer_runtime(use_transformer=True, transformer_model_name="mini-model")

    assert first is second
    assert FakePipeline.created == 1
    assert status["warm_completed"] is True
    assert status["cached_pipeline_count"] == 1
    assert transformer_runtime_status()["cache_keys"][0]["model_name"] == "mini-model"
