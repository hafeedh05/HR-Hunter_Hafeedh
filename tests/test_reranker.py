from hr_hunter.briefing import build_search_brief
from hr_hunter.models import CandidateProfile
from hr_hunter.reranker import (
    LOW_MEMORY_CPU_RERANKER_MODEL,
    _ensure_transformer_reranker_memory_budget,
    _resolve_transformer_model_name,
    parse_reranker_settings,
    rerank_candidates,
)


class _FakeBackend:
    def __init__(self, scores):
        self._scores = scores

    def score_pairs(self, pairs):
        assert len(pairs) == len(self._scores)
        return list(self._scores)


def test_parse_reranker_settings_reads_brief_config() -> None:
    brief = build_search_brief(
        {
            "id": "reranker-config-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "provider_settings": {
                "reranker": {
                    "enabled": True,
                    "model_name": "BAAI/bge-reranker-v2-m3",
                    "top_n": 12,
                    "weight": 0.4,
                }
            },
        }
    )

    settings = parse_reranker_settings(brief)

    assert settings.enabled is True
    assert settings.model_name == "BAAI/bge-reranker-v2-m3"
    assert settings.top_n == 12
    assert settings.weight == 0.4


def test_rerank_candidates_blends_scores_and_reorders(monkeypatch) -> None:
    brief = build_search_brief(
        {
            "id": "reranker-run-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "provider_settings": {
                "reranker": {
                    "enabled": True,
                    "model_name": "BAAI/bge-reranker-v2-m3",
                    "top_n": 2,
                    "weight": 0.5,
                }
            },
        }
    )
    candidates = [
        CandidateProfile(full_name="Alpha", score=80.0, verification_status="verified"),
        CandidateProfile(full_name="Beta", score=60.0, verification_status="verified"),
        CandidateProfile(full_name="Gamma", score=50.0, verification_status="review"),
    ]
    monkeypatch.setattr(
        "hr_hunter.reranker._load_backend",
        lambda model_name, device: _FakeBackend([0.1, 0.9]),
    )

    reranked = rerank_candidates(brief, candidates)

    assert [candidate.full_name for candidate in reranked[:2]] == ["Beta", "Alpha"]
    assert reranked[0].reranker_score == 0.9
    assert reranked[1].reranker_score == 0.1
    assert reranked[0].ranking_model_version == "BAAI/bge-reranker-v2-m3"
    assert reranked[2].reranker_score == 0.0


def test_rerank_candidates_prioritize_scopeful_candidates_into_rerank_window(monkeypatch) -> None:
    brief = build_search_brief(
        {
            "id": "reranker-priority-window-test",
            "role_title": "Digital Marketing Manager",
            "titles": ["Digital Marketing Manager"],
            "geography": {
                "location_name": "Dubai",
                "country": "United Arab Emirates",
                "location_hints": ["Dubai", "United Arab Emirates"],
            },
            "provider_settings": {
                "reranker": {
                    "enabled": True,
                    "model_name": "BAAI/bge-reranker-v2-m3",
                    "top_n": 2,
                    "weight": 0.4,
                }
            },
        }
    )
    candidates = [
        CandidateProfile(
            full_name="Noisy High Score",
            score=91.0,
            verification_status="review",
            current_title_match=False,
            location_aligned=False,
            location_precision_bucket="outside_target_area",
            current_function_fit=0.24,
            skill_overlap_score=0.12,
            parser_confidence=0.8,
            evidence_quality_score=0.72,
        ),
        CandidateProfile(
            full_name="Precise Fit One",
            score=58.0,
            verification_status="reject",
            current_title_match=True,
            location_aligned=True,
            location_precision_bucket="named_target_location",
            current_function_fit=0.78,
            skill_overlap_score=0.55,
            parser_confidence=0.46,
            evidence_quality_score=0.4,
        ),
        CandidateProfile(
            full_name="Precise Fit Two",
            score=56.0,
            verification_status="reject",
            current_title_match=True,
            location_aligned=True,
            location_precision_bucket="within_radius",
            current_function_fit=0.74,
            skill_overlap_score=0.52,
            parser_confidence=0.48,
            evidence_quality_score=0.39,
        ),
    ]
    monkeypatch.setattr(
        "hr_hunter.reranker._load_backend",
        lambda model_name, device: _FakeBackend([0.92, 0.81]),
    )

    reranked = rerank_candidates(brief, candidates)

    scored_candidates = {candidate.full_name: candidate for candidate in reranked}
    assert scored_candidates["Noisy High Score"].reranker_score == 0.0
    assert scored_candidates["Precise Fit One"].reranker_score > 0.0
    assert scored_candidates["Precise Fit Two"].reranker_score > 0.0


def test_rerank_candidates_reapply_guardrail_caps(monkeypatch) -> None:
    brief = build_search_brief(
        {
            "id": "reranker-cap-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "provider_settings": {
                "reranker": {
                    "enabled": True,
                    "model_name": "BAAI/bge-reranker-v2-m3",
                    "top_n": 1,
                    "weight": 1.0,
                }
            },
        }
    )
    candidates = [
        CandidateProfile(
            full_name="Blocked",
            score=30.0,
            verification_status="reject",
            cap_reasons=["hard_exclude"],
        )
    ]
    monkeypatch.setattr(
        "hr_hunter.reranker._load_backend",
        lambda model_name, device: _FakeBackend([0.99]),
    )

    reranked = rerank_candidates(brief, candidates)

    assert reranked[0].score == 35.0
    assert reranked[0].verification_status == "reject"


def test_low_memory_guard_skips_transformer_reranker(monkeypatch) -> None:
    monkeypatch.setattr("hr_hunter.reranker._memory_snapshot_bytes", lambda: (2 * 1024**3, 900 * 1024**2))
    monkeypatch.delenv("HR_HUNTER_RERANKER_ALLOW_LOW_MEMORY", raising=False)
    monkeypatch.delenv("HR_HUNTER_RERANKER_MIN_TOTAL_MEMORY_GB", raising=False)
    monkeypatch.delenv("HR_HUNTER_RERANKER_MIN_AVAILABLE_MEMORY_GB", raising=False)

    try:
        _ensure_transformer_reranker_memory_budget("cpu")
    except RuntimeError as exc:
        assert "low-memory host" in str(exc)
    else:  # pragma: no cover - guard should always fire in this scenario
        raise AssertionError("expected low-memory guard to skip transformer reranker")


def test_low_memory_guard_allows_small_cross_encoder_profile(monkeypatch) -> None:
    monkeypatch.setattr("hr_hunter.reranker._memory_snapshot_bytes", lambda: (2 * 1024**3, 900 * 1024**2))
    monkeypatch.delenv("HR_HUNTER_RERANKER_ALLOW_LOW_MEMORY", raising=False)
    monkeypatch.delenv("HR_HUNTER_RERANKER_MIN_TOTAL_MEMORY_GB_LOWMEM", raising=False)
    monkeypatch.delenv("HR_HUNTER_RERANKER_MIN_AVAILABLE_MEMORY_GB_LOWMEM", raising=False)

    _ensure_transformer_reranker_memory_budget("cpu", LOW_MEMORY_CPU_RERANKER_MODEL)


def test_resolve_transformer_model_name_auto_downgrades_bge_on_low_memory_cpu(monkeypatch) -> None:
    monkeypatch.setattr("hr_hunter.reranker._memory_snapshot_bytes", lambda: (2 * 1024**3, 900 * 1024**2))
    monkeypatch.delenv("HR_HUNTER_RERANKER_AUTO_DOWNGRADE", raising=False)
    monkeypatch.delenv("HR_HUNTER_RERANKER_AUTO_DOWNGRADE_MAX_TOTAL_MEMORY_GB", raising=False)

    resolved = _resolve_transformer_model_name("BAAI/bge-reranker-v2-m3", "cpu")

    assert resolved == LOW_MEMORY_CPU_RERANKER_MODEL


def test_rerank_candidates_fallbacks_when_backend_load_is_blocked(monkeypatch) -> None:
    brief = build_search_brief(
        {
            "id": "reranker-fallback-test",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "provider_settings": {
                "reranker": {
                    "enabled": True,
                    "model_name": "BAAI/bge-reranker-v2-m3",
                    "top_n": 2,
                    "weight": 0.35,
                }
            },
        }
    )
    candidates = [
        CandidateProfile(
            full_name="Alpha",
            score=80.0,
            verification_status="verified",
            semantic_similarity_score=0.30,
            title_similarity_score=0.20,
            skill_overlap_score=0.10,
        ),
        CandidateProfile(
            full_name="Beta",
            score=75.0,
            verification_status="review",
            semantic_similarity_score=0.85,
            title_similarity_score=0.70,
            skill_overlap_score=0.60,
        ),
    ]
    monkeypatch.setattr(
        "hr_hunter.reranker._load_backend",
        lambda model_name, device: (_ for _ in ()).throw(RuntimeError("low-memory host")),
    )

    reranked = rerank_candidates(brief, candidates)

    assert reranked[0].ranking_model_version == "fallback-lexical-reranker-v1"
    assert reranked[0].reranker_score > 0.0
    assert reranked[1].ranking_model_version == "fallback-lexical-reranker-v1"
    assert reranked[0].reranker_score > reranked[1].reranker_score
    assert any("low-memory host" in note for note in reranked[0].verification_notes)
