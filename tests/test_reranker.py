from hr_hunter.briefing import build_search_brief
from hr_hunter.models import CandidateProfile
from hr_hunter.reranker import parse_reranker_settings, rerank_candidates


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
