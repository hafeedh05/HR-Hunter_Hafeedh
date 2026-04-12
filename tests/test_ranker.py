from pathlib import Path

import pytest

from hr_hunter.briefing import build_search_brief
from hr_hunter.models import CandidateProfile
from hr_hunter.ranker import (
    LEARNED_RANKER_FEATURES,
    apply_learned_ranker,
    build_learned_feature_map,
    rank_candidate,
    train_learned_ranker,
)
from hr_hunter.scoring import score_candidate


class _FakeBooster:
    def __init__(self, predictions):
        self._predictions = predictions

    def predict(self, feature_matrix):
        assert len(feature_matrix) == len(self._predictions)
        return list(self._predictions)


def _build_ranker_brief():
    return build_search_brief(
        {
            "id": "ranker-brief",
            "role_title": "Brand Manager",
            "titles": ["Brand Manager"],
            "company_targets": ["Unilever"],
            "geography": {"location_name": "Dublin", "country": "Ireland"},
            "anchors": {
                "title": "critical",
                "company": "important",
                "location": "important",
            },
            "provider_settings": {
                "learned_ranker": {
                    "enabled": True,
                    "weight": 1.0,
                }
            },
        }
    )


def _training_row(query_id: str, candidate_id: str, candidate_name: str, label: int, title_similarity: float, company_match: float, heuristic_score: float) -> dict:
    payload = {name: 0.0 for name in LEARNED_RANKER_FEATURES}
    payload.update(
        {
            "title_similarity": title_similarity,
            "company_match": company_match,
            "location_match": 0.8,
            "skill_overlap": 0.75,
            "industry_fit": 0.7,
            "years_fit": 0.6,
            "current_function_fit": 0.8,
            "parser_confidence": 0.9,
            "evidence_quality": 0.85,
            "semantic_similarity": 0.65,
            "source_quality_score": 0.7,
            "reranker_score": 0.4,
            "heuristic_score": heuristic_score,
            "anchor_title_similarity": 1.0,
            "anchor_company_match": 0.75,
            "anchor_location_match": 0.75,
        }
    )
    return {
        "query_id": query_id,
        "brief_id": "brief",
        "candidate_id": candidate_id,
        "candidate_name": candidate_name,
        "recruiter_id": "rec-1",
        "label": label,
        "feature_json": payload,
        "model_version": "heuristic-anchor-ranker-v1",
        "final_score": heuristic_score,
        "reranker_score": 0.4,
        "status": "review",
    }


def test_build_learned_feature_map_includes_anchor_values() -> None:
    brief = _build_ranker_brief()
    candidate = CandidateProfile(
        full_name="Alice Match",
        title_similarity_score=0.95,
        company_match_score=1.0,
        location_match_score=0.8,
        skill_overlap_score=0.7,
        industry_fit_score=0.75,
        years_fit_score=0.6,
        current_function_fit=0.85,
        parser_confidence=0.9,
        evidence_quality_score=0.8,
        semantic_similarity_score=0.55,
        reranker_score=0.4,
        years_experience_gap=-2.0,
        score=78.0,
    )

    feature_map = build_learned_feature_map(candidate, brief)

    assert feature_map["title_similarity"] == 0.95
    assert feature_map["heuristic_score"] == 78.0
    assert feature_map["years_experience_gap"] == 2.0
    assert feature_map["anchor_title_similarity"] == brief.anchor_weights["title_similarity"]
    assert feature_map["anchor_company_match"] == brief.anchor_weights["company_match"]


def test_apply_learned_ranker_respects_caps(monkeypatch) -> None:
    brief = _build_ranker_brief()
    blocked = CandidateProfile(
        full_name="Blocked Candidate",
        score=30.0,
        verification_status="reject",
        cap_reasons=["hard_exclude"],
        feature_scores={"title_similarity": 0.2},
    )
    open_candidate = CandidateProfile(
        full_name="Open Candidate",
        score=60.0,
        verification_status="review",
        feature_scores={"title_similarity": 0.8},
    )
    monkeypatch.setattr(
        "hr_hunter.ranker._load_learned_ranker",
        lambda model_dir: (_FakeBooster([10.0, 1.0]), {"model_version": "lightgbm-lambdarank-test"}),
    )

    ranked = apply_learned_ranker(brief, [blocked, open_candidate])

    assert ranked[0].score == 35.0
    assert ranked[0].ranking_model_version == "lightgbm-lambdarank-test"
    assert "learned_ranker:lightgbm-lambdarank-test" in ranked[0].verification_notes


def test_train_learned_ranker_writes_artifacts(tmp_path: Path) -> None:
    pytest.importorskip("lightgbm")
    rows = [
        _training_row("brief-1:rec-1", "winner-1", "Winner 1", 4, 0.95, 1.0, 82.0),
        _training_row("brief-1:rec-1", "loser-1", "Loser 1", 0, 0.2, 0.1, 35.0),
        _training_row("brief-2:rec-1", "winner-2", "Winner 2", 4, 0.9, 1.0, 80.0),
        _training_row("brief-2:rec-1", "loser-2", "Loser 2", 0, 0.1, 0.2, 30.0),
    ]

    result = train_learned_ranker(rows, model_dir=tmp_path / "trained-model", n_estimators=8, num_leaves=7)

    assert Path(result["model_path"]).exists()
    assert Path(result["metadata_path"]).exists()
    assert result["training_row_count"] == 4
    assert result["query_count"] == 2


def test_focused_precision_brief_rejects_weak_partial_matches() -> None:
    brief = build_search_brief(
        {
            "id": "focused-ranker-brief",
            "role_title": "Data Analyst",
            "titles": ["Data Analyst"],
            "geography": {"location_name": "Dubai", "country": "United Arab Emirates"},
            "required_keywords": ["SQL", "Python"],
            "provider_settings": {
                "retrieval": {
                    "include_broad_slice": False,
                    "include_discovery_slices": False,
                    "include_history_slices": False,
                }
            },
            "anchors": {
                "title": "critical",
                "skills": "critical",
                "location": "important",
            },
        }
    )

    strong = score_candidate(
        CandidateProfile(
            full_name="Strong Match",
            current_title="Data Analyst",
            current_company="Careem",
            location_name="Dubai, United Arab Emirates",
            linkedin_url="https://www.linkedin.com/in/strong-match",
            summary="Hands-on SQL and Python analyst with dashboard reporting experience.",
        ),
        brief,
    )
    weak = score_candidate(
        CandidateProfile(
            full_name="Weak Match",
            current_title="Business Analyst",
            current_company="Careem",
            location_name="United Arab Emirates",
            linkedin_url="https://www.linkedin.com/in/weak-match",
            summary="Excel reporting and stakeholder support specialist.",
        ),
        brief,
    )

    assert strong.score > weak.score
    assert strong.verification_status in {"verified", "review"}
    assert weak.verification_status == "reject"
    assert "required_skills_missing" in weak.cap_reasons or "required_skills_partial" in weak.cap_reasons


def test_focused_precision_brief_caps_exact_role_without_skill_proof_to_review() -> None:
    brief = build_search_brief(
        {
            "id": "focused-ranker-marketing-brief",
            "role_title": "Digital Marketing Manager",
            "titles": ["Digital Marketing Manager"],
            "geography": {"location_name": "Dubai", "country": "United Arab Emirates"},
            "required_keywords": ["Google Ads", "Meta Ads", "GA4"],
            "provider_settings": {
                "retrieval": {
                    "include_broad_slice": False,
                    "include_discovery_slices": False,
                    "include_history_slices": False,
                }
            },
            "anchors": {
                "title": "critical",
                "skills": "critical",
                "location": "important",
                "function": "important",
            },
        }
    )

    candidate = score_candidate(
        CandidateProfile(
            full_name="Exact Role Sparse Profile",
            current_title="Digital Marketing Manager",
            current_company="Samsung",
            location_name="Dubai, United Arab Emirates",
            linkedin_url="https://www.linkedin.com/in/exact-role-sparse-profile",
            summary="Digital Marketing Manager for Samsung Gulf in Dubai.",
        ),
        brief,
    )

    assert candidate.current_function_fit >= 0.72
    assert candidate.verification_status == "review"
    assert "required_skills_unconfirmed" in candidate.cap_reasons


def test_title_market_priority_brief_promotes_exact_title_country_matches_even_with_sparse_industry() -> None:
    brief = build_search_brief(
        {
            "id": "title-market-priority-brief",
            "role_title": "Digital Marketing Manager",
            "titles": ["Digital Marketing Manager"],
            "geography": {"country": "United Arab Emirates"},
            "required_keywords": ["Google Ads", "Meta Ads"],
            "industry_keywords": ["ecommerce"],
        }
    )

    candidate = score_candidate(
        CandidateProfile(
            full_name="Country Match Marketer",
            current_title="Digital Marketing Manager",
            current_company="Retail Group",
            location_name="United Arab Emirates",
            linkedin_url="https://www.linkedin.com/in/country-match-marketer",
            summary="Digital Marketing Manager leading paid media and performance growth.",
        ),
        brief,
    )

    assert candidate.location_precision_bucket == "country_only"
    assert candidate.location_aligned is True
    assert candidate.verification_status in {"review", "verified"}
    assert "title_market_match" in " ".join(candidate.verification_notes)
