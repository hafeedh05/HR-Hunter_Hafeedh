from hr_hunter_transformer.models import CandidateEntity
from hr_hunter_transformer.pipeline import _final_candidate_order_key, _verification_window_size


def test_verification_window_expands_beyond_requested_target() -> None:
    assert _verification_window_size(495, 300) == 495
    assert _verification_window_size(1272, 300) == 900
    assert _verification_window_size(80, 300) == 80


def test_final_candidate_order_prioritizes_verified_before_review() -> None:
    review = CandidateEntity(
        full_name="High Score Review",
        canonical_key="review",
        score=99.0,
        verification_confidence=0.92,
        verification_status="review",
    )
    verified = CandidateEntity(
        full_name="Lower Score Verified",
        canonical_key="verified",
        score=70.0,
        verification_confidence=0.78,
        verification_status="verified",
    )
    rejected = CandidateEntity(
        full_name="Rejected",
        canonical_key="reject",
        score=100.0,
        verification_confidence=0.95,
        verification_status="reject",
    )

    assert sorted([review, rejected, verified], key=_final_candidate_order_key) == [verified, review, rejected]
