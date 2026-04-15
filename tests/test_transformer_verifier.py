from hr_hunter_transformer.models import CandidateEntity, EvidenceRecord, SearchBrief
from hr_hunter_transformer.verifier import verify_candidate


def test_transformer_verifier_marks_strong_candidate_verified() -> None:
    brief = SearchBrief(role_title="Interior Design Manager", titles=["Interior Design Manager"], countries=["United Arab Emirates"], cities=["Dubai"])
    candidate = CandidateEntity(
        full_name="Alya Noor",
        canonical_key="alya-noor",
        current_title="Interior Design Manager",
        current_company="Studio X",
        current_location="Dubai",
        role_family="design_architecture",
        title_match=True,
        company_match=True,
        location_match=True,
        current_role_proof_count=3,
        current_company_confirmed=True,
        current_title_confirmed=True,
        current_location_confirmed=True,
        title_match_score=1.0,
        company_match_score=0.9,
        location_match_score=0.9,
        currentness_score=0.9,
        source_trust_score=0.85,
        score=82.0,
        evidence=[
            EvidenceRecord(
                source_url="https://linkedin.com/in/alya-noor",
                source_domain="linkedin.com",
                source_type="scrapingbee_google",
                page_title="Alya Noor | Interior Design Manager",
                page_snippet="Interior Design Manager at Studio X in Dubai",
                full_name="Alya Noor",
                current_title="Interior Design Manager",
                current_company="Studio X",
                current_location="Dubai",
                role_family="design_architecture",
                title_match=True,
                company_match=True,
                location_match=True,
                current_role_signal=True,
                confidence=0.92,
            )
        ],
    )
    verified = verify_candidate(candidate, brief)
    assert verified.verification_status == "verified"
    assert verified.verification_confidence >= 0.78


def test_transformer_verifier_keeps_dense_role_candidate_in_review_without_company_proof() -> None:
    brief = SearchBrief(
        role_title="Supply Chain Manager",
        titles=["Supply Chain Manager", "Supply Planning Manager"],
        countries=["United Arab Emirates", "Saudi Arabia"],
        cities=["Dubai", "Riyadh"],
    )
    candidate = CandidateEntity(
        full_name="Ahmad Hassan",
        canonical_key="ahmad-hassan",
        current_title="Supply Chain Manager",
        current_company="",
        current_location="Dubai",
        role_family="supply_chain",
        title_match=True,
        company_match=False,
        location_match=True,
        current_role_proof_count=2,
        current_company_confirmed=False,
        current_title_confirmed=True,
        current_location_confirmed=True,
        title_match_score=0.95,
        company_match_score=0.22,
        location_match_score=0.82,
        currentness_score=0.86,
        source_trust_score=0.88,
        score=70.5,
        evidence=[
            EvidenceRecord(
                source_url="https://www.linkedin.com/in/ahmad-hassan",
                source_domain="linkedin.com",
                source_type="scrapingbee_google",
                page_title="Ahmad Hassan | Supply Chain Manager",
                page_snippet="Supply Chain Manager in Dubai",
                full_name="Ahmad Hassan",
                current_title="Supply Chain Manager",
                current_company="",
                current_location="Dubai",
                role_family="supply_chain",
                title_match=True,
                company_match=False,
                location_match=True,
                current_role_signal=True,
                confidence=0.84,
                title_confidence=0.92,
                company_confidence=0.2,
                location_confidence=0.82,
                currentness_confidence=0.84,
                freshness_confidence=0.62,
            )
        ],
    )
    verified = verify_candidate(candidate, brief)
    assert verified.verification_status == "review"
    assert "missing_current_company_confirmation" in verified.diagnostics


def test_transformer_verifier_keeps_executive_candidate_in_review_without_company_proof() -> None:
    brief = SearchBrief(
        role_title="Chief Executive Officer (CEO)",
        titles=["Chief Executive Officer (CEO)", "CEO"],
        countries=["United Arab Emirates"],
        cities=["Dubai"],
    )
    candidate = CandidateEntity(
        full_name="Ahmad Al Mohdar",
        canonical_key="ahmad-al-mohdar",
        current_title="Chief Executive Officer",
        current_company="",
        current_location="Dubai",
        role_family="executive",
        title_match=True,
        company_match=False,
        location_match=True,
        current_role_proof_count=2,
        current_company_confirmed=False,
        current_title_confirmed=True,
        current_location_confirmed=True,
        title_match_score=0.95,
        company_match_score=0.2,
        location_match_score=0.84,
        currentness_score=0.9,
        source_trust_score=0.92,
        score=74.5,
        evidence=[
            EvidenceRecord(
                source_url="https://www.linkedin.com/in/ahmad-al-mohdar",
                source_domain="linkedin.com",
                source_type="scrapingbee_google",
                page_title="Ahmad Al Mohdar | Chief Executive Officer",
                page_snippet="Chief Executive Officer in Dubai",
                full_name="Ahmad Al Mohdar",
                current_title="Chief Executive Officer",
                current_company="",
                current_location="Dubai",
                role_family="executive",
                title_match=True,
                company_match=False,
                location_match=True,
                current_role_signal=True,
                confidence=0.92,
                title_confidence=0.94,
                company_confidence=0.18,
                location_confidence=0.84,
                currentness_confidence=0.88,
                freshness_confidence=0.7,
            )
        ],
    )
    verified = verify_candidate(candidate, brief)
    assert verified.verification_status == "review"
    assert "missing_current_company_confirmation" in verified.diagnostics
