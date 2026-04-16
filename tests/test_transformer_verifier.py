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


def test_transformer_verifier_allows_dense_role_with_consensus_and_industry_support() -> None:
    brief = SearchBrief(
        role_title="Supply Chain Manager",
        titles=["Supply Chain Manager", "Supply Planning Manager"],
        countries=["United Arab Emirates"],
        cities=["Dubai"],
        required_keywords=["S&OP", "Demand Planning", "Inventory Optimization"],
        industry_keywords=["logistics", "retail"],
    )
    candidate = CandidateEntity(
        full_name="Maha Saeed",
        canonical_key="maha-saeed",
        current_title="Supply Chain Manager",
        current_company="Example Distribution",
        current_location="Dubai",
        role_family="supply_chain",
        title_match=True,
        company_match=False,
        location_match=True,
        current_role_proof_count=2,
        current_company_confirmed=False,
        current_title_confirmed=True,
        current_location_confirmed=True,
        company_support_count=2,
        company_consensus_score=0.74,
        title_match_score=0.94,
        skill_match_score=0.68,
        industry_match_score=0.58,
        company_match_score=0.46,
        location_match_score=0.84,
        currentness_score=0.86,
        source_trust_score=0.88,
        score=68.5,
        evidence=[
            EvidenceRecord(
                source_url="https://www.linkedin.com/in/maha-saeed",
                source_domain="linkedin.com",
                source_type="scrapingbee_google",
                page_title="Maha Saeed | Supply Chain Manager",
                page_snippet="Supply Chain Manager handling S&OP and logistics in Dubai",
                full_name="Maha Saeed",
                current_title="Supply Chain Manager",
                current_company="Example Distribution",
                current_location="Dubai",
                role_family="supply_chain",
                title_match=True,
                company_match=False,
                location_match=True,
                current_role_signal=True,
                confidence=0.88,
                title_confidence=0.92,
                company_confidence=0.46,
                location_confidence=0.84,
                currentness_confidence=0.84,
                freshness_confidence=0.62,
                supporting_keywords=["S&OP", "logistics", "Demand Planning"],
            ),
            EvidenceRecord(
                source_url="https://example.com/maha-saeed",
                source_domain="example.com",
                source_type="scrapingbee_google",
                page_title="Maha Saeed profile",
                page_snippet="Inventory Optimization and retail distribution leader in Dubai",
                full_name="Maha Saeed",
                current_title="Supply Chain Manager",
                current_company="Example Distribution",
                current_location="Dubai",
                role_family="supply_chain",
                title_match=True,
                company_match=False,
                location_match=True,
                current_role_signal=True,
                confidence=0.82,
                title_confidence=0.9,
                company_confidence=0.42,
                location_confidence=0.72,
                currentness_confidence=0.8,
                freshness_confidence=0.62,
                supporting_keywords=["Inventory Optimization", "retail"],
            ),
        ],
    )

    verified = verify_candidate(candidate, brief)

    assert verified.verification_status == "verified"


def test_transformer_verifier_demotes_adjacent_architecture_title_to_review() -> None:
    brief = SearchBrief(
        role_title="Project Architect",
        titles=["Project Architect", "Senior Architect", "Design Manager", "Architect"],
        countries=["United Arab Emirates"],
        cities=["Dubai"],
    )
    candidate = CandidateEntity(
        full_name="Maya Noor",
        canonical_key="maya-noor",
        current_title="Interior Designer",
        current_company="Design Studio",
        current_location="Dubai",
        role_family="design_architecture",
        title_match=True,
        company_match=True,
        location_match=True,
        current_role_proof_count=2,
        current_company_confirmed=True,
        current_title_confirmed=True,
        current_location_confirmed=True,
        title_match_score=0.9,
        company_match_score=0.84,
        location_match_score=0.82,
        currentness_score=0.84,
        source_trust_score=0.8,
        score=76.0,
        evidence=[
            EvidenceRecord(
                source_url="https://linkedin.com/in/maya-noor",
                source_domain="linkedin.com",
                source_type="scrapingbee_google",
                page_title="Maya Noor | Interior Designer",
                page_snippet="Interior Designer in Dubai",
                full_name="Maya Noor",
                current_title="Interior Designer",
                current_company="Design Studio",
                current_location="Dubai",
                role_family="design_architecture",
                title_match=True,
                company_match=True,
                location_match=True,
                current_role_signal=True,
                confidence=0.88,
                title_confidence=0.9,
                company_confidence=0.84,
                location_confidence=0.82,
                currentness_confidence=0.8,
                freshness_confidence=0.66,
            )
        ],
    )

    verified = verify_candidate(candidate, brief)

    assert verified.verification_status == "review"
    assert "adjacent_title_leakage" in verified.diagnostics


def test_transformer_verifier_keeps_executive_with_malformed_company_in_review() -> None:
    brief = SearchBrief(
        role_title="Chief Executive Officer (CEO)",
        titles=["Chief Executive Officer", "CEO", "Managing Director"],
        countries=["United Arab Emirates"],
        cities=["Dubai"],
        industry_keywords=["retail", "consumer goods"],
    )
    candidate = CandidateEntity(
        full_name="Omar Malik",
        canonical_key="omar-malik",
        current_title="Chief Executive Officer",
        current_company="Board",
        current_location="Dubai",
        role_family="executive",
        title_match=True,
        company_match=False,
        location_match=True,
        current_role_proof_count=2,
        current_company_confirmed=False,
        current_title_confirmed=True,
        current_location_confirmed=True,
        title_match_score=0.96,
        company_match_score=0.35,
        company_consensus_score=0.38,
        location_match_score=0.84,
        currentness_score=0.88,
        source_trust_score=0.84,
        score=77.0,
        evidence=[
            EvidenceRecord(
                source_url="https://linkedin.com/in/omar-malik",
                source_domain="linkedin.com",
                source_type="scrapingbee_google",
                page_title="Omar Malik | CEO",
                page_snippet="CEO in Dubai with board and transformation exposure",
                full_name="Omar Malik",
                current_title="Chief Executive Officer",
                current_company="Board",
                current_location="Dubai",
                role_family="executive",
                title_match=True,
                company_match=False,
                location_match=True,
                current_role_signal=True,
                confidence=0.9,
                title_confidence=0.94,
                company_confidence=0.34,
                location_confidence=0.84,
                currentness_confidence=0.86,
                freshness_confidence=0.7,
            )
        ],
    )

    verified = verify_candidate(candidate, brief)

    assert verified.verification_status == "review"
    assert "weak_company_or_industry_signals" in verified.diagnostics


def test_transformer_verifier_keeps_weak_adjacent_executive_in_review() -> None:
    brief = SearchBrief(
        role_title="Chief Executive Officer (CEO)",
        titles=["Chief Executive Officer", "CEO", "Managing Director", "General Manager", "Country Manager"],
        countries=["United Arab Emirates"],
        cities=["Dubai"],
        required_keywords=["P&L", "Growth", "Strategy"],
        industry_keywords=["retail", "consumer goods"],
    )
    candidate = CandidateEntity(
        full_name="Rami Haddad",
        canonical_key="rami-haddad",
        current_title="General Manager",
        current_company="Regional Foods Company",
        current_location="Dubai",
        role_family="executive",
        title_match=True,
        company_match=False,
        location_match=True,
        current_role_proof_count=2,
        current_company_confirmed=True,
        current_title_confirmed=True,
        current_location_confirmed=True,
        title_match_score=0.72,
        company_match_score=0.46,
        company_consensus_score=0.7,
        location_match_score=0.82,
        skill_match_score=0.16,
        industry_match_score=0.12,
        currentness_score=0.88,
        source_trust_score=0.9,
        score=78.0,
        evidence=[
            EvidenceRecord(
                source_url="https://linkedin.com/in/rami-haddad",
                source_domain="linkedin.com",
                source_type="scrapingbee_google",
                page_title="Rami Haddad | General Manager",
                page_snippet="General Manager in Dubai with commercial leadership experience",
                full_name="Rami Haddad",
                current_title="General Manager",
                current_company="Regional Foods Company",
                current_location="Dubai",
                role_family="executive",
                title_match=True,
                company_match=False,
                location_match=True,
                current_role_signal=True,
                confidence=0.9,
                title_confidence=0.9,
                company_confidence=0.84,
                location_confidence=0.82,
                currentness_confidence=0.86,
                freshness_confidence=0.72,
            )
        ],
    )

    verified = verify_candidate(candidate, brief)

    assert verified.verification_status == "review"
    assert "weak_company_or_industry_signals" in verified.diagnostics


def test_transformer_verifier_allows_strong_adjacent_executive_with_brief_relevance() -> None:
    brief = SearchBrief(
        role_title="Chief Executive Officer (CEO)",
        titles=["Chief Executive Officer", "CEO", "Managing Director", "General Manager", "Country Manager"],
        countries=["United Arab Emirates"],
        cities=["Dubai"],
        company_targets=["Marina Home Interiors"],
        peer_company_targets=["Home Centre", "Landmark Group"],
        required_keywords=["P&L", "Growth", "Strategy"],
        industry_keywords=["retail", "consumer goods", "home furnishings"],
    )
    candidate = CandidateEntity(
        full_name="Arja Taaveniku",
        canonical_key="arja-taaveniku",
        current_title="Managing Director",
        current_company="Home Centre (Landmark Group)",
        current_location="Dubai",
        role_family="executive",
        title_match=True,
        company_match=False,
        peer_company_match=True,
        location_match=True,
        current_role_proof_count=2,
        current_company_confirmed=True,
        current_title_confirmed=True,
        current_location_confirmed=True,
        title_match_score=0.82,
        company_match_score=0.86,
        company_consensus_score=0.82,
        location_match_score=0.84,
        skill_match_score=0.62,
        industry_match_score=0.46,
        currentness_score=0.9,
        source_trust_score=0.92,
        score=79.5,
        evidence=[
            EvidenceRecord(
                source_url="https://linkedin.com/in/arja-taaveniku",
                source_domain="linkedin.com",
                source_type="scrapingbee_google",
                page_title="Arja Taaveniku | Managing Director",
                page_snippet="Managing Director leading Home Centre retail growth in Dubai",
                full_name="Arja Taaveniku",
                current_title="Managing Director",
                current_company="Home Centre (Landmark Group)",
                current_location="Dubai",
                role_family="executive",
                title_match=True,
                company_match=False,
                peer_company_match=True,
                location_match=True,
                current_role_signal=True,
                confidence=0.92,
                title_confidence=0.9,
                company_confidence=0.88,
                location_confidence=0.84,
                currentness_confidence=0.88,
                freshness_confidence=0.72,
                supporting_keywords=["P&L", "Growth", "retail"],
            )
        ],
    )

    verified = verify_candidate(candidate, brief)

    assert verified.verification_status == "verified"


def test_transformer_verifier_fast_tracks_exact_company_executive_with_strong_current_role() -> None:
    brief = SearchBrief(
        role_title="Chief Executive Officer (CEO)",
        titles=["Chief Executive Officer", "CEO", "Managing Director", "General Manager"],
        countries=["United Arab Emirates"],
        cities=["Dubai"],
        company_targets=["Pan Emirates"],
        peer_company_targets=["Home Centre"],
        required_keywords=["P&L", "Growth", "Strategy"],
        industry_keywords=["retail", "home furnishings"],
    )
    candidate = CandidateEntity(
        full_name="Sachin Tiwari",
        canonical_key="sachin-tiwari",
        current_title="General Manager",
        current_company="Pan Emirates",
        current_location="Dubai",
        role_family="executive",
        title_match=True,
        company_match=True,
        location_match=True,
        current_role_proof_count=1,
        current_company_confirmed=True,
        current_title_confirmed=True,
        current_location_confirmed=True,
        title_match_score=0.62,
        company_match_score=0.92,
        company_consensus_score=0.78,
        location_match_score=0.7,
        skill_match_score=0.4,
        industry_match_score=0.32,
        currentness_score=0.82,
        source_trust_score=0.88,
        verification_confidence=0.0,
        score=56.5,
        evidence=[
            EvidenceRecord(
                source_url="https://linkedin.com/in/sachin-tiwari",
                source_domain="linkedin.com",
                source_type="scrapingbee_google",
                page_title="Sachin Tiwari | General Manager at Pan Emirates",
                page_snippet="General Manager at Pan Emirates in Dubai",
                full_name="Sachin Tiwari",
                current_title="General Manager",
                current_company="Pan Emirates",
                current_location="Dubai",
                role_family="executive",
                title_match=True,
                company_match=True,
                location_match=True,
                current_role_signal=True,
                confidence=0.88,
                title_confidence=0.76,
                company_confidence=0.92,
                location_confidence=0.7,
                currentness_confidence=0.82,
                freshness_confidence=0.65,
                supporting_keywords=["P&L", "Growth", "retail"],
            )
        ],
    )

    verified = verify_candidate(candidate, brief)

    assert verified.verification_status == "verified"


def test_transformer_verifier_keeps_requested_architect_title_with_strong_company_evidence_verified() -> None:
    brief = SearchBrief(
        role_title="Project Architect",
        titles=["Project Architect", "Senior Architect", "Design Manager", "Architect"],
        countries=["United Arab Emirates"],
        cities=["Dubai"],
        required_keywords=["Architecture", "Revit", "Design Management", "AutoCAD", "Fit-out"],
        industry_keywords=["architecture", "design", "real estate", "construction"],
    )
    candidate = CandidateEntity(
        full_name="Sana Iqbal",
        canonical_key="sana-iqbal",
        current_title="Architect",
        current_company="Azizi Developments",
        current_location="Dubai",
        role_family="design_architecture",
        title_match=True,
        company_match=False,
        location_match=True,
        current_role_proof_count=2,
        current_company_confirmed=True,
        current_title_confirmed=True,
        current_location_confirmed=True,
        title_match_score=0.91,
        company_match_score=0.84,
        company_consensus_score=0.92,
        location_match_score=1.0,
        skill_match_score=0.12,
        industry_match_score=0.18,
        currentness_score=0.86,
        source_trust_score=0.82,
        semantic_fit=0.48,
        score=76.0,
        evidence=[
            EvidenceRecord(
                source_url="https://theorg.com/org/azizi-developments/org-chart/sana-iqbal",
                source_domain="theorg.com",
                source_type="scrapingbee_google",
                page_title="Sana Iqbal - Architect at Azizi Developments",
                page_snippet="Architect in Dubai with design coordination experience",
                full_name="Sana Iqbal",
                current_title="Architect",
                current_company="Azizi Developments",
                current_location="Dubai",
                role_family="design_architecture",
                title_match=True,
                company_match=False,
                location_match=True,
                current_role_signal=True,
                confidence=0.84,
                title_confidence=0.9,
                company_confidence=0.84,
                location_confidence=1.0,
                currentness_confidence=0.84,
                freshness_confidence=0.64,
                supporting_keywords=["Architecture"],
            )
        ],
    )

    verified = verify_candidate(candidate, brief)

    assert verified.verification_status == "verified"
    assert "generic_title_match" not in verified.diagnostics


def test_transformer_verifier_keeps_architect_with_bad_company_identity_in_review() -> None:
    brief = SearchBrief(
        role_title="Project Architect",
        titles=["Project Architect", "Senior Architect", "Design Manager", "Architect"],
        countries=["United Arab Emirates"],
        cities=["Dubai"],
    )
    candidate = CandidateEntity(
        full_name="Shahal Mannara",
        canonical_key="shahal-mannara",
        current_title="Architect",
        current_company="Intern",
        current_location="Dubai",
        role_family="design_architecture",
        title_match=True,
        company_match=False,
        location_match=True,
        current_role_proof_count=2,
        current_company_confirmed=False,
        current_title_confirmed=True,
        current_location_confirmed=True,
        title_match_score=0.91,
        company_match_score=0.76,
        company_consensus_score=0.92,
        location_match_score=1.0,
        skill_match_score=0.18,
        industry_match_score=0.25,
        currentness_score=0.86,
        source_trust_score=0.82,
        semantic_fit=0.5,
        score=74.0,
        evidence=[
            EvidenceRecord(
                source_url="https://behance.net/shahal-mannara",
                source_domain="behance.net",
                source_type="scrapingbee_google",
                page_title="Shahal Mannara - Architect",
                page_snippet="Architect in Dubai with fit-out and mixed-use project experience",
                full_name="Shahal Mannara",
                current_title="Architect",
                current_company="Intern",
                current_location="Dubai",
                role_family="design_architecture",
                title_match=True,
                company_match=False,
                location_match=True,
                current_role_signal=True,
                confidence=0.84,
                title_confidence=0.9,
                company_confidence=0.76,
                location_confidence=1.0,
                currentness_confidence=0.84,
                freshness_confidence=0.64,
                supporting_keywords=["Architecture", "Fit-out"],
            )
        ],
    )

    verified = verify_candidate(candidate, brief)

    assert verified.verification_status == "review"
    assert "weak_company_identity" in verified.diagnostics


def test_transformer_verifier_keeps_dense_role_with_bad_company_identity_in_review() -> None:
    brief = SearchBrief(
        role_title="Supply Chain Manager",
        titles=["Supply Chain Manager", "Supply Planning Manager"],
        countries=["United Arab Emirates"],
        cities=["Dubai"],
        required_keywords=["S&OP", "Demand Planning", "Inventory Optimization"],
        industry_keywords=["logistics", "retail"],
    )
    candidate = CandidateEntity(
        full_name="Farah Malik",
        canonical_key="farah-malik",
        current_title="Supply Chain Manager",
        current_company="Complete Logistics and supply chain process",
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
        company_match_score=0.52,
        company_consensus_score=0.72,
        location_match_score=0.84,
        skill_match_score=0.66,
        industry_match_score=0.58,
        currentness_score=0.86,
        source_trust_score=0.84,
        score=71.0,
        evidence=[
            EvidenceRecord(
                source_url="https://linkedin.com/in/farah-malik",
                source_domain="linkedin.com",
                source_type="scrapingbee_google",
                page_title="Farah Malik | Supply Chain Manager",
                page_snippet="Supply Chain Manager in Dubai leading S&OP and logistics",
                full_name="Farah Malik",
                current_title="Supply Chain Manager",
                current_company="Complete Logistics and supply chain process",
                current_location="Dubai",
                role_family="supply_chain",
                title_match=True,
                company_match=False,
                location_match=True,
                current_role_signal=True,
                confidence=0.86,
                title_confidence=0.94,
                company_confidence=0.52,
                location_confidence=0.84,
                currentness_confidence=0.84,
                freshness_confidence=0.64,
                supporting_keywords=["S&OP", "Demand Planning", "logistics"],
            )
        ],
    )

    verified = verify_candidate(candidate, brief)

    assert verified.verification_status == "review"
    assert "weak_company_identity" in verified.diagnostics
