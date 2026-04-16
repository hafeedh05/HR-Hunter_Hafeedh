from dataclasses import replace

from hr_hunter_transformer.models import CandidateEntity, EvidenceRecord, SearchBrief
from hr_hunter_transformer.ranking import VerificationAwareRanker


def _supply_chain_brief() -> SearchBrief:
    return SearchBrief(
        role_title="Supply Chain Manager",
        titles=["Supply Chain Manager", "Supply Planning Manager"],
        countries=["United Arab Emirates"],
        cities=["Dubai"],
        required_keywords=["S&OP", "Demand Planning", "Inventory Optimization"],
        preferred_keywords=["Warehouse Operations", "SAP"],
        industry_keywords=["logistics", "retail"],
    )


def test_dense_role_score_stays_strong_without_explicit_industry_evidence() -> None:
    brief = _supply_chain_brief()
    candidate = CandidateEntity(
        full_name="Layla Karim",
        canonical_key="layla-karim",
        current_title="Supply Chain Manager",
        current_company="Landmark Group",
        current_location="Dubai",
        role_family="supply_chain",
        title_match=True,
        company_match=True,
        location_match=True,
        current_role_proof_count=2,
        current_company_confirmed=True,
        current_title_confirmed=True,
        current_location_confirmed=True,
        title_match_score=0.96,
        company_match_score=0.88,
        location_match_score=0.86,
        currentness_score=0.84,
        source_trust_score=0.82,
        semantic_similarity=0.76,
        score=0.0,
        evidence=[
            EvidenceRecord(
                source_url="https://linkedin.com/in/layla-karim",
                source_domain="linkedin.com",
                source_type="scrapingbee_google",
                page_title="Layla Karim | Supply Chain Manager",
                page_snippet="Supply Chain Manager at Landmark Group in Dubai",
                full_name="Layla Karim",
                current_title="Supply Chain Manager",
                current_company="Landmark Group",
                current_location="Dubai",
                role_family="supply_chain",
                title_match=True,
                company_match=True,
                location_match=True,
                current_role_signal=True,
                confidence=0.9,
                title_confidence=0.95,
                company_confidence=0.88,
                location_confidence=0.86,
                currentness_confidence=0.84,
                freshness_confidence=0.72,
                supporting_keywords=["S&OP", "Demand Planning"],
            )
        ],
    )

    ranked = VerificationAwareRanker().score(candidate, brief)

    assert ranked.score >= 70.0
    assert ranked.verification_confidence >= 0.7
    assert "title_match" in ranked.notes
    assert "current_role_proof" in ranked.notes


def test_company_and_industry_evidence_breaks_ties_without_collapsing_base_fit() -> None:
    brief = _supply_chain_brief()
    base = CandidateEntity(
        full_name="Noura Saad",
        canonical_key="noura-saad",
        current_title="Supply Chain Manager",
        current_company="Regional Distributor",
        current_location="Dubai",
        role_family="supply_chain",
        title_match=True,
        company_match=False,
        location_match=True,
        current_role_proof_count=2,
        current_company_confirmed=False,
        current_title_confirmed=True,
        current_location_confirmed=True,
        title_match_score=0.94,
        company_match_score=0.32,
        location_match_score=0.82,
        currentness_score=0.82,
        source_trust_score=0.8,
        semantic_similarity=0.74,
        skill_match_score=0.52,
        score=0.0,
        evidence=[
            EvidenceRecord(
                source_url="https://linkedin.com/in/noura-saad",
                source_domain="linkedin.com",
                source_type="scrapingbee_google",
                page_title="Noura Saad | Supply Chain Manager",
                page_snippet="Supply Chain Manager in Dubai handling S&OP and planning",
                full_name="Noura Saad",
                current_title="Supply Chain Manager",
                current_company="Regional Distributor",
                current_location="Dubai",
                role_family="supply_chain",
                title_match=True,
                company_match=False,
                location_match=True,
                current_role_signal=True,
                confidence=0.84,
                title_confidence=0.9,
                company_confidence=0.32,
                location_confidence=0.82,
                currentness_confidence=0.8,
                freshness_confidence=0.66,
                supporting_keywords=["S&OP", "Demand Planning"],
            )
        ],
    )
    boosted = replace(
        base,
        canonical_key="noura-saad-boosted",
        company_support_count=2,
        company_consensus_score=0.74,
        industry_match_score=0.58,
        evidence=[
            EvidenceRecord(
                source_url="https://example.com/noura-saad",
                source_domain="example.com",
                source_type="scrapingbee_google",
                page_title="Noura Saad profile",
                page_snippet="Supply Chain Manager with retail logistics and S&OP leadership in Dubai",
                full_name="Noura Saad",
                current_title="Supply Chain Manager",
                current_company="Regional Distributor",
                current_location="Dubai",
                role_family="supply_chain",
                title_match=True,
                company_match=False,
                location_match=True,
                current_role_signal=True,
                confidence=0.86,
                title_confidence=0.9,
                company_confidence=0.35,
                location_confidence=0.82,
                currentness_confidence=0.8,
                freshness_confidence=0.66,
                supporting_keywords=["S&OP", "Demand Planning", "logistics", "retail"],
            )
        ],
    )

    ranker = VerificationAwareRanker()
    base_ranked = ranker.score(base, brief)
    boosted_ranked = ranker.score(boosted, brief)

    assert base_ranked.score >= 66.0
    assert boosted_ranked.score > base_ranked.score
    assert "company_consensus" in boosted_ranked.notes
    assert "industry_signal" in boosted_ranked.notes


def test_executive_ranking_penalizes_weak_adjacent_titles() -> None:
    brief = SearchBrief(
        role_title="Chief Executive Officer (CEO)",
        titles=["Chief Executive Officer", "CEO", "Managing Director", "General Manager", "Country Manager"],
        countries=["United Arab Emirates"],
        cities=["Dubai"],
        required_keywords=["P&L", "Growth", "Strategy"],
        industry_keywords=["retail", "consumer goods"],
    )
    adjacent = CandidateEntity(
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
        company_consensus_score=0.68,
        location_match_score=0.84,
        skill_match_score=0.18,
        industry_match_score=0.12,
        currentness_score=0.88,
        source_trust_score=0.88,
        semantic_similarity=0.78,
        score=0.0,
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
                confidence=0.88,
                title_confidence=0.88,
                company_confidence=0.84,
                location_confidence=0.84,
                currentness_confidence=0.86,
                freshness_confidence=0.72,
            )
        ],
    )
    canonical = replace(
        adjacent,
        canonical_key="ahmad-al-mohdar",
        full_name="Ahmad Al Mohdar",
        current_title="Chief Executive Officer",
        current_company="FNON Trading Company",
        title_match_score=0.95,
        company_match_score=0.78,
        company_consensus_score=0.8,
        skill_match_score=0.52,
        industry_match_score=0.38,
        semantic_similarity=0.79,
        evidence=[
            EvidenceRecord(
                source_url="https://linkedin.com/in/ahmad-al-mohdar",
                source_domain="linkedin.com",
                source_type="scrapingbee_google",
                page_title="Ahmad Al Mohdar | Chief Executive Officer",
                page_snippet="Chief Executive Officer driving retail growth and strategy in Dubai",
                full_name="Ahmad Al Mohdar",
                current_title="Chief Executive Officer",
                current_company="FNON Trading Company",
                current_location="Dubai",
                role_family="executive",
                title_match=True,
                company_match=True,
                location_match=True,
                current_role_signal=True,
                confidence=0.9,
                title_confidence=0.94,
                company_confidence=0.88,
                location_confidence=0.84,
                currentness_confidence=0.88,
                freshness_confidence=0.72,
                supporting_keywords=["P&L", "Growth", "retail"],
            )
        ],
    )

    ranker = VerificationAwareRanker()
    adjacent_ranked = ranker.score(adjacent, brief)
    canonical_ranked = ranker.score(canonical, brief)

    assert "adjacent_title_risk" in adjacent_ranked.notes
    assert canonical_ranked.score > adjacent_ranked.score
    assert "primary_title_match" in canonical_ranked.notes


def test_dense_role_ranking_penalizes_malformed_company_identity() -> None:
    brief = _supply_chain_brief()
    malformed = CandidateEntity(
        full_name="Farah Malik",
        canonical_key="farah-malik-bad",
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
        skill_match_score=0.58,
        industry_match_score=0.54,
        currentness_score=0.86,
        source_trust_score=0.84,
        semantic_similarity=0.76,
        score=0.0,
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
                freshness_confidence=0.66,
                supporting_keywords=["S&OP", "Demand Planning", "logistics"],
            )
        ],
    )
    clean = replace(
        malformed,
        canonical_key="farah-malik-clean",
        current_company="Unilever",
        evidence=[
            replace(
                malformed.evidence[0],
                current_company="Unilever",
                page_snippet="Supply Chain Manager at Unilever in Dubai leading S&OP and logistics",
            )
        ],
    )

    ranker = VerificationAwareRanker()
    malformed_ranked = ranker.score(malformed, brief)
    clean_ranked = ranker.score(clean, brief)

    assert clean_ranked.score > malformed_ranked.score
    assert "weak_company_identity" in malformed_ranked.notes
    assert "strong_company_identity" in clean_ranked.notes


def test_design_architecture_ranking_keeps_exact_requested_architect_title_without_generic_risk() -> None:
    brief = SearchBrief(
        role_title="Project Architect",
        titles=["Project Architect", "Senior Architect", "Design Manager", "Architect"],
        countries=["United Arab Emirates"],
        cities=["Dubai"],
        required_keywords=["Architecture", "Revit", "AutoCAD"],
        industry_keywords=["architecture", "design", "construction"],
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
        current_role_proof_count=3,
        current_company_confirmed=True,
        current_title_confirmed=True,
        current_location_confirmed=True,
        title_match_score=0.91,
        company_match_score=0.84,
        company_consensus_score=0.92,
        location_match_score=1.0,
        skill_match_score=0.18,
        industry_match_score=0.25,
        currentness_score=0.86,
        source_trust_score=0.82,
        semantic_similarity=0.5,
        score=0.0,
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

    ranked = VerificationAwareRanker().score(candidate, brief)

    assert ranked.score >= 72.0
    assert "generic_title_risk" not in ranked.notes
