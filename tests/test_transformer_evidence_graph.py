from hr_hunter_transformer.evidence_graph import EvidenceGraphBuilder
from hr_hunter_transformer.models import EvidenceRecord, SearchBrief


def test_evidence_graph_splits_same_name_on_conflicting_company_anchors() -> None:
    builder = EvidenceGraphBuilder()
    records = [
        EvidenceRecord(
            source_url="https://linkedin.com/in/sara-khan-alpha",
            source_domain="linkedin.com",
            source_type="scrapingbee_google",
            page_title="Sara Khan | Project Architect",
            page_snippet="Project Architect at Alpha Studio in Dubai",
            full_name="Sara Khan",
            current_title="Project Architect",
            current_company="Alpha Studio",
            current_location="Dubai",
            role_family="design_architecture",
            title_match=True,
            company_match=False,
            location_match=True,
            current_role_signal=True,
            confidence=0.91,
            title_confidence=0.9,
            company_confidence=0.84,
            location_confidence=0.8,
            currentness_confidence=0.84,
            freshness_confidence=0.62,
        ),
        EvidenceRecord(
            source_url="https://archinect.com/sara-khan-alpha",
            source_domain="archinect.com",
            source_type="scrapingbee_google",
            page_title="Sara Khan - Alpha Studio",
            page_snippet="Architect at Alpha Studio",
            full_name="Sara Khan",
            current_title="Project Architect",
            current_company="Alpha Studio",
            current_location="Dubai",
            role_family="design_architecture",
            title_match=True,
            company_match=False,
            location_match=True,
            current_role_signal=True,
            confidence=0.83,
            title_confidence=0.82,
            company_confidence=0.8,
            location_confidence=0.72,
            currentness_confidence=0.8,
            freshness_confidence=0.62,
        ),
        EvidenceRecord(
            source_url="https://linkedin.com/in/sara-khan-beta",
            source_domain="linkedin.com",
            source_type="scrapingbee_google",
            page_title="Sara Khan | Project Architect",
            page_snippet="Project Architect at Beta Design in Abu Dhabi",
            full_name="Sara Khan",
            current_title="Project Architect",
            current_company="Beta Design",
            current_location="Abu Dhabi",
            role_family="design_architecture",
            title_match=True,
            company_match=False,
            location_match=True,
            current_role_signal=True,
            confidence=0.9,
            title_confidence=0.9,
            company_confidence=0.86,
            location_confidence=0.78,
            currentness_confidence=0.84,
            freshness_confidence=0.62,
        ),
    ]

    entities = builder.merge(records)

    assert len(entities) == 2
    assert {entity.current_company for entity in entities} == {"Alpha Studio", "Beta Design"}


def test_evidence_graph_promotes_chosen_company_into_target_match_flags() -> None:
    builder = EvidenceGraphBuilder()
    brief = SearchBrief(
        role_title="Chief Executive Officer",
        company_targets=["Home Centre"],
        peer_company_targets=["Chalhoub Group"],
    )
    records = [
        EvidenceRecord(
            source_url="https://linkedin.com/in/ceo-home-centre",
            source_domain="linkedin.com",
            source_type="scrapingbee_google",
            page_title="A. Leader | CEO | Home Centre",
            page_snippet="Chief Executive Officer at Home Centre in Dubai",
            full_name="A Leader",
            current_title="Chief Executive Officer",
            current_company="Home Centre",
            current_location="Dubai",
            role_family="executive",
            title_match=True,
            company_match=False,
            location_match=True,
            current_role_signal=True,
            confidence=0.91,
            title_confidence=0.9,
            company_confidence=0.55,
            location_confidence=0.8,
            currentness_confidence=0.82,
            freshness_confidence=0.6,
        ),
    ]

    entities = builder.merge(records, brief)

    assert len(entities) == 1
    assert entities[0].current_company == "Home Centre"
    assert entities[0].company_match is True
    assert entities[0].company_match_score >= 0.9
