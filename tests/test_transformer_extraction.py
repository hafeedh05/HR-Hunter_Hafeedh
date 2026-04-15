from hr_hunter_transformer.extraction import ProfileExtractor
from hr_hunter_transformer.models import RawSearchHit, SearchBrief


def test_extractor_rejects_hiring_post_as_person() -> None:
    extractor = ProfileExtractor()
    brief = SearchBrief(role_title="Supply Chain Manager", titles=["Supply Chain Manager"])
    hit = RawSearchHit(
        title="We're hiring | Senior Supply Chain Manager",
        snippet="Join our team in Riyadh",
        url="https://www.linkedin.com/in/we-are-hiring",
        source="scrapingbee_google",
    )
    assert extractor.extract(hit, brief) is None


def test_extractor_strips_bad_company_like_parenthesized_ceo() -> None:
    extractor = ProfileExtractor()
    brief = SearchBrief(role_title="Chief Executive Officer (CEO)", titles=["Chief Executive Officer (CEO)", "CEO"])
    hit = RawSearchHit(
        title="Ahmad Al Mohdar | Chief Executive Officer | (CEO)",
        snippet="Chief Executive Officer in Dubai",
        url="https://www.linkedin.com/in/ahmad-al-mohdar",
        source="scrapingbee_google",
    )
    record = extractor.extract(hit, brief)
    assert record is not None
    assert record.current_company == ""


def test_extractor_normalizes_company_prefixed_with_at_symbol() -> None:
    extractor = ProfileExtractor()
    brief = SearchBrief(role_title="AI Engineer", titles=["AI Engineer"])
    hit = RawSearchHit(
        title="Amaan Patel | AI Engineer | @Aldar",
        snippet="AI Engineer at @Aldar in Abu Dhabi",
        url="https://www.linkedin.com/in/amaan-patel",
        source="scrapingbee_google",
    )
    record = extractor.extract(hit, brief)
    assert record is not None
    assert record.current_company == "Aldar"
