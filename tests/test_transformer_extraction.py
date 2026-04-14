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
