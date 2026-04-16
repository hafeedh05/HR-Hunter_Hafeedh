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


def test_extractor_infers_country_from_regional_linkedin_host() -> None:
    extractor = ProfileExtractor()
    brief = SearchBrief(
        role_title="Supply Chain Manager",
        titles=["Supply Chain Manager"],
        countries=["United Arab Emirates"],
        cities=["Dubai", "Abu Dhabi"],
    )
    hit = RawSearchHit(
        title="Maya Khan | Supply Chain Manager | Amazon",
        snippet="Supply Chain Manager at Amazon",
        url="https://ae.linkedin.com/in/maya-khan",
        source="scrapingbee_google",
    )

    record = extractor.extract(hit, brief)

    assert record is not None
    assert record.current_location == "United Arab Emirates"
    assert record.location_match is True
    assert record.location_confidence == 0.44


def test_extractor_keeps_company_like_bayt_employers() -> None:
    extractor = ProfileExtractor()
    brief = SearchBrief(
        role_title="Supply Chain Manager",
        titles=["Supply Chain Manager"],
        countries=["United Arab Emirates"],
        cities=["Dubai"],
    )
    hits = [
        RawSearchHit(
            title="Sarvesh Poddar | Bayt.com",
            snippet="Supply Chain Manager at Landmark Retail LLC,. United Arab Emirates - Dubai; My current job since November 2023.",
            url="https://people.bayt.com/sarvesh-poddar-85860919/",
            source="scrapingbee_google",
        ),
        RawSearchHit(
            title="Jay Chandran | Bayt.com",
            snippet="SUPPLY CHAIN MANAGER at AL FUTTAIM. United Arab Emirates - Dubai; My current job since April 2021.",
            url="https://people.bayt.com/jay-chandran-89472658/",
            source="scrapingbee_google",
        ),
        RawSearchHit(
            title="Mohamed Khalil | Bayt.com",
            snippet="Supply Chain Manager ME. at ASSA ABLOY Opening Solutions ME., Dubai, UAE",
            url="https://people.bayt.com/mohamed-khalil-1898595/",
            source="scrapingbee_google",
        ),
    ]

    companies = [extractor.extract(hit, brief).current_company for hit in hits]

    assert companies == [
        "Landmark Retail LLC",
        "AL FUTTAIM",
        "ASSA ABLOY Opening Solutions ME",
    ]


def test_extractor_matches_peer_company_inside_parenthetical_parent_brand() -> None:
    extractor = ProfileExtractor()
    brief = SearchBrief(
        role_title="Chief Executive Officer",
        titles=["Chief Executive Officer", "CEO"],
        peer_company_targets=["Home Centre", "Landmark Group"],
        countries=["United Arab Emirates"],
    )
    hit = RawSearchHit(
        title="Arja Taaveniku | Chief Executive Officer | Home Centre (Landmark Group)",
        snippet="CEO at Home Centre (Landmark Group), United Arab Emirates",
        url="https://ae.linkedin.com/in/arja-taaveniku",
        source="scrapingbee_google",
    )

    record = extractor.extract(hit, brief)

    assert record is not None
    assert record.current_company == "Home Centre"
    assert record.peer_company_match is True


def test_extractor_keeps_peer_company_from_snippet_when_not_in_exact_targets() -> None:
    extractor = ProfileExtractor()
    brief = SearchBrief(
        role_title="Chief Executive Officer",
        titles=["Chief Executive Officer", "CEO"],
        company_targets=["Pottery Barn"],
        peer_company_targets=["Chalhoub Group"],
        countries=["United Arab Emirates"],
    )
    hit = RawSearchHit(
        title="Nadia Karim | Chief Executive Officer",
        snippet="Chief Executive Officer at Chalhoub Group in Dubai",
        url="https://ae.linkedin.com/in/nadia-karim",
        source="scrapingbee_google",
    )

    record = extractor.extract(hit, brief)

    assert record is not None
    assert record.current_company == "Chalhoub Group"
    assert record.peer_company_match is True
