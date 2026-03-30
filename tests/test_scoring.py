from hr_hunter.briefing import build_search_brief
from hr_hunter.models import CandidateProfile
from hr_hunter.scoring import score_candidate


def test_score_candidate_marks_high_fit_as_verified() -> None:
    brief = build_search_brief(
        {
            "id": "score-test",
            "role_title": "Global Product Manager",
            "titles": ["Global Product Manager", "Senior Product Manager"],
            "title_keywords": ["product manager", "product"],
            "company_targets": ["Procter & Gamble"],
            "company_aliases": {"Procter & Gamble": ["P&G"]},
            "geography": {
                "location_name": "Drogheda",
                "country": "Ireland",
                "center_latitude": 53.7179,
                "center_longitude": -6.3561,
                "radius_miles": 60,
            },
            "required_keywords": ["product strategy", "commercial"],
            "preferred_keywords": ["CPG"],
            "minimum_years_experience": 10,
        }
    )

    candidate = CandidateProfile(
        full_name="Jane Search",
        current_title="Global Product Manager",
        current_company="Procter & Gamble",
        location_name="Drogheda, Ireland",
        location_geo="53.7179,-6.3561",
        linkedin_url="https://www.linkedin.com/in/jane-search",
        summary="Product strategy and commercial leadership in CPG.",
        experience=[
            {
                "company": {"name": "Procter & Gamble"},
                "start_date": "2010-01-01",
            }
        ],
    )

    scored = score_candidate(candidate, brief)

    assert scored.verification_status == "verified"
    assert scored.score >= 70
    assert "Procter & Gamble" in scored.matched_companies
