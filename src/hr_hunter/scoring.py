from __future__ import annotations

import re
from datetime import datetime
from typing import Dict, Iterable, List, Optional

from hr_hunter.briefing import normalize_text
from hr_hunter.geo import distance_from_center
from hr_hunter.models import CandidateProfile, SearchBrief


def parse_year(value: object) -> Optional[int]:
    if value is None:
        return None
    text = str(value)
    match = re.search(r"(19|20)\d{2}", text)
    if not match:
        return None
    return int(match.group(0))


def derive_years_experience(candidate: CandidateProfile) -> Optional[float]:
    if candidate.years_experience is not None:
        return candidate.years_experience

    starts: List[int] = []
    latest_end = datetime.utcnow().year

    for item in candidate.experience:
        start_year = parse_year(item.get("start_date") or item.get("start"))
        end_year = parse_year(item.get("end_date") or item.get("end"))
        if start_year:
            starts.append(start_year)
        if end_year:
            latest_end = max(latest_end, end_year)

    if not starts:
        return None

    years = max(0, latest_end - min(starts))
    candidate.years_experience = float(years)
    return candidate.years_experience


def extract_experience_companies(candidate: CandidateProfile) -> List[str]:
    companies = []
    for item in candidate.experience:
        company = item.get("company")
        if isinstance(company, dict):
            company = company.get("name")
        elif not company:
            company = item.get("company_name")
        if company:
            companies.append(str(company))
    return companies


def best_title_match(title: str, targets: Iterable[str], keywords: Iterable[str]) -> Dict[str, object]:
    normalized_title = normalize_text(title)
    result = {"score": 0.0, "matches": []}

    for target in targets:
        normalized_target = normalize_text(target)
        if normalized_title == normalized_target:
            result["score"] = max(result["score"], 35.0)
            result["matches"] = [target]
        elif normalized_target and normalized_target in normalized_title:
            result["score"] = max(result["score"], 28.0)
            result["matches"] = [target]

    for keyword in keywords:
        normalized_keyword = normalize_text(keyword)
        if normalized_keyword and normalized_keyword in normalized_title:
            result["score"] = max(result["score"], 18.0)
            if keyword not in result["matches"]:
                result["matches"].append(keyword)

    return result


def best_company_match(
    current_company: str,
    experience_companies: Iterable[str],
    aliases: Dict[str, List[str]],
) -> Dict[str, object]:
    normalized_current = normalize_text(current_company)
    normalized_experience = [normalize_text(company) for company in experience_companies]
    result = {"score": 0.0, "matches": []}

    for company, alias_values in aliases.items():
        normalized_aliases = [normalize_text(alias) for alias in alias_values]
        if normalized_current and normalized_current in normalized_aliases:
            result["score"] = max(result["score"], 35.0)
            result["matches"] = [company]
            continue

        if any(experience_company in normalized_aliases for experience_company in normalized_experience):
            result["score"] = max(result["score"], 18.0)
            if company not in result["matches"]:
                result["matches"].append(company)

    return result


def keyword_hits(text_parts: Iterable[str], keywords: Iterable[str]) -> int:
    haystack = normalize_text(" ".join(part for part in text_parts if part))
    hits = 0
    for keyword in keywords:
        normalized_keyword = normalize_text(keyword)
        if normalized_keyword and normalized_keyword in haystack:
            hits += 1
    return hits


def score_candidate(candidate: CandidateProfile, brief: SearchBrief) -> CandidateProfile:
    notes: List[str] = []
    title_match = best_title_match(candidate.current_title, brief.titles, brief.title_keywords)
    company_match = best_company_match(
        candidate.current_company,
        extract_experience_companies(candidate),
        brief.company_aliases,
    )

    candidate.matched_titles = list(title_match["matches"])
    candidate.matched_companies = list(company_match["matches"])

    score = 0.0
    score += float(title_match["score"])
    score += float(company_match["score"])

    if candidate.linkedin_url:
        score += 5.0
        notes.append("public profile url present")

    if candidate.current_title and candidate.current_company:
        score += 5.0
        notes.append("current title and company present")

    candidate.distance_miles = distance_from_center(brief.geography, candidate.location_geo)
    if candidate.distance_miles is not None:
        if candidate.distance_miles <= brief.geography.radius_miles:
            score += 20.0
            notes.append("within target radius")
        elif candidate.distance_miles <= brief.geography.radius_miles * 1.5:
            score += 8.0
            notes.append("near target radius")
        else:
            score -= 12.0
            notes.append("outside target radius")
    else:
        location_hits = keyword_hits(
            [candidate.location_name, candidate.summary],
            [brief.geography.country] + brief.geography.location_hints,
        )
        if location_hits:
            score += min(10.0, float(location_hits * 4))
            notes.append("location hints matched")

    required_hits = keyword_hits(
        [candidate.summary, candidate.industry or "", candidate.current_title],
        brief.required_keywords,
    )
    preferred_hits = keyword_hits(
        [candidate.summary, candidate.industry or "", candidate.current_title],
        brief.preferred_keywords,
    )
    if required_hits:
        score += min(12.0, float(required_hits * 3))
        notes.append("required keyword hit")
    if preferred_hits:
        score += min(8.0, float(preferred_hits * 2))
        notes.append("preferred keyword hit")

    years_experience = derive_years_experience(candidate)
    if years_experience is not None and brief.minimum_years_experience is not None:
        if years_experience >= brief.minimum_years_experience:
            score += 10.0
            notes.append("meets experience floor")
        elif years_experience >= brief.minimum_years_experience - 2:
            score += 4.0
            notes.append("near experience floor")
        else:
            score -= 6.0
            notes.append("below experience floor")

    candidate.score = round(min(max(score, 0.0), 100.0), 2)

    company_exact = company_match["score"] >= 35.0
    title_strong = title_match["score"] >= 28.0
    location_good = candidate.distance_miles is None or (
        brief.geography.radius_miles <= 0
        or candidate.distance_miles <= brief.geography.radius_miles * 1.5
    )

    if company_exact and title_strong and location_good and candidate.score >= 70:
        candidate.verification_status = "verified"
    elif candidate.score >= 45:
        candidate.verification_status = "review"
    else:
        candidate.verification_status = "reject"

    candidate.verification_notes = notes
    return candidate


def sort_candidates(candidates: List[CandidateProfile]) -> List[CandidateProfile]:
    status_rank = {"verified": 0, "review": 1, "reject": 2}
    return sorted(
        candidates,
        key=lambda candidate: (
            status_rank.get(candidate.verification_status, 9),
            -candidate.score,
            candidate.full_name.lower(),
        ),
    )
