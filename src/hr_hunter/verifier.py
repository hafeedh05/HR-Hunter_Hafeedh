from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List
from urllib.parse import urlparse

import httpx

from hr_hunter.briefing import normalize_text, unique_preserving_order
from hr_hunter.models import CandidateProfile, EvidenceRecord, SearchBrief, SearchRunReport
from hr_hunter.providers.scrapingbee import ScrapingBeeGoogleClient
from hr_hunter.scoring import sort_candidates


YEAR_PATTERN = re.compile(r"\b(19|20)\d{2}\b")


def parse_year(value: str) -> int | None:
    match = YEAR_PATTERN.search(value)
    if not match:
        return None
    return int(match.group(0))


def title_tokens(candidate: CandidateProfile, brief: SearchBrief) -> List[str]:
    values = [
        candidate.current_title.split(" - ")[0].strip(),
        *brief.titles,
        *brief.title_keywords,
    ]
    return unique_preserving_order([value for value in values if value])


def company_aliases(candidate: CandidateProfile, brief: SearchBrief) -> List[str]:
    if candidate.current_company in brief.company_aliases:
        return brief.company_aliases[candidate.current_company]

    aliases = [candidate.current_company] if candidate.current_company else []
    for company, values in brief.company_aliases.items():
        if company == candidate.current_company:
            aliases.extend(values)
    return unique_preserving_order([alias for alias in aliases if alias])


class PublicEvidenceVerifier:
    def __init__(self, settings: Dict[str, Any] | None = None):
        settings = settings or {}
        self.client = ScrapingBeeGoogleClient()
        self.country_code = settings.get("country_code", "ie")
        self.language = settings.get("language", "en")
        self.light_request = bool(settings.get("light_request", True))
        self.pages_per_query = int(settings.get("pages_per_query", 1))
        self.queries_per_candidate = int(settings.get("queries_per_candidate", 2))
        self.results_per_query = int(settings.get("results_per_query", 10))

    def is_configured(self) -> bool:
        return self.client.is_configured()

    def build_queries(self, candidate: CandidateProfile, brief: SearchBrief) -> List[str]:
        name_term = f'"{candidate.full_name}"' if candidate.full_name else ""
        company_terms = " OR ".join(f'"{alias}"' for alias in company_aliases(candidate, brief)[:3])
        title_terms = " OR ".join(f'"{token}"' for token in title_tokens(candidate, brief)[:5])
        location_terms = " OR ".join(
            f'"{hint}"'
            for hint in unique_preserving_order(
                [candidate.location_name, brief.geography.location_name, *brief.geography.location_hints[:3]]
            )
            if hint
        )

        queries = [
            " ".join(
                part
                for part in [
                    name_term,
                    f"({company_terms})" if company_terms else "",
                    f"({title_terms})" if title_terms else "",
                    f"({location_terms})" if location_terms else "",
                    "-site:linkedin.com -site:ie.linkedin.com",
                ]
                if part
            ),
            " ".join(
                part
                for part in [
                    name_term,
                    f"({company_terms})" if company_terms else "",
                    f'("{brief.geography.country}")' if brief.geography.country else "",
                    "-site:linkedin.com -site:ie.linkedin.com",
                ]
                if part
            ),
            " ".join(
                part
                for part in [
                    name_term,
                    f"({title_terms})" if title_terms else "",
                    f"({company_terms})" if company_terms else "",
                    "-site:linkedin.com -site:ie.linkedin.com",
                ]
                if part
            ),
        ]
        return unique_preserving_order([query for query in queries if query])[: self.queries_per_candidate]

    def build_record(
        self,
        candidate: CandidateProfile,
        brief: SearchBrief,
        query: str,
        result: Dict[str, Any],
    ) -> EvidenceRecord:
        title = result.get("title", "") or ""
        snippet = result.get("description") or result.get("snippet") or ""
        source_url = result.get("url") or result.get("link") or ""
        source_domain = (urlparse(source_url).netloc or result.get("domain") or "").lower()
        combined = " ".join([title, snippet, source_url])
        normalized = normalize_text(combined)
        normalized_name = normalize_text(candidate.full_name)

        matched_company = ""
        for alias in company_aliases(candidate, brief):
            normalized_alias = normalize_text(alias)
            if normalized_alias and normalized_alias in normalized:
                matched_company = candidate.current_company or alias
                break

        matched_titles: List[str] = []
        for token in title_tokens(candidate, brief):
            normalized_token = normalize_text(token)
            if normalized_token and normalized_token in normalized:
                matched_titles.append(token)

        location_match = False
        for hint in unique_preserving_order(
            [candidate.location_name, brief.geography.location_name, brief.geography.country, *brief.geography.location_hints]
        ):
            normalized_hint = normalize_text(hint)
            if normalized_hint and normalized_hint in normalized:
                location_match = True
                break

        recency_year = parse_year(combined)
        confidence = 0.0
        if normalized_name and normalized_name in normalized:
            confidence += 0.35
        if matched_company:
            confidence += 0.25
        if matched_titles:
            confidence += min(0.2, 0.1 * len(matched_titles))
        if location_match:
            confidence += 0.1
        if source_domain and "linkedin.com" not in source_domain:
            confidence += 0.1

        return EvidenceRecord(
            query=query,
            source_url=source_url,
            source_domain=source_domain,
            title=title,
            snippet=snippet,
            name_match=bool(normalized_name and normalized_name in normalized),
            company_match=matched_company,
            title_matches=unique_preserving_order(matched_titles),
            location_match=location_match,
            recency_year=recency_year,
            confidence=round(min(confidence, 1.0), 2),
            raw=result,
        )

    async def collect_evidence(
        self,
        candidate: CandidateProfile,
        brief: SearchBrief,
    ) -> tuple[List[EvidenceRecord], int]:
        if not self.is_configured():
            return [], 0

        evidence: Dict[str, EvidenceRecord] = {}
        request_count = 0
        queries = self.build_queries(candidate, brief)
        async with httpx.AsyncClient(timeout=30.0) as client:
            for query in queries:
                for page in range(1, self.pages_per_query + 1):
                    response = await self.client.search(
                        client,
                        query,
                        page=page,
                        country_code=self.country_code,
                        language=self.language,
                        light_request=self.light_request,
                    )
                    request_count += 1
                    if response.status_code >= 400:
                        continue
                    payload = response.json()
                    for result in payload.get("organic_results", [])[: self.results_per_query]:
                        record = self.build_record(candidate, brief, query, result)
                        if not (record.name_match or record.company_match or record.title_matches):
                            continue
                        key = record.source_url or f"{record.source_domain}:{record.title}"
                        existing = evidence.get(key)
                        if existing is None or record.confidence > existing.confidence:
                            evidence[key] = record
        ordered = sorted(
            evidence.values(),
            key=lambda record: (
                -record.confidence,
                record.source_domain,
                record.source_url,
            ),
        )
        return ordered, request_count

    def apply_evidence(
        self,
        candidate: CandidateProfile,
        evidence_records: List[EvidenceRecord],
    ) -> CandidateProfile:
        candidate.evidence_records = evidence_records
        candidate.last_verified_at = datetime.now(timezone.utc).isoformat()
        if not evidence_records:
            candidate.evidence_confidence = 0.0
            candidate.evidence_verdict = "missing"
            candidate.verification_notes.append("no public corroboration found")
            return candidate

        strong_records = [record for record in evidence_records if record.confidence >= 0.65]
        non_linkedin_domains = {
            record.source_domain for record in evidence_records if record.source_domain and "linkedin.com" not in record.source_domain
        }
        corroborated_records = [
            record
            for record in evidence_records
            if record.name_match and record.company_match and (record.title_matches or record.location_match)
        ]
        latest_year = max((record.recency_year or 0) for record in evidence_records)
        stale_data_risk = bool(latest_year and latest_year < datetime.now(timezone.utc).year - 2)

        top_confidences = [record.confidence for record in evidence_records[:3]]
        evidence_confidence = sum(top_confidences) / max(len(top_confidences), 1)
        if corroborated_records:
            evidence_confidence += 0.1
        if non_linkedin_domains:
            evidence_confidence += 0.1
        if stale_data_risk:
            evidence_confidence -= 0.1
        evidence_confidence = round(min(max(evidence_confidence, 0.0), 1.0), 2)

        candidate.evidence_confidence = evidence_confidence
        candidate.stale_data_risk = stale_data_risk

        if len(corroborated_records) >= 2:
            candidate.evidence_verdict = "corroborated"
            candidate.verification_notes.append(
                f"{len(corroborated_records)} public sources corroborate company/title"
            )
        elif strong_records:
            candidate.evidence_verdict = "supported"
            candidate.verification_notes.append(
                f"{len(strong_records)} public sources materially support the match"
            )
        else:
            candidate.evidence_verdict = "weak"
            candidate.verification_notes.append("public evidence exists but remains weak")

        if non_linkedin_domains:
            candidate.verification_notes.append(
                f"{len(non_linkedin_domains)} non-LinkedIn domains corroborate the profile"
            )

        if stale_data_risk:
            candidate.verification_notes.append("public evidence looks stale")
            candidate.score = round(max(0.0, candidate.score - 6.0), 2)

        if candidate.evidence_verdict == "corroborated":
            candidate.score = round(min(100.0, candidate.score + 8.0), 2)
            if candidate.verification_status in {"review", "reject"} and candidate.score >= 55.0:
                candidate.verification_status = "verified"
        elif candidate.evidence_verdict == "supported":
            candidate.score = round(min(100.0, candidate.score + 4.0), 2)
            if candidate.verification_status == "reject" and candidate.score >= 40.0:
                candidate.verification_status = "review"

        return candidate

    async def verify_candidates(
        self,
        candidates: List[CandidateProfile],
        brief: SearchBrief,
        limit: int,
    ) -> Dict[str, Any]:
        request_count = 0
        verified_count = 0
        reviewed_count = 0

        for candidate in candidates[:limit]:
            evidence_records, used_requests = await self.collect_evidence(candidate, brief)
            request_count += used_requests
            before_status = candidate.verification_status
            self.apply_evidence(candidate, evidence_records)
            if candidate.verification_status == "verified" and before_status != "verified":
                verified_count += 1
            if candidate.verification_status == "review" and before_status == "reject":
                reviewed_count += 1

        return {
            "candidates_checked": min(limit, len(candidates)),
            "requests_used": request_count,
            "promoted_to_verified": verified_count,
            "promoted_to_review": reviewed_count,
        }


def refresh_report_summary(report: SearchRunReport, verification_stats: Dict[str, Any] | None = None) -> None:
    verified = len([candidate for candidate in report.candidates if candidate.verification_status == "verified"])
    review = len([candidate for candidate in report.candidates if candidate.verification_status == "review"])
    rejected = len([candidate for candidate in report.candidates if candidate.verification_status == "reject"])
    corroborated = len(
        [candidate for candidate in report.candidates if candidate.evidence_verdict == "corroborated"]
    )
    supported = len(
        [candidate for candidate in report.candidates if candidate.evidence_verdict == "supported"]
    )

    report.candidates = sort_candidates(report.candidates)
    report.summary.update(
        {
            "candidate_count": len(report.candidates),
            "verified_count": verified,
            "review_count": review,
            "reject_count": rejected,
            "evidence_corroborated_count": corroborated,
            "evidence_supported_count": supported,
        }
    )
    if verification_stats:
        report.summary["verification_stats"] = verification_stats
