from __future__ import annotations

import csv
import json
import re
from dataclasses import asdict
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Set, Tuple

from hr_hunter.candidate_order import (
    candidate_is_in_scope,
    candidate_is_precise_market_match,
    candidate_priority_sort_tuple,
)
from hr_hunter.features import looks_like_non_company_fragment
from hr_hunter.identity import candidate_identity_keys, candidate_primary_key
from hr_hunter.models import (
    CandidateProfile,
    EvidenceRecord,
    ProviderRunResult,
    SearchRunReport,
)


TITLE_FAMILY_KEYWORDS = (
    ("product_marketing", ("product marketing",)),
    ("product_portfolio", ("portfolio",)),
    ("category", ("category",)),
    ("brand", ("brand",)),
    ("product", ("product",)),
)
CSV_FIELDNAMES = [
    "identity_key",
    "full_name",
    "verification_status",
    "qualification_tier",
    "in_scope",
    "scope_bucket",
    "score",
    "current_title",
    "current_company",
    "location_name",
    "company_match_context",
    "employment_signal",
    "title_similarity_score",
    "company_match_score",
    "location_match_score",
    "skill_overlap_score",
    "industry_fit_score",
    "years_fit_score",
    "semantic_similarity_score",
    "matched_titles",
    "matched_companies",
    "key_signals",
    "linkedin_url",
    "source_url",
    "source",
]


def _clean_reason_values(values: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    ordered: List[str] = []
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        if text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


def _serialize_multi_value(values: Iterable[str]) -> str:
    return "; ".join(_clean_reason_values(values))


def _repair_display_text(value: object) -> str:
    text = str(value or "")
    if not text.strip():
        return ""
    cleaned = text.replace("\u200f", "").replace("\u200e", "").replace("\ufeff", "")
    replacements = {
        "â€¢": "•",
        "â€“": "–",
        "â€”": "—",
        "â€™": "’",
        "â€œ": '"',
        "â€\x9d": '"',
        "Â": "",
    }
    for source, target in replacements.items():
        cleaned = cleaned.replace(source, target)
    if any(token in cleaned for token in ("Ã", "Â", "â€", "â€¢")):
        try:
            repaired = cleaned.encode("latin1").decode("utf-8")
            if repaired:
                cleaned = repaired
        except Exception:
            pass
    cleaned = cleaned.replace("•", " | ")
    cleaned = re.sub(r"\s*\|\s*", " | ", cleaned)
    cleaned = re.sub(r"(\s*\|\s*)+$", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" |")
    return cleaned


def _company_match_context(candidate: CandidateProfile) -> str:
    if candidate.current_target_company_match and candidate.target_company_history_match:
        return "Current and past target company"
    if candidate.current_target_company_match:
        return "Current target company"
    if candidate.target_company_history_match:
        return "Past target company"
    return "No target company match"


def _employment_signal(candidate: CandidateProfile) -> str:
    notes_text = " ".join(_clean_reason_values(candidate.verification_notes)).lower()
    if "open_to_work_signal" in notes_text:
        return "Open to work signal"
    if candidate.current_employment_confirmed:
        return "Current role confirmed"
    if candidate.current_company.strip():
        return "Current company listed"
    return "No current role signal"


def _humanize_candidate_note(note: str) -> str:
    text = str(note or "").strip()
    if not text:
        return ""
    lower = text.lower()
    if lower.startswith(("reranker:", "learned_ranker:", "ranker_bonus:", "ranker_penalty:", "parser_confidence:", "evidence_quality:")):
        return ""
    label_map = {
        "current_function_fit": "Function fit",
        "location_match": "Location",
        "skill_overlap": "Skills",
        "industry_fit": "Industry",
        "years_fit": "Experience",
        "employment_status": "Employment",
        "company_match": "Company",
        "title_similarity": "Title",
    }
    if ":" not in text:
        return ""
    prefix, value = text.split(":", 1)
    label = label_map.get(prefix.strip())
    if not label:
        return ""
    detail = _repair_display_text(value.replace("_", " ").strip())
    return f"{label}: {detail}" if detail else ""


def _candidate_key_signals(candidate: CandidateProfile) -> str:
    signals: List[str] = []
    if candidate.current_title_match:
        signals.append("Current title aligned")
    if candidate.current_target_company_match:
        signals.append("Current target company match")
    elif candidate.target_company_history_match:
        signals.append("Past target company match")
    if candidate.location_aligned:
        signals.append("Target location aligned")
    for note in candidate.verification_notes:
        formatted = _humanize_candidate_note(note)
        if formatted:
            signals.append(formatted)
    return _serialize_multi_value(signals[:5])


def _title_family_text(candidate: CandidateProfile) -> str:
    parts = [candidate.current_title, *candidate.matched_titles]
    return " ".join(part.strip().lower() for part in parts if str(part).strip())


def _infer_matched_title_family(candidate: CandidateProfile) -> str:
    if candidate.matched_title_family:
        return candidate.matched_title_family
    haystack = _title_family_text(candidate)
    if not haystack:
        return "other"
    for family, keywords in TITLE_FAMILY_KEYWORDS:
        if any(keyword in haystack for keyword in keywords):
            return family
    return "other"


def _infer_location_precision_bucket(candidate: CandidateProfile) -> str:
    legacy_bucket_aliases = {
        "country_only_ireland": "country_only",
        "outside_ireland": "outside_target_area",
        "named_ireland_location": "named_target_location",
        "named_ireland_locality": "named_target_location",
    }
    if candidate.location_precision_bucket:
        return legacy_bucket_aliases.get(candidate.location_precision_bucket, candidate.location_precision_bucket)
    if candidate.distance_miles is not None:
        return "geo_distance"
    if candidate.current_location_confirmed:
        return "current_evidence"
    if candidate.location_aligned:
        return "text_aligned"
    if candidate.location_name:
        return "profile_text"
    return "unknown"


def _infer_current_role_proof_count(candidate: CandidateProfile) -> int:
    if candidate.current_role_proof_count:
        return candidate.current_role_proof_count
    return sum(1 for record in candidate.evidence_records if record.current_employment_signal)


def _infer_source_quality_score(candidate: CandidateProfile, proof_count: int) -> float:
    if candidate.source_quality_score > 0.0:
        return candidate.source_quality_score
    score = 0.0
    source = (candidate.source or "").strip().lower()
    source_url = (candidate.source_url or "").strip().lower()
    linkedin_url = (candidate.linkedin_url or "").strip().lower()

    if source:
        score += 0.2

    if "linkedin.com/in/" in linkedin_url or "linkedin.com/in/" in source_url:
        score += 0.25

    if any(record.profile_signal for record in candidate.evidence_records):
        score += 0.15
    if proof_count > 0:
        score += 0.2
    if candidate.current_company_confirmed or candidate.current_title_confirmed:
        score += 0.05

    return round(min(score, 1.0), 3)


def _infer_evidence_freshness_year(candidate: CandidateProfile) -> int | None:
    if candidate.evidence_freshness_year is not None:
        return candidate.evidence_freshness_year
    years = [
        int(record.recency_year)
        for record in candidate.evidence_records
        if isinstance(record.recency_year, int)
    ]
    return max(years) if years else None


def _infer_current_function_fit(candidate: CandidateProfile, matched_title_family: str) -> float:
    if candidate.current_function_fit > 0.0:
        return candidate.current_function_fit
    if candidate.current_title_match:
        return 1.0
    if matched_title_family != "other" and candidate.matched_titles:
        return 0.85
    if matched_title_family != "other":
        return 0.7
    if candidate.matched_titles:
        return 0.55
    return 0.0


def _infer_industry_fit_score(candidate: CandidateProfile) -> float:
    if candidate.industry_fit_score > 0.0:
        return candidate.industry_fit_score
    if candidate.current_fmcg_fit > 0.0:
        return candidate.current_fmcg_fit
    if candidate.current_target_company_match and candidate.industry_aligned:
        return 1.0
    if candidate.industry_aligned:
        return 0.75
    if candidate.target_company_history_match:
        return 0.5
    return 0.0


def _infer_current_fmcg_fit(candidate: CandidateProfile) -> float:
    return _infer_industry_fit_score(candidate)


def _infer_title_similarity_score(candidate: CandidateProfile, function_fit: float) -> float:
    if candidate.title_similarity_score > 0.0:
        return candidate.title_similarity_score
    if candidate.current_title_match:
        return 1.0
    if candidate.matched_titles:
        return max(0.7, function_fit)
    return function_fit


def _infer_company_match_score(candidate: CandidateProfile) -> float:
    if candidate.company_match_score > 0.0:
        return candidate.company_match_score
    if candidate.current_target_company_match:
        return 1.0
    if candidate.target_company_history_match:
        return 0.45
    return 0.0


def _infer_location_match_score(candidate: CandidateProfile) -> float:
    if candidate.location_match_score > 0.0:
        return candidate.location_match_score
    bucket = _infer_location_precision_bucket(candidate)
    if bucket == "within_radius":
        return 1.0
    if bucket in {
        "within_expanded_radius",
        "named_target_location",
        "named_profile_location",
        "geo_distance",
        "current_evidence",
    }:
        return 0.75
    if bucket in {"country_only", "text_aligned", "profile_text"}:
        return 0.35
    return 0.0


def _candidate_market_match(candidate: CandidateProfile) -> bool:
    bucket = _infer_location_precision_bucket(candidate)
    return bool(candidate.location_aligned) or bucket in {
        "within_radius",
        "within_expanded_radius",
        "priority_target_location",
        "named_target_location",
        "secondary_target_location",
        "named_profile_location",
        "country_only",
        "text_aligned",
        "profile_text",
    }


def _candidate_precise_market_match(candidate: CandidateProfile) -> bool:
    bucket = _infer_location_precision_bucket(candidate)
    return bucket in {
        "within_radius",
        "within_expanded_radius",
        "priority_target_location",
        "named_target_location",
        "secondary_target_location",
        "named_profile_location",
        "geo_distance",
        "current_evidence",
    }


def _infer_in_scope(candidate: CandidateProfile) -> bool:
    parser_confidence = _infer_parser_confidence(candidate)
    if "parser_confidence_too_low" in set(candidate.cap_reasons or []):
        return False
    return bool(candidate.current_title_match and _candidate_market_match(candidate) and parser_confidence >= 0.35)


def _infer_scope_bucket(candidate: CandidateProfile) -> str:
    if _infer_in_scope(candidate):
        return "in_scope_precise" if _candidate_precise_market_match(candidate) else "in_scope_country"
    if candidate.current_title_match:
        return "title_only"
    if _candidate_market_match(candidate):
        return "market_only"
    return "out_of_scope"


def _infer_skill_overlap_score(candidate: CandidateProfile, function_fit: float, industry_fit: float) -> float:
    if candidate.skill_overlap_score > 0.0:
        return candidate.skill_overlap_score
    if candidate.current_title_match and candidate.industry_aligned:
        return 0.85
    if candidate.current_title_match:
        return 0.7
    if function_fit >= 0.7:
        return 0.65
    return round(max(function_fit, industry_fit * 0.5), 3)


def _infer_years_fit_score(candidate: CandidateProfile) -> float:
    if candidate.years_fit_score > 0.0:
        return candidate.years_fit_score
    if candidate.years_experience is None:
        return 0.0
    return 0.5


def _infer_parser_confidence(candidate: CandidateProfile) -> float:
    if candidate.parser_confidence > 0.0:
        return candidate.parser_confidence
    score = 0.0
    if candidate.current_title:
        score += 0.3
    if candidate.current_company:
        score += 0.3
    if candidate.location_name or candidate.location_geo:
        score += 0.15
    if candidate.summary:
        score += 0.1
    if candidate.source_url or candidate.linkedin_url:
        score += 0.1
    if candidate.current_company and looks_like_non_company_fragment(candidate.current_company):
        score -= 0.35
    return round(min(max(score, 0.0), 1.0), 3)


def _infer_evidence_quality_score(candidate: CandidateProfile) -> float:
    if candidate.evidence_quality_score > 0.0:
        return candidate.evidence_quality_score
    score = 0.0
    if candidate.linkedin_url:
        score += 0.3
    if candidate.source_url:
        score += 0.2
    if candidate.current_company_confirmed or candidate.current_title_confirmed:
        score += 0.15
    if candidate.current_location_confirmed:
        score += 0.1
    if candidate.current_employment_confirmed:
        score += 0.15
    if candidate.evidence_records:
        score += min(0.1, 0.03 * len(candidate.evidence_records))
    return round(min(score, 1.0), 3)


def _infer_qualification_tier(candidate: CandidateProfile, function_fit: float, fmcg_fit: float) -> str:
    if candidate.verification_status == "verified":
        return "strict_verified"
    if candidate.verification_status == "review":
        return "search_qualified"
    if candidate.score >= 45.0 and candidate.location_aligned and function_fit >= 0.7 and fmcg_fit >= 0.6:
        return "search_qualified"
    return "weak"


def _infer_cap_reasons(
    candidate: CandidateProfile,
    qualification_tier: str,
    proof_count: int,
) -> List[str]:
    if candidate.cap_reasons:
        return _clean_reason_values(candidate.cap_reasons)
    reasons: List[str] = []
    if qualification_tier != "strict_verified":
        if not candidate.current_company_confirmed:
            reasons.append("missing_current_company_confirmation")
        if not candidate.current_title_confirmed:
            reasons.append("missing_current_title_confirmation")
        if not candidate.current_location_confirmed:
            reasons.append("missing_precise_location_confirmation")
        if proof_count == 0 or not candidate.current_employment_confirmed:
            reasons.append("missing_current_role_proof")
        if candidate.stale_data_risk:
            reasons.append("stale_evidence_risk")
        if candidate.target_company_history_match and not candidate.current_target_company_match:
            reasons.append("target_company_history_only")
    return _clean_reason_values(reasons)


def _infer_disqualifier_reasons(
    candidate: CandidateProfile,
    qualification_tier: str,
    function_fit: float,
    industry_fit: float,
) -> List[str]:
    if candidate.disqualifier_reasons:
        return _clean_reason_values(candidate.disqualifier_reasons)
    reasons: List[str] = []
    if qualification_tier == "weak":
        if not candidate.location_aligned:
            reasons.append("location_not_aligned")
        if function_fit < 0.6:
            reasons.append("low_function_fit")
        if industry_fit < 0.6:
            reasons.append("low_industry_fit")
        if not candidate.current_target_company_match and not candidate.target_company_history_match:
            reasons.append("no_target_company_signal")
        if not candidate.current_employment_confirmed:
            reasons.append("missing_current_role_proof")
        if candidate.stale_data_risk:
            reasons.append("stale_evidence_risk")
    return _clean_reason_values(reasons)


def hydrate_candidate_reporting(candidate: CandidateProfile) -> CandidateProfile:
    matched_title_family = _infer_matched_title_family(candidate)
    current_role_proof_count = _infer_current_role_proof_count(candidate)
    evidence_freshness_year = _infer_evidence_freshness_year(candidate)
    current_function_fit = _infer_current_function_fit(candidate, matched_title_family)
    industry_fit_score = _infer_industry_fit_score(candidate)
    current_fmcg_fit = _infer_current_fmcg_fit(candidate)
    title_similarity_score = _infer_title_similarity_score(candidate, current_function_fit)
    company_match_score = _infer_company_match_score(candidate)
    location_match_score = _infer_location_match_score(candidate)
    skill_overlap_score = _infer_skill_overlap_score(candidate, current_function_fit, industry_fit_score)
    years_fit_score = _infer_years_fit_score(candidate)
    parser_confidence = _infer_parser_confidence(candidate)
    evidence_quality_score = _infer_evidence_quality_score(candidate)
    qualification_tier = _infer_qualification_tier(candidate, current_function_fit, industry_fit_score)
    in_scope = _infer_in_scope(candidate)
    precise_market_in_scope = bool(in_scope and _candidate_precise_market_match(candidate))
    scope_bucket = _infer_scope_bucket(candidate)

    candidate.matched_title_family = matched_title_family
    candidate.location_precision_bucket = _infer_location_precision_bucket(candidate)
    candidate.current_role_proof_count = current_role_proof_count
    candidate.source_quality_score = _infer_source_quality_score(candidate, current_role_proof_count)
    candidate.evidence_freshness_year = evidence_freshness_year
    candidate.title_similarity_score = round(title_similarity_score, 3)
    candidate.company_match_score = round(company_match_score, 3)
    candidate.location_match_score = round(location_match_score, 3)
    candidate.skill_overlap_score = round(skill_overlap_score, 3)
    candidate.industry_fit_score = round(industry_fit_score, 3)
    candidate.years_fit_score = round(years_fit_score, 3)
    candidate.parser_confidence = round(parser_confidence, 3)
    candidate.evidence_quality_score = round(evidence_quality_score, 3)
    candidate.current_function_fit = round(current_function_fit, 3)
    candidate.current_fmcg_fit = round(current_fmcg_fit, 3)
    candidate.in_scope = in_scope
    candidate.precise_market_in_scope = precise_market_in_scope
    candidate.scope_bucket = scope_bucket
    if not candidate.feature_scores:
        candidate.feature_scores = {
            "title_similarity": candidate.title_similarity_score,
            "company_match": candidate.company_match_score,
            "location_match": candidate.location_match_score,
            "skill_overlap": candidate.skill_overlap_score,
            "industry_fit": candidate.industry_fit_score,
            "years_fit": candidate.years_fit_score,
            "current_function_fit": candidate.current_function_fit,
            "parser_confidence": candidate.parser_confidence,
            "evidence_quality": candidate.evidence_quality_score,
            "semantic_similarity": candidate.semantic_similarity_score,
        }
    candidate.qualification_tier = qualification_tier
    candidate.cap_reasons = _infer_cap_reasons(candidate, qualification_tier, current_role_proof_count)
    candidate.disqualifier_reasons = _infer_disqualifier_reasons(
        candidate,
        qualification_tier,
        current_function_fit,
        industry_fit_score,
    )
    return candidate


def hydrate_report_reporting(report: SearchRunReport) -> SearchRunReport:
    report.candidates = [hydrate_candidate_reporting(candidate) for candidate in report.candidates]
    for result in report.provider_results:
        result.candidates = [hydrate_candidate_reporting(candidate) for candidate in result.candidates]
    report.summary = build_reporting_summary(report.candidates, report.summary)
    return report


def build_reporting_summary(
    candidates: Iterable[CandidateProfile],
    base_summary: Dict[str, object] | None = None,
) -> Dict[str, object]:
    hydrated_candidates = [hydrate_candidate_reporting(candidate) for candidate in candidates]
    scope_counts = {
        "in_scope": len([candidate for candidate in hydrated_candidates if candidate.in_scope]),
        "precise_in_scope": len([candidate for candidate in hydrated_candidates if candidate.precise_market_in_scope]),
        "title_match": len([candidate for candidate in hydrated_candidates if candidate.current_title_match]),
        "market_match": len([candidate for candidate in hydrated_candidates if _candidate_market_match(candidate)]),
    }
    verification_counts = {
        "verified": len([candidate for candidate in hydrated_candidates if candidate.verification_status == "verified"]),
        "review": len([candidate for candidate in hydrated_candidates if candidate.verification_status == "review"]),
        "reject": len([candidate for candidate in hydrated_candidates if candidate.verification_status == "reject"]),
    }
    qualification_counts = {
        "strict_verified": len(
            [candidate for candidate in hydrated_candidates if candidate.qualification_tier == "strict_verified"]
        ),
        "search_qualified": len(
            [candidate for candidate in hydrated_candidates if candidate.qualification_tier == "search_qualified"]
        ),
        "weak": len([candidate for candidate in hydrated_candidates if candidate.qualification_tier == "weak"]),
    }
    summary = dict(base_summary or {})
    summary.update(
        {
            "candidate_count": len(hydrated_candidates),
            "in_scope_count": scope_counts["in_scope"],
            "precise_in_scope_count": scope_counts["precise_in_scope"],
            "title_match_count": scope_counts["title_match"],
            "market_match_count": scope_counts["market_match"],
            "verified_count": verification_counts["verified"],
            "review_count": verification_counts["review"],
            "reject_count": verification_counts["reject"],
            "strict_verified_count": qualification_counts["strict_verified"],
            "search_qualified_count": qualification_counts["search_qualified"],
            "weak_count": qualification_counts["weak"],
            "verification_status_counts": verification_counts,
            "qualification_tier_counts": qualification_counts,
            "scope_counts": scope_counts,
            "ranking_model_versions": sorted(
                {
                    candidate.ranking_model_version
                    for candidate in hydrated_candidates
                    if candidate.ranking_model_version
                }
            ),
        }
    )
    summary["quality_diagnostics"] = build_quality_diagnostics(hydrated_candidates, summary)
    return summary


def build_scope_progress_counts(
    candidates: Iterable[CandidateProfile],
) -> Dict[str, int]:
    hydrated_candidates = [hydrate_candidate_reporting(candidate) for candidate in candidates]
    return {
        "in_scope_count": len([candidate for candidate in hydrated_candidates if candidate.in_scope]),
        "precise_in_scope_count": len(
            [candidate for candidate in hydrated_candidates if candidate.precise_market_in_scope]
        ),
        "title_match_count": len([candidate for candidate in hydrated_candidates if candidate.current_title_match]),
        "market_match_count": len([candidate for candidate in hydrated_candidates if _candidate_market_match(candidate)]),
    }


def prioritize_verification_candidates(
    candidates: Iterable[CandidateProfile],
    *,
    company_required: bool,
) -> List[CandidateProfile]:
    hydrated_candidates = [hydrate_candidate_reporting(candidate) for candidate in candidates]

    return sorted(
        hydrated_candidates,
        key=lambda candidate: candidate_priority_sort_tuple(
            candidate,
            None,
            phase="verification",
            company_required=company_required,
        ),
    )


def prepare_verification_candidate_order(
    candidates: Iterable[CandidateProfile],
    *,
    company_required: bool,
    verification_limit: int,
    scope_target: int = 0,
) -> List[CandidateProfile]:
    prioritized = prioritize_verification_candidates(
        candidates,
        company_required=company_required,
    )
    shortlist_limit = max(0, min(int(verification_limit or 0), len(prioritized)))
    scope_goal = max(0, min(int(scope_target or 0), shortlist_limit))
    if shortlist_limit <= 0 or scope_goal <= 0:
        return prioritized

    selected_keys: Set[str] = set()
    shortlisted: List[CandidateProfile] = []
    remainder: List[CandidateProfile] = []

    def _identity_key(candidate: CandidateProfile) -> str:
        return candidate_primary_key(candidate) or f"memory:{id(candidate)}"

    for candidate in prioritized:
        identity_key = _identity_key(candidate)
        if (
            candidate_is_in_scope(candidate)
            and candidate_is_precise_market_match(candidate)
            and len(shortlisted) < scope_goal
            and identity_key not in selected_keys
        ):
            shortlisted.append(candidate)
            selected_keys.add(identity_key)
            continue
        remainder.append(candidate)

    if len(shortlisted) < scope_goal:
        secondary_remainder: List[CandidateProfile] = []
        for candidate in remainder:
            identity_key = _identity_key(candidate)
            if (
                candidate_is_in_scope(candidate)
                and len(shortlisted) < scope_goal
                and identity_key not in selected_keys
            ):
                shortlisted.append(candidate)
                selected_keys.add(identity_key)
                continue
            secondary_remainder.append(candidate)
        remainder = secondary_remainder

    if len(shortlisted) < shortlist_limit:
        for candidate in remainder:
            identity_key = _identity_key(candidate)
            if identity_key in selected_keys:
                continue
            shortlisted.append(candidate)
            selected_keys.add(identity_key)
            if len(shortlisted) >= shortlist_limit:
                break

    trailing = [
        candidate
        for candidate in prioritized
        if _identity_key(candidate) not in selected_keys
    ]
    return [*shortlisted, *trailing]


def _diagnostic_issue(
    *,
    key: str,
    label: str,
    count: int,
    total: int,
    severity: str,
    message: str,
    action: str,
) -> Dict[str, object]:
    share = (float(count) / float(total)) if total else 0.0
    return {
        "key": key,
        "label": label,
        "count": int(max(0, count)),
        "share": round(max(0.0, share), 3),
        "severity": severity,
        "message": message,
        "action": action,
    }


def build_quality_diagnostics(
    candidates: Iterable[CandidateProfile],
    summary: Dict[str, object] | None = None,
) -> Dict[str, object]:
    hydrated_candidates = list(candidates)
    total = len(hydrated_candidates)
    base_summary = dict(summary or {})
    pipeline_metrics = dict(base_summary.get("pipeline_metrics", {}) or {})
    target_range = list(base_summary.get("target_range", []) or [])
    target = int(target_range[-1] or total or 0) if target_range else total
    if total <= 0:
        return {
            "enabled": False,
            "yield_status": "unknown",
            "headline": "No candidate data yet.",
            "verified_rate": 0.0,
            "issues": [],
        }

    verified_count = len([candidate for candidate in hydrated_candidates if candidate.verification_status == "verified"])
    review_count = len([candidate for candidate in hydrated_candidates if candidate.verification_status == "review"])
    reject_count = len([candidate for candidate in hydrated_candidates if candidate.verification_status == "reject"])
    verified_rate = verified_count / max(1, total)
    review_rate = review_count / max(1, total)
    reject_rate = reject_count / max(1, total)

    title_gap_count = len(
        [
            candidate
            for candidate in hydrated_candidates
            if (not candidate.current_title_match)
            or "title_alignment_required" in set(candidate.cap_reasons or [])
        ]
    )
    geo_gap_count = len(
        [
            candidate
            for candidate in hydrated_candidates
            if (not candidate.location_aligned)
            or candidate.location_precision_bucket in {"outside_target_area", "unknown_location"}
            or {"outside_target_area", "precise_location_required"}.intersection(set(candidate.cap_reasons or []))
        ]
    )
    weak_anchor_count = len(
        [
            candidate
            for candidate in hydrated_candidates
            if candidate.skill_overlap_score < 0.25
            and candidate.current_function_fit < 0.55
            and candidate.years_fit_score < 0.45
        ]
    )
    company_industry_gap_count = len(
        [
            candidate
            for candidate in hydrated_candidates
            if not candidate.current_target_company_match
            and not candidate.target_company_history_match
            and candidate.industry_fit_score < 0.25
        ]
    )
    parser_gap_count = len(
        [
            candidate
            for candidate in hydrated_candidates
            if candidate.parser_confidence < 0.45
            or candidate.evidence_quality_score < 0.35
            or "parser_confidence_too_low" in set(candidate.cap_reasons or [])
        ]
    )
    strict_cap_count = len(
        [
            candidate
            for candidate in hydrated_candidates
            if {
                "outside_target_area",
                "precise_location_required",
                "current_function_review",
                "current_target_company_required",
                "target_company_history_required",
                "title_alignment_required",
            }.intersection(set(candidate.cap_reasons or []))
        ]
    )
    loose_match_count = len(
        [
            candidate
            for candidate in hydrated_candidates
            if candidate.verification_status == "reject"
            and (
                (not candidate.current_title_match)
                or candidate.skill_overlap_score < 0.15
                or candidate.current_function_fit < 0.35
            )
        ]
    )
    unique_after_dedupe = int(pipeline_metrics.get("unique_after_dedupe", total) or total)
    raw_found = int(pipeline_metrics.get("raw_found", unique_after_dedupe) or unique_after_dedupe)
    scarcity_gap = max(0, target - unique_after_dedupe)
    company_match_mode = str(base_summary.get("company_match_mode", "both") or "both").strip().lower()
    exact_company_scope = company_match_mode == "current_only"
    company_scope_gap_count = len(
        [
            candidate
            for candidate in hydrated_candidates
            if not candidate.current_target_company_match and candidate.location_aligned
        ]
    )
    executive_role_text = " ".join(
        str(value or "").strip().lower()
        for value in [
            base_summary.get("role_title", ""),
            *list(base_summary.get("titles", []) or []),
        ]
    )
    executive_brief = any(
        hint in executive_role_text
        for hint in (
            "ceo",
            "chief executive officer",
            "chief",
            "president",
            "managing director",
            "general manager",
            "vice president",
            "vp",
        )
    )
    public_evidence_gap_count = len(
        [
            candidate
            for candidate in hydrated_candidates
            if candidate.current_role_proof_count <= 0
            or candidate.parser_confidence < 0.45
            or candidate.evidence_quality_score < 0.35
            or "missing_current_role_proof" in set(candidate.cap_reasons or [])
        ]
    )

    issues: List[Dict[str, object]] = []
    if title_gap_count / max(1, total) >= 0.28:
        issues.append(
            _diagnostic_issue(
                key="title_mismatch",
                label="Title mismatch",
                count=title_gap_count,
                total=total,
                severity="high" if title_gap_count / max(1, total) >= 0.45 else "medium",
                message=(
                    "A large share of candidates surfaced with adjacent titles rather than clean role-family matches "
                    "for the brief."
                ),
                action=(
                    "Tighten the accepted title family around the roles you will actually interview, and remove "
                    "looser adjacent titles if they are not genuinely in scope."
                ),
            )
        )
    if geo_gap_count / max(1, total) >= 0.24:
        issues.append(
            _diagnostic_issue(
                key="geo_mismatch",
                label="Geo mismatch",
                count=geo_gap_count,
                total=total,
                severity="high" if geo_gap_count / max(1, total) >= 0.4 else "medium",
                message=(
                    "Many candidates are outside the priority markets or only have weak public location evidence, "
                    "which drags verified yield down."
                ),
                action=(
                    "Keep the first countries and cities as the true priority markets. Remove low-priority geos if "
                    "the mandate must stay concentrated."
                ),
            )
        )
    if weak_anchor_count / max(1, total) >= 0.22:
        issues.append(
            _diagnostic_issue(
                key="weak_must_have_anchors",
                label="Weak must-have anchors",
                count=weak_anchor_count,
                total=total,
                severity="medium",
                message=(
                    "A meaningful share of candidates have broadly relevant profile signals but weak overlap on the "
                    "must-have skills or operating anchors."
                ),
                action=(
                    "Add 4 to 6 must-have phrases that are hard to fake, such as owned channels, tools, scope, "
                    "customer segment, or operating context."
                ),
            )
        )
    if company_industry_gap_count / max(1, total) >= 0.22:
        issues.append(
            _diagnostic_issue(
                key="weak_company_or_industry_signals",
                label="Weak company / industry signals",
                count=company_industry_gap_count,
                total=total,
                severity="medium",
                message=(
                    "Too few candidates show target-company history or strong industry signals, so the search is "
                    "leaning on adjacent profiles."
                ),
                action=(
                    "Add more truly comparable companies and industry markers instead of broad sector labels that pull "
                    "in weak adjacencies."
                ),
            )
        )
    if exact_company_scope and company_scope_gap_count / max(1, total) >= 0.35:
        issues.append(
            _diagnostic_issue(
                key="company_scope_too_strict",
                label="Company scope may be too strict",
                count=company_scope_gap_count,
                total=total,
                severity="medium",
                message=(
                    "Most shortlisted profiles miss the exact current-company requirement, which usually means peer "
                    "companies are being treated as hard gates instead of sourcing hints."
                ),
                action=(
                    "Use exact current companies only for true must-have employers. Move comparable brands into Peer "
                    "Companies to Source From so HR Hunter can source broadly without faking company matches."
                ),
            )
        )
    if parser_gap_count / max(1, total) >= 0.24:
        issues.append(
            _diagnostic_issue(
                key="parser_confidence",
                label="Parser confidence / public evidence",
                count=parser_gap_count,
                total=total,
                severity="medium",
                message=(
                    "Public profile data is thin or ambiguous for many candidates, which blocks confident verification "
                    "even when the profile looks directionally relevant."
                ),
                action=(
                    "Broaden into markets with richer public profile coverage, or relax exact-company expectations so "
                    "better-documented adjacent candidates can surface."
                ),
            )
        )
    if executive_brief and public_evidence_gap_count / max(1, total) >= 0.28:
        issues.append(
            _diagnostic_issue(
                key="public_executive_evidence_thin",
                label="Public executive evidence is thin",
                count=public_evidence_gap_count,
                total=total,
                severity="high" if verified_rate < 0.12 else "medium",
                message=(
                    "Many executive profiles look directionally relevant but do not have enough public current-role "
                    "evidence to verify cleanly."
                ),
                action=(
                    "Keep the must-current company list small, move comparable brands into Peer Companies to Source "
                    "From, and add public-proof anchors like P&L ownership, board exposure, retail footprint, or "
                    "turnaround scope."
                ),
            )
        )
    if strict_cap_count / max(1, total) >= 0.3 and review_rate >= 0.3:
        issues.append(
            _diagnostic_issue(
                key="filters_too_strict",
                label="Filters may be too strict",
                count=strict_cap_count,
                total=total,
                severity="medium",
                message=(
                    "A large share of candidates are getting capped into review because one hard constraint is missing, "
                    "even though the rest of the profile is directionally strong."
                ),
                action=(
                    "Decide which anchors are truly mandatory. If location or exact-company history is only a preference, "
                    "soften it so strong adjacent leaders can verify."
                ),
            )
        )
    elif loose_match_count / max(1, total) >= 0.35 and reject_rate >= 0.45:
        issues.append(
            _diagnostic_issue(
                key="filters_too_loose",
                label="Filters may be too loose",
                count=loose_match_count,
                total=total,
                severity="medium",
                message=(
                    "The search is pulling too many adjacent candidates with weak title or operating-fit evidence, "
                    "which floods the run with rejects."
                ),
                action=(
                    "Narrow the title family, strengthen must-have anchors, or trim weak discovery keywords so the run "
                    "stays high-signal."
                ),
            )
        )
    if target > 0 and ((scarcity_gap / max(1, target)) >= 0.18 or raw_found < max(target, int(target * 1.4))):
        issues.append(
            _diagnostic_issue(
                key="market_scarcity",
                label="Market scarcity",
                count=max(scarcity_gap, max(0, target - raw_found)),
                total=max(1, target),
                severity="high" if verified_rate < 0.2 else "medium",
                message=(
                    "The market did not produce enough unique in-scope candidates to honestly support a much higher "
                    "verified count."
                ),
                action=(
                    "Expand the highest-priority geos, allow one adjacent role family, or accept that the verified "
                    "ceiling is lower for this market."
                ),
            )
        )

    issue_priority = {
        "market_scarcity": 0,
        "title_mismatch": 1,
        "geo_mismatch": 2,
        "parser_confidence": 3,
        "public_executive_evidence_thin": 4,
        "weak_must_have_anchors": 5,
        "weak_company_or_industry_signals": 6,
        "company_scope_too_strict": 7,
        "filters_too_strict": 8,
        "filters_too_loose": 9,
    }
    issues = sorted(
        issues,
        key=lambda issue: (
            issue_priority.get(str(issue.get("key", "")), 99),
            -float(issue.get("share", 0.0)),
            -int(issue.get("count", 0)),
        ),
    )
    if verified_rate >= 0.4:
        headline = "Verified yield looks healthy."
        yield_status = "healthy"
    elif verified_rate >= 0.2:
        headline = "Verified yield is usable but constrained."
        yield_status = "constrained"
    else:
        headline = "Verified yield is low for this brief."
        yield_status = "low"

    return {
        "enabled": bool(issues) or yield_status != "healthy",
        "yield_status": yield_status,
        "headline": headline,
        "verified_rate": round(verified_rate, 3),
        "verified_count": verified_count,
        "review_count": review_count,
        "reject_count": reject_count,
        "raw_found": raw_found,
        "unique_after_dedupe": unique_after_dedupe,
        "issues": issues[:4],
    }


def filter_new_candidates(candidates: Iterable[CandidateProfile], seen_keys: Set[str]) -> List[CandidateProfile]:
    net_new: List[CandidateProfile] = []
    observed = set(seen_keys)
    for candidate in candidates:
        candidate_keys = candidate_identity_keys(candidate)
        if candidate_keys and not candidate_keys.isdisjoint(observed):
            continue
        observed.update(candidate_keys)
        net_new.append(candidate)
    return net_new


def candidate_to_row(candidate: CandidateProfile) -> Dict[str, object]:
    hydrated = hydrate_candidate_reporting(candidate)
    return {
        "identity_key": candidate_primary_key(hydrated),
        "full_name": _repair_display_text(hydrated.full_name),
        "verification_status": hydrated.verification_status,
        "qualification_tier": hydrated.qualification_tier,
        "in_scope": hydrated.in_scope,
        "scope_bucket": hydrated.scope_bucket,
        "score": hydrated.score,
        "current_title": _repair_display_text(hydrated.current_title),
        "current_company": _repair_display_text(hydrated.current_company),
        "location_name": _repair_display_text(hydrated.location_name),
        "company_match_context": _company_match_context(hydrated),
        "employment_signal": _employment_signal(hydrated),
        "title_similarity_score": round(hydrated.title_similarity_score, 3),
        "company_match_score": round(hydrated.company_match_score, 3),
        "location_match_score": round(hydrated.location_match_score, 3),
        "skill_overlap_score": round(hydrated.skill_overlap_score, 3),
        "industry_fit_score": round(hydrated.industry_fit_score, 3),
        "years_fit_score": round(hydrated.years_fit_score, 3),
        "semantic_similarity_score": round(hydrated.semantic_similarity_score, 3),
        "matched_titles": _serialize_multi_value([_repair_display_text(value) for value in hydrated.matched_titles]),
        "matched_companies": _serialize_multi_value([_repair_display_text(value) for value in hydrated.matched_companies]),
        "key_signals": _candidate_key_signals(hydrated),
        "linkedin_url": hydrated.linkedin_url,
        "source_url": hydrated.source_url,
        "source": hydrated.source,
    }


def write_candidates_csv(candidates: Iterable[CandidateProfile], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        for candidate in candidates:
            writer.writerow(candidate_to_row(candidate))
    return path


def iter_report_paths(paths: Iterable[Path]) -> Iterator[Path]:
    for path in paths:
        resolved = path.expanduser().resolve()
        if resolved.is_file() and resolved.suffix.lower() == ".json":
            yield resolved
            continue
        if resolved.is_dir():
            for child in sorted(resolved.glob("*.json")):
                if child.is_file():
                    yield child.resolve()


def collect_seen_candidate_keys(paths: Iterable[Path]) -> Set[str]:
    seen: Set[str] = set()
    for path in iter_report_paths(paths):
        try:
            report = load_report(path)
        except Exception:
            continue
        for candidate in report.candidates:
            seen.update(candidate_identity_keys(candidate))
    return seen


def collect_seen_provider_queries(paths: Iterable[Path]) -> Dict[str, Set[str]]:
    seen: Dict[str, Set[str]] = {}
    for path in iter_report_paths(paths):
        try:
            report = load_report(path)
        except Exception:
            continue
        for result in report.provider_results:
            provider_seen = seen.setdefault(result.provider_name, set())
            diagnostics_queries = result.diagnostics.get("queries", [])
            if not isinstance(diagnostics_queries, list):
                continue
            for item in diagnostics_queries:
                if not isinstance(item, dict):
                    continue
                search_query = str(item.get("search", "")).strip()
                if search_query:
                    provider_seen.add(search_query)
                    continue
                query_payload = item.get("query")
                if query_payload:
                    provider_seen.add(json.dumps(query_payload, sort_keys=True))
    return seen


def write_report(
    report: SearchRunReport,
    output_dir: Path,
    *,
    csv_candidate_limit: int | None = None,
) -> Tuple[Path, Path]:
    hydrate_report_reporting(report)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{report.run_id}.json"
    csv_path = output_dir / f"{report.run_id}.csv"
    csv_candidates = report.candidates
    if csv_candidate_limit is not None:
        csv_candidates = report.candidates[: max(0, int(csv_candidate_limit))]

    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(asdict(report), handle, indent=2)

    write_candidates_csv(csv_candidates, csv_path)

    return json_path, csv_path


def load_report(path: Path) -> SearchRunReport:
    payload = json.loads(path.read_text(encoding="utf-8"))
    provider_results = [
        ProviderRunResult(
            provider_name=result.get("provider_name", ""),
            executed=bool(result.get("executed", False)),
            dry_run=bool(result.get("dry_run", False)),
            request_count=int(result.get("request_count", 0)),
            candidate_count=int(result.get("candidate_count", 0)),
            candidates=[build_candidate(candidate) for candidate in result.get("candidates", [])],
            diagnostics=result.get("diagnostics", {}),
            errors=list(result.get("errors", [])),
        )
        for result in payload.get("provider_results", [])
    ]
    report = SearchRunReport(
        run_id=payload.get("run_id", ""),
        brief_id=payload.get("brief_id", ""),
        dry_run=bool(payload.get("dry_run", False)),
        generated_at=payload.get("generated_at", ""),
        provider_results=provider_results,
        candidates=[build_candidate(candidate) for candidate in payload.get("candidates", [])],
        summary=payload.get("summary", {}),
    )
    return hydrate_report_reporting(report)


def build_candidate(payload: dict) -> CandidateProfile:
    candidate = CandidateProfile(
        full_name=payload.get("full_name", ""),
        current_title=payload.get("current_title", ""),
        current_company=payload.get("current_company", ""),
        location_name=payload.get("location_name", ""),
        location_geo=payload.get("location_geo"),
        linkedin_url=payload.get("linkedin_url"),
        source=payload.get("source", ""),
        source_url=payload.get("source_url"),
        summary=payload.get("summary", ""),
        years_experience=payload.get("years_experience"),
        industry=payload.get("industry"),
        experience=list(payload.get("experience", [])),
        matched_titles=list(payload.get("matched_titles", [])),
        matched_companies=list(payload.get("matched_companies", [])),
        distance_miles=payload.get("distance_miles"),
        current_target_company_match=bool(payload.get("current_target_company_match", False)),
        target_company_history_match=bool(payload.get("target_company_history_match", False)),
        current_title_match=bool(payload.get("current_title_match", False)),
        in_scope=bool(payload.get("in_scope", False)),
        precise_market_in_scope=bool(payload.get("precise_market_in_scope", False)),
        scope_bucket=payload.get("scope_bucket", "out_of_scope"),
        industry_aligned=bool(payload.get("industry_aligned", False)),
        location_aligned=bool(payload.get("location_aligned", False)),
        current_company_confirmed=bool(payload.get("current_company_confirmed", False)),
        current_title_confirmed=bool(payload.get("current_title_confirmed", False)),
        current_location_confirmed=bool(payload.get("current_location_confirmed", False)),
        precise_location_confirmed=bool(payload.get("precise_location_confirmed", False)),
        current_employment_confirmed=bool(payload.get("current_employment_confirmed", False)),
        verification_status=payload.get("verification_status", "review"),
        qualification_tier=payload.get("qualification_tier", "weak"),
        cap_reasons=list(payload.get("cap_reasons", [])),
        disqualifier_reasons=list(payload.get("disqualifier_reasons", [])),
        matched_title_family=payload.get("matched_title_family", ""),
        location_precision_bucket=payload.get("location_precision_bucket", "unknown"),
        current_role_proof_count=int(payload.get("current_role_proof_count", 0) or 0),
        source_quality_score=float(payload.get("source_quality_score", 0.0)),
        evidence_freshness_year=payload.get("evidence_freshness_year"),
        current_function_fit=float(payload.get("current_function_fit", 0.0)),
        current_fmcg_fit=float(payload.get("current_fmcg_fit", 0.0)),
        parser_confidence=float(payload.get("parser_confidence", 0.0)),
        evidence_quality_score=float(payload.get("evidence_quality_score", 0.0)),
        title_similarity_score=float(payload.get("title_similarity_score", 0.0)),
        company_match_score=float(payload.get("company_match_score", 0.0)),
        location_match_score=float(payload.get("location_match_score", 0.0)),
        skill_overlap_score=float(payload.get("skill_overlap_score", 0.0)),
        industry_fit_score=float(payload.get("industry_fit_score", 0.0)),
        years_fit_score=float(payload.get("years_fit_score", 0.0)),
        years_experience_gap=payload.get("years_experience_gap"),
        semantic_similarity_score=float(payload.get("semantic_similarity_score", 0.0)),
        reranker_score=float(payload.get("reranker_score", 0.0)),
        ranking_model_version=payload.get("ranking_model_version", ""),
        feature_scores=dict(payload.get("feature_scores", {})),
        anchor_scores=dict(payload.get("anchor_scores", {})),
        verification_notes=list(payload.get("verification_notes", [])),
        search_strategies=list(payload.get("search_strategies", [])),
        evidence_records=[
            EvidenceRecord(
                query=record.get("query", ""),
                source_url=record.get("source_url", ""),
                source_domain=record.get("source_domain", ""),
                title=record.get("title", ""),
                snippet=record.get("snippet", ""),
                source_type=record.get("source_type", "search_result"),
                name_match=bool(record.get("name_match", False)),
                company_match=record.get("company_match", ""),
                title_matches=list(record.get("title_matches", [])),
                location_match=bool(record.get("location_match", False)),
                location_match_text=record.get("location_match_text", ""),
                precise_location_match=bool(record.get("precise_location_match", False)),
                profile_signal=bool(record.get("profile_signal", False)),
                current_employment_signal=bool(record.get("current_employment_signal", False)),
                recency_year=record.get("recency_year"),
                confidence=float(record.get("confidence", 0.0)),
                raw=record.get("raw", {}),
            )
            for record in payload.get("evidence_records", [])
        ],
        evidence_confidence=float(payload.get("evidence_confidence", 0.0)),
        evidence_verdict=payload.get("evidence_verdict", ""),
        stale_data_risk=bool(payload.get("stale_data_risk", False)),
        last_verified_at=payload.get("last_verified_at"),
        score=float(payload.get("score", 0.0)),
        raw=payload.get("raw", {}),
    )
    return hydrate_candidate_reporting(candidate)
