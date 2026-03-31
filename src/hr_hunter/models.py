from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class GeoSpec:
    location_name: str
    country: str = ""
    center_latitude: Optional[float] = None
    center_longitude: Optional[float] = None
    radius_miles: float = 0.0
    location_hints: List[str] = field(default_factory=list)


@dataclass
class SearchBrief:
    id: str
    role_title: str
    brief_document_path: Optional[str]
    brief_summary: str
    titles: List[str]
    title_keywords: List[str]
    company_targets: List[str]
    company_aliases: Dict[str, List[str]]
    geography: GeoSpec
    required_keywords: List[str]
    preferred_keywords: List[str]
    portfolio_keywords: List[str]
    commercial_keywords: List[str]
    leadership_keywords: List[str]
    scope_keywords: List[str]
    seniority_levels: List[str]
    minimum_years_experience: Optional[int]
    result_target_min: int
    result_target_max: int
    max_profiles: int
    industry_keywords: List[str] = field(default_factory=list)
    exclude_title_keywords: List[str] = field(default_factory=list)
    provider_settings: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    document_text: str = ""


@dataclass
class SearchSlice:
    id: str
    description: str
    companies: List[str]
    titles: List[str]
    title_keywords: List[str]
    query_keywords: List[str]
    search_mode: str
    limit: int


@dataclass
class EvidenceRecord:
    query: str = ""
    source_url: str = ""
    source_domain: str = ""
    title: str = ""
    snippet: str = ""
    source_type: str = "search_result"
    name_match: bool = False
    company_match: str = ""
    title_matches: List[str] = field(default_factory=list)
    location_match: bool = False
    profile_signal: bool = False
    current_employment_signal: bool = False
    recency_year: Optional[int] = None
    confidence: float = 0.0
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CandidateProfile:
    full_name: str = ""
    current_title: str = ""
    current_company: str = ""
    location_name: str = ""
    location_geo: Optional[str] = None
    linkedin_url: Optional[str] = None
    source: str = ""
    source_url: Optional[str] = None
    summary: str = ""
    years_experience: Optional[float] = None
    industry: Optional[str] = None
    experience: List[Dict[str, Any]] = field(default_factory=list)
    matched_titles: List[str] = field(default_factory=list)
    matched_companies: List[str] = field(default_factory=list)
    distance_miles: Optional[float] = None
    current_target_company_match: bool = False
    target_company_history_match: bool = False
    current_title_match: bool = False
    industry_aligned: bool = False
    location_aligned: bool = False
    current_company_confirmed: bool = False
    current_title_confirmed: bool = False
    current_location_confirmed: bool = False
    current_employment_confirmed: bool = False
    verification_status: str = "review"
    qualification_tier: str = "weak"
    cap_reasons: List[str] = field(default_factory=list)
    disqualifier_reasons: List[str] = field(default_factory=list)
    matched_title_family: str = ""
    location_precision_bucket: str = "unknown"
    current_role_proof_count: int = 0
    source_quality_score: float = 0.0
    evidence_freshness_year: Optional[int] = None
    current_function_fit: float = 0.0
    current_fmcg_fit: float = 0.0
    verification_notes: List[str] = field(default_factory=list)
    evidence_records: List[EvidenceRecord] = field(default_factory=list)
    evidence_confidence: float = 0.0
    evidence_verdict: str = ""
    stale_data_risk: bool = False
    last_verified_at: Optional[str] = None
    score: float = 0.0
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProviderRunResult:
    provider_name: str
    executed: bool
    dry_run: bool
    request_count: int = 0
    candidate_count: int = 0
    candidates: List[CandidateProfile] = field(default_factory=list)
    diagnostics: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)


@dataclass
class SearchRunReport:
    run_id: str
    brief_id: str
    dry_run: bool
    generated_at: str
    provider_results: List[ProviderRunResult]
    candidates: List[CandidateProfile]
    summary: Dict[str, Any] = field(default_factory=dict)
