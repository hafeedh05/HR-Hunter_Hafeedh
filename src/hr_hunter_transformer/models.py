from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class SearchBrief:
    role_title: str
    titles: list[str] = field(default_factory=list)
    countries: list[str] = field(default_factory=list)
    cities: list[str] = field(default_factory=list)
    company_targets: list[str] = field(default_factory=list)
    peer_company_targets: list[str] = field(default_factory=list)
    exclude_title_keywords: list[str] = field(default_factory=list)
    exclude_company_keywords: list[str] = field(default_factory=list)
    required_keywords: list[str] = field(default_factory=list)
    preferred_keywords: list[str] = field(default_factory=list)
    industry_keywords: list[str] = field(default_factory=list)
    target_count: int = 300
    company_match_mode: str = "both"
    allow_adjacent_titles: bool = True
    exact_company_scope: bool = False
    strict_market_scope: bool = False


@dataclass(slots=True)
class RoleUnderstanding:
    normalized_title: str
    role_family: str
    role_subfamily: str
    family_confidence: float
    title_variants: list[str] = field(default_factory=list)
    adjacent_titles: list[str] = field(default_factory=list)
    inferred_skills: list[str] = field(default_factory=list)
    seniority_hint: str = "manager"
    search_complexity: str = "balanced"


@dataclass(slots=True)
class QueryTask:
    query_text: str
    query_type: str
    source_pack: str
    page_budget: int = 1


@dataclass(slots=True)
class QueryPlan:
    role_understanding: RoleUnderstanding
    queries: list[QueryTask] = field(default_factory=list)
    max_queries: int = 60
    pages_per_query: int = 1
    parallel_requests: int = 8


@dataclass(slots=True)
class RawSearchHit:
    title: str
    snippet: str
    url: str
    source: str = "public_web"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class EvidenceRecord:
    source_url: str
    source_domain: str
    source_type: str
    page_title: str
    page_snippet: str
    full_name: str
    current_title: str
    current_company: str
    current_location: str
    role_family: str
    title_match: bool
    company_match: bool
    location_match: bool
    current_role_signal: bool
    confidence: float
    peer_company_match: bool = False
    title_confidence: float = 0.0
    company_confidence: float = 0.0
    location_confidence: float = 0.0
    currentness_confidence: float = 0.0
    freshness_confidence: float = 0.0
    supporting_keywords: list[str] = field(default_factory=list)


@dataclass(slots=True)
class CandidateEntity:
    full_name: str
    canonical_key: str
    current_title: str = ""
    current_company: str = ""
    current_location: str = ""
    role_family: str = "other"
    evidence: list[EvidenceRecord] = field(default_factory=list)
    title_match: bool = False
    company_match: bool = False
    peer_company_match: bool = False
    location_match: bool = False
    current_role_proof_count: int = 0
    current_company_confirmed: bool = False
    current_title_confirmed: bool = False
    current_location_confirmed: bool = False
    company_support_count: int = 0
    title_support_count: int = 0
    location_support_count: int = 0
    source_domains: list[str] = field(default_factory=list)
    semantic_fit: float = 0.0
    title_match_score: float = 0.0
    skill_match_score: float = 0.0
    company_match_score: float = 0.0
    company_quality_score: float = 0.0
    company_consensus_score: float = 0.0
    location_match_score: float = 0.0
    location_consensus_score: float = 0.0
    title_consensus_score: float = 0.0
    industry_match_score: float = 0.0
    seniority_match_score: float = 0.0
    currentness_score: float = 0.0
    source_trust_score: float = 0.0
    verification_confidence: float = 0.0
    semantic_similarity: float = 0.0
    evidence_conflict_score: float = 0.0
    score: float = 0.0
    verification_status: str = "reject"
    notes: list[str] = field(default_factory=list)
    diagnostics: list[str] = field(default_factory=list)


@dataclass(slots=True)
class PipelineMetrics:
    raw_found: int = 0
    extracted_records: int = 0
    unique_candidates: int = 0
    queries_planned: int = 0
    queries_completed: int = 0
    verified_count: int = 0
    review_count: int = 0
    reject_count: int = 0


@dataclass(slots=True)
class PipelineResult:
    candidates: list[CandidateEntity]
    metrics: PipelineMetrics
    role_understanding: RoleUnderstanding
    query_plan: QueryPlan
    telemetry_events: list[dict[str, Any]] = field(default_factory=list)
