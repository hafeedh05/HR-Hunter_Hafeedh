from __future__ import annotations

import re
from collections import Counter, defaultdict

from hr_hunter_transformer.company_quality import company_quality_score
from hr_hunter_transformer.models import CandidateEntity, EvidenceRecord, SearchBrief
from hr_hunter_transformer.role_profiles import normalize_text


def _canonical_person_key(full_name: str) -> str:
    return normalize_text(full_name)


def _sanitize_name(value: str) -> str:
    cleaned = str(value or "").strip()
    cleaned = cleaned.split("(")[0].strip()
    parts = [part for part in cleaned.split() if part]
    while parts and any(char.isdigit() for char in parts[-1]):
        parts.pop()
    while parts and parts[-1].lower() in {"mba", "cscp", "cppm", "cscm", "pmp", "phd"}:
        parts.pop()
    return " ".join(parts[:5]).strip()


def _looks_like_bad_company(value: str, current_title: str = "") -> bool:
    return company_quality_score(value, current_title) < 0.18


def _best_company(values: list[EvidenceRecord], current_title: str) -> str:
    scored_companies: dict[str, float] = defaultdict(float)
    representative_values: dict[str, str] = {}
    for record in values:
        company = str(record.current_company or "").strip()
        if not company or _looks_like_bad_company(company, current_title):
            continue
        normalized_company = normalize_text(company)
        quality = company_quality_score(company, current_title, record.role_family)
        score = (
            quality * 1.8
            + max(0.0, record.company_confidence)
            + (0.55 if record.company_match else 0.0)
            + (0.2 if record.peer_company_match else 0.0)
            + (0.2 if record.current_role_signal else 0.0)
        )
        scored_companies[normalized_company] += score
        existing_value = representative_values.get(normalized_company, "")
        if not existing_value or len(company) > len(existing_value):
            representative_values[normalized_company] = company
    if not scored_companies:
        return ""
    best_key = max(scored_companies, key=lambda key: scored_companies[key])
    return representative_values.get(best_key, "")


def _strong_company_key(record: EvidenceRecord) -> str:
    if (
        not record.current_company
        or record.company_confidence < 0.58
        or company_quality_score(record.current_company, record.current_title, record.role_family) < 0.5
    ):
        return ""
    return normalize_text(record.current_company)


def _strong_location_key(record: EvidenceRecord) -> str:
    if not record.current_location or record.location_confidence < 0.6:
        return ""
    return normalize_text(record.current_location)


def _dominant_role_family(values: list[EvidenceRecord]) -> str:
    role_counter = Counter(record.role_family for record in values if record.role_family)
    return role_counter.most_common(1)[0][0] if role_counter else ""


def _cluster_records(values: list[EvidenceRecord]) -> list[list[EvidenceRecord]]:
    ordered = sorted(
        values,
        key=lambda record: (
            1 if record.current_role_signal else 0,
            record.confidence,
            record.company_confidence,
            record.location_confidence,
        ),
        reverse=True,
    )
    clusters: list[list[EvidenceRecord]] = []
    for record in ordered:
        company_key = _strong_company_key(record)
        location_key = _strong_location_key(record)
        best_index = -1
        best_score = -1.0
        for index, cluster in enumerate(clusters):
            dominant_family = _dominant_role_family(cluster)
            cluster_company_keys = {_strong_company_key(candidate) for candidate in cluster if _strong_company_key(candidate)}
            cluster_location_keys = {_strong_location_key(candidate) for candidate in cluster if _strong_location_key(candidate)}
            if dominant_family and record.role_family and dominant_family != record.role_family and record.current_role_signal:
                continue
            if company_key and cluster_company_keys and company_key not in cluster_company_keys:
                continue
            if not company_key and location_key and not cluster_company_keys and cluster_location_keys and location_key not in cluster_location_keys:
                continue
            score = 0.0
            if dominant_family and dominant_family == record.role_family:
                score += 2.5
            if company_key and company_key in cluster_company_keys:
                score += 4.0
            if location_key and location_key in cluster_location_keys:
                score += 1.5
            if record.current_role_signal:
                score += 0.5
            score += max((candidate.confidence for candidate in cluster), default=0.0) * 0.25
            if score > best_score:
                best_score = score
                best_index = index
        if best_index >= 0:
            clusters[best_index].append(record)
        else:
            clusters.append([record])
    return clusters


def _matching_count(values: list[EvidenceRecord], attribute: str, chosen_value: str) -> int:
    normalized = normalize_text(chosen_value)
    if not normalized:
        return 0
    return sum(1 for record in values if normalize_text(getattr(record, attribute, "")) == normalized)


def _consensus_score(matches: int, total: int) -> float:
    if matches <= 0 or total <= 0:
        return 0.0
    return round(min(1.0, matches / max(1, total)), 4)


def _conflict_score(values: list[EvidenceRecord]) -> float:
    strong_company_keys = {_strong_company_key(record) for record in values if _strong_company_key(record)}
    strong_location_keys = {_strong_location_key(record) for record in values if _strong_location_key(record)}
    strong_role_families = {normalize_text(record.role_family) for record in values if record.role_family and record.current_role_signal}
    company_conflicts = max(0, len(strong_company_keys) - 1)
    location_conflicts = max(0, len(strong_location_keys) - 1)
    family_conflicts = max(0, len(strong_role_families) - 1)
    return round(
        min(
            1.0,
            0.44 * company_conflicts
            + 0.22 * location_conflicts
            + 0.28 * family_conflicts,
        ),
        4,
    )


class EvidenceGraphBuilder:
    def merge(self, records: list[EvidenceRecord], brief: SearchBrief | None = None) -> list[CandidateEntity]:
        grouped: dict[str, list[EvidenceRecord]] = defaultdict(list)
        for record in records:
            key = _canonical_person_key(record.full_name)
            if key:
                grouped[key].append(record)

        exact_company_targets = {
            normalize_text(value)
            for value in (brief.company_targets if brief else [])
            if normalize_text(value)
        }
        peer_company_targets = {
            normalize_text(value)
            for value in (brief.peer_company_targets if brief else [])
            if normalize_text(value)
        }
        entities: list[CandidateEntity] = []
        for key, values in grouped.items():
            for cluster in _cluster_records(values):
                title_counter = Counter(record.current_title for record in cluster if record.current_title)
                location_counter = Counter(record.current_location for record in cluster if record.current_location)
                role_counter = Counter(record.role_family for record in cluster if record.role_family)
                resolved_family = role_counter.most_common(1)[0][0] if role_counter else "other"
                chosen_title = title_counter.most_common(1)[0][0] if title_counter else ""
                chosen_location = location_counter.most_common(1)[0][0] if location_counter else ""
                chosen_company = _best_company(cluster, chosen_title)
                company_quality = company_quality_score(chosen_company, chosen_title, resolved_family)
                title_support_count = _matching_count(cluster, "current_title", chosen_title)
                location_support_count = _matching_count(cluster, "current_location", chosen_location)
                valid_company_records = [
                    record
                    for record in cluster
                    if record.current_company and company_quality_score(record.current_company, chosen_title, record.role_family) >= 0.32
                ]
                company_support_count = sum(
                    1
                    for record in valid_company_records
                    if normalize_text(record.current_company) == normalize_text(chosen_company)
                )
                company_consensus_score = _consensus_score(company_support_count, len(valid_company_records))
                title_consensus_score = _consensus_score(title_support_count, len([record for record in cluster if record.current_title]))
                location_consensus_score = _consensus_score(location_support_count, len([record for record in cluster if record.current_location]))
                current_company_confirmed = any(
                    record.current_company
                    and company_quality_score(record.current_company, chosen_title, record.role_family) >= 0.42
                    and record.company_confidence >= 0.55
                    and record.current_role_signal
                    for record in cluster
                ) or bool(chosen_company and company_quality >= 0.42 and company_support_count >= 2 and company_consensus_score >= 0.66)
                current_title_confirmed = any(
                    record.current_title and record.title_confidence >= 0.65 and record.current_role_signal
                    for record in cluster
                ) or bool(chosen_title and title_support_count >= 2 and title_consensus_score >= 0.68)
                current_location_confirmed = any(
                    record.current_location and record.location_confidence >= 0.48
                    for record in cluster
                ) or bool(chosen_location and location_support_count >= 2 and location_consensus_score >= 0.7)
                chosen_company_key = normalize_text(chosen_company)
                chosen_company_is_target = bool(chosen_company_key and chosen_company_key in exact_company_targets)
                chosen_company_is_peer = bool(chosen_company_key and chosen_company_key in peer_company_targets)
                entity = CandidateEntity(
                    full_name=_sanitize_name(max((record.full_name for record in cluster), key=len)),
                    canonical_key=key,
                    current_title=chosen_title,
                    current_company=chosen_company,
                    current_location=chosen_location,
                    role_family=resolved_family,
                    evidence=sorted(cluster, key=lambda record: record.confidence, reverse=True),
                    title_match=any(record.title_match for record in cluster),
                    company_match=chosen_company_is_target,
                    peer_company_match=chosen_company_is_peer,
                    location_match=any(record.location_match for record in cluster),
                    current_role_proof_count=sum(1 for record in cluster if record.current_role_signal),
                    current_company_confirmed=current_company_confirmed,
                    current_title_confirmed=current_title_confirmed,
                    current_location_confirmed=current_location_confirmed,
                    company_support_count=company_support_count,
                    title_support_count=title_support_count,
                    location_support_count=location_support_count,
                    source_domains=sorted({record.source_domain for record in cluster if record.source_domain}),
                )
                if chosen_company_is_target:
                    entity.company_match = True
                if chosen_company_is_peer:
                    entity.peer_company_match = True
                entity.title_match_score = round(max((record.title_confidence for record in cluster), default=0.0), 4)
                entity.company_match_score = round(max((record.company_confidence for record in cluster), default=0.0), 4)
                if chosen_company_is_target:
                    entity.company_match_score = max(entity.company_match_score, 0.92)
                elif chosen_company_is_peer:
                    entity.company_match_score = max(entity.company_match_score, 0.72)
                entity.company_quality_score = company_quality
                entity.company_consensus_score = company_consensus_score
                entity.location_match_score = round(max((record.location_confidence for record in cluster), default=0.0), 4)
                entity.location_consensus_score = location_consensus_score
                entity.title_consensus_score = title_consensus_score
                entity.currentness_score = round(max((record.currentness_confidence for record in cluster), default=0.0), 4)
                entity.source_trust_score = round(
                    min(1.0, sum(record.confidence for record in cluster[:3]) / max(1, min(3, len(cluster)))),
                    4,
                )
                entity.evidence_conflict_score = _conflict_score(cluster)
                entities.append(entity)
        return entities
