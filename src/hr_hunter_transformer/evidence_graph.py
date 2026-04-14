from __future__ import annotations

from collections import Counter, defaultdict

from hr_hunter_transformer.models import CandidateEntity, EvidenceRecord
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
    cleaned = str(value or "").strip(" -|,.;:")
    lowered = cleaned.lower()
    if not cleaned:
        return True
    if lowered in {"at", "dr", "experience", "educational", "profile"}:
        return True
    if any(month in lowered for month in ("jan ", "feb ", "mar ", "apr ", "may ", "jun ", "jul ", "aug ", "sep ", "oct ", "nov ", "dec ")):
        return True
    if " from may " in lowered or " from jan " in lowered or " from feb " in lowered or " from mar " in lowered or " from apr " in lowered or " from jun " in lowered or " from jul " in lowered or " from aug " in lowered or " from sep " in lowered or " from oct " in lowered or " from nov " in lowered or " from dec " in lowered:
        return True
    if lowered.startswith(("view org chart", "org ...", "view manager")):
        return True
    if current_title and normalize_text(cleaned) == normalize_text(current_title):
        return True
    if " is a " in lowered or "manager at" in lowered or "engineer at" in lowered or "planner at" in lowered:
        return True
    return False


def _best_company(values: list[EvidenceRecord], current_title: str) -> str:
    candidates = [record.current_company for record in values if record.current_company and not _looks_like_bad_company(record.current_company, current_title)]
    if not candidates:
        return ""
    company_counter = Counter(candidates)
    return company_counter.most_common(1)[0][0]


class EvidenceGraphBuilder:
    def merge(self, records: list[EvidenceRecord]) -> list[CandidateEntity]:
        grouped: dict[str, list[EvidenceRecord]] = defaultdict(list)
        for record in records:
            key = _canonical_person_key(record.full_name)
            if key:
                grouped[key].append(record)

        entities: list[CandidateEntity] = []
        for key, values in grouped.items():
            title_counter = Counter(record.current_title for record in values if record.current_title)
            location_counter = Counter(record.current_location for record in values if record.current_location)
            role_counter = Counter(record.role_family for record in values if record.role_family)
            chosen_title = title_counter.most_common(1)[0][0] if title_counter else ""
            chosen_company = _best_company(values, chosen_title)
            entity = CandidateEntity(
                full_name=_sanitize_name(max((record.full_name for record in values), key=len)),
                canonical_key=key,
                current_title=chosen_title,
                current_company=chosen_company,
                current_location=location_counter.most_common(1)[0][0] if location_counter else "",
                role_family=role_counter.most_common(1)[0][0] if role_counter else "other",
                evidence=sorted(values, key=lambda record: record.confidence, reverse=True),
                title_match=any(record.title_match for record in values),
                company_match=any(record.company_match for record in values),
                location_match=any(record.location_match for record in values),
                current_role_proof_count=sum(1 for record in values if record.current_role_signal),
                current_company_confirmed=any(
                    record.current_company
                    and not _looks_like_bad_company(record.current_company, chosen_title)
                    and record.company_confidence >= 0.55
                    and record.current_role_signal
                    for record in values
                ),
                current_title_confirmed=any(record.current_title and record.title_confidence >= 0.65 and record.current_role_signal for record in values),
                current_location_confirmed=any(record.current_location and record.location_confidence >= 0.48 for record in values),
                source_domains=sorted({record.source_domain for record in values if record.source_domain}),
            )
            entity.title_match_score = round(max((record.title_confidence for record in values), default=0.0), 4)
            entity.company_match_score = round(max((record.company_confidence for record in values), default=0.0), 4)
            entity.location_match_score = round(max((record.location_confidence for record in values), default=0.0), 4)
            entity.currentness_score = round(max((record.currentness_confidence for record in values), default=0.0), 4)
            entity.source_trust_score = round(
                min(1.0, sum(record.confidence for record in values[:3]) / max(1, min(3, len(values)))),
                4,
            )
            entities.append(entity)
        return entities
