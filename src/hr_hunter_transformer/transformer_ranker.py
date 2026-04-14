from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from functools import lru_cache
from typing import Protocol

from hr_hunter_transformer.models import CandidateEntity, SearchBrief
from hr_hunter_transformer.role_profiles import normalize_text


class TextEncoder(Protocol):
    def encode(self, text: str) -> list[float]:
        ...

    def usage_summary(self) -> dict[str, int | str | bool]:
        ...


def _cosine_similarity(left: list[float], right: list[float]) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    dot = sum(a * b for a, b in zip(left, right))
    left_norm = math.sqrt(sum(a * a for a in left))
    right_norm = math.sqrt(sum(b * b for b in right))
    if left_norm <= 0.0 or right_norm <= 0.0:
        return 0.0
    return dot / (left_norm * right_norm)


class HashingTextEncoder:
    def __init__(self, dimensions: int = 64) -> None:
        self.dimensions = dimensions
        self.encode_calls = 0
        self.total_tokens = 0

    def encode(self, text: str) -> list[float]:
        normalized = normalize_text(text)
        vector = [0.0] * self.dimensions
        if not normalized:
            return vector
        self.encode_calls += 1
        self.total_tokens += len(normalized.split())
        for token in normalized.split():
            digest = hashlib.sha256(token.encode("utf-8")).digest()
            index = int.from_bytes(digest[:2], "big") % self.dimensions
            sign = 1.0 if digest[2] % 2 == 0 else -1.0
            vector[index] += sign
        norm = math.sqrt(sum(value * value for value in vector))
        if norm > 0:
            vector = [value / norm for value in vector]
        return vector

    def usage_summary(self) -> dict[str, int | str | bool]:
        return {
            "encoder_type": "hashing",
            "model_name": "hashing-fallback",
            "encode_calls": int(self.encode_calls),
            "total_tokens": int(self.total_tokens),
            "cache_enabled": False,
            "cache_entries": 0,
        }


class HFTransformerTextEncoder:
    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2") -> None:
        from transformers import AutoModel, AutoTokenizer
        import torch

        self._torch = torch
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name)
        self.model.eval()
        self.model_name = model_name
        self.encode_calls = 0
        self.total_tokens = 0
        self.max_sequence_length = 256

    @lru_cache(maxsize=2048)
    def encode(self, text: str) -> list[float]:
        normalized = normalize_text(text)
        if not normalized:
            return []
        encoded = self.tokenizer(
            normalized,
            return_tensors="pt",
            truncation=True,
            padding=True,
            max_length=self.max_sequence_length,
        )
        attention_mask = encoded["attention_mask"]
        token_count = int(attention_mask.sum().item()) if hasattr(attention_mask, "sum") else 0
        self.encode_calls += 1
        self.total_tokens += max(0, token_count)
        with self._torch.no_grad():
            model_out = self.model(**encoded)
        token_embeddings = model_out.last_hidden_state
        pooled_mask = attention_mask.unsqueeze(-1)
        masked = token_embeddings * pooled_mask
        summed = masked.sum(dim=1)
        counts = pooled_mask.sum(dim=1).clamp(min=1)
        pooled = summed / counts
        vector = pooled[0].tolist()
        norm = math.sqrt(sum(value * value for value in vector))
        if norm > 0:
            vector = [value / norm for value in vector]
        return vector

    def usage_summary(self) -> dict[str, int | str | bool]:
        cache_info = self.encode.cache_info()
        return {
            "encoder_type": "hf_transformer",
            "model_name": self.model_name,
            "encode_calls": int(self.encode_calls),
            "total_tokens": int(self.total_tokens),
            "cache_enabled": True,
            "cache_entries": int(cache_info.currsize),
            "cache_hits": int(cache_info.hits),
            "cache_misses": int(cache_info.misses),
            "max_sequence_length": int(self.max_sequence_length),
        }


@dataclass(slots=True)
class TransformerScorer:
    encoder: TextEncoder
    weight: float = 18.0

    def brief_text(self, brief: SearchBrief) -> str:
        parts = [
            brief.role_title,
            " ".join(brief.titles),
            " ".join(brief.required_keywords),
            " ".join(brief.preferred_keywords),
            " ".join(brief.industry_keywords),
            " ".join(brief.company_targets),
            " ".join(brief.cities),
            " ".join(brief.countries),
        ]
        return " | ".join(part.strip() for part in parts if part and part.strip())

    def candidate_text(self, candidate: CandidateEntity) -> str:
        top_evidence = " ".join(
            f"{record.page_title} {record.page_snippet}"
            for record in candidate.evidence[:2]
        )
        return " | ".join(
            part.strip()
            for part in [
                candidate.full_name,
                candidate.current_title,
                candidate.current_company,
                candidate.current_location,
                candidate.role_family,
                " ".join(candidate.source_domains),
                top_evidence,
            ]
            if part and part.strip()
        )

    def score(self, brief: SearchBrief, candidate: CandidateEntity) -> float:
        brief_vector = self.encoder.encode(self.brief_text(brief))
        candidate_vector = self.encoder.encode(self.candidate_text(candidate))
        similarity = _cosine_similarity(brief_vector, candidate_vector)
        candidate.semantic_similarity = round(similarity, 4)
        return round(similarity * self.weight, 2)

    def usage_summary(self) -> dict[str, int | str | bool]:
        return dict(self.encoder.usage_summary())
