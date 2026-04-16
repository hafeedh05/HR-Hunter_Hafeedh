from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from hr_hunter.config import resolve_feedback_db_path, resolve_output_dir
from hr_hunter_transformer.calibration import load_transformer_calibration_model
from hr_hunter_transformer.evidence_graph import EvidenceGraphBuilder
from hr_hunter_transformer.extraction import ProfileExtractor
from hr_hunter_transformer.models import PipelineMetrics, PipelineResult, QueryPlan, RawSearchHit, SearchBrief
from hr_hunter_transformer.query_planner import build_query_plan
from hr_hunter_transformer.role_profiles import normalize_text
from hr_hunter_transformer.ranking import VerificationAwareRanker
from hr_hunter_transformer.telemetry import StageTelemetry, stage_percent
from hr_hunter_transformer.transformer_ranker import HFTransformerTextEncoder, HashingTextEncoder, TransformerScorer
from hr_hunter_transformer.verifier import verify_candidates


def _verification_window_size(total_candidates: int, target_count: int) -> int:
    if total_candidates <= 0:
        return 0
    target = max(1, int(target_count or 300))
    return min(total_candidates, max(target, target * 3))


def _final_candidate_order_key(candidate: Any) -> tuple[float, float, float, float, float, str]:
    status_priority = {
        "verified": 0,
        "review": 1,
        "reject": 2,
    }.get(str(candidate.verification_status or "").lower(), 3)
    return (
        float(status_priority),
        -float(candidate.score or 0.0),
        -float(candidate.verification_confidence or 0.0),
        -float(candidate.company_consensus_score or 0.0),
        -float(candidate.industry_match_score or 0.0),
        normalize_text(candidate.full_name),
    )


class CandidateIntelligencePipeline:
    def __init__(
        self,
        *,
        use_transformer: bool = False,
        transformer_model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
    ) -> None:
        self.extractor = ProfileExtractor()
        self.graph = EvidenceGraphBuilder()
        self.encoder_name = "none"
        self.calibration_model = load_transformer_calibration_model(
            str(resolve_feedback_db_path()),
            str(resolve_output_dir()),
        )
        transformer_scorer = None
        if use_transformer:
            try:
                transformer_scorer = TransformerScorer(HFTransformerTextEncoder(transformer_model_name))
                self.encoder_name = "hf_transformer"
            except Exception:
                transformer_scorer = TransformerScorer(HashingTextEncoder())
                self.encoder_name = "hashing_fallback"
        self.ranker = VerificationAwareRanker(
            transformer_scorer=transformer_scorer,
            calibration_model=self.calibration_model,
        )

    def build_query_plan(self, brief: SearchBrief) -> QueryPlan:
        return build_query_plan(brief)

    def run(
        self,
        brief: SearchBrief,
        hits: list[RawSearchHit],
        *,
        query_plan: QueryPlan | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> PipelineResult:
        telemetry = StageTelemetry()
        plan = query_plan or self.build_query_plan(brief)

        event = telemetry.emit(
            "extraction_running",
            message=f"Retrieval complete. Processing {len(hits)} raw hits.",
            percent=stage_percent("extraction_running"),
            raw_found=len(hits),
            extracted_records=0,
        )
        if progress_callback:
            progress_callback(event)

        records = []
        for hit in hits:
            record = self.extractor.extract(hit, brief)
            if record is not None:
                records.append(record)

        entities = self.graph.merge(records)
        event = telemetry.emit(
            "entity_resolution",
            message=f"Merged evidence into {len(entities)} candidate entities.",
            percent=stage_percent("entity_resolution"),
            unique_after_dedupe=len(entities),
        )
        if progress_callback:
            progress_callback(event)

        ranked = self.ranker.rank(entities, brief)
        verification_window = ranked[: _verification_window_size(len(ranked), brief.target_count)]
        event = telemetry.emit(
            "scoring",
            message=f"Scored {len(verification_window)} candidates for verification-aware final ordering.",
            percent=stage_percent("scoring"),
            reranked_count=len(verification_window),
            rerank_target=min(brief.target_count, len(entities)),
        )
        if progress_callback:
            progress_callback(event)

        verified_window = verify_candidates(verification_window, brief)
        candidates = sorted(verified_window, key=_final_candidate_order_key)[: brief.target_count]
        verified_count = sum(candidate.verification_status == "verified" for candidate in candidates)
        review_count = sum(candidate.verification_status == "review" for candidate in candidates)
        reject_count = sum(candidate.verification_status == "reject" for candidate in candidates)
        event = telemetry.emit(
            "verification",
            message="Applied canonical transformer verification thresholds.",
            percent=stage_percent("verification"),
            verified_count=verified_count,
            review_count=review_count,
            reject_count=reject_count,
        )
        if progress_callback:
            progress_callback(event)

        metrics = PipelineMetrics(
            raw_found=len(hits),
            extracted_records=len(records),
            unique_candidates=len(entities),
            queries_planned=len(plan.queries),
            queries_completed=len(plan.queries),
            verified_count=verified_count,
            review_count=review_count,
            reject_count=reject_count,
        )
        final_event = telemetry.emit(
            "finalizing",
            message="Finalizing transformer shortlist.",
            percent=stage_percent("finalizing"),
            finalized_count=len(candidates),
        )
        if progress_callback:
            progress_callback(final_event)
        complete_event = telemetry.emit("completed", message="Transformer pipeline completed.", percent=100)
        if progress_callback:
            progress_callback(complete_event)

        return PipelineResult(
            candidates=candidates,
            metrics=metrics,
            role_understanding=plan.role_understanding,
            query_plan=plan,
            telemetry_events=telemetry.events(),
        )

    def usage_summary(self) -> dict[str, int | str | bool]:
        scorer = self.ranker.transformer_scorer
        if scorer is None:
            return {
                "encoder_type": "none",
                "model_name": "",
                "encode_calls": 0,
                "total_tokens": 0,
                "cache_enabled": False,
                "cache_entries": 0,
            }
        summary = dict(scorer.usage_summary())
        summary.setdefault("encoder_name", self.encoder_name)
        summary["calibration_enabled"] = self.calibration_model is not None
        summary["calibration_training_rows"] = int(self.calibration_model.training_rows) if self.calibration_model else 0
        summary["calibration_positive_rows"] = int(self.calibration_model.positive_rows) if self.calibration_model else 0
        return summary
