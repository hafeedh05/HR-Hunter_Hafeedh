import os
import json

from hr_hunter import config as main_config
from hr_hunter_transformer.calibration import load_transformer_calibration_model
from hr_hunter_transformer import config as transformer_config
from hr_hunter_transformer.family_learning import load_family_learning_stats


def test_transformer_resolve_secret_uses_main_app_resolver(monkeypatch):
    monkeypatch.delenv("SCRAPINGBEE_API_KEY", raising=False)
    monkeypatch.setattr(transformer_config, "DEFAULT_SECRET_ENV_FILES", ())
    monkeypatch.setattr(
        main_config,
        "resolve_secret",
        lambda name, default=None: "shared-secret" if name == "SCRAPINGBEE_API_KEY" else default,
    )

    resolved = transformer_config.resolve_secret("SCRAPINGBEE_API_KEY")

    assert resolved == "shared-secret"
    assert os.environ["SCRAPINGBEE_API_KEY"] == "shared-secret"


def test_transformer_resolve_secret_returns_default_when_unavailable(monkeypatch):
    monkeypatch.delenv("SCRAPINGBEE_API_KEY", raising=False)
    monkeypatch.setattr(transformer_config, "DEFAULT_SECRET_ENV_FILES", ())
    monkeypatch.setattr(main_config, "resolve_secret", lambda name, default=None: None)

    resolved = transformer_config.resolve_secret("SCRAPINGBEE_API_KEY", default="fallback")

    assert resolved == "fallback"


def test_family_learning_uses_main_app_output_dir(tmp_path, monkeypatch):
    report_dir = tmp_path / "shared-output"
    report_dir.mkdir()
    (report_dir / "supply-chain-report.json").write_text(
        json.dumps(
            {
                "summary": {
                    "role_family": "supply_chain",
                    "candidate_count": 100,
                    "requested_candidate_limit": 100,
                    "verified_count": 40,
                    "review_count": 60,
                    "reject_count": 0,
                },
                "candidates": [],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("HR_HUNTER_OUTPUT_DIR", str(report_dir))
    load_family_learning_stats.cache_clear()

    stats = load_family_learning_stats()

    assert stats["supply_chain"].run_count == 1
    assert stats["supply_chain"].average_verified_rate == 0.4


def test_transformer_calibration_uses_main_app_output_dir(tmp_path, monkeypatch):
    report_dir = tmp_path / "shared-output"
    report_dir.mkdir()
    candidates = []
    for index in range(24):
        candidates.append(
            {
                "verification_status": "verified" if index < 12 else "reject",
                "current_title_match": True,
                "current_target_company_match": index % 2 == 0,
                "current_location_confirmed": True,
                "current_company_confirmed": True,
                "current_title_confirmed": True,
                "current_role_proof_count": 2,
                "semantic_similarity_score": 0.72,
                "industry_fit_score": 0.55,
                "feature_scores": {
                    "skill_overlap": 0.66,
                    "industry_fit": 0.55,
                },
                "anchor_scores": {
                    "company_consensus": 0.7,
                    "industry_match": 0.55,
                },
                "evidence_records": [
                    {
                        "source_domain": "linkedin.com",
                        "confidence": 0.88,
                        "supporting_keywords": ["s&op", "logistics"],
                    }
                ],
            }
        )
    (report_dir / "calibration-report.json").write_text(
        json.dumps({"summary": {"candidate_count": len(candidates)}, "candidates": candidates}),
        encoding="utf-8",
    )
    monkeypatch.setenv("HR_HUNTER_OUTPUT_DIR", str(report_dir))
    load_transformer_calibration_model.cache_clear()

    model = load_transformer_calibration_model()

    assert model is not None
    assert model.training_rows >= len(candidates)
