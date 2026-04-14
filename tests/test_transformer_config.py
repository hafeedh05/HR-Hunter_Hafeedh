import os

from hr_hunter import config as main_config
from hr_hunter_transformer import config as transformer_config


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
