import pytest
from dagster import build_asset_context, DataVersion


pytestmark = [pytest.mark.unit]


def test_llm_models_source_returns_dataversion(monkeypatch):
    # Import lazily to avoid import-time side effects
    from daydreaming_dagster.assets import sources as src

    # Make fingerprint deterministic and avoid filesystem access
    monkeypatch.setattr(src, "_fingerprint_paths", lambda paths: "deadbeef", raising=True)

    ctx = build_asset_context()

    # Should not raise and should return a DataVersion, not a raw string
    # observable_source_asset returns a SourceAsset; call its observe_fn
    result = src.llm_models_source.observe_fn(ctx)

    assert isinstance(result.data_version, DataVersion)
    assert result.data_version == DataVersion("deadbeef")


@pytest.mark.parametrize(
    "fn_name",
    [
        "raw_concepts_source",
        "llm_models_source",
        "link_templates_source",
        "essay_templates_source",
        "evaluation_templates_source",
    ],
)
def test_all_sources_return_dataversion(monkeypatch, fn_name):
    from daydreaming_dagster.assets import sources as src

    monkeypatch.setattr(src, "_fingerprint_paths", lambda paths: "cafebabe", raising=True)
    ctx = build_asset_context()

    sa = getattr(src, fn_name)
    result = sa.observe_fn(ctx)

    assert isinstance(result.data_version, DataVersion)
    assert result.data_version == DataVersion("cafebabe")
