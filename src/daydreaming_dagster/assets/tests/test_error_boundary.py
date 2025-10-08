import pytest
from dagster import Failure

from daydreaming_dagster.assets._error_boundary import with_asset_error_boundary
from daydreaming_dagster.utils.errors import DDError, Err


class _StubLogger:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def error(self, message: str) -> None:
        self.calls.append(("error", message))


def test_error_boundary_converts_dd_error_to_failure(monkeypatch):
    stub = _StubLogger()
    monkeypatch.setattr(
        "daydreaming_dagster.assets._error_boundary.get_dagster_logger",
        lambda: stub,
    )

    @with_asset_error_boundary(stage="draft")
    def _asset():
        raise DDError(Err.INVALID_CONFIG, ctx={"detail": "bad"})

    with pytest.raises(Failure) as failure_info:
        _asset()

    failure = failure_info.value
    assert "INVALID_CONFIG" in failure.description
    assert failure.metadata["error_code"].value == "INVALID_CONFIG"
    assert failure.metadata["error_ctx"].value == {"detail": "bad"}
    assert stub.calls == [("error", "[draft] INVALID_CONFIG: {'detail': 'bad'}")]


def test_error_boundary_passes_through_existing_failure(monkeypatch):
    stub = _StubLogger()
    monkeypatch.setattr(
        "daydreaming_dagster.assets._error_boundary.get_dagster_logger",
        lambda: stub,
    )

    @with_asset_error_boundary(stage="draft")
    def _asset():
        raise Failure(description="boom")

    with pytest.raises(Failure) as failure_info:
        _asset()

    assert failure_info.value.description == "boom"
    assert stub.calls == []
