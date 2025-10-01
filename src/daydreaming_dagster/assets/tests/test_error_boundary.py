from __future__ import annotations

from daydreaming_dagster.assets._error_boundary import resume_notice


class _StubLogger:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def info(self, message: str) -> None:
        self.calls.append(("info", message))

    def warning(self, message: str) -> None:
        self.calls.append(("warning", message))

    def error(self, message: str) -> None:
        self.calls.append(("error", message))


def test_resume_notice_no_log_by_default(monkeypatch):
    stub = _StubLogger()
    monkeypatch.setattr(
        "daydreaming_dagster.assets._error_boundary.get_dagster_logger",
        lambda: stub,
    )

    payload = resume_notice(
        stage="evaluation",
        gen_id="GEN-123",
        artifact="parsed",
        reason="parsed_exists",
    )

    assert payload["resume"] is True
    assert payload["resume_stage"] == "evaluation"
    assert payload["resume_gen_id"] == "GEN-123"
    assert payload["resume_artifact"] == "parsed"
    assert payload["resume_reason"] == "parsed_exists"
    assert payload["resume_message"].startswith("Resume: stage=evaluation")
    assert stub.calls == []


def test_resume_notice_logs_when_requested(monkeypatch):
    stub = _StubLogger()
    monkeypatch.setattr(
        "daydreaming_dagster.assets._error_boundary.get_dagster_logger",
        lambda: stub,
    )

    payload = resume_notice(
        stage="evaluation",
        gen_id="GEN-456",
        artifact="raw",
        reason="missing_raw_metadata",
        emit_log=True,
        log_level="warning",
    )

    assert ("warning", payload["resume_message"]) in stub.calls
