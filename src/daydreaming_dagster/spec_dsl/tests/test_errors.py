from daydreaming_dagster.spec_dsl.errors import SpecDslError, SpecDslErrorCode


def test_spec_dsl_error_str_includes_context() -> None:
    err = SpecDslError(SpecDslErrorCode.INVALID_SPEC, ctx={"axis": "draft"})
    assert err.code is SpecDslErrorCode.INVALID_SPEC
    assert "draft" in str(err)


def test_spec_dsl_error_without_ctx_defaults_to_name() -> None:
    err = SpecDslError(SpecDslErrorCode.INVALID_SPEC)
    assert str(err) == "INVALID_SPEC"

