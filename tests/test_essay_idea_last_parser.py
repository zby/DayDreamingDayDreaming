import pytest

from daydreaming_dagster.utils.draft_parsers import parse_essay_idea_last


def test_extraction_picks_highest_stage():
    content = (
        "Some preface\n"
        "<essay-idea stage=\"1\">\nFirst idea paragraph.\n</essay-idea>\n\n"
        "Interleaving text\n"
        "<essay-idea stage=\"2\">\nSecond idea paragraph (final).\n</essay-idea>\n"
    )
    result = parse_essay_idea_last(content)
    assert result == "Second idea paragraph (final)."


def test_extraction_fallbacks_to_last_when_no_stage_attrs():
    content = (
        "<essay-idea>\nStage 1 idea paragraph.\n</essay-idea>\n\n"
        "<essay-idea>\nStage 2 idea paragraph.\n</essay-idea>\n"
    )
    result = parse_essay_idea_last(content)
    assert result == "Stage 2 idea paragraph."


def test_preserves_inner_newlines_verbatim():
    content = (
        "<essay-idea stage=\"3\">\n"
        "Line one.\n"
        "\n"
        "Line three after blank line.\n"
        "</essay-idea>\n"
    )
    result = parse_essay_idea_last(content)
    assert result == "Line one.\n\nLine three after blank line."


def test_raises_when_no_essay_idea_blocks_found():
    content = "No tags here.\n<thinking>but thinking exists</thinking>\n"
    with pytest.raises(ValueError, match="No <essay-idea> blocks"):
        parse_essay_idea_last(content)
