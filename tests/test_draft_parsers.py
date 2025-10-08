from daydreaming_dagster.utils.draft_parsers import parse_essay_block_lenient


def test_parse_essay_block_lenient_strips_thinking_block_when_no_tags():
    raw_text = (
        "<thinking>reasoning</thinking>\n"
        "First line of answer.\nSecond line."
    )

    parsed = parse_essay_block_lenient(raw_text)

    assert "thinking" not in parsed.lower()
    assert parsed.startswith("First line of answer.")


def test_parse_essay_block_lenient_drops_thinking_before_block_extraction():
    raw_text = (
        "<thinking>chain</thinking>\n"
        "<essay>\nFinal answer\n</essay>\n"
    )

    parsed = parse_essay_block_lenient(raw_text)

    assert parsed == "Final answer"
