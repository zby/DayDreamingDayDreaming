import re


ESSAY_IDEA_BLOCK_RE = re.compile(
    r"<essay-idea(?:\s+[^>]*)?>\n([\s\S]*?)\n</essay-idea>", re.MULTILINE
)


def parse_essay_idea_last(text: str) -> str:
    """Extract the last (or highest-stage) <essay-idea> block from links output.

    Contract assumed by deliberate-rolling-thread-v1:
    - Tags are on their own lines.
    - One <essay-idea ...> per step; 'stage' attribute may be present.
    - We select by highest numeric stage when available; else last occurrence.
    """
    # Find all idea blocks with their spans for attribute extraction
    matches = list(ESSAY_IDEA_BLOCK_RE.finditer(text))
    if not matches:
        raise ValueError("No <essay-idea> blocks found in links output")

    # For each match, try to extract stage attribute from the opening tag segment
    stage_attr_re = re.compile(r"<essay-idea[^>]*stage=\"(\d+)\"", re.MULTILINE)
    staged = []
    for m in matches:
        # Grab the opening line from the original text
        start = m.start()
        # Find the line start
        line_start = text.rfind("\n", 0, start) + 1
        opening_line = text[line_start : text.find("\n", start) if text.find("\n", start) != -1 else m.start()]
        stage_val = None
        m_stage = stage_attr_re.search(text[line_start : m.end()])
        if m_stage:
            try:
                stage_val = int(m_stage.group(1))
            except ValueError:
                stage_val = None
        staged.append((stage_val, m.group(1)))

    # Prefer highest stage; if none have stage, pick the last
    if any(s is not None for s, _ in staged):
        best_stage = max(s for s, _ in staged if s is not None)
        # If multiple with same stage, pick the last among them
        candidates = [content for s, content in staged if s == best_stage]
        return candidates[-1]
    else:
        return staged[-1][1]

