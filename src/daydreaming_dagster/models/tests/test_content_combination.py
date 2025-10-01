import pytest
from daydreaming_dagster.models.concept import Concept
from daydreaming_dagster.models.content_combination import ContentCombination
from daydreaming_dagster.utils.errors import DDError


def test_from_concepts_basic():
    """Test basic content combination creation."""
    concepts = [
        Concept("c1", "Concept One", {"sentence": "Short 1", "paragraph": "Long 1"}),
        Concept("c2", "Concept Two", {"sentence": "Short 2", "paragraph": "Long 2"})
    ]
    
    combo = ContentCombination.from_concepts(
        concepts,
        "paragraph",
        combo_id="combo_basic",
    )
    
    assert len(combo.contents) == 2
    assert combo.contents[0] == {"name": "Concept One", "content": "Long 1"}
    assert combo.contents[1] == {"name": "Concept Two", "content": "Long 2"}
    assert combo.metadata["strategy"] == "single_level"
    assert combo.metadata["level"] == "paragraph"
    
    # Test new fields
    assert isinstance(combo.combo_id, str)
    assert len(combo.combo_id) > 0
    assert combo.concept_ids == ["c1", "c2"]


def test_from_concepts_with_explicit_combo_id():
    """Test content combination with explicit combo_id."""
    concepts = [
        Concept("c1", "Concept One", {"paragraph": "Long 1"}),
        Concept("c2", "Concept Two", {"paragraph": "Long 2"})
    ]
    
    combo = ContentCombination.from_concepts(concepts, "paragraph", combo_id="combo_123")
    
    assert combo.combo_id == "combo_123"
    assert combo.concept_ids == ["c1", "c2"]


def test_from_concepts_fallback():
    """Test fallback when requested level is missing."""
    concepts = [
        Concept("c1", "Concept One", {"sentence": "Short 1"}),  # No paragraph
        Concept("c2", "Concept Two", {"paragraph": "Long 2"})   # Has paragraph
    ]
    
    combo = ContentCombination.from_concepts(
        concepts,
        "paragraph",
        combo_id="combo_fallback",
    )
    
    assert combo.contents[0]["content"] == "Short 1"  # Fell back to sentence
    assert combo.contents[1]["content"] == "Long 2"   # Used requested paragraph


def test_resolve_content_fallback_chain():
    """Test the complete fallback chain."""
    concept_empty = Concept("empty", "Empty Concept", {})
    concept_sentence_only = Concept("sent", "Sentence Only", {"sentence": "Only sentence"})
    concept_all = Concept("all", "All Levels", {"sentence": "S", "paragraph": "P", "article": "A"})
    
    # Empty concept falls back to name
    assert ContentCombination._resolve_content(concept_empty, "paragraph") == "Empty Concept"
    
    # Sentence-only concept falls back to sentence when paragraph requested
    assert ContentCombination._resolve_content(concept_sentence_only, "paragraph") == "Only sentence"
    
    # Full concept uses requested level
    assert ContentCombination._resolve_content(concept_all, "article") == "A"
    assert ContentCombination._resolve_content(concept_all, "paragraph") == "P"


def test_from_concepts_requires_combo_id():
    """Missing combo_id should raise DDError to avoid nondeterministic hashes."""
    concepts = [
        Concept("c1", "One", {"paragraph": "P1"}),
    ]

    with pytest.raises(DDError) as exc:
        ContentCombination.from_concepts(concepts, "paragraph")

    assert exc.value.code is not None
    assert exc.value.ctx.get("reason") == "missing_combo_id"


def test_from_concepts_multi():
    """Test multi-level combination generation."""
    concepts = [
        Concept("c1", "One", {"sentence": "S1", "paragraph": "P1"}),
        Concept("c2", "Two", {"sentence": "S2", "paragraph": "P2"})
    ]
    
    combos = ContentCombination.from_concepts_multi(concepts)
    
    # Should generate all combinations: (S1,S2), (S1,P2), (P1,S2), (P1,P2)
    assert len(combos) == 4
    
    # Check one specific combination
    combo_sp = next(c for c in combos if c.metadata["level_combination"] == ("sentence", "paragraph"))
    assert combo_sp.contents[0]["content"] == "S1"
    assert combo_sp.contents[1]["content"] == "P2"


def test_get_available_levels():
    """Test available levels detection."""
    concepts = [
        Concept("c1", "One", {"sentence": "S1", "paragraph": "P1"}),
        Concept("c2", "Two", {"sentence": "S2"}),  # Missing paragraph
        Concept("c3", "Three", {"paragraph": ""})  # Empty paragraph
    ]
    
    available = ContentCombination._get_available_levels(concepts)
    
    assert available["c1"] == ["sentence", "paragraph"]
    assert available["c2"] == ["sentence"]
    assert available["c3"] == []  # Empty content not included
