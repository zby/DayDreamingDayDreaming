"""Tests for generation response parser utility."""

import pytest
from daydreaming_dagster.utils.generation_response_parser import (
    parse_generation_response,
    extract_essay_only,
    validate_essay_content,
    ParseStrategy
)


class TestParseGenerationResponse:
    """Test parsing of generation responses with different formats."""
    
    def test_xml_tags_format(self):
        """Test parsing responses with XML-like tags."""
        response = """
<thinking>
This is the thinking section with analysis.
</thinking>

<essay>
This is the main essay content.
It has multiple paragraphs.
And continues with more content.
</essay>

<endnotes>
These are the endnotes.
</endnotes>
"""
        
        result = parse_generation_response(response, "xml_tags")
        
        assert result["essay"] == "This is the main essay content.\nIt has multiple paragraphs.\nAnd continues with more content."
        assert result["thinking"] == "This is the thinking section with analysis."
        assert result["endnotes"] == "These are the endnotes."
        assert result["parsing_notes"]["strategy"] == "xml_tags"
        assert "essay" in result["parsing_notes"]["sections_found"]
        assert "thinking" in result["parsing_notes"]["sections_found"]
        assert "endnotes" in result["parsing_notes"]["sections_found"]
    
    def test_xml_tags_format_missing_sections(self):
        """Test parsing when some sections are missing."""
        response = """
<essay>
This is just the essay content.
No other sections.
</essay>
"""
        
        result = parse_generation_response(response, "xml_tags")
        
        assert result["essay"] == "This is just the essay content.\nNo other sections."
        assert result["thinking"] is None
        assert result["endnotes"] is None
        assert result["parsing_notes"]["strategy"] == "xml_tags"
    
    def test_section_headers_format(self):
        """Test parsing responses with section headers."""
        response = """
Thinking:
This is the thinking section.

Essay:
This is the main essay content.
With multiple paragraphs.

Endnotes:
These are the endnotes.
"""
        
        result = parse_generation_response(response, "section_headers")
        
        assert result["essay"] == "This is the main essay content.\nWith multiple paragraphs."
        assert result["thinking"] == "This is the thinking section."
        assert result["endnotes"] == "These are the endnotes."
        assert result["parsing_notes"]["strategy"] == "section_headers"
    
    def test_fallback_format(self):
        """Test fallback parsing when no structure is found."""
        response = """
This is a free-form response without any clear structure.
It just contains essay content directly.
No tags or headers to parse.
"""
        
        result = parse_generation_response(response, "fallback")
        
        assert result["essay"] == "This is a free-form response without any clear structure.\nIt just contains essay content directly.\nNo tags or headers to parse."
        assert result["thinking"] is None
        assert result["endnotes"] is None
        assert result["parsing_notes"]["strategy"] == "fallback"
        assert "note" in result["parsing_notes"]
    
    def test_strategy_fallback_behavior(self):
        """Test that strategies fall back gracefully."""
        response = "Just plain text without structure."
        
        # Should fall back to fallback strategy when xml_tags fails
        result = parse_generation_response(response, "xml_tags")
        
        assert result["essay"] == "Just plain text without structure."
        assert result["parsing_notes"]["strategy"] == "fallback"
    
    def test_empty_response(self):
        """Test handling of empty responses."""
        with pytest.raises(ValueError, match="Empty or whitespace-only response"):
            parse_generation_response("")
        
        with pytest.raises(ValueError, match="Empty or whitespace-only response"):
            parse_generation_response("   \n\n  ")
    
    def test_missing_essay_tags(self):
        """Test handling when <essay> tags are missing - should fall back gracefully."""
        response = """
<thinking>
Some thinking content.
</thinking>

<endnotes>
Some endnotes.
</endnotes>
"""
        
        # Should fall back to fallback strategy when essay tags are missing
        result = parse_generation_response(response, "xml_tags")
        
        # The entire response becomes the essay content (including XML tags)
        assert "<thinking>" in result["essay"]
        assert "<endnotes>" in result["essay"]
        assert "Some thinking content." in result["essay"]
        assert "Some endnotes." in result["essay"]
        assert result["thinking"] is None
        assert result["endnotes"] is None
        assert result["parsing_notes"]["strategy"] == "fallback"


class TestExtractEssayOnly:
    """Test the simplified essay extraction function."""
    
    def test_extract_essay_only_xml_tags(self):
        """Test extracting just the essay from XML-tagged response."""
        response = """
<thinking>Analysis here.</thinking>
<essay>This is the essay.</essay>
<endnotes>Notes here.</endnotes>
"""
        
        essay = extract_essay_only(response, "xml_tags")
        assert essay == "This is the essay."
    
    def test_extract_essay_only_fallback(self):
        """Test extracting essay from unstructured response."""
        response = "This is the entire essay content."
        
        essay = extract_essay_only(response, "fallback")
        assert essay == "This is the entire essay content."


class TestValidateEssayContent:
    """Test essay content validation."""
    
    def test_valid_essay(self):
        """Test validation of a good essay."""
        essay = """
This is a valid essay with multiple paragraphs that contains sufficient content to pass validation.

It has proper sentence structure and punctuation throughout the entire document. The essay explores
various topics and provides comprehensive coverage of the subject matter at hand.

The content is substantial enough to be considered complete and meets all the requirements for
a well-structured essay including adequate word count, proper paragraph breaks, and complete
sentences with appropriate punctuation marks.

This demonstrates that the validation function correctly identifies valid essays that meet
all the necessary criteria for acceptance in the system.
"""
        
        result = validate_essay_content(essay)
        
        assert result["is_valid"] is True
        assert result["error"] is None
        assert result["metrics"]["word_count"] > 50
        assert result["metrics"]["has_paragraphs"] is True
        assert result["metrics"]["has_sentences"] is True
        assert result["metrics"]["has_minimum_content"] is True
    
    def test_invalid_essay_too_short(self):
        """Test validation of an essay that's too short."""
        essay = "Too short."
        
        result = validate_essay_content(essay)
        
        assert result["is_valid"] is False
        assert "incomplete or malformed" in result["error"]
        assert result["metrics"]["word_count"] < 50
        assert result["metrics"]["has_minimum_content"] is False
    
    def test_invalid_essay_no_paragraphs(self):
        """Test validation of an essay without paragraphs."""
        essay = "This is a single paragraph essay that goes on and on without any paragraph breaks. It has enough words but lacks proper structure. The content is there but it's not formatted as a proper essay should be."
        
        result = validate_essay_content(essay)
        
        assert result["is_valid"] is False
        assert result["metrics"]["has_paragraphs"] is False
    
    def test_empty_essay(self):
        """Test validation of empty essay."""
        result = validate_essay_content("")
        
        assert result["is_valid"] is False
        assert result["error"] == "Empty essay content"
        assert result["metrics"] == {}


class TestIntegration:
    """Test integration of parsing and validation."""
    
    def test_parse_and_validate_workflow(self):
        """Test the complete workflow of parsing and validating."""
        response = """
<thinking>
Analysis and reasoning here.
</thinking>

<essay>
This is a well-structured essay that contains sufficient content for validation purposes and demonstrates
comprehensive coverage of the subject matter being discussed.

It has multiple paragraphs and proper content organization throughout the document. The essay includes
detailed explanations and thorough analysis of various concepts and ideas that are relevant to the topic.

The validation should pass because this essay meets all the required criteria including adequate word count,
proper paragraph structure, complete sentences with appropriate punctuation, and substantial content that
demonstrates thoughtful consideration of the subject matter.

This comprehensive approach ensures that the integration test validates both parsing and validation
functionality working together seamlessly in the complete workflow.
</essay>

<endnotes>
Additional notes and references.
</endnotes>
"""
        
        # Parse the response
        parsed = parse_generation_response(response)
        
        # Extract and validate the essay
        essay = extract_essay_only(response)
        validation = validate_essay_content(essay)
        
        # Verify the workflow works
        assert parsed["essay"] == essay
        assert validation["is_valid"] is True
        assert validation["metrics"]["word_count"] > 20
