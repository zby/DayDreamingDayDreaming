"""Utilities for parsing LLM generation responses to extract essay portions."""

import re
from typing import Literal, Optional, Dict, Any

ParseStrategy = Literal["xml_tags", "section_headers", "fallback"]


def get_parsing_strategy(template_id: str, model_name: str) -> ParseStrategy:
    """Determine the appropriate parsing strategy based on template and model.
    
    Args:
        template_id: The generation template identifier
        model_name: The LLM model name
        
    Returns:
        The recommended parsing strategy for this template/model combination
    """
    # Legacy templates with section headers (pre-XML era)
    section_header_templates = [
        "creative-synthesis-v3",
        "research-discovery-v3"
    ]
    
    # Legacy templates that need fallback parsing (very old templates)
    fallback_templates = [
        # Add any very old templates here that don't have structured output
    ]
    
    # Determine strategy based on template
    if template_id in section_header_templates:
        return "section_headers"
    elif template_id in fallback_templates:
        return "fallback"
    else:
        # Default for modern templates: assume XML tags structure
        return "xml_tags"


def parse_generation_response(response_text: str, strategy: ParseStrategy = "xml_tags") -> Dict[str, Any]:
    """Parse LLM generation response to extract different sections, especially the essay.
    
    Args:
        response_text: Raw LLM generation response text
        strategy: The parsing strategy to use, in order of preference
        
    Returns:
        Dictionary with parsed sections:
        - essay: The main essay content (required)
        - thinking: The thinking/analysis section if found
        - endnotes: The endnotes section if found
        - full_response: The complete original response
        - parsing_notes: Information about how parsing was performed
    """
    if not response_text or not response_text.strip():
        raise ValueError("Empty or whitespace-only response")
    
    # Try strategies in order of preference
    if strategy == "xml_tags":
        try:
            return _parse_xml_tags_format(response_text)
        except ValueError:
            pass
    
    if strategy == "section_headers":
        try:
            return _parse_section_headers_format(response_text)
        except ValueError:
            pass
    
    # Fallback: treat entire response as essay
    return _parse_fallback_format(response_text)


def _parse_xml_tags_format(response_text: str) -> Dict[str, Any]:
    """Parse responses using XML-like tags like <essay>, <thinking>, <endnotes>."""
    
    # Check if this looks like XML format (has some XML-like tags)
    has_xml_tags = bool(re.search(r'<\w+>.*?</\w+>', response_text, re.DOTALL))
    
    # Extract essay section (required)
    essay_match = re.search(r'<essay>(.*?)</essay>', response_text, re.DOTALL | re.IGNORECASE)
    if not essay_match:
        # If it has XML tags but no essay tags, that's an error
        if has_xml_tags:
            raise ValueError("No <essay> tags found in response")
        # Otherwise, let it fall back to other strategies
        raise ValueError("No XML structure found in response")
    
    essay_content = essay_match.group(1).strip()
    
    # Extract thinking section (optional)
    thinking_content = None
    thinking_match = re.search(r'<thinking>(.*?)</thinking>', response_text, re.DOTALL | re.IGNORECASE)
    if thinking_match:
        thinking_content = thinking_match.group(1).strip()
    
    # Extract endnotes section (optional)
    endnotes_content = None
    endnotes_match = re.search(r'<endnotes>(.*?)</endnotes>', response_text, re.DOTALL | re.IGNORECASE)
    if endnotes_match:
        endnotes_content = endnotes_match.group(1).strip()
    
    # Build sections found list
    sections_found = []
    if essay_content is not None:
        sections_found.append("essay")
    if thinking_content is not None:
        sections_found.append("thinking")
    if endnotes_content is not None:
        sections_found.append("endnotes")
    
    return {
        "essay": essay_content,
        "thinking": thinking_content,
        "endnotes": endnotes_content,
        "full_response": response_text,
        "parsing_notes": {
            "strategy": "xml_tags",
            "sections_found": sections_found,
            "essay_length": len(essay_content),
            "total_length": len(response_text)
        }
    }


def _parse_section_headers_format(response_text: str) -> Dict[str, Any]:
    """Parse responses using section headers like 'Essay:', 'Thinking:', etc."""
    
    # Look for common section headers
    sections = {}
    current_section = None
    current_content = []
    
    lines = response_text.split('\n')
    
    for line in lines:
        line = line.strip()
        
        # Check for section headers
        if re.match(r'^(Essay|Thinking|Endnotes|Analysis|Conclusion):\s*$', line, re.IGNORECASE):
            # Save previous section if exists
            if current_section and current_content:
                sections[current_section.lower()] = '\n'.join(current_content).strip()
            
            # Start new section
            current_section = line.split(':')[0].lower()
            current_content = []
        elif current_section:
            current_content.append(line)
        elif not current_section and line:
            # Content before first section header
            current_content.append(line)
    
    # Save final section
    if current_section and current_content:
        sections[current_section.lower()] = '\n'.join(current_content).strip()
    
    # If no sections found, treat everything as essay
    if not sections:
        sections['essay'] = response_text.strip()
    
    # Ensure essay section exists
    if 'essay' not in sections:
        # Use the first section as essay, or the entire response
        essay_content = next(iter(sections.values())) if sections else response_text.strip()
        sections['essay'] = essay_content
    
    return {
        "essay": sections['essay'],
        "thinking": sections.get('thinking'),
        "endnotes": sections.get('endnotes'),
        "full_response": response_text,
        "parsing_notes": {
            "strategy": "section_headers",
            "sections_found": list(sections.keys()),
            "essay_length": len(sections['essay']),
            "total_length": len(response_text)
        }
    }


def _parse_fallback_format(response_text: str) -> Dict[str, Any]:
    """Fallback parsing: treat entire response as essay."""
    
    return {
        "essay": response_text.strip(),
        "thinking": None,
        "endnotes": None,
        "full_response": response_text,
        "parsing_notes": {
            "strategy": "fallback",
            "sections_found": ["essay"],
            "essay_length": len(response_text.strip()),
            "total_length": len(response_text),
            "note": "No structured sections found, treating entire response as essay"
        }
    }


def extract_essay_only(response_text: str, strategy: ParseStrategy = "xml_tags") -> str:
    """Extract only the essay portion from a generation response.
    
    Args:
        response_text: Raw LLM generation response text
        strategy: The parsing strategy to use
        
    Returns:
        The essay content as a string
        
    Raises:
        ValueError: If response cannot be parsed or essay not found
    """
    parsed = parse_generation_response(response_text, strategy)
    return parsed["essay"]


def validate_essay_content(essay_content: str) -> Dict[str, Any]:
    """Validate extracted essay content for quality and completeness.
    
    Args:
        essay_content: The extracted essay content
        
    Returns:
        Dictionary with validation results and metrics
    """
    if not essay_content:
        return {
            "is_valid": False,
            "error": "Empty essay content",
            "metrics": {}
        }
    
    # Basic metrics
    word_count = len(essay_content.split())
    char_count = len(essay_content)
    line_count = len(essay_content.split('\n'))
    
    # Quality checks
    has_paragraphs = len(essay_content.split('\n\n')) > 1
    has_sentences = essay_content.count('.') > 0
    has_content = word_count > 50  # Minimum reasonable essay length
    
    # Determine if valid
    is_valid = has_content and has_paragraphs and has_sentences
    
    return {
        "is_valid": is_valid,
        "error": None if is_valid else "Essay content appears incomplete or malformed",
        "metrics": {
            "word_count": word_count,
            "char_count": char_count,
            "line_count": line_count,
            "has_paragraphs": has_paragraphs,
            "has_sentences": has_sentences,
            "has_minimum_content": has_content
        }
    }
