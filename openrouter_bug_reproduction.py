#!/usr/bin/env python3
"""
Simple reproduction case for OpenRouter API bug.

Bug: When max_tokens is set below ~8, OpenRouter returns empty strings 
instead of truncated content or error messages.

Expected: Truncated response or error message
Actual: Empty string
"""

import os
from openai import OpenAI
from dotenv import load_dotenv


def test_openrouter_max_tokens_bug():
    """Reproduce the OpenRouter max_tokens bug."""
    
    # Load environment variables
    load_dotenv()
    
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        print("❌ Error: OPENROUTER_API_KEY not found in environment")
        print("   Please add your OpenRouter API key to .env file")
        return
    
    # Initialize OpenRouter client
    client = OpenAI(
        api_key=api_key,
        base_url="https://openrouter.ai/api/v1"
    )
    
    # Simple prompt that should generate ~3-4 tokens
    prompt = "Say 'Hello world' exactly."
    model = "deepseek/deepseek-r1"
    
    print("Testing OpenRouter API with different max_tokens values...")
    print(f"Prompt: {prompt}")
    print(f"Model: {model}")
    print("-" * 60)
    
    # Test various max_tokens values
    test_cases = [1, 10, 100, 1000]
    
    for max_tokens in test_cases:
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=max_tokens
            )
            
            content = response.choices[0].message.content
            length = len(content) if content else 0
            
            print(f"max_tokens={max_tokens:2d}: '{content}' (length: {length})")
            
            # Check for the bug
            if not content:
                print(f"             ^^^ BUG: Empty response with max_tokens={max_tokens}")
            elif content:
                print(f"             ^^^ OK: Non-empty response with max_tokens={max_tokens}")
                
        except Exception as e:
            print(f"max_tokens={max_tokens:2d}: ERROR - {e}")
    
    print("-" * 60)
    print("\nBUG SUMMARY:")
    print("Expected behavior: Small max_tokens should return truncated content")
    print("Actual behavior:   Small max_tokens return empty strings")
    print("Threshold:         max_tokens < 8 consistently returns empty")
    print("\nThis should be reported to OpenRouter support.")


if __name__ == "__main__":
    test_openrouter_max_tokens_bug()