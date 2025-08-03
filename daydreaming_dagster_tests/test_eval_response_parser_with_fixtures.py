"""Test evaluation response parser with fixture files."""

import pytest
from pathlib import Path
from daydreaming_dagster.utils.eval_response_parser import parse_llm_response


class TestEvalResponseParser:
    """Test the evaluation response parser with known fixture files."""
    
    @pytest.fixture
    def fixtures_dir(self):
        """Path to evaluation response fixtures."""
        return Path(__file__).parent / "fixtures" / "eval_responses"
    
    def test_known_fixture_scores(self, fixtures_dir):
        """Test parser on known fixture files with expected scores."""
        # Expected scores based on manual verification of fixture files
        expected_scores = {
            "004.txt": 7.0,   # **Total Score: 7/10**
            "014.txt": 4.0,   # **4/10** 
            "019.txt": 8.0,   # Score pattern
            "022.txt": 2.0,   # **2/10** after ### **Final Score**
            "027.txt": 7.0,   # Score pattern
            "034.txt": 7.0,   # Score pattern
            "035.txt": 5.0,   # **SCORE: 5/10**
            "039.txt": 4.0,   # Score pattern
            "053.txt": 8.0,   # **SCORE: 8/10**
            "057.txt": 3.0,   # Score pattern  
            "076.txt": 6.5,   # **Total Score: 6.5/10**
            "105.txt": 8.0,   # Score pattern
            "110.txt": 3.0,   # **3/10**
            "112.txt": 6.0,   # Score pattern
            "128.txt": 4.0,   # Score pattern
            "163.txt": 4.0,   # **Final Score**: **4/10**
        }
        
        for filename, expected_score in expected_scores.items():
            fixture_path = fixtures_dir / filename
            
            # Ensure fixture file exists
            assert fixture_path.exists(), f"Fixture file {filename} not found"
            
            # Read file content
            with open(fixture_path, 'r', encoding='utf-8') as f:
                response_text = f.read()
            
            # Parse the response
            result = parse_llm_response(response_text)
            
            # Verify the score matches expected value
            assert result["score"] == expected_score, (
                f"File {filename}: expected {expected_score}, got {result['score']}"
            )
            assert result["error"] is None, (
                f"File {filename}: unexpected error: {result['error']}"
            )
    
    def test_parser_handles_various_formats(self, fixtures_dir):
        """Test that parser handles various score formatting patterns."""
        test_cases = [
            ("004.txt", "Total Score"),  # **Total Score: 7/10**
            ("035.txt", "SCORE"),        # **SCORE: 5/10**
            ("076.txt", "decimal"),      # **Total Score: 6.5/10**
            ("022.txt", "no_colon"),     # ### **Final Score** \n **2/10**
            ("163.txt", "final_score"),  # **Final Score**: \n **4/10**
        ]
        
        for filename, format_type in test_cases:
            fixture_path = fixtures_dir / filename
            
            with open(fixture_path, 'r', encoding='utf-8') as f:
                response_text = f.read()
            
            # Should parse without errors
            result = parse_llm_response(response_text)
            assert isinstance(result["score"], float), (
                f"File {filename} ({format_type}): score should be float, got {type(result['score'])}"
            )
            assert 0 <= result["score"] <= 10, (
                f"File {filename} ({format_type}): score should be 0-10, got {result['score']}"
            )
    
    def test_parser_error_handling(self):
        """Test parser error handling with invalid inputs."""
        
        # Empty response
        with pytest.raises(ValueError, match="Empty or whitespace-only response"):
            parse_llm_response("")
        
        # Response with no score
        with pytest.raises(ValueError, match="No SCORE field found in response"):
            parse_llm_response("This is just some text without any score.")
        
        # Score out of range should be caught during validation
        # (This would need a crafted response, but our current parser validates range)
    
    def test_parser_returns_correct_structure(self, fixtures_dir):
        """Test that parser returns expected dictionary structure."""
        fixture_path = fixtures_dir / "004.txt"
        
        with open(fixture_path, 'r', encoding='utf-8') as f:
            response_text = f.read()
        
        result = parse_llm_response(response_text)
        
        # Check structure
        assert isinstance(result, dict)
        assert "score" in result
        assert "error" in result
        assert isinstance(result["score"], float)
        assert result["error"] is None or isinstance(result["error"], str)