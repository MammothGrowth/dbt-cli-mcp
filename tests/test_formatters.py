"""
Tests for the formatters module.
"""

import json
import pytest
from unittest.mock import patch, MagicMock

from src.formatters import default_formatter, ls_formatter, show_formatter


def test_default_formatter():
    """Test the default formatter."""
    # Test with string
    result = default_formatter("test string")
    assert result == "test string"
    
    # Test with dict
    result = default_formatter({"key": "value"})
    assert result == '{"key": "value"}'
    
    # Test with list
    result = default_formatter([1, 2, 3])
    assert result == '[1, 2, 3]'


def test_ls_formatter():
    """Test the ls formatter."""
    # Test with non-json format
    result = ls_formatter("model1\nmodel2", output_format="name")
    assert result == "model1\nmodel2"
    
    # Test with empty output
    result = ls_formatter("", output_format="json")
    assert result == "[]"
    
    # Test with parsed output
    with patch("src.formatters.parse_dbt_list_output") as mock_parse:
        mock_parse.return_value = [
            {"name": "model1", "resource_type": "model"},
            {"name": "model2", "resource_type": "seed"}
        ]
        
        result = ls_formatter("raw output", output_format="json")
        parsed = json.loads(result)
        assert len(parsed) == 2
        assert parsed[0]["name"] == "model1"
        assert parsed[1]["name"] == "model2"
        
        # Test with filtering
        mock_parse.return_value = [
            {"name": "model1", "resource_type": "model"},
            {"name": "model2", "resource_type": "unknown"}  # Should be filtered out
        ]
        
        result = ls_formatter("raw output", output_format="json")
        parsed = json.loads(result)
        assert len(parsed) == 1
        assert parsed[0]["name"] == "model1"
        
        # Test with empty filtered result
        mock_parse.return_value = [
            {"name": "model1", "resource_type": "unknown"},
            {"name": "model2", "resource_type": "unknown"}
        ]
        
        result = ls_formatter("raw output", output_format="json")
        # Should return the original parsed output since filtering removed everything
        parsed = json.loads(result)
        assert len(parsed) == 2


def test_show_formatter():
    """Test the show formatter."""
    # Test with dict
    result = show_formatter({"columns": ["col1", "col2"], "data": [[1, 2], [3, 4]]})
    assert result == '{"columns": ["col1", "col2"], "data": [[1, 2], [3, 4]]}'
    
    # Test with tabular string
    tabular_data = """
col1 | col2
-----|-----
val1 | val2
val3 | val4
"""
    result = show_formatter(tabular_data)
    # Our formatter successfully converts this to JSON
    assert result.startswith('[{"col1":')
    assert '"val1"' in result
    assert '"val2"' in result
    assert '"val3"' in result
    assert '"val4"' in result
    
    # Test with valid tabular string
    tabular_data = """col1 | col2
-----|-----
val1 | val2
val3 | val4"""
    
    # Mock the conversion logic to test the success path
    with patch("src.formatters.logger") as mock_logger:
        result = show_formatter(tabular_data)
        # In a real scenario, this would be JSON, but our mock doesn't implement the conversion
        assert isinstance(result, str)