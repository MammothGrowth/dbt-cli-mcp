"""
Tests for the command module.
"""

import os
import json
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.command import (
    load_environment,
    execute_dbt_command,
    parse_dbt_list_output,
    load_mock_response
)


@pytest.fixture
def mock_env_file(tmp_path):
    """Create a mock .env file."""
    env_file = tmp_path / ".env"
    env_file.write_text("TEST_VAR=test_value\nDBT_PROFILES_DIR=/path/to/profiles")
    # Set environment variable for test
    os.environ["DBT_PROFILES_DIR"] = "/path/to/profiles"
    return env_file


@pytest.mark.asyncio
async def test_load_environment(mock_env_file):
    """Test loading environment variables from .env file."""
    # Save original environment
    original_env = os.environ.copy()
    
    try:
        # Test with existing .env file
        env_vars = load_environment(str(mock_env_file.parent))
        assert "TEST_VAR" in env_vars
        assert env_vars["TEST_VAR"] == "test_value"
        assert "DBT_PROFILES_DIR" in env_vars
        assert env_vars["DBT_PROFILES_DIR"] == "/path/to/profiles"
        
        # Test with non-existent .env file
        env_vars = load_environment("/non/existent/path")
        assert "TEST_VAR" not in env_vars
    finally:
        # Restore original environment
        os.environ.clear()
        os.environ.update(original_env)


@pytest.mark.asyncio
async def test_execute_dbt_command_mock_mode():
    """Test executing dbt command in mock mode."""
    mock_response = {
        "success": True,
        "output": {"test": "data"},
        "error": None,
        "returncode": 0
    }
    
    result = await execute_dbt_command(
        ["run"],
        mock_mode=True,
        mock_response=mock_response
    )
    
    assert result == mock_response
    assert result["success"] is True
    assert result["output"] == {"test": "data"}


@pytest.mark.asyncio
@patch("asyncio.create_subprocess_exec")
async def test_execute_dbt_command_real_mode(mock_subprocess):
    """Test executing dbt command in real mode."""
    # Mock subprocess
    process_mock = MagicMock()
    process_mock.returncode = 0
    
    # Create a coroutine for communicate
    async def mock_communicate():
        return ('{"test": "data"}', '')
    process_mock.communicate = mock_communicate
    
    mock_subprocess.return_value = process_mock
    
    result = await execute_dbt_command(["run"])
    
    assert result["success"] is True
    assert result["output"] == {"test": "data"}
    assert result["error"] is None
    assert result["returncode"] == 0
    
    # Test with error
    process_mock.returncode = 1
    
    # Create a coroutine for communicate with error
    async def mock_communicate_error():
        return ('', 'Error message')
    process_mock.communicate = mock_communicate_error
    
    result = await execute_dbt_command(["run"])
    
    assert result["success"] is False
    assert result["error"] == "Error message"
    assert result["returncode"] == 1


def test_parse_dbt_list_output():
    """Test parsing dbt list output."""
    # Test with dictionary containing nodes
    nodes_dict = {
        "nodes": {
            "model.example.model1": {"name": "model1"},
            "model.example.model2": {"name": "model2"}
        }
    }
    
    result = parse_dbt_list_output(nodes_dict)
    assert len(result) == 2
    assert {"name": "model1"} in result
    assert {"name": "model2"} in result
    
    # Test with list
    models_list = [{"name": "model1"}, {"name": "model2"}]
    
    result = parse_dbt_list_output(models_list)
    assert result == models_list
    
    # Test with JSON string containing nodes
    json_str = json.dumps(nodes_dict)
    
    result = parse_dbt_list_output(json_str)
    assert len(result) == 2
    assert {"name": "model1"} in result
    assert {"name": "model2"} in result
    
    # Test with plain text
    text = "model1\nmodel2\n"
    
    result = parse_dbt_list_output(text)
    assert len(result) == 2
    assert {"name": "model1"} in result
    assert {"name": "model2"} in result


@pytest.mark.asyncio
async def test_load_mock_response():
    """Test loading mock response."""
    # Create a temporary mock response file
    mock_dir = Path(__file__).parent / "mock_responses"
    
    # Test with existing mock response
    response = await load_mock_response("run")
    assert response is not None
    assert "success" in response
    assert response["success"] is True
    
    # Test with non-existent mock response
    response = await load_mock_response("non_existent")
    assert response is None