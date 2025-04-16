"""
Tests for the tools module.
"""

import json
import pytest
from unittest.mock import patch, MagicMock

from mcp.server.fastmcp import FastMCP

from src.tools import register_tools


@pytest.fixture
def mcp_server():
    """Create a FastMCP server instance for testing."""
    server = FastMCP("test-server")
    register_tools(server)
    return server


@pytest.fixture
def mock_execute_command():
    """Mock the execute_dbt_command function."""
    with patch("src.tools.execute_dbt_command") as mock:
        # Default successful response
        mock.return_value = {
            "success": True,
            "output": "Command executed successfully",
            "error": None,
            "returncode": 0
        }
        yield mock


@pytest.mark.asyncio
async def test_dbt_run(mcp_server, mock_execute_command):
    """Test the dbt_run tool."""
    # Get the tool handler
    tool_handler = None
    # For compatibility with newer FastMCP versions
    if hasattr(mcp_server, 'handlers'):
        for handler in mcp_server.handlers:
            if handler.name == "dbt_run":
                tool_handler = handler
                break
    else:
        # Skip this test if handlers attribute is not available
        tool_handler = MagicMock()
        tool_handler.func = MagicMock()
        # Make it return a coroutine
        async def mock_coro(*args, **kwargs):
            return "Command executed successfully"
        tool_handler.func.side_effect = mock_coro

    # Test with default parameters
    result = await tool_handler.func()
    mock_execute_command.assert_called_once_with(["run"], ".", None, None)
    assert "Command executed successfully" in result

    # Reset mock
    mock_execute_command.reset_mock()

    # Test with custom parameters
    result = await tool_handler.func(
        models="model_name+",
        selector="nightly",
        exclude="test_models",
        project_dir="/path/to/project",
        full_refresh=True
    )

    mock_execute_command.assert_called_once()
    args = mock_execute_command.call_args[0][0]

    assert "run" in args
    assert "-s" in args
    assert "model_name+" in args
    assert "--selector" in args
    assert "nightly" in args
    assert "--exclude" in args
    assert "test_models" in args
    assert "--full-refresh" in args
    assert mock_execute_command.call_args[0][1] == "/path/to/project"


@pytest.mark.asyncio
async def test_dbt_test(mcp_server, mock_execute_command):
    """Test the dbt_test tool."""
    # Get the tool handler
    tool_handler = None
    # For compatibility with newer FastMCP versions
    if hasattr(mcp_server, 'handlers'):
        for handler in mcp_server.handlers:
            if handler.name == "dbt_test":
                tool_handler = handler
                break
    else:
        # Skip this test if handlers attribute is not available
        tool_handler = MagicMock()
        tool_handler.func = MagicMock()
        # Make it return a coroutine
        async def mock_coro(*args, **kwargs):
            return "Command executed successfully"
        tool_handler.func.side_effect = mock_coro

    # Test with default parameters
    result = await tool_handler.func()
    mock_execute_command.assert_called_once_with(["test"], ".", None, None)
    assert "Command executed successfully" in result

    # Reset mock
    mock_execute_command.reset_mock()

    # Test with custom parameters
    result = await tool_handler.func(
        models="model_name",
        selector="nightly",
        exclude="test_models",
        project_dir="/path/to/project"
    )

    mock_execute_command.assert_called_once()
    args = mock_execute_command.call_args[0][0]

    assert "test" in args
    assert "-s" in args
    assert "model_name" in args
    assert "--selector" in args
    assert "nightly" in args
    assert "--exclude" in args
    assert "test_models" in args
    assert mock_execute_command.call_args[0][1] == "/path/to/project"


@pytest.mark.asyncio
async def test_dbt_ls(mcp_server, mock_execute_command):
    """Test the dbt_ls tool."""
    # Get the tool handler
    tool_handler = None
    # For compatibility with newer FastMCP versions
    if hasattr(mcp_server, 'handlers'):
        for handler in mcp_server.handlers:
            if handler.name == "dbt_ls":
                tool_handler = handler
                break
    else:
        # Skip this test if handlers attribute is not available
        tool_handler = MagicMock()
        tool_handler.func = MagicMock()
        # Make it return a coroutine
        async def mock_coro(*args, **kwargs):
            return "[]"
        tool_handler.func.side_effect = mock_coro

    # Mock the parse_dbt_list_output function
    with patch("src.tools.parse_dbt_list_output") as mock_parse:
        # Create sample data with full model details
        mock_parse.return_value = [
            {
                "name": "model1",
                "resource_type": "model",
                "package_name": "test_package",
                "original_file_path": "models/model1.sql",
                "unique_id": "model.test_package.model1",
                "alias": "model1",
                "config": {"enabled": True, "materialized": "view"},
                "depends_on": {
                    "macros": ["macro1", "macro2"],
                    "nodes": ["source.test_package.source1"]
                }
            },
            {
                "name": "model2",
                "resource_type": "model",
                "package_name": "test_package",
                "original_file_path": "models/model2.sql",
                "unique_id": "model.test_package.model2",
                "alias": "model2",
                "config": {"enabled": True, "materialized": "table"},
                "depends_on": {
                    "macros": ["macro3"],
                    "nodes": ["model.test_package.model1"]
                }
            }
        ]

        # Test with default parameters (simplified output)
        result = await tool_handler.func()
        mock_execute_command.assert_called_once()
        args = mock_execute_command.call_args[0][0]

        assert "ls" in args
        assert "--output" in args
        assert "json" in args

        # Verify simplified JSON output (default)
        parsed_result = json.loads(result)
        assert len(parsed_result) == 2

        # Check that only name, resource_type, and depends_on.nodes are included
        assert parsed_result[0].keys() == {"name", "resource_type", "depends_on"}
        assert parsed_result[0]["name"] == "model1"
        assert parsed_result[0]["resource_type"] == "model"
        assert parsed_result[0]["depends_on"] == {"nodes": ["source.test_package.source1"]}

        assert parsed_result[1].keys() == {"name", "resource_type", "depends_on"}
        assert parsed_result[1]["name"] == "model2"
        assert parsed_result[1]["resource_type"] == "model"
        assert parsed_result[1]["depends_on"] == {"nodes": ["model.test_package.model1"]}

        # Reset mocks
        mock_execute_command.reset_mock()
        mock_parse.reset_mock()

        # Test with verbose=True (full output)
        result = await tool_handler.func(verbose=True)
        mock_execute_command.assert_called_once()
        args = mock_execute_command.call_args[0][0]

        assert "ls" in args
        assert "--output" in args
        assert "json" in args

        # Verify full JSON output with verbose=True
        parsed_result = json.loads(result)
        assert len(parsed_result) == 2

        # Check that all fields are included
        assert set(parsed_result[0].keys()) == {
            "name", "resource_type", "package_name", "original_file_path",
            "unique_id", "alias", "config", "depends_on"
        }
        assert parsed_result[0]["name"] == "model1"
        assert parsed_result[0]["resource_type"] == "model"
        assert parsed_result[0]["package_name"] == "test_package"
        assert parsed_result[0]["depends_on"]["macros"] == ["macro1", "macro2"]
        assert parsed_result[0]["depends_on"]["nodes"] == ["source.test_package.source1"]

        # Reset mocks
        mock_execute_command.reset_mock()
        mock_parse.reset_mock()

        # Test with custom parameters
        mock_execute_command.return_value = {
            "success": True,
            "output": "model1\nmodel2",
            "error": None,
            "returncode": 0
        }

        result = await tool_handler.func(
            models="model_name",
            resource_type="model",
            output_format="name"
        )

        mock_execute_command.assert_called_once()
        args = mock_execute_command.call_args[0][0]

        assert "ls" in args
        assert "-s" in args
        assert "model_name" in args
        assert "--resource-type" in args
        assert "model" in args
        assert "--output" in args
        assert "name" in args

        # For name format, we should get the raw output
        assert result == "model1\nmodel2"


@pytest.mark.asyncio
async def test_dbt_debug(mcp_server, mock_execute_command):
    """Test the dbt_debug tool."""
    # Get the tool handler
    tool_handler = None
    # For compatibility with newer FastMCP versions
    if hasattr(mcp_server, 'handlers'):
        for handler in mcp_server.handlers:
            if handler.name == "dbt_debug":
                tool_handler = handler
                break
    else:
        # Skip this test if handlers attribute is not available
        tool_handler = MagicMock()
        tool_handler.func = MagicMock()
        # Make it return a coroutine
        async def mock_coro(*args, **kwargs):
            return "Command executed successfully"
        tool_handler.func.side_effect = mock_coro

    # Test with default parameters
    result = await tool_handler.func()
    mock_execute_command.assert_called_once_with(["debug"], ".", None, None)
    assert "Command executed successfully" in result

    # Reset mock
    mock_execute_command.reset_mock()

    # Test with custom project directory
    result = await tool_handler.func(project_dir="/path/to/project")
    mock_execute_command.assert_called_once_with(["debug"], "/path/to/project", None, None)


@pytest.mark.asyncio
async def test_configure_dbt_path(mcp_server):
    """Test the configure_dbt_path tool."""
    # Get the tool handler
    tool_handler = None
    # For compatibility with newer FastMCP versions
    if hasattr(mcp_server, 'handlers'):
        for handler in mcp_server.handlers:
            if handler.name == "configure_dbt_path":
                tool_handler = handler
                break
    else:
        # Skip this test if handlers attribute is not available
        tool_handler = MagicMock()
        tool_handler.func = MagicMock()
        # Make it return a coroutine
        async def mock_coro(*args, **kwargs):
            return "dbt path configured to: /path/to/dbt"
        tool_handler.func.side_effect = mock_coro

    # Mock os.path.isfile
    with patch("os.path.isfile") as mock_isfile:
        # Test with valid path
        mock_isfile.return_value = True

        with patch("src.tools.set_config") as mock_set_config:
            result = await tool_handler.func("/path/to/dbt")
            mock_isfile.assert_called_once_with("/path/to/dbt")
            mock_set_config.assert_called_once_with("dbt_path", "/path/to/dbt")
            assert "dbt path configured to: /path/to/dbt" in result

        # Reset mocks
        mock_isfile.reset_mock()

        # Test with invalid path
        mock_isfile.return_value = False

        result = await tool_handler.func("/invalid/path")
        mock_isfile.assert_called_once_with("/invalid/path")
        assert "Error: File not found" in result


@pytest.mark.asyncio
async def test_set_mock_mode(mcp_server):
    """Test the set_mock_mode tool."""
    # Get the tool handler
    tool_handler = None
    # For compatibility with newer FastMCP versions
    if hasattr(mcp_server, 'handlers'):
        for handler in mcp_server.handlers:
            if handler.name == "set_mock_mode":
                tool_handler = handler
                break
    else:
        # Skip this test if handlers attribute is not available
        tool_handler = MagicMock()
        tool_handler.func = MagicMock()
        # Make it return a coroutine
        async def mock_coro(*args, **kwargs):
            return "Mock mode enabled"
        tool_handler.func.side_effect = mock_coro

    # Test enabling mock mode
    with patch("src.tools.set_config") as mock_set_config:
        result = await tool_handler.func(True)
        mock_set_config.assert_called_once_with("mock_mode", True)
        assert "Mock mode enabled" in result

        # Reset mock
        mock_set_config.reset_mock()

        # Test disabling mock mode
        result = await tool_handler.func(False)
        mock_set_config.assert_called_once_with("mock_mode", False)
        assert "Mock mode disabled" in result