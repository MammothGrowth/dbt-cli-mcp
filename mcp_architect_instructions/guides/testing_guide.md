# Testing Guide for MCP Servers

## Overview

This guide outlines the required testing approaches for Model Context Protocol (MCP) servers. Comprehensive testing is essential to ensure reliability, maintainability, and correct functioning of MCP servers in all scenarios.

## Testing Requirements

Every MCP server MUST implement the following types of tests:

1. **Unit Tests**: Tests for individual functions and methods
2. **Integration Tests**: Tests for how components work together
3. **End-to-End Tests**: Tests for the complete workflow
4. **Edge Case Tests**: Tests for unusual or extreme situations

## Test-Driven Development Approach

For optimal results, follow a test-driven development (TDD) approach:

1. **Write tests first**: Before implementing the functionality, write tests that define the expected behavior
2. **Run tests to see them fail**: Verify that the tests fail as expected
3. **Implement the functionality**: Write the code to make the tests pass
4. **Run tests again**: Verify that the tests now pass
5. **Refactor**: Clean up the code while ensuring tests continue to pass

This approach ensures that your implementation meets the requirements from the start and helps prevent regressions.

## Setting Up the Testing Environment

### pytest.ini Configuration

Create a `pytest.ini` file in the project root with the following configuration:

```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*

# Log format
log_cli = true
log_cli_level = INFO
log_cli_format = %(asctime)s [%(levelname)8s] %(message)s (%(filename)s:%(lineno)s)
log_cli_date_format = %Y-%m-%d %H:%M:%S

# Test selection options
addopts = --strict-markers -v
```

### conftest.py Configuration

Create a `tests/conftest.py` file with shared fixtures and configurations:

```python
import os
import sys
import pytest
import logging

# Add parent directory to path to allow imports from the main package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Configure test logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("test_logger")

# Define fixtures that can be used across tests
@pytest.fixture
def test_fixtures_dir():
    """Return the path to the test fixtures directory."""
    return os.path.join(os.path.dirname(__file__), 'fixtures')

@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set mock environment variables for testing."""
    monkeypatch.setenv("API_KEY", "test_api_key")
    monkeypatch.setenv("DEBUG_MODE", "true")
    # Add other environment variables as needed
```

## Unit Testing MCP Components

### Testing Tools

For each MCP tool, write tests that:

1. Test the tool with valid inputs
2. Test the tool with invalid inputs
3. Test error handling
4. Mock external dependencies

Example:

```python
# tests/test_tools.py
import pytest
from unittest.mock import patch, MagicMock
import sys
import os
from mcp.shared.exceptions import McpError

# Import the module containing your tools
from my_mcp_server import my_tool, process_request

def test_process_request():
    """Test the core business logic function."""
    result = process_request("test_value", 42, True)
    assert "Processed test_value with 42" in result
    assert "optional: True" in result

@pytest.mark.asyncio
async def test_my_tool():
    """Test the my_tool MCP tool function."""
    # Test the tool directly
    result = await my_tool("test_param", 123, True)
    assert "Processed test_param with 123" in result
    
    # Test with different parameters
    result = await my_tool("other_value", 456, False)
    assert "Processed other_value with 456" in result

@pytest.mark.asyncio
async def test_my_tool_error_handling():
    """Test error handling in the my_tool function."""
    # Mock process_request to raise an exception
    with patch('my_mcp_server.process_request', side_effect=ValueError("Test error")):
        with pytest.raises(McpError) as excinfo:
            await my_tool("test", 123)
        assert "Test error" in str(excinfo.value)
```

### Testing Resources

For MCP resources, test both the URI template matching and the resource content:

```python
# tests/test_resources.py
import pytest
from unittest.mock import patch, MagicMock
from my_mcp_server import get_resource

def test_resource_uri_matching():
    """Test that the resource URI template matches correctly."""
    # This would depend on your specific implementation
    # Example: test that "my-resource://123" routes to get_resource with resource_id="123"
    pass

def test_get_resource():
    """Test the resource retrieval function."""
    # Mock any external dependencies
    with patch('my_mcp_server.fetch_resource', return_value="test resource content"):
        result = get_resource("test-id")
        assert result == "test resource content"
```

## Integration Testing

Integration tests verify that multiple components work correctly together:

```python
# tests/test_integration.py
import pytest
from unittest.mock import patch, MagicMock
import requests
import json
from my_mcp_server import my_tool, fetch_external_data, process_data

@pytest.mark.asyncio
async def test_integration_flow():
    """Test the complete integration flow with mocked external API."""
    # Mock the external API
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": [{"id": 1, "value": "test"}]}
    
    with patch('requests.get', return_value=mock_response):
        # Call the tool that uses multiple components
        result = await my_tool("test_param", 123)
        
        # Verify the result includes processed data
        assert "Processed test_param" in result
        assert "id: 1" in result
```

## End-to-End Testing

End-to-end tests verify the complete workflow from input to output:

```python
# tests/test_e2e.py
import pytest
import subprocess
import json
import os

def test_cli_mode():
    """Test running the server in CLI mode."""
    # Run the CLI command
    result = subprocess.run(
        ["uv", "run", "-s", "my_mcp_server.py", "--param1", "test", "--param2", "123"],
        capture_output=True,
        text=True,
        env=os.environ.copy()
    )
    
    # Verify output
    assert result.returncode == 0
    assert "Processed test with 123" in result.stdout

def test_server_initialization():
    """Test that the server initializes correctly in test mode."""
    # Run with --test flag
    result = subprocess.run(
        ["uv", "run", "-s", "my_mcp_server.py", "--test"],
        capture_output=True,
        text=True,
        env=os.environ.copy()
    )
    
    # Verify output
    assert result.returncode == 0
    assert "initialization test successful" in result.stdout
```

## Testing with External Dependencies

When testing code that relies on external APIs or services:

1. Always mock the external dependency in unit tests
2. Optionally test against real APIs in integration tests (if available)
3. Use VCR or similar tools to record and replay API responses

Example with requests-mock:

```python
# tests/test_api_integration.py
import pytest
import requests_mock
from my_mcp_server import fetch_weather_data

def test_fetch_weather_with_mock():
    """Test weather fetching with mocked API."""
    with requests_mock.Mocker() as m:
        # Mock the API endpoint
        m.get(
            "https://api.example.com/weather?city=London",
            json={"temperature": 20, "conditions": "sunny"}
        )
        
        # Call the function
        result = fetch_weather_data("London")
        
        # Verify result
        assert result["temperature"] == 20
        assert result["conditions"] == "sunny"
```

## Testing Error Scenarios

Always test how your code handles errors:

```python
# tests/test_error_handling.py
import pytest
import requests
from unittest.mock import patch
from mcp.shared.exceptions import McpError
from my_mcp_server import fetch_data

@pytest.mark.asyncio
async def test_api_error():
    """Test handling of API errors."""
    # Mock requests to raise an exception
    with patch('requests.get', side_effect=requests.RequestException("Connection error")):
        # Verify the function raises a proper McpError
        with pytest.raises(McpError) as excinfo:
            await fetch_data("test")
        
        # Check error details
        assert "Connection error" in str(excinfo.value)
        assert excinfo.value.error_data.code == "INTERNAL_ERROR"

@pytest.mark.asyncio
async def test_rate_limit():
    """Test handling of rate limiting."""
    # Create mock response for rate limit
    mock_response = MagicMock()
    mock_response.status_code = 429
    mock_response.json.return_value = {"error": "Rate limit exceeded"}
    
    with patch('requests.get', return_value=mock_response):
        with pytest.raises(McpError) as excinfo:
            await fetch_data("test")
        
        assert "Rate limit" in str(excinfo.value)
```

## Running Tests with UV

Always use `uv` to run tests to ensure dependencies are correctly loaded:

```bash
# Run all tests
uv run -m pytest

# Run specific test file
uv run -m pytest tests/test_tools.py

# Run specific test function
uv run -m pytest tests/test_tools.py::test_my_tool

# Run with verbose output
uv run -m pytest -v

# Run with coverage report
uv run -m pytest --cov=my_mcp_server
```

## Test Coverage

Aim for at least 90% code coverage:

```bash
# Run with coverage
uv run -m pytest --cov=my_mcp_server

# Generate HTML coverage report
uv run -m pytest --cov=my_mcp_server --cov-report=html
```

## Task-Level Testing Requirements

Each implementation task MUST include its own testing requirements:

1. **Unit Tests**: Tests for the specific functionality implemented in the task
2. **Integration Tests**: Tests to ensure the new functionality works with existing code
3. **Regression Tests**: Tests to ensure existing functionality is not broken

## Testing After Each Task

After completing each task, you MUST:

1. Run the tests for the current task:
   ```bash
   uv run -m pytest tests/test_current_task.py
   ```

2. Run regression tests to ensure existing functionality still works:
   ```bash
   uv run -m pytest
   ```

3. Document any test failures and fixes in the work progress log

## Best Practices for Effective Testing

1. **Test Isolation**: Each test should be independent and not rely on other tests
2. **Descriptive Test Names**: Use clear, descriptive names that explain what's being tested
3. **One Assertion Per Test**: Focus each test on a single behavior or requirement
4. **Mock External Dependencies**: Always mock external APIs, databases, and file systems
5. **Test Edge Cases**: Include tests for boundary conditions and unusual inputs
6. **Test Error Handling**: Verify that errors are handled gracefully
7. **Keep Tests Fast**: Tests should execute quickly to encourage frequent running
8. **Use Fixtures for Common Setup**: Reuse setup code with pytest fixtures
9. **Document Test Requirements**: Clearly document what each test verifies
10. **Run Tests Frequently**: Run tests after every significant change

## Next Steps

After implementing tests, refer to:
- [Registration Guide](registration_guide.md) for registering your MCP server
- [Implementation Guide](implementation_guide.md) for MCP server implementation patterns