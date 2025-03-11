# Environment Setup Guide for MCP Servers

## Overview

This guide covers how to set up the development environment for Model Context Protocol (MCP) servers in Python using uv for dependency management.

## Installing uv

`uv` is a fast Python package installer and environment manager. To install it:

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Verify installation
uv --version
```

## Initial Project Structure

Create a new directory for your MCP server project:

```bash
mkdir my-mcp-server
cd my-mcp-server
```

## Setting Up Testing Environment

Create a proper pytest configuration to ensure tests work correctly with uv:

1. Create a `pytest.ini` file in the project root:

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

2. Create a `tests/__init__.py` file to make the tests directory a package:

```python
# tests/__init__.py
# This file makes the tests directory a package
```

3. Create a `tests/conftest.py` file for shared fixtures:

```python
# tests/conftest.py
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
```

## Running Your MCP Server


### With Environment Variables

For environment variables, create a `.env` file:

```
API_KEY=your_api_key_here
DEBUG_MODE=true
```

Then run with the `--env-file` option:

```bash
uv run my_mcp_server.py --env-file .env
```

Or export environment variables directly:

```bash
export API_KEY=your_api_key_here
uv run my_mcp_server.py
```


## Next Steps

After setting up your environment, refer to:
- [Project Structure Guide](project_structure_guide.md) for required project organization
- [Dependency Guide](dependency_guide.md) for dependency management with uv
- [Implementation Guide](implementation_guide.md) for MCP server implementation patterns