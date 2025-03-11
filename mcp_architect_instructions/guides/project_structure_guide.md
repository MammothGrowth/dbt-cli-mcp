# Project Structure Guide for MCP Servers

## Overview

This guide details the required organization for Model Context Protocol (MCP) server projects. Following this standardized structure ensures maintainability, testability, and ease of understanding across MCP server implementations.

## Required Project Organization

For standardization and maintainability, you MUST organize your MCP server project using the following structure:

```
my-mcp-server/
├── README.md                 # Project documentation
├── my_mcp_server.py          # Main MCP server implementation (single-file approach)
├── planning/                 # Planning artifacts directory (all colocated)
│   ├── implementation_plan.md # Architect's implementation plan
│   ├── work_progress_log.md  # Detailed work progress tracking
│   └── tasks/                # Task definitions directory
│       ├── T1_Project_Setup.md
│       ├── T2_Component1.md
│       └── T3_Component2.md
├── docs/                     # Additional documentation directory
│   └── architecture.md       # Architecture documentation
├── tests/                    # Test directory
│   ├── __init__.py           # Package initialization for tests
│   ├── conftest.py           # Pytest configuration and fixtures
│   ├── test_my_mcp_server.py # Server tests
│   └── test_utils.py         # Utility tests
├── pytest.ini                # Pytest configuration
└── .env.example              # Example environment variables file
```

## Root Directory Files

### README.md

The README.md file MUST contain:

1. Project name and purpose
2. Installation instructions
3. Usage instructions (both as CLI and MCP server)
4. Required environment variables description
5. Example commands
6. Testing instructions

Example:

```markdown
# Weather MCP Server

MCP server for retrieving weather forecasts from OpenWeather API.

## Installation

This project uses uv for dependency management.

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Usage

### Running as MCP Server

```bash
export OPENWEATHER_API_KEY=your_api_key_here
uv run -s weather_mcp.py
```

### Running as CLI

```bash
uv run -s weather_mcp.py --city "New York" --days 3
```

## Environment Variables

- `OPENWEATHER_API_KEY`: Your OpenWeather API key (required)

## Testing

```bash
uv run -m pytest
```
```

### my_mcp_server.py

This is the main MCP server implementation file. For simple MCP servers, a single-file approach is preferred. For more complex servers, consider using a multi-file approach with a `src/` directory.

### .env.example

This file MUST list all required environment variables without actual values:

```
# API keys
API_KEY=
API_SECRET=

# Settings
DEBUG_MODE=
```

## Planning Directory (planning/)

The planning directory contains all colocated planning artifacts to ensure they remain together and easy to reference:

### implementation_plan.md

The detailed implementation plan created during the planning phase. It should follow the structure in [Implementation Plan Template](../templates/implementation_plan_template.md).

### work_progress_log.md

Tracks implementation progress across tasks. See [Work Progress Log Template](../templates/work_progress_log_template.md) for the required structure.

### tasks/ Directory

Contains individual task definition files, one per task, following the [Task Template](../templates/task_template.md):

- `T1_Project_Setup.md`: Task 1 definition
- `T2_Component1.md`: Task 2 definition
- And so on...

Each task file should be prefixed with its task ID for easy reference and sequencing.

## Documentation Directory (docs/)

### architecture.md

This document should describe the high-level architecture of your MCP server, including:

1. Component diagram
2. Data flow
3. Integration points
4. Design decisions and rationales

## Tests Directory (tests/)

### __init__.py

A blank file that makes the tests directory a Python package.

### conftest.py

Contains shared fixtures and configuration for pytest. At minimum, should include:

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
```

### Test files

Test files should be named to clearly identify what they're testing:

- `test_my_mcp_server.py`: Tests for the main server functionality
- `test_utils.py`: Tests for utility functions
- `test_integration.py`: Integration tests
- `test_e2e.py`: End-to-end tests
- `test_performance.py`: Performance tests

## Multi-File Organization (For Complex MCP Servers)

For more complex MCP servers, use this multi-file organization:

```
my-mcp-server/
├── README.md                 # Project documentation
├── main.py                   # Entry point (thin wrapper)
├── planning/                 # Planning artifacts (all colocated)
│   ├── implementation_plan.md # Architect's implementation plan
│   ├── work_progress_log.md  # Work progress tracking
│   └── tasks/                # Task definitions
│       ├── T1_Project_Setup.md
│       ├── T2_Component1.md
│       └── T3_Component2.md
├── src/                      # Source code directory
│   ├── __init__.py           # Package initialization
│   ├── server.py             # MCP server implementation
│   ├── tools/                # Tool implementations
│   │   ├── __init__.py
│   │   └── my_tools.py
│   ├── resources/            # Resource implementations
│   │   ├── __init__.py
│   │   └── my_resources.py
│   └── utils/                # Utility functions
│       ├── __init__.py
│       └── helpers.py
├── docs/                     # Additional documentation
│   └── architecture.md
├── tests/                    # Test directory
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_server.py
│   ├── test_tools.py
│   └── test_resources.py
├── pytest.ini                # Pytest configuration
└── .env.example              # Example environment variables
```

## Best Practices

1. **Colocate Planning Artifacts**: Always keep implementation plan, task definitions, and work progress log together in the planning/ directory.
2. **Single Responsibility**: Each file should have a single responsibility.
3. **Consistent Naming**: Use consistent naming conventions.
4. **Logical Organization**: Group related files together.
5. **Documentation**: Document the purpose of each directory and main files.
6. **Separation of Concerns**: Separate tools, resources, and utilities.

By following this structure, your MCP server will be well-organized, maintainable, and easier for others to understand and contribute to.