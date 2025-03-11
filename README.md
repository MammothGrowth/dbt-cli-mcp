# DBT CLI MCP Server

A Model Context Protocol (MCP) server that wraps the dbt CLI tool, enabling AI coding agents to interact with dbt projects through standardized MCP tools.

## Features

- Execute dbt commands through MCP tools
- Support for all major dbt operations (run, test, compile, etc.)
- Mock mode for testing without a dbt installation
- Environment variable management for dbt projects
- Configurable dbt executable path

## Installation

### Prerequisites

- Python 3.8 or higher
- `uv` tool for Python environment management
- dbt CLI (optional, can run in mock mode for testing)

### Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/dbt-cli-mcp.git
cd dbt-cli-mcp

# Create and activate a virtual environment
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
uv add mcp[cli] python-dotenv

# For development, install development dependencies
uv add -d pytest pytest-asyncio pytest-cov
```

## Usage

### Running the Server

```bash
# Run the server using MCP development tools
mcp dev dbt_cli_mcp/server.py

# Or run directly
python -m dbt_cli_mcp.server

# With custom configuration
python -m dbt_cli_mcp.server --dbt-path /path/to/dbt --log-level DEBUG
```

### Command Line Options

- `--dbt-path`: Path to dbt executable (default: "dbt")
- `--env-file`: Path to environment file (default: ".env")
- `--log-level`: Logging level (default: "INFO")
- `--mock-mode`: Enable mock mode for testing (default: false)

### Environment Variables

The server can also be configured using environment variables:

- `DBT_PATH`: Path to dbt executable
- `ENV_FILE`: Path to environment file
- `LOG_LEVEL`: Logging level
- `MOCK_MODE`: Enable mock mode for testing

### Using with MCP Clients

To use the server with an MCP client like Claude for Desktop, add it to the client's configuration:

```json
{
  "mcpServers": {
    "dbt": {
      "command": "uv",
      "args": ["--directory", "/path/to/dbt-cli-mcp", "run", "dbt_cli_mcp/server.py"],
      "env": {
        "DBT_PATH": "/absolute/path/to/dbt",
        "ENV_FILE": ".env"
      }
    }
  }
}
```

## Available Tools

The server provides the following MCP tools:

- `dbt_run`: Run dbt models
- `dbt_test`: Run dbt tests
- `dbt_ls`: List dbt resources
- `dbt_compile`: Compile dbt models
- `dbt_debug`: Debug dbt project setup
- `dbt_deps`: Install dbt package dependencies
- `dbt_seed`: Load CSV files as seed data
- `dbt_show`: Preview model results
- `dbt_build`: Run build command
- `configure_dbt_path`: Configure dbt executable path
- `set_mock_mode`: Enable or disable mock mode for testing

## Development

### Testing

```bash
# Run all tests
python -m pytest

# Run tests with coverage report
python -m pytest --cov=dbt_cli_mcp

# Run specific test file
python -m pytest tests/test_command.py
```

### Mock Mode

The server can run in mock mode, which doesn't require a dbt installation. This is useful for testing and development:

```bash
# Run in mock mode
python -m dbt_cli_mcp.server --mock-mode
```

In mock mode, the server will use mock responses from the `tests/mock_responses` directory instead of executing actual dbt commands.

## License

MIT