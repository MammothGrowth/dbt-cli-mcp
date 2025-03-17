# DBT CLI MCP Server

A Model Context Protocol (MCP) server that wraps the dbt CLI tool, enabling AI coding agents to interact with dbt projects through standardized MCP tools.

## Features

- Execute dbt commands through MCP tools
- Support for all major dbt operations (run, test, compile, etc.)
- Command-line interface for direct interaction
- Environment variable management for dbt projects
- Configurable dbt executable path

## Installation

### Prerequisites

- Python 3.10 or higher
- `uv` tool for Python environment management
- dbt CLI installed

### Setup

```bash
# Clone the repository with submodules
git clone --recurse-submodules https://github.com/yourusername/dbt-cli-mcp.git
cd dbt-cli-mcp

# If you already cloned without --recurse-submodules, initialize the submodule
# git submodule update --init

# Create and activate a virtual environment
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
uv pip install -e .

# For development, install development dependencies
uv pip install -e ".[dev]"
```

## Usage

### Command Line Interface

The package provides a command-line interface for direct interaction with dbt:

```bash
# Run dbt models
dbt-mcp run --models customers --project-dir /path/to/project

# List dbt resources
dbt-mcp ls --resource-type model --output-format json

# Run dbt tests
dbt-mcp test --project-dir /path/to/project

# Get help
dbt-mcp --help
dbt-mcp run --help
```

You can also use the module directly:

```bash
python -m src.cli run --models customers --project-dir /path/to/project
```

### Running the Server

```bash
# Run the server using MCP development tools
mcp dev src/server.py

# Or run directly
python -m src.server

# With custom configuration
python -m src.server --dbt-path /path/to/dbt --log-level DEBUG
```

### Command Line Options

- `--dbt-path`: Path to dbt executable (default: "dbt")
- `--env-file`: Path to environment file (default: ".env")
- `--log-level`: Logging level (default: "INFO")

### Environment Variables

The server can also be configured using environment variables:

- `DBT_PATH`: Path to dbt executable
- `ENV_FILE`: Path to environment file
- `LOG_LEVEL`: Logging level

### Using with MCP Clients

To use the server with an MCP client like Claude for Desktop, add it to the client's configuration:

```json
{
  "mcpServers": {
    "dbt": {
      "command": "uv",
      "args": ["--directory", "/path/to/dbt-cli-mcp", "run", "src/server.py"],
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

## Development

### Integration Tests

The project includes integration tests that verify functionality against a real dbt project:

```bash
# Run all integration tests
python integration_tests/run_all.py

# Run a specific integration test
python integration_tests/test_dbt_run.py
```

#### Test Project Setup

The integration tests use the jaffle_shop_duckdb project which is included as a Git submodule in the dbt_integration_tests directory. When you clone the repository with `--recurse-submodules` as mentioned in the Setup section, this will automatically be initialized.

If you need to update the test project to the latest version from the original repository:

```bash
git submodule update --remote dbt_integration_tests/jaffle_shop_duckdb
```

If you're seeing errors about missing files in the jaffle_shop_duckdb directory, you may need to initialize the submodule:

```bash
git submodule update --init
```

## Converting to Git Submodule

If you have an existing clone of this repository with a local copy of jaffle_shop_duckdb and want to convert it to use a Git submodule instead, follow these steps:

```bash
# Remove the current local copy
rm -rf dbt_integration_tests/jaffle_shop_duckdb

# Add the GitHub repository as a submodule
git submodule add https://github.com/dbt-labs/jaffle_shop_duckdb dbt_integration_tests/jaffle_shop_duckdb

# Commit the changes
git commit -m "Replace jaffle_shop_duckdb with Git submodule"
```

This approach keeps the jaffle_shop_duckdb code out of your repository while still making it available for tests.

## License

MIT