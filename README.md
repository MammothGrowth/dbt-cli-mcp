# DBT CLI MCP Server

A Model Context Protocol (MCP) server that wraps the dbt CLI tool, enabling AI coding agents to interact with dbt projects through standardized MCP tools.

## Features

- Execute dbt commands through MCP tools
- Support for all major dbt operations (run, test, compile, etc.)
- Command-line interface for direct interaction
- Environment variable management for dbt projects
- Configurable dbt executable path
- Flexible profiles.yml location configuration

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

# Run dbt models with a custom profiles directory
dbt-mcp run --models customers --project-dir /path/to/project --profiles-dir /path/to/profiles

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

### Command Line Options

- `--dbt-path`: Path to dbt executable (default: "dbt")
- `--env-file`: Path to environment file (default: ".env")
- `--log-level`: Logging level (default: "INFO")
- `--profiles-dir`: Path to directory containing profiles.yml file (defaults to project-dir if not specified)

### Environment Variables

The server can also be configured using environment variables:

- `DBT_PATH`: Path to dbt executable
- `ENV_FILE`: Path to environment file
- `LOG_LEVEL`: Logging level
- `DBT_PROFILES_DIR`: Path to directory containing profiles.yml file

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
        // You can also set DBT_PROFILES_DIR here for a server-wide default
      }
    }
  }
}
```

## ⚠️ IMPORTANT: Absolute Project Path Required ⚠️

When using any tool from this MCP server, you **MUST** specify the **FULL ABSOLUTE PATH** to your dbt project directory with the `project_dir` parameter. Relative paths will not work correctly.

```json
// ❌ INCORRECT - Will NOT work
{
  "project_dir": "."
}

// ✅ CORRECT - Will work
{
  "project_dir": "/Users/username/path/to/your/dbt/project"
}
```

See the [complete dbt MCP usage guide](docs/dbt_mcp_guide.md) for more detailed instructions and examples.

## Available Tools

The server provides the following MCP tools:

- `dbt_run`: Run dbt models (requires absolute `project_dir`)
- `dbt_test`: Run dbt tests (requires absolute `project_dir`)
- `dbt_ls`: List dbt resources (requires absolute `project_dir`)
- `dbt_compile`: Compile dbt models (requires absolute `project_dir`)
- `dbt_debug`: Debug dbt project setup (requires absolute `project_dir`)
- `dbt_deps`: Install dbt package dependencies (requires absolute `project_dir`)
- `dbt_seed`: Load CSV files as seed data (requires absolute `project_dir`)
- `dbt_show`: Preview model results (requires absolute `project_dir`)
<arguments>
{
  "models": "customers",
  "project_dir": "/path/to/dbt/project",
  "limit": 10
}
</arguments>
</use_mcp_tool>
```

### dbt Profiles Configuration

When using the dbt MCP tools, it's important to understand how dbt profiles are handled:

1. The `project_dir` parameter **MUST** be an absolute path (e.g., `/Users/username/project` not `.`) that points to a directory containing both:
   - A valid `dbt_project.yml` file
   - A valid `profiles.yml` file with the profile referenced in the project

2. The MCP server automatically sets the `DBT_PROFILES_DIR` environment variable to the absolute path of the directory specified in `project_dir`. This tells dbt where to look for the profiles.yml file.

3. If you encounter a "Could not find profile named 'X'" error, it means either:
   - The profiles.yml file is missing from the project directory
   - The profiles.yml file doesn't contain the profile referenced in dbt_project.yml
   - You provided a relative path instead of an absolute path for `project_dir`

Example of a valid profiles.yml file:

```yaml
jaffle_shop:  # This name must match the profile in dbt_project.yml
  target: dev
  outputs:
    dev:
      type: duckdb
      path: 'jaffle_shop.duckdb'
      threads: 24
```

When running commands through the MCP server, ensure your project directory is structured correctly with both configuration files present.

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

## License

MIT