# dbt CLI MCP Server

## Overview

The dbt CLI MCP Server provides tools for running dbt commands through the Model Context Protocol. It allows AI assistants to execute dbt operations on your data projects directly.

## Installation and Setup

1. Install the MCP server
2. Enable it in your client (Claude, Cline, or other MCP-compatible client)

## ⚠️ Important: Project Path Requirement ⚠️

**When using any tool from this MCP server, you MUST specify the fully qualified (absolute) path to your dbt project directory.**

```
# ❌ INCORRECT - will not work
{
  "project_dir": "."
}

# ✅ CORRECT - will work
{
  "project_dir": "/Users/username/path/to/your/dbt/project"
}
```

### Why this is required:

The MCP server runs in its own environment, separate from your client application. When you use relative paths like `.` (current directory), they resolve relative to the server's location, not your project. Providing the full path ensures the server can correctly locate and operate on your dbt project files.

## Available Tools

This MCP server provides the following tools for working with dbt:

| Tool | Description | Required Parameters |
|------|-------------|---------------------|
| `dbt_run` | Runs dbt models | `project_dir` (full path) |
| `dbt_test` | Runs dbt tests | `project_dir` (full path) |
| `dbt_compile` | Compiles dbt models | `project_dir` (full path) |
| `dbt_ls` | Lists resources in a dbt project (simplified output by default, full details with `verbose: true`) | `project_dir` (full path) |
| `dbt_debug` | Validates project setup | `project_dir` (full path) |
| `dbt_deps` | Installs package dependencies | `project_dir` (full path) |
| `dbt_seed` | Loads seed data | `project_dir` (full path) |
| `dbt_build` | Runs seeds, tests, snapshots, and models | `project_dir` (full path) |
| `dbt_show` | Previews results of a model | `models`, `project_dir` (full path) |

## Usage Examples

### Example 1: Running dbt models

```json
{
  "models": "model_name",
  "project_dir": "/Users/username/dbt_projects/analytics"
}
```

### Example 2: Listing dbt resources

#### Simplified output (default)
```json
{
  "resource_type": "model",
  "project_dir": "/Users/username/dbt_projects/analytics",
  "output_format": "json"
}
```

This returns a simplified JSON with only `name`, `resource_type`, and `depends_on.nodes` for each resource:

```json
[
  {
    "name": "customers",
    "resource_type": "model",
    "depends_on": {
      "nodes": ["model.jaffle_shop.stg_customers", "model.jaffle_shop.stg_orders"]
    }
  }
]
```

#### Verbose output (full details)
```json
{
  "resource_type": "model",
  "project_dir": "/Users/username/dbt_projects/analytics",
  "output_format": "json",
  "verbose": true
}
```

This returns the complete resource information including all configuration details.

### Example 3: Testing dbt models

```json
{
  "models": "my_model",
  "project_dir": "/Users/username/dbt_projects/analytics"
}
```

## Troubleshooting

### Common Issues

1. **"Project not found" or similar errors**
   - Make sure you're providing the full absolute path to your dbt project
   - Check that the path exists and contains a valid dbt_project.yml file

2. **Permissions errors**
   - Ensure the MCP server has access to the project directory
   - Check file permissions on your dbt project files

3. **Connection errors**
   - Verify that your profiles.yml is correctly configured
   - Check database credentials and connectivity

## Need Help?

If you're experiencing issues with the dbt CLI MCP Server, check the documentation or open an issue on the GitHub repository.