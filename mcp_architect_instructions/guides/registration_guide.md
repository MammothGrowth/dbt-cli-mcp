# MCP Server Registration Guide

## Overview

This guide explains how to register your Model Context Protocol (MCP) server with Claude or RooCode. Registration allows the AI assistant to discover and use your MCP server's capabilities.

## Configuration for Claude/RooCode

To register your MCP server, you need to add it to the appropriate MCP settings file based on the client you're using.

### For RooCode (VSCode Extension)

Edit the file at: `/Users/username/Library/Application Support/Code/User/globalStorage/rooveterinaryinc.roo-cline/settings/cline_mcp_settings.json`

Add your MCP server configuration:

```json
{
  "mcpServers": {
    "my-server": {
      "command": "uv",
      "args": ["run", "-s", "/path/to/my_mcp_server.py"],
      "env": {
        "API_KEY": "your_api_key_here"
      },
      "disabled": false,
      "alwaysAllow": []
    }
  }
}
```

### For Claude Desktop

Edit the file at: `~/Library/Application Support/Claude/claude_desktop_config.json`

The format is the same as for RooCode:

```json
{
  "mcpServers": {
    "my-server": {
      "command": "uv",
      "args": ["run", "-s", "/path/to/my_mcp_server.py"],
      "env": {
        "API_KEY": "your_api_key_here"
      },
      "disabled": false,
      "alwaysAllow": []
    }
  }
}
```

## Configuration Fields

The MCP server configuration includes the following fields:

| Field | Description | Required |
|-------|-------------|----------|
| `command` | The command to run (typically `"uv"`) | Yes |
| `args` | Array of command arguments, including the script path | Yes |
| `env` | Object containing environment variables | No |
| `disabled` | Boolean indicating if the server is disabled | No (defaults to `false`) |
| `alwaysAllow` | Array of function names that don't require permission | No (defaults to `[]`) |

### Command and Args

For MCP servers using uv and inline dependencies:

```json
"command": "uv",
"args": ["run", "-s", "/absolute/path/to/my_mcp_server.py"]
```

For MCP servers with environment files:

```json
"command": "uv",
"args": ["run", "--env-file", "/path/to/.env", "-s", "/path/to/my_mcp_server.py"]
```

### Environment Variables

The `env` field contains environment variables as key-value pairs:

```json
"env": {
  "API_KEY": "your_api_key_here",
  "DEBUG": "true",
  "TIMEOUT_SECONDS": "30"
}
```

### Disabled Flag

The `disabled` flag determines whether the MCP server is active:

```json
"disabled": false  // Server is active
"disabled": true   // Server is inactive
```

### Always Allow

The `alwaysAllow` array lists functions that can be called without explicit user permission:

```json
"alwaysAllow": ["get_weather", "convert_units"]
```

Use this cautiously, as it bypasses the normal permission system.

## Environment Variables Management

When configuring your MCP server, follow these rules for managing environment variables:

1. **NEVER hardcode sensitive keys** in your MCP server code
2. **ALWAYS use environment variables** for all API keys and secrets
3. **ALWAYS provide clear error messages** when required environment variables are missing
4. **ALWAYS document all required environment variables** in your README
5. **ALWAYS include a `.env.example` file** in your project showing required variables (without values)

Example environment variable validation in your code:

```python
api_key = os.environ.get("API_KEY")
if not api_key:
    raise ValueError("API_KEY environment variable is required")
```

## Registration Best Practices

1. **Use descriptive server names**: Choose a name that clearly indicates the server's purpose
2. **Use absolute paths**: Always use absolute paths to avoid working directory issues
3. **Include version information**: Consider including version in the server name or documentation
4. **Document environment variables**: Clearly document all required environment variables
5. **Test before registration**: Verify the server works with the `--test` flag before registering
6. **Security checks**: Ensure sensitive information is only in environment variables, not hardcoded

## Multi-Environment Configuration

For running the same MCP server in different environments:

```json
{
  "mcpServers": {
    "weather-prod": {
      "command": "uv",
      "args": ["run", "-s", "/path/to/weather_mcp.py"],
      "env": {
        "API_KEY": "production_key_here",
        "ENVIRONMENT": "production"
      },
      "disabled": false
    },
    "weather-dev": {
      "command": "uv",
      "args": ["run", "-s", "/path/to/weather_mcp.py"],
      "env": {
        "API_KEY": "development_key_here",
        "ENVIRONMENT": "development",
        "DEBUG": "true"
      },
      "disabled": false
    }
  }
}
```

## Security Considerations

When registering MCP servers, consider these security practices:

1. **API Keys**: Store API keys only in the environment variables, never in code
2. **Least Privilege**: Use API keys with the minimum necessary permissions
3. **Rotate Credentials**: Regularly rotate API keys and other credentials
4. **Validation**: Always validate and sanitize inputs to prevent injection attacks
5. **Authorization**: Implement authorization checks for sensitive operations
6. **Rate Limiting**: Implement rate limiting to prevent abuse

## Troubleshooting Registration

If you encounter issues with registration:

1. **Verify path**: Ensure the path to your script is correct and absolute
2. **Check permissions**: Verify the script has execute permissions
3. **Test directly**: Run with `uv run -s my_mcp_server.py --test` to verify it works
4. **Check environment**: Ensure all required environment variables are set
5. **Review logs**: Look for error messages in the console or logs
6. **Check JSON syntax**: Ensure your config file has valid JSON syntax

## Next Steps

After registering your MCP server:
- Restart the Claude application or RooCode extension to load the new configuration
- Test the server by asking Claude to use one of your server's tools
- Monitor logs for any errors or issues