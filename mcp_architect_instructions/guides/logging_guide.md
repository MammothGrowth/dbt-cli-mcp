# Logging Guide for MCP Servers

## Overview

This guide covers best practices for implementing logging in Model Context Protocol (MCP) servers. Proper logging is essential for debugging, monitoring, and ensuring the reliability of MCP servers.

## Logging Configuration

MCP servers MUST implement a standardized logging system to help with debugging and monitoring. The following implementation is REQUIRED:

```python
import logging

def configure_logging(debug=False):
    """Configure logging based on specified verbosity level.
    
    Args:
        debug: Whether to enable debug-level logging
    """
    # Create a logger with a descriptive name
    logger = logging.getLogger("my_mcp_server")
    
    # Clear any existing handlers
    if logger.handlers:
        logger.handlers.clear()
    
    # Configure the appropriate log level
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    
    # Create a console handler with better formatting
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger

# Initialize logger
logger = configure_logging()
```

This exact pattern MUST be followed to ensure consistent logging across all MCP servers.

## Using Logging Levels Effectively

Use appropriate logging levels to categorize messages based on their importance:

```python
# Critical errors that prevent operation
logger.critical("Failed to start: cannot connect to required service")

# Errors that affect functionality but don't prevent operation
logger.error("API request failed: {e}", exc_info=True)

# Warnings about potential issues
logger.warning("Rate limit approaching: {rate_limit_remaining} requests remaining")

# General information about normal operation
logger.info("Processing request for object: {object_name}")

# Detailed information useful for debugging
logger.debug("Request parameters: {params}")
```

### Logging Level Guidelines

- **CRITICAL (50)**: Use for fatal errors that prevent the server from functioning at all
- **ERROR (40)**: Use for errors that affect some functionality but don't prevent basic operation
- **WARNING (30)**: Use for potential issues that don't immediately affect functionality
- **INFO (20)**: Use for normal operational information and significant events
- **DEBUG (10)**: Use for detailed troubleshooting information

## Debug Mode in CLI

For CLI tools, implement a `--debug` flag using argparse to enable more verbose logging:

```python
parser = argparse.ArgumentParser(description="My MCP Server")
parser.add_argument(
    "--debug", action="store_true", help="Enable debug logging"
)

args = parser.parse_args()
logger = configure_logging(debug=args.debug)
```

This allows users to get more detailed log output when troubleshooting issues.

## Required Logging Practices

All MCP servers MUST follow these logging practices:

### 1. Use Structured Logging for Machine-Parseable Logs

Include key-value pairs in log messages to make them easily parseable:

```python
# GOOD
logger.info(f"Processing request: method={method}, object_id={object_id}, user={user}")

# AVOID
logger.info(f"Processing a request for {object_id}")
```

### 2. Log at Appropriate Levels

- **CRITICAL**: For errors that prevent the server from functioning
- **ERROR**: For errors that affect functionality but don't prevent operation
- **WARNING**: For potential issues that don't affect functionality
- **INFO**: For normal operation events (server start/stop, request handling)
- **DEBUG**: For detailed troubleshooting information only

### 3. Include Context in All Log Messages

Every log message should include sufficient context to understand what it refers to:

```python
# GOOD
logger.info(f"Successfully retrieved resource: id={resource_id}, size={len(data)} bytes")

# AVOID
logger.info("Successfully retrieved resource")
```

### 4. Log Start/End of All Significant Operations

For important operations, log both the start and completion:

```python
logger.info(f"Starting data processing job: job_id={job_id}")
# ... processing ...
logger.info(f"Completed data processing job: job_id={job_id}, records_processed={count}")
```

### 5. Never Log Sensitive Data

Never include sensitive information in logs:

```python
# BAD - logs API key
logger.debug(f"Making API request with key: {api_key}")

# GOOD - masks sensitive data
logger.debug(f"Making API request with key: {api_key[:4]}...{api_key[-4:]}")
```

Sensitive data includes:
- API keys and secrets
- Passwords and tokens
- Personal identifiable information
- Authentication credentials

### 6. Always Log Exceptions with Traceback

When catching exceptions, always include the traceback:

```python
try:
    # Code that might raise an exception
    result = api_client.fetch_data(params)
except Exception as e:
    # Always include exc_info=True to get the traceback
    logger.error(f"Error fetching data: {str(e)}", exc_info=True)
    raise
```

### 7. Use Consistent Log Formatting

Use the same log formatting across your entire application:

```python
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
```

### 8. Include Timing Information for Performance-Critical Operations

For operations where performance matters, include timing information:

```python
import time

start_time = time.time()
# ... operation ...
duration = time.time() - start_time
logger.info(f"Completed operation in {duration:.2f} seconds")
```

## Logging in MCP Tools

When implementing MCP tools, follow this pattern for logging:

```python
@mcp.tool()
async def my_tool(param1: str, param2: int, optional_param: bool = True) -> str:
    """Tool description."""
    try:
        # Log the function call at debug level
        logger.debug(f"my_tool called with params: {param1}, {param2}, {optional_param}")
        
        # Your tool implementation here
        result = f"Processed {param1} with {param2}"
        
        # Log successful completion at info level
        logger.info(f"Successfully processed request: param1={param1}")
        
        return result
    except Exception as e:
        # Log the error with traceback
        logger.error(f"Error in my_tool: {str(e)}", exc_info=True)
        
        # Convert exceptions to MCP errors
        raise McpError(ErrorData(
            code=INTERNAL_ERROR,
            message=f"Error in my_tool: {str(e)}"
        ))
```

## Logging Configuration for CLI vs MCP Server Mode

Configure logging differently based on the mode:

```python
if __name__ == "__main__":
    # If running in CLI mode
    if len(sys.argv) > 1:
        args = parse_args()
        # Configure logging based on CLI arguments
        logger = configure_logging(debug=args.debug)
    # If running in MCP server mode
    else:
        # Configure default logging for MCP server mode
        logger = configure_logging(debug=os.environ.get("DEBUG") == "true")
```

## Troubleshooting with Logs

Effective logging is critical for troubleshooting. Ensure your logs provide enough information to:

1. Identify what went wrong
2. Determine the cause of the issue
3. Understand the context of the error
4. Reproduce the problem if needed

Example of good troubleshooting logs:

```
2023-04-15 14:32:10 - my_mcp_server - INFO - Starting API request: url=https://api.example.com/data, method=GET
2023-04-15 14:32:11 - my_mcp_server - WARNING - API rate limit header: remaining=5, limit=100
2023-04-15 14:32:12 - my_mcp_server - ERROR - API request failed: status=429, message=Too Many Requests
2023-04-15 14:32:12 - my_mcp_server - INFO - Initiating retry: attempt=1, backoff=2.0 seconds
```

## Next Steps

After implementing logging, refer to:
- [Error Handling](implementation_guide.md#error-handling-strategy) in the Implementation Guide
- [Testing Guide](testing_guide.md) for testing with different log levels