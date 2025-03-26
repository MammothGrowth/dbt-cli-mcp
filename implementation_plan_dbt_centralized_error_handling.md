# Implementation Plan: Centralized Error Handling for DBT CLI MCP

## Overview

This document outlines the implementation plan for centralizing error handling and output formatting in the DBT CLI MCP server. Currently, each tool in `src/tools.py` handles errors from `execute_dbt_command` with nearly identical but duplicated code, with some variations in output formatting.

## Problem Statement

1. **Code Duplication**: Error handling logic is repeated across 8+ tool functions
2. **Inconsistent Error Messages**: Some tools include more debug info than others
3. **Varied Output Formats**: Each tool formats successful results differently
4. **Maintenance Challenges**: Changes to error handling require updates in multiple places
5. **Error Message Quality**: Command output should always be included in error messages for better debugging

## Proposed Solution

Create a centralized system to process command results with:
1. A unified result processing function in `command.py`
2. Tool-specific output formatters for specialized handling
3. Consistent error formatting with command output always included
4. Simplified tool implementations

## Implementation Details

### 1. Add Result Processor to `command.py`

Add a new function to `command.py` that will centralize result processing:

```python
async def process_command_result(
    result: Dict[str, Any],
    command_name: str,
    output_formatter: Optional[Callable] = None,
    include_debug_info: bool = False
) -> str:
    """
    Process the result of a dbt command execution.
    
    Args:
        result: The result dictionary from execute_dbt_command
        command_name: The name of the dbt command (e.g. "run", "test")
        output_formatter: Optional function to format successful output
        include_debug_info: Whether to include additional debug info in error messages
        
    Returns:
        Formatted output or error message
    """
    if not result["success"]:
        error_msg = f"Error executing dbt {command_name}: {result['error']}"
        
        # Always include command output in error messages
        if "output" in result and result["output"]:
            error_msg += f"\nOutput: {result['output']}"
        
        # Include additional debug info if requested
        if include_debug_info:
            error_msg += f"\n\nCommand details:"
            error_msg += f"\nReturn code: {result.get('returncode', 'Unknown')}"
            # Add any other debug info that might be useful
        
        return error_msg
    
    # Format successful output
    if output_formatter:
        return output_formatter(result["output"])
    
    # Default output formatting
    return json.dumps(result["output"]) if isinstance(result["output"], (dict, list)) else str(result["output"])
```

### 2. Create Output Formatters

Create a new file `src/formatters.py` with specialized formatters for different command types:

```python
"""
Output formatters for different dbt commands.

This module contains functions to format the output of dbt commands
in different ways based on the command type and output format.
"""

import json
import logging
import re
from typing import Any, Dict, List, Union

from src.command import parse_dbt_list_output

# Logger for this module
logger = logging.getLogger(__name__)

def default_formatter(output: Any) -> str:
    """
    Default formatter for command outputs.
    
    Args:
        output: Command output
        
    Returns:
        Formatted output string
    """
    return json.dumps(output) if isinstance(output, (dict, list)) else str(output)

def ls_formatter(output: Any, output_format: str = "json") -> str:
    """
    Formatter for dbt ls command output.
    
    Args:
        output: The command output
        output_format: The output format (json, name, path, or selector)
        
    Returns:
        Formatted output string
    """
    # For name, path, or selector formats, return the raw output as string
    if output_format != "json":
        logger.info(f"Returning raw output as string for format: {output_format}")
        return str(output)
    
    # For json format, parse the output and return as JSON
    logger.info("Parsing dbt ls output as JSON")
    
    # Return raw output if it's an empty string or None
    if not output:
        logger.warning("dbt ls returned empty output")
        return "[]"
    
    # Special handling for dbt Cloud CLI output format
    if isinstance(output, list) and all(isinstance(item, dict) and "name" in item for item in output):
        # Complex parsing logic for dbt Cloud CLI output format
        # ... (copy the existing complex parsing logic from dbt_ls tool)
        # This is just a placeholder - actual implementation will use the 
        # existing parsing logic from the dbt_ls tool
        parsed = parse_dbt_list_output(output)
    else:
        # Standard parsing for regular dbt CLI output
        parsed = parse_dbt_list_output(output)
    
    # Filter and sort parsed output
    # ... (copy the existing filtering and sorting logic from dbt_ls tool)
    
    # Return JSON output
    return json.dumps(parsed, indent=2)

def show_formatter(output: Any) -> str:
    """
    Formatter for dbt show command output.
    
    Args:
        output: The command output
        
    Returns:
        Formatted output string
    """
    # Try to convert tabular output to JSON if needed
    if not isinstance(output, (dict, list)):
        try:
            # Simple conversion of tabular data to JSON
            # ... (copy the existing conversion logic from dbt_show tool)
            pass
        except Exception as e:
            logger.warning(f"Failed to convert tabular output to JSON: {e}")
    
    # Default output handling
    return json.dumps(output) if isinstance(output, (dict, list)) else str(output)
```

### 3. Update Tool Implementations in `src/tools.py`

Update each tool to use the centralized result processor. Here are examples for a few tools:

#### dbt_run tool:

```python
@mcp.tool()
async def dbt_run(
    models: Optional[str] = Field(
        default=None,
        description="Specific models to run, using the dbt selection syntax (e.g., \"model_name+\")"
    ),
    # ... other parameters
) -> str:
    """Run dbt models..."""
    command = ["run"]
    
    # ... parameter handling
    
    result = await execute_dbt_command(command, project_dir, profiles_dir)
    
    # Use the centralized result processor
    return await process_command_result(result, command_name="run")
```

#### dbt_ls tool (with custom formatter):

```python
@mcp.tool()
async def dbt_ls(
    # ... parameters
    output_format: str = Field(
        default="json",
        description="Output format (json, name, path, or selector)"
    )
) -> str:
    """List dbt resources..."""
    # ... parameter handling
    
    result = await execute_dbt_command(command, project_dir, profiles_dir)
    
    # Use the centralized result processor with ls_formatter
    from functools import partial
    formatter = partial(ls_formatter, output_format=output_format)
    
    return await process_command_result(
        result,
        command_name="ls",
        output_formatter=formatter,
        include_debug_info=True  # Include extra debug info for this command
    )
```

#### dbt_show tool (with custom formatter):

```python
@mcp.tool()
async def dbt_show(
    # ... parameters
) -> str:
    """Preview the results of a model..."""
    # ... parameter handling
    
    result = await execute_dbt_command(command, project_dir, profiles_dir)
    
    # Use the centralized result processor with show_formatter
    return await process_command_result(
        result,
        command_name="show",
        output_formatter=show_formatter
    )
```

### 4. Update Tests

Update the relevant tests to verify the centralized result processing:

1. Add unit tests for `process_command_result` in `tests/test_command.py`
2. Add unit tests for the formatters in a new file `tests/test_formatters.py`
3. Update tool tests to verify they use the centralized processor
4. Ensure integration tests still pass with the refactored code

## Implementation Steps

1. **Add result processor**: Add the `process_command_result` function to `command.py`
2. **Create formatters**: Create a new file `src/formatters.py` with specialized formatters
3. **Update tool implementations**: Refactor each tool in `src/tools.py` to use the centralized system
4. **Update tests**: Add and update tests to verify the refactored code
5. **Test thoroughly**: Run all unit and integration tests to ensure everything works correctly

## Benefits

1. **Reduced Duplication**: Eliminates duplicated error handling code
2. **Consistent Errors**: All tools will display errors in the same format
3. **Better Debugging**: Error messages will always include command output
4. **Easier Maintenance**: Changes to error handling can be made in one place
5. **Flexibility**: Still allows for tool-specific output handling

## Next Steps

To implement this plan, we need to switch to Code mode which allows editing Python files.