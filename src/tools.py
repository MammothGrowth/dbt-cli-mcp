"""
MCP tool implementations for the DBT CLI MCP Server.

This module defines all the MCP tools that map to dbt CLI commands.
Each tool is a function decorated with @mcp.tool() that handles a specific dbt command.
"""

import logging
import json
import re
from typing import Optional, Dict, Any, List

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from src.command import execute_dbt_command, parse_dbt_list_output
from src.config import get_config, set_config

# Logger for this module
logger = logging.getLogger(__name__)


def register_tools(mcp: FastMCP) -> None:
    """
    Register all tools with the MCP server.
    
    Args:
        mcp: The FastMCP server instance
    """
    
    @mcp.tool()
    async def dbt_run(
        models: Optional[str] = Field(
            default=None,
            description="Specific models to run, using the dbt selection syntax (e.g., \"model_name+\")"
        ),
        selector: Optional[str] = Field(
            default=None,
            description="Named selector to use"
        ),
        exclude: Optional[str] = Field(
            default=None,
            description="Models to exclude"
        ),
        project_dir: str = Field(
            default=".",
            description="Directory containing the dbt project"
        ),
        full_refresh: bool = Field(
            default=False,
            description="Whether to perform a full refresh"
        )
    ) -> str:
        """Run dbt models. An AI agent should use this tool when it needs to execute dbt models to transform data and build analytical tables in the data warehouse. This is essential for refreshing data or implementing new data transformations in a project.
        
        Returns:
            Output from the dbt run command as text (this command does not support JSON output format)
        """
        command = ["run"]
        
        if models:
            command.extend(["-s", models])
        
        if selector:
            command.extend(["--selector", selector])
        
        if exclude:
            command.extend(["--exclude", exclude])
        
        if full_refresh:
            command.append("--full-refresh")
            
        # The --no-print flag is not supported by dbt Cloud CLI
        # We'll rely on proper parsing to handle any print macros
        
        result = await execute_dbt_command(command, project_dir)
        
        if not result["success"]:
            error_msg = f"Error executing dbt run: {result['error']}"
            if 'output' in result and result['output']:
                error_msg += f"\nOutput: {result['output']}"
            return error_msg
        
        return json.dumps(result["output"]) if isinstance(result["output"], (dict, list)) else str(result["output"])

    @mcp.tool()
    async def dbt_test(
        models: Optional[str] = Field(
            default=None,
            description="Specific models to test, using the dbt selection syntax"
        ),
        selector: Optional[str] = Field(
            default=None,
            description="Named selector to use"
        ),
        exclude: Optional[str] = Field(
            default=None,
            description="Models to exclude"
        ),
        project_dir: str = Field(
            default=".",
            description="Directory containing the dbt project"
        )
    ) -> str:
        """Run dbt tests. An AI agent should use this tool when it needs to validate data quality and integrity by running tests defined in a dbt project. This helps ensure that data transformations meet expected business rules and constraints before being used for analysis or reporting.
        
        Returns:
            Output from the dbt test command as text (this command does not support JSON output format)
        """
        command = ["test"]
        
        if models:
            command.extend(["-s", models])
        
        if selector:
            command.extend(["--selector", selector])
        
        if exclude:
            command.extend(["--exclude", exclude])
            
        # The --no-print flag is not supported by dbt Cloud CLI
        # We'll rely on proper parsing to handle any print macros
        
        result = await execute_dbt_command(command, project_dir)
        
        if not result["success"]:
            error_msg = f"Error executing dbt test: {result['error']}"
            if result["output"]:
                error_msg += f"\nOutput: {result['output']}"
            return error_msg
        
        return json.dumps(result["output"]) if isinstance(result["output"], (dict, list)) else str(result["output"])

    @mcp.tool()
    async def dbt_ls(
        models: Optional[str] = Field(
            default=None,
            description="Specific models to list, using the dbt selection syntax"
        ),
        selector: Optional[str] = Field(
            default=None,
            description="Named selector to use"
        ),
        exclude: Optional[str] = Field(
            default=None,
            description="Models to exclude"
        ),
        resource_type: Optional[str] = Field(
            default=None,
            description="Type of resource to list (model, test, source, etc.)"
        ),
        project_dir: str = Field(
            default=".",
            description="Directory containing the dbt project"
        ),
        output_format: str = Field(
            default="json",
            description="Output format (json, name, path, or selector)"
        )
    ) -> str:
        """List dbt resources. An AI agent should use this tool when it needs to discover available models, tests, sources, and other resources within a dbt project. This helps the agent understand the project structure, identify dependencies, and select specific resources for other operations like running or testing.
        
        Returns:
            When output_format is 'json' (default), returns a JSON string with parsed resource data.
            When output_format is 'name', 'path', or 'selector', returns plain text with the respective format.
        """
        # Log diagnostic information
        logger.info(f"Starting dbt_ls with project_dir={project_dir}, output_format={output_format}")
        
        command = ["ls"]
        
        if models:
            command.extend(["-s", models])
        
        if selector:
            command.extend(["--selector", selector])
        
        if exclude:
            command.extend(["--exclude", exclude])
        
        if resource_type:
            command.extend(["--resource-type", resource_type])
        
        command.extend(["--output", output_format])
        
        command.extend(["--quiet"])
        
        logger.info(f"Executing dbt command: dbt {' '.join(command)}")
        result = await execute_dbt_command(command, project_dir)
        logger.info(f"dbt command result: success={result['success']}, returncode={result.get('returncode')}")
        
        if not result["success"]:
            error_msg = f"Error executing dbt ls: {result['error']}"
            # Include the output and additional debug info in the error message
            if "output" in result and result["output"]:
                error_msg += f"\nOutput: {result['output']}"
            
            # Add debug info about command execution
            error_msg += f"\n\nCommand details:"
            error_msg += f"\nProject directory: {project_dir}"
            error_msg += f"\nOutput format: {output_format}"
            error_msg += f"\nModels: {models or 'None'}"
            error_msg += f"\nReturn code: {result.get('returncode', 'Unknown')}"
            
            logger.error(error_msg)
            return error_msg
        
        # Log raw output for debugging
        logger.info(f"Raw output type: {type(result['output'])}")
        if isinstance(result['output'], str):
            logger.info(f"Raw output length: {len(result['output'])}")
            logger.info(f"Raw output preview: {result['output'][:100]}...")
        elif isinstance(result['output'], (list, dict)):
            logger.info(f"Raw output structure: {type(result['output'])}, length: {len(result['output'] if isinstance(result['output'], list) else result['output'].keys())}")
        
        # For json format, parse the output and return as JSON
        if output_format == "json":
            # Return raw output if it's an empty string or None
            if not result["output"]:
                logger.warning("dbt ls returned empty output")
                return "[]"

            logger.info("Parsing dbt ls output as JSON")
            
            # Special handling for dbt Cloud CLI output
            if isinstance(result["output"], list) and all(isinstance(item, dict) and "name" in item for item in result["output"]):
                logger.info("Detected dbt Cloud CLI output format")
                
                # Extract all JSON objects from log lines with timestamps
                extracted_models = []
                
                for item in result["output"]:
                    name_value = item["name"]
                    
                    # Skip log messages with ANSI color codes
                    if '\x1b[' in name_value:
                        logger.debug(f"Skipping log message with ANSI color codes: {name_value[:30]}...")
                        continue
                    
                    # Skip log messages that don't contain model data
                    if any(log_msg in name_value for log_msg in [
                        "Sending project", "Created invocation", "Waiting for",
                        "Streaming", "Running dbt", "Invocation has finished",
                        "Running with dbt=", "Registered adapter:", "Found",
                        "Unable to do partial parsing", "Starting", "Completed"
                    ]):
                        logger.debug(f"Skipping log message: {name_value[:30]}...")
                        continue
                    
                    # Extract model data from timestamped JSON lines
                    timestamp_prefix_match = re.match(r'^(\d\d:\d\d:\d\d)\s+(.+)$', name_value)
                    if timestamp_prefix_match:
                        json_string = timestamp_prefix_match.group(2)
                        try:
                            model_data = json.loads(json_string)
                            if isinstance(model_data, dict) and "name" in model_data and "resource_type" in model_data:
                                extracted_models.append(model_data)
                        except json.JSONDecodeError:
                            logger.debug(f"Failed to parse JSON from: {json_string[:30]}...")
                            continue
                
                if extracted_models:
                    logger.info(f"Successfully extracted {len(extracted_models)} models from dbt Cloud CLI output")
                    
                    # Sort the results by resource_type and name for better readability
                    extracted_models.sort(key=lambda x: (x.get("resource_type", ""), x.get("name", "")))
                    
                    json_output = json.dumps(extracted_models, indent=2)
                    logger.info(f"Final JSON output length: {len(json_output)}")
                    return json_output
                else:
                    logger.warning("No valid model data found in dbt Cloud CLI output")
                    return "[]"
            else:
                # Standard parsing for regular dbt CLI output
                parsed = parse_dbt_list_output(result["output"])
                
                # Log parsed output for debugging
                logger.info(f"Parsed output type: {type(parsed)}, length: {len(parsed)}")
                
                # Return empty array if parsing failed
                if not parsed:
                    logger.warning("Failed to parse dbt ls output")
                    return "[]"
                
                # Filter out any empty or non-model entries
                filtered_parsed = [item for item in parsed if isinstance(item, dict) and
                                  item.get("resource_type") in ["model", "seed", "test", "source", "snapshot"]]
                
                # Log filtered output for debugging
                logger.info(f"Filtered output length: {len(filtered_parsed)}")
                
                # Sort the results by resource_type and name for better readability
                filtered_parsed.sort(key=lambda x: (x.get("resource_type", ""), x.get("name", "")))
                
                # Return full parsed output if filtering removed everything
                if not filtered_parsed and parsed:
                    logger.warning("Filtering removed all items, returning original parsed output")
                    json_output = json.dumps(parsed, indent=2)
                    logger.info(f"Final JSON output length: {len(json_output)}")
                    return json_output
                
                json_output = json.dumps(filtered_parsed, indent=2)
                logger.info(f"Final JSON output length: {len(json_output)}")
                return json_output
        
        # For name, path, or selector formats, return the raw output as string
        logger.info(f"Returning raw output as string for format: {output_format}")
        return str(result["output"])

    @mcp.tool()
    async def dbt_compile(
        models: Optional[str] = Field(
            default=None,
            description="Specific models to compile, using the dbt selection syntax"
        ),
        selector: Optional[str] = Field(
            default=None,
            description="Named selector to use"
        ),
        exclude: Optional[str] = Field(
            default=None,
            description="Models to exclude"
        ),
        project_dir: str = Field(
            default=".",
            description="Directory containing the dbt project"
        )
    ) -> str:
        """Compile dbt models. An AI agent should use this tool when it needs to generate the SQL that will be executed without actually running it against the database. This is valuable for validating SQL syntax, previewing transformations, or investigating how dbt interprets models before committing to execution.
        
        Returns:
            Output from the dbt compile command as text (this command does not support JSON output format)
        """
        command = ["compile"]
        
        if models:
            command.extend(["-s", models])
        
        if selector:
            command.extend(["--selector", selector])
        
        if exclude:
            command.extend(["--exclude", exclude])
            
        # The --no-print flag is not supported by dbt Cloud CLI
        # We'll rely on proper parsing to handle any print macros
        
        result = await execute_dbt_command(command, project_dir)
        
        if not result["success"]:
            error_msg = f"Error executing dbt compile: {result['error']}"
            if result["output"]:
                error_msg += f"\nOutput: {result['output']}"
            return error_msg
        
        return json.dumps(result["output"]) if isinstance(result["output"], (dict, list)) else str(result["output"])

    @mcp.tool()
    async def dbt_debug(
        project_dir: str = Field(
            default=".",
            description="Directory containing the dbt project"
        )
    ) -> str:
        """Run dbt debug to validate the project setup. An AI agent should use this tool when it needs to troubleshoot configuration issues, check database connectivity, or verify that all project dependencies are properly installed. This is essential for diagnosing problems before attempting to run models or tests.
        
        Returns:
            Output from the dbt debug command as text (this command does not support JSON output format)
        """
        command = ["debug"]
        
        # The --no-print flag is not supported by dbt Cloud CLI
        # We'll rely on proper parsing to handle any print macros
        
        result = await execute_dbt_command(command, project_dir)
        
        if not result["success"]:
            error_msg = f"Error executing dbt debug: {result['error']}"
            if result["output"]:
                error_msg += f"\nOutput: {result['output']}"
            return error_msg
        
        return json.dumps(result["output"]) if isinstance(result["output"], (dict, list)) else str(result["output"])

    @mcp.tool()
    async def dbt_deps(
        project_dir: str = Field(
            default=".",
            description="Directory containing the dbt project"
        )
    ) -> str:
        """Install dbt package dependencies. An AI agent should use this tool when it needs to install or update external packages that the dbt project depends on. This ensures that all required modules, macros, and models from other packages are available before running the project.
        
        Returns:
            Output from the dbt deps command as text (this command does not support JSON output format)
        """
        command = ["deps"]
        
        # The --no-print flag is not supported by dbt Cloud CLI
        # We'll rely on proper parsing to handle any print macros
        
        result = await execute_dbt_command(command, project_dir)
        
        if not result["success"]:
            error_msg = f"Error executing dbt deps: {result['error']}"
            if result["output"]:
                error_msg += f"\nOutput: {result['output']}"
            return error_msg
        
        return json.dumps(result["output"]) if isinstance(result["output"], (dict, list)) else str(result["output"])

    @mcp.tool()
    async def dbt_seed(
        selector: Optional[str] = Field(
            default=None,
            description="Named selector to use"
        ),
        exclude: Optional[str] = Field(
            default=None,
            description="Seeds to exclude"
        ),
        project_dir: str = Field(
            default=".",
            description="Directory containing the dbt project"
        )
    ) -> str:
        """Load CSV files as seed data. An AI agent should use this tool when it needs to load initial data from CSV files into the database. This is essential for creating reference tables, test datasets, or any static data that models will depend on.
        
        Returns:
            Output from the dbt seed command as text (this command does not support JSON output format)
        """
        command = ["seed"]
        
        # The --no-print flag is not supported by dbt Cloud CLI
        # We'll rely on proper parsing to handle any print macros
        
        if selector:
            command.extend(["--selector", selector])
        
        if exclude:
            command.extend(["--exclude", exclude])
        
        result = await execute_dbt_command(command, project_dir)
        
        if not result["success"]:
            error_msg = f"Error executing dbt seed: {result['error']}"
            if result["output"]:
                error_msg += f"\nOutput: {result['output']}"
            return error_msg
        
        return json.dumps(result["output"]) if isinstance(result["output"], (dict, list)) else str(result["output"])

    @mcp.tool()
    async def dbt_show(
        models: str = Field(
            description="Specific model to show"
        ),
        project_dir: str = Field(
            default=".",
            description="Directory containing the dbt project"
        ),
        limit: Optional[int] = Field(
            default=None,
            description="Limit the number of rows returned"
        )
    ) -> str:
        """Preview the results of a model. An AI agent should use this tool when it needs to preview data from a specific model without materializing it. This helps inspect transformation results, debug issues, or demonstrate how data looks after processing without modifying the target database.
        
        Returns:
            Output from the dbt show command as text (this command does not support JSON output format)
        """
        command = ["show", "-s", models]
        
        if limit:
            command.extend(["--limit", str(limit)])
            
        # The --no-print flag is not supported by dbt Cloud CLI
        # We'll rely on proper parsing to handle any print macros
        
        result = await execute_dbt_command(command, project_dir)
        
        if not result["success"]:
            error_msg = f"Error executing dbt show: {result['error']}"
            if result["output"]:
                error_msg += f"\nOutput: {result['output']}"
            return error_msg
        
        return json.dumps(result["output"]) if isinstance(result["output"], (dict, list)) else str(result["output"])

    @mcp.tool()
    async def dbt_build(
        models: Optional[str] = Field(
            default=None,
            description="Specific models to build, using the dbt selection syntax"
        ),
        selector: Optional[str] = Field(
            default=None,
            description="Named selector to use"
        ),
        exclude: Optional[str] = Field(
            default=None,
            description="Models to exclude"
        ),
        project_dir: str = Field(
            default=".",
            description="Directory containing the dbt project"
        ),
        full_refresh: bool = Field(
            default=False,
            description="Whether to perform a full refresh"
        )
    ) -> str:
        """Run build command (seeds, tests, snapshots, and models). An AI agent should use this tool when it needs to execute a comprehensive build process that runs seeds, snapshots, models, and tests in the correct order. This is ideal for complete project deployment or ensuring all components work together.
        
        Returns:
            Output from the dbt build command as text (this command does not support JSON output format)
        """
        command = ["build"]
        
        if models:
            command.extend(["-s", models])
        
        if selector:
            command.extend(["--selector", selector])
        
        if exclude:
            command.extend(["--exclude", exclude])
        
        if full_refresh:
            command.append("--full-refresh")
            
        # The --no-print flag is not supported by dbt Cloud CLI
        # We'll rely on proper parsing to handle any print macros
        
        result = await execute_dbt_command(command, project_dir)
        
        if not result["success"]:
            error_msg = f"Error executing dbt build: {result['error']}"
            if result["output"]:
                error_msg += f"\nOutput: {result['output']}"
            return error_msg
        
        return json.dumps(result["output"]) if isinstance(result["output"], (dict, list)) else str(result["output"])

    logger.info("Registered all dbt tools")