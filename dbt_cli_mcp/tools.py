"""
MCP tool implementations for the DBT CLI MCP Server.

This module defines all the MCP tools that map to dbt CLI commands.
Each tool is a function decorated with @mcp.tool() that handles a specific dbt command.
"""

import logging
import json
from typing import Optional, Dict, Any, List

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from dbt_cli_mcp.command import execute_dbt_command, parse_dbt_list_output, load_mock_response
from dbt_cli_mcp.config import get_config, set_config

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
        ),
        no_print: bool = Field(
            default=False,
            description="Suppress print() macros in the output"
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
            
        if no_print:
            command.append("--no-print")
        
        # Check if we're in mock mode and load mock response
        mock_mode = get_config("mock_mode", False)
        mock_response = None
        if mock_mode:
            mock_response = await load_mock_response("run")
        
        result = await execute_dbt_command(command, project_dir, mock_mode, mock_response)
        
        if not result["success"]:
            return f"Error executing dbt run: {result['error']}"
        
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
        ),
        no_print: bool = Field(
            default=False,
            description="Suppress print() macros in the output"
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
            
        if no_print:
            command.append("--no-print")
        
        # Check if we're in mock mode and load mock response
        mock_mode = get_config("mock_mode", False)
        mock_response = None
        if mock_mode:
            mock_response = await load_mock_response("test")
        
        result = await execute_dbt_command(command, project_dir, mock_mode, mock_response)
        
        if not result["success"]:
            return f"Error executing dbt test: {result['error']}"
        
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
            description="Output format (json or text)"
        ),
        no_print: bool = Field(
            default=False,
            description="Suppress print() macros in the output"
        )
    ) -> str:
        """List dbt resources. An AI agent should use this tool when it needs to discover available models, tests, sources, and other resources within a dbt project. This helps the agent understand the project structure, identify dependencies, and select specific resources for other operations like running or testing.
        
        Returns:
            When output_format is 'json' (default), returns a JSON string with parsed resource data. When output_format is 'text', returns plain text with resource names
        """
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
        
        if no_print:
            command.append("--no-print")
        
        # Check if we're in mock mode and load mock response
        mock_mode = get_config("mock_mode", False)
        mock_response = None
        if mock_mode:
            mock_response = await load_mock_response("ls")
        
        result = await execute_dbt_command(command, project_dir, mock_mode, mock_response)
        
        if not result["success"]:
            return f"Error executing dbt ls: {result['error']}"
        
        if output_format == "json":
            parsed = parse_dbt_list_output(result["output"])
            return json.dumps(parsed)
        
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
        ),
        no_print: bool = Field(
            default=False,
            description="Suppress print() macros in the output"
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
            
        if no_print:
            command.append("--no-print")
        
        # Check if we're in mock mode and load mock response
        mock_mode = get_config("mock_mode", False)
        mock_response = None
        if mock_mode:
            mock_response = await load_mock_response("compile")
        
        result = await execute_dbt_command(command, project_dir, mock_mode, mock_response)
        
        if not result["success"]:
            return f"Error executing dbt compile: {result['error']}"
        
        return json.dumps(result["output"]) if isinstance(result["output"], (dict, list)) else str(result["output"])

    @mcp.tool()
    async def dbt_debug(
        project_dir: str = Field(
            default=".",
            description="Directory containing the dbt project"
        ),
        no_print: bool = Field(
            default=False,
            description="Suppress print() macros in the output"
        )
    ) -> str:
        """Run dbt debug to validate the project setup. An AI agent should use this tool when it needs to troubleshoot configuration issues, check database connectivity, or verify that all project dependencies are properly installed. This is essential for diagnosing problems before attempting to run models or tests.
        
        Returns:
            Output from the dbt debug command as text (this command does not support JSON output format)
        """
        command = ["debug"]
        
        if no_print:
            command.append("--no-print")
        
        # Check if we're in mock mode and load mock response
        mock_mode = get_config("mock_mode", False)
        mock_response = None
        if mock_mode:
            mock_response = await load_mock_response("debug")
        
        result = await execute_dbt_command(command, project_dir, mock_mode, mock_response)
        
        if not result["success"]:
            return f"Error executing dbt debug: {result['error']}"
        
        return json.dumps(result["output"]) if isinstance(result["output"], (dict, list)) else str(result["output"])

    @mcp.tool()
    async def dbt_deps(
        project_dir: str = Field(
            default=".",
            description="Directory containing the dbt project"
        ),
        no_print: bool = Field(
            default=False,
            description="Suppress print() macros in the output"
        )
    ) -> str:
        """Install dbt package dependencies. An AI agent should use this tool when it needs to install or update external packages that the dbt project depends on. This ensures that all required modules, macros, and models from other packages are available before running the project.
        
        Returns:
            Output from the dbt deps command as text (this command does not support JSON output format)
        """
        command = ["deps"]
        
        if no_print:
            command.append("--no-print")
        
        # Check if we're in mock mode and load mock response
        mock_mode = get_config("mock_mode", False)
        mock_response = None
        if mock_mode:
            mock_response = await load_mock_response("deps")
        
        result = await execute_dbt_command(command, project_dir, mock_mode, mock_response)
        
        if not result["success"]:
            return f"Error executing dbt deps: {result['error']}"
        
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
        ),
        no_print: bool = Field(
            default=False,
            description="Suppress print() macros in the output"
        )
    ) -> str:
        """Load CSV files as seed data. An AI agent should use this tool when it needs to load initial data from CSV files into the database. This is essential for creating reference tables, test datasets, or any static data that models will depend on.
        
        Returns:
            Output from the dbt seed command as text (this command does not support JSON output format)
        """
        command = ["seed"]
        
        if no_print:
            command.append("--no-print")
        
        if selector:
            command.extend(["--selector", selector])
        
        if exclude:
            command.extend(["--exclude", exclude])
        
        # Check if we're in mock mode and load mock response
        mock_mode = get_config("mock_mode", False)
        mock_response = None
        if mock_mode:
            mock_response = await load_mock_response("seed")
        
        result = await execute_dbt_command(command, project_dir, mock_mode, mock_response)
        
        if not result["success"]:
            return f"Error executing dbt seed: {result['error']}"
        
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
        ),
        no_print: bool = Field(
            default=False,
            description="Suppress print() macros in the output"
        )
    ) -> str:
        """Preview the results of a model. An AI agent should use this tool when it needs to preview data from a specific model without materializing it. This helps inspect transformation results, debug issues, or demonstrate how data looks after processing without modifying the target database.
        
        Returns:
            Output from the dbt show command as text (this command does not support JSON output format)
        """
        command = ["show", "-s", models]
        
        if limit:
            command.extend(["--limit", str(limit)])
            
        if no_print:
            command.append("--no-print")
        
        # Check if we're in mock mode and load mock response
        mock_mode = get_config("mock_mode", False)
        mock_response = None
        if mock_mode:
            mock_response = await load_mock_response("show")
        
        result = await execute_dbt_command(command, project_dir, mock_mode, mock_response)
        
        if not result["success"]:
            return f"Error executing dbt show: {result['error']}"
        
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
        ),
        no_print: bool = Field(
            default=False,
            description="Suppress print() macros in the output"
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
            
        if no_print:
            command.append("--no-print")
        
        # Check if we're in mock mode and load mock response
        mock_mode = get_config("mock_mode", False)
        mock_response = None
        if mock_mode:
            mock_response = await load_mock_response("build")
        
        result = await execute_dbt_command(command, project_dir, mock_mode, mock_response)
        
        if not result["success"]:
            return f"Error executing dbt build: {result['error']}"
        
        return json.dumps(result["output"]) if isinstance(result["output"], (dict, list)) else str(result["output"])

    @mcp.tool()
    async def configure_dbt_path(
        path: str = Field(
            description="Absolute path to the dbt executable"
        )
    ) -> str:
        """Configure the path to the dbt executable. An AI agent should use this tool when it needs to set up or change the path to the dbt executable. This is important during initial setup or when switching between different dbt installations or environments.
        
        Returns:
            Confirmation message
        """
        import os
        
        if not os.path.isfile(path):
            return f"Error: File not found at {path}"
        
        set_config("dbt_path", path)
        return f"dbt path configured to: {path}"

    @mcp.tool()
    async def set_mock_mode(
        enabled: bool = Field(
            description="Whether to enable mock mode"
        )
    ) -> str:
        """Enable or disable mock mode for testing. An AI agent should use this tool when it needs to toggle mock mode for testing purposes. This allows the agent to simulate dbt command execution without actually running them against a database, which is useful for development, testing, or demonstrating functionality.
        
        Returns:
            Confirmation message
        """
        set_config("mock_mode", enabled)
        return f"Mock mode {'enabled' if enabled else 'disabled'}"

    logger.info("Registered all dbt tools")