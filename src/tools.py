"""
MCP tool implementations for the DBT CLI MCP Server.

This module defines all the MCP tools that map to dbt CLI commands.
Each tool is a function decorated with @mcp.tool() that handles a specific dbt command.
"""

import logging
import json
import re
from typing import Optional, Dict, Any, List
from functools import partial

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from src.command import execute_dbt_command, parse_dbt_list_output, process_command_result
from src.config import get_config, set_config
from src.formatters import default_formatter, ls_formatter, show_formatter

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
        profiles_dir: Optional[str] = Field(
            default=None,
            description="Directory containing the profiles.yml file (defaults to project_dir if not specified)"
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
        
        result = await execute_dbt_command(command, project_dir, profiles_dir)
        
        # Use the centralized result processor
        return await process_command_result(result, command_name="run")

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
        profiles_dir: Optional[str] = Field(
            default=None,
            description="Directory containing the profiles.yml file (defaults to project_dir if not specified)"
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
        
        result = await execute_dbt_command(command, project_dir, profiles_dir)
        
        # Use the centralized result processor
        return await process_command_result(result, command_name="test")

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
        profiles_dir: Optional[str] = Field(
            default=None,
            description="Directory containing the profiles.yml file (defaults to project_dir if not specified)"
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
        result = await execute_dbt_command(command, project_dir, profiles_dir)
        logger.info(f"dbt command result: success={result['success']}, returncode={result.get('returncode')}")
        
        # Use the centralized result processor with ls_formatter
        formatter = partial(ls_formatter, output_format=output_format)
        
        return await process_command_result(
            result,
            command_name="ls",
            output_formatter=formatter,
            include_debug_info=True  # Include extra debug info for this command
        )

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
        profiles_dir: Optional[str] = Field(
            default=None,
            description="Directory containing the profiles.yml file (defaults to project_dir if not specified)"
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
        
        result = await execute_dbt_command(command, project_dir, profiles_dir)
        
        # Use the centralized result processor
        return await process_command_result(result, command_name="compile")

    @mcp.tool()
    async def dbt_debug(
        project_dir: str = Field(
            default=".",
            description="Directory containing the dbt project"
        ),
        profiles_dir: Optional[str] = Field(
            default=None,
            description="Directory containing the profiles.yml file (defaults to project_dir if not specified)"
        )
    ) -> str:
        """Run dbt debug to validate the project setup. An AI agent should use this tool when it needs to troubleshoot configuration issues, check database connectivity, or verify that all project dependencies are properly installed. This is essential for diagnosing problems before attempting to run models or tests.
        
        Returns:
            Output from the dbt debug command as text (this command does not support JSON output format)
        """
        command = ["debug"]
        
        # The --no-print flag is not supported by dbt Cloud CLI
        # We'll rely on proper parsing to handle any print macros
        
        result = await execute_dbt_command(command, project_dir, profiles_dir)
        
        # Use the centralized result processor
        return await process_command_result(result, command_name="debug")

    @mcp.tool()
    async def dbt_deps(
        project_dir: str = Field(
            default=".",
            description="Directory containing the dbt project"
        ),
        profiles_dir: Optional[str] = Field(
            default=None,
            description="Directory containing the profiles.yml file (defaults to project_dir if not specified)"
        )
    ) -> str:
        """Install dbt package dependencies. An AI agent should use this tool when it needs to install or update external packages that the dbt project depends on. This ensures that all required modules, macros, and models from other packages are available before running the project.
        
        Returns:
            Output from the dbt deps command as text (this command does not support JSON output format)
        """
        command = ["deps"]
        
        # The --no-print flag is not supported by dbt Cloud CLI
        # We'll rely on proper parsing to handle any print macros
        
        result = await execute_dbt_command(command, project_dir, profiles_dir)
        
        # Use the centralized result processor
        return await process_command_result(result, command_name="deps")

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
        profiles_dir: Optional[str] = Field(
            default=None,
            description="Directory containing the profiles.yml file (defaults to project_dir if not specified)"
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
        
        result = await execute_dbt_command(command, project_dir, profiles_dir)
        
        # Use the centralized result processor
        return await process_command_result(result, command_name="seed")

    @mcp.tool()
    async def dbt_show(
        models: str = Field(
            description="Specific model to show. For model references, use standard dbt syntax like 'model_name'. For inline SQL, use the format 'select * from {{ ref(\"model_name\") }}' to reference other models."
        ),
        project_dir: str = Field(
            default=".",
            description="Directory containing the dbt project"
        ),
        profiles_dir: Optional[str] = Field(
            default=None,
            description="Directory containing the profiles.yml file (defaults to project_dir if not specified)"
        ),
        limit: Optional[int] = Field(
            default=None,
            description="Limit the number of rows returned"
        ),
        output: Optional[str] = Field(
            default="json",
            description="Output format (json, table, etc.)"
        )
    ) -> str:
        """Preview the results of a model. An AI agent should use this tool when it needs to preview data from a specific model without materializing it. This helps inspect transformation results, debug issues, or demonstrate how data looks after processing without modifying the target database.
        
        Returns:
            Output from the dbt show command, defaulting to JSON format if not specified
        """
        # Check if models parameter contains inline SQL
        is_inline_sql = models.strip().lower().startswith('select ')
        
        # If it's inline SQL, strip out any LIMIT clause as we'll handle it with the --limit parameter
        if is_inline_sql:
            # Use regex to remove LIMIT clause from the SQL
            models = re.sub(r'\bLIMIT\s+\d+\b', '', models, flags=re.IGNORECASE)
        
        command = ["show", "-s", models]
        
        if limit:
            command.extend(["--limit", str(limit)])
        
        # Note: We don't pass the output parameter to the dbt CLI command
        # as it's handled by the MCP server's format parameter
        # The --no-print flag is not supported by dbt Cloud CLI
        # We'll rely on proper parsing to handle any print macros
        
        result = await execute_dbt_command(command, project_dir, profiles_dir)
        
        # Use the centralized result processor with show_formatter
        return await process_command_result(
            result,
            command_name="show",
            output_formatter=show_formatter
        )

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
        profiles_dir: Optional[str] = Field(
            default=None,
            description="Directory containing the profiles.yml file (defaults to project_dir if not specified)"
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
        
        result = await execute_dbt_command(command, project_dir, profiles_dir)
        
        # Use the centralized result processor
        return await process_command_result(result, command_name="build")

    logger.info("Registered all dbt tools")