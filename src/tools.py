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
            description="ABSOLUTE PATH to the directory containing the dbt project (e.g. '/Users/username/projects/dbt_project' not '.')"
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
            description="ABSOLUTE PATH to the directory containing the dbt project (e.g. '/Users/username/projects/dbt_project' not '.')"
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
            description="Specific models to list, using the dbt selection syntax. Note that you probably want to specify your selection here e.g. silver.fact"
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
            description="ABSOLUTE PATH to the directory containing the dbt project (e.g. '/Users/username/projects/dbt_project' not '.')"
        ),
        profiles_dir: Optional[str] = Field(
            default=None,
            description="Directory containing the profiles.yml file (defaults to project_dir if not specified)"
        ),
        output_format: str = Field(
            default="json",
            description="Output format (json, name, path, or selector)"
        ),
        verbose: bool = Field(
            default=False,
            description="Return full JSON output instead of simplified version"
        )
    ) -> str:
        """List dbt resources. An AI agent should use this tool when it needs to discover available models, tests, sources, and other resources within a dbt project. This helps the agent understand the project structure, identify dependencies, and select specific resources for other operations like running or testing.

        Returns:
            When output_format is 'json' (default):
              - With verbose=False (default): returns a simplified JSON with only name, resource_type, and depends_on.nodes
              - With verbose=True: returns a full JSON with all resource details
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
        formatter = partial(ls_formatter, output_format=output_format, verbose=verbose)

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
            description="ABSOLUTE PATH to the directory containing the dbt project (e.g. '/Users/username/projects/dbt_project' not '.')"
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
            description="ABSOLUTE PATH to the directory containing the dbt project (e.g. '/Users/username/projects/dbt_project' not '.')"
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
            description="ABSOLUTE PATH to the directory containing the dbt project (e.g. '/Users/username/projects/dbt_project' not '.')"
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
            description="ABSOLUTE PATH to the directory containing the dbt project (e.g. '/Users/username/projects/dbt_project' not '.')"
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
            description="ABSOLUTE PATH to the directory containing the dbt project (e.g. '/Users/username/projects/dbt_project' not '.')"
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
        # Use enhanced SQL detection
        is_inline_sql, sql_type = is_inline_sql_query(models)

        # If it's SQL, check for security risks
        if is_inline_sql:
            has_risk, risk_reason = contains_mutation_risk(models)
            if has_risk:
                logger.warning(f"Security risk detected in SQL: {risk_reason}")
                error_result = {
                    "success": False,
                    "output": f"Security validation failed: {risk_reason}. For security reasons, mutation operations are not allowed.",
                    "error": "SecurityValidationError",
                    "returncode": 1
                }
                return await process_command_result(
                    error_result,
                    command_name="show",
                    include_debug_info=True
                )

        logger.info(f"dbt_show called with models={models}, is_inline_sql={is_inline_sql}")

        # If it's inline SQL, strip out any LIMIT clause as we'll handle it with the --limit parameter
        if is_inline_sql:
            # Use regex to remove LIMIT clause from the SQL
            original_models = models
            models = re.sub(r'\bLIMIT\s+\d+\b', '', models, flags=re.IGNORECASE)
            logger.info(f"Stripped LIMIT clause: {original_models} -> {models}")

            # For inline SQL, use the --inline flag with the SQL as its value
            command = ["show", f"--inline={models}", "--output", output or "json"]

            # Only add --limit if the inline type is WITH or SELECT (select_inline vs meta_inline)
            if limit and sql_type in ["WITH", "SELECT"]:
                command.extend(["--limit", str(limit)])

            logger.info(f"Executing dbt command: {' '.join(command)}")
            # Don't use --quiet for inline SQL to ensure we get error messages
            result = await execute_dbt_command(command, project_dir, profiles_dir)

            logger.info(f"Command result: success={result['success']}, returncode={result.get('returncode')}")
            if isinstance(result["output"], str):
                logger.info(f"Output (first 100 chars): {result['output'][:100]}")
            elif isinstance(result["output"], (dict, list)):
                logger.info(f"Output structure: {json.dumps(result['output'])[:100]}")

            # Check for specific error patterns in the output
            if not result["success"] or (
                isinstance(result["output"], str) and
                any(err in result["output"].lower() for err in ["error", "failed", "syntax", "exception"])
            ):
                logger.warning(f"Error detected in output: {result['output'][:200]}")
                error_result = {
                    "success": False,
                    "output": f"Error executing inline SQL\n{result['output']}",
                    "error": result["error"],
                    "returncode": result["returncode"]
                }
                return await process_command_result(
                    error_result,
                    command_name="show",
                    include_debug_info=True
                )
        else:
            # For regular model references, check if the model exists first
            check_command = ["ls", "-s", models]
            check_result = await execute_dbt_command(check_command, project_dir, profiles_dir)

            # If the model doesn't exist, return the error message
            if not check_result["success"] or "does not match any enabled nodes" in str(check_result["output"]):
                error_result = {
                    "success": False,
                    "output": f"Model does not exist or is not enabled\n{check_result['output']}",
                    "error": check_result["error"],
                    "returncode": check_result["returncode"]
                }
                return await process_command_result(
                    error_result,
                    command_name="show",
                    include_debug_info=True
                )

            # If the model exists, run the show command with --quiet and --output json
            command = ["show", "-s", models, "--quiet", "--output", output or "json"]

            if limit:
                command.extend(["--limit", str(limit)])

            result = await execute_dbt_command(command, project_dir, profiles_dir)

        # Use the centralized result processor
        return await process_command_result(
            result,
            command_name="show",
            output_formatter=show_formatter,
            include_debug_info=True
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
            description="ABSOLUTE PATH to the directory containing the dbt project (e.g. '/Users/username/projects/dbt_project' not '.')"
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


def is_inline_sql_query(query: str) -> tuple[bool, Optional[str]]:
    """
    Determine if the given string is an inline SQL query or a model reference.

    This function uses multiple heuristics to determine if a string is likely
    an SQL query rather than a model name:
    1. Checks for common SQL keywords at the beginning
    2. Looks for SQL syntax patterns
    3. Considers length and complexity
    4. Handles SQL with comments (both single-line and multi-line)
    5. Recognizes dbt templating syntax

    Args:
        query: The string to check

    Returns:
        A tuple of (is_sql, sql_type) where:
        - is_sql: True if the input is SQL, False otherwise
        - sql_type: The type of SQL statement if is_sql is True, None otherwise
          (e.g., "SELECT", "WITH", "SHOW", etc.)
    """
    # Normalize the query by trimming whitespace
    normalized_query = query.strip()

    # Skip empty queries
    if not normalized_query:
        return False, None

    # Check if the query contains SQL comments
    has_single_line_comment = '--' in normalized_query
    has_multi_line_comment = '/*' in normalized_query and '*/' in normalized_query

    # If the query only contains comments, it's still SQL
    if has_single_line_comment or has_multi_line_comment:
        # Check if it's only comments by removing them and seeing if anything remains
        # Remove /* */ style comments
        sql_no_comments = re.sub(r'/\*.*?\*/', ' ', normalized_query, flags=re.DOTALL)
        # Remove -- style comments
        sql_no_comments = re.sub(r'--.*?$', ' ', sql_no_comments, flags=re.MULTILINE)
        # Normalize whitespace
        sql_no_comments = ' '.join(sql_no_comments.split()).strip()

        if not sql_no_comments:
            # If nothing remains after removing comments, it's only comments
            return True, "COMMENT"

    # Convert to lowercase for case-insensitive matching
    normalized_query_lower = normalized_query.lower()

    # Check for SQL comments at the beginning and skip them for detection
    # This handles both single-line (--) and multi-line (/* */) comments
    comment_pattern = r'^(\s*(--[^\n]*\n|\s*/\*.*?\*/\s*)*\s*)'
    match = re.match(comment_pattern, normalized_query_lower, re.DOTALL)
    if match:
        # Skip past the comments for keyword detection
        start_pos = match.end()
        if start_pos >= len(normalized_query_lower):
            # If the query is only comments, it's still SQL
            return True, "COMMENT"
        normalized_query_lower = normalized_query_lower[start_pos:]

    # Common SQL statement starting keywords
    sql_starters = {
        'select': 'SELECT',
        'with': 'WITH',
        'show': 'SHOW',
        'describe': 'DESCRIBE',
        'explain': 'EXPLAIN',
        'analyze': 'ANALYZE',
        'use': 'USE',
        'set': 'SET'
    }

    # Check if the query starts with a common SQL keyword
    for keyword, sql_type in sql_starters.items():
        if normalized_query_lower.startswith(keyword + ' '):
            return True, sql_type

    # Check for more complex patterns like CTEs
    # WITH clause followed by identifier and AS
    cte_pattern = r'^\s*with\s+[a-z0-9_]+\s+as\s*\('
    if re.search(cte_pattern, normalized_query_lower, re.IGNORECASE):
        return True, "WITH"

    # Check for Jinja templating with SQL inside
    jinja_sql_pattern = r'{{\s*sql\s*}}'
    if re.search(jinja_sql_pattern, normalized_query_lower):
        return True, "JINJA"

    # Check for dbt ref or source macros which indicate SQL
    dbt_macro_pattern = r'{{\s*(ref|source)\s*\(\s*[\'"]'
    if re.search(dbt_macro_pattern, normalized_query_lower):
        return True, "DBT_MACRO"

    # If the query contains certain SQL syntax elements, it's likely SQL
    sql_syntax_elements = [
        r'\bfrom\s+[a-z0-9_]+',  # FROM clause
        r'\bjoin\s+[a-z0-9_]+',   # JOIN clause
        r'\bwhere\s+',            # WHERE clause
        r'\bgroup\s+by\s+',       # GROUP BY clause
        r'\border\s+by\s+',       # ORDER BY clause
        r'\bhaving\s+',           # HAVING clause
        r'\bunion\s+',            # UNION operator
        r'\bcase\s+when\s+'       # CASE expression
    ]

    for pattern in sql_syntax_elements:
        if re.search(pattern, normalized_query_lower, re.IGNORECASE):
            return True, "SQL_SYNTAX"

    # If the query is long and contains spaces, it's more likely to be SQL than a model name
    if len(normalized_query_lower) > 30 and ' ' in normalized_query_lower:
        return True, "COMPLEX"

    # If none of the above conditions are met, it's likely a model name
    return False, None


def contains_mutation_risk(sql: str) -> tuple[bool, str]:
    """
    Check if the SQL query contains potentially dangerous operations.

    This function scans SQL for operations that could modify or delete data,
    which should be prohibited in a read-only context like dbt show.

    Args:
        sql: The SQL query to check

    Returns:
        A tuple of (has_risk, reason) where:
        - has_risk: True if the query contains risky operations, False otherwise
        - reason: A description of the risk if has_risk is True, empty string otherwise
    """
    # Normalize the SQL by removing comments and extra whitespace
    # This helps prevent comment-based evasion techniques

    # Remove /* */ style comments
    sql_no_comments = re.sub(r'/\*.*?\*/', ' ', sql, flags=re.DOTALL)

    # Remove -- style comments
    sql_no_comments = re.sub(r'--.*?$', ' ', sql_no_comments, flags=re.MULTILINE)

    # Normalize whitespace
    normalized_sql = ' '.join(sql_no_comments.split()).lower()

    # Check for multiple SQL statements (potential SQL injection)
    # This needs to be checked first to ensure proper error message
    if ';' in normalized_sql:
        # Check if there's actual SQL after the semicolon
        statements = normalized_sql.split(';')
        if len(statements) > 1:
            for stmt in statements[1:]:
                if stmt.strip():
                    return True, "Multiple SQL statements detected - potential SQL injection risk"

    # Dangerous SQL operations patterns
    dangerous_patterns = [
        # Data modification operations
        (r'\bdelete\s+from\b', "DELETE operation detected"),
        (r'\btruncate\s+table\b', "TRUNCATE operation detected"),
        (r'\bdrop\s+table\b', "DROP TABLE operation detected"),
        (r'\bdrop\s+database\b', "DROP DATABASE operation detected"),
        (r'\bdrop\s+schema\b', "DROP SCHEMA operation detected"),
        (r'\balter\s+table\b', "ALTER TABLE operation detected"),
        (r'\bcreate\s+table\b', "CREATE TABLE operation detected"),
        (r'\bcreate\s+or\s+replace\b', "CREATE OR REPLACE operation detected"),
        (r'\binsert\s+into\b', "INSERT operation detected"),
        (r'\bupdate\s+.*?\bset\b', "UPDATE operation detected"),
        (r'\bmerge\s+into\b', "MERGE operation detected"),

        # Database administration operations
        (r'\bgrant\b', "GRANT operation detected"),
        (r'\brevoke\b', "REVOKE operation detected"),
        (r'\bcreate\s+user\b', "CREATE USER operation detected"),
        (r'\balter\s+user\b', "ALTER USER operation detected"),
        (r'\bdrop\s+user\b', "DROP USER operation detected"),

        # Execution of arbitrary code
        (r'\bexec\b', "EXEC operation detected"),
        (r'\bexecute\s+immediate\b', "EXECUTE IMMEDIATE detected"),
        (r'\bcall\b', "CALL procedure detected")
    ]

    # Check for each dangerous pattern
    for pattern, reason in dangerous_patterns:
        if re.search(pattern, normalized_sql, re.IGNORECASE):
            return True, reason

    # Check for specific Snowflake commands that could be risky
    snowflake_patterns = [
        (r'\bcopy\s+into\b', "Snowflake COPY INTO operation detected"),
        (r'\bunload\s+to\b', "Snowflake UNLOAD operation detected"),
        (r'\bput\b', "Snowflake PUT operation detected"),
        (r'\bremove\b', "Snowflake REMOVE operation detected"),
        (r'\bmodify\b', "Snowflake MODIFY operation detected")
    ]

    for pattern, reason in snowflake_patterns:
        if re.search(pattern, normalized_sql, re.IGNORECASE):
            return True, reason

    # No risks detected
    return False, ""