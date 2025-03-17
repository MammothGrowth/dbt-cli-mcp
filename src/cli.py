#!/usr/bin/env python3
"""
Command-line interface for dbt tools.
"""

import os
import sys
import json
import asyncio
import argparse
from pathlib import Path
from typing import Dict, Any, Optional, List

# No need for these imports anymore
from src.config import initialize as initialize_config


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="DBT CLI MCP Command Line Interface")
    
    # Global options
    parser.add_argument(
        "--dbt-path",
        help="Path to dbt executable",
        default=os.environ.get("DBT_PATH", "dbt")
    )
    parser.add_argument(
        "--env-file",
        help="Path to environment file",
        default=os.environ.get("ENV_FILE", ".env")
    )
    parser.add_argument(
        "--log-level",
        help="Logging level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default=os.environ.get("LOG_LEVEL", "INFO")
    )
    parser.add_argument(
        "--format",
        help="Output format",
        choices=["text", "json"],
        default="text"
    )
    
    # Set up subparsers for each command
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # dbt_run command
    run_parser = subparsers.add_parser("run", help="Run dbt models")
    run_parser.add_argument("--models", help="Specific models to run")
    run_parser.add_argument("--selector", help="Named selector to use")
    run_parser.add_argument("--exclude", help="Models to exclude")
    run_parser.add_argument("--project-dir", help="Directory containing the dbt project", default=".")
    run_parser.add_argument("--full-refresh", help="Perform a full refresh", action="store_true")
    
    # dbt_test command
    test_parser = subparsers.add_parser("test", help="Run dbt tests")
    test_parser.add_argument("--models", help="Specific models to test")
    test_parser.add_argument("--selector", help="Named selector to use")
    test_parser.add_argument("--exclude", help="Models to exclude")
    test_parser.add_argument("--project-dir", help="Directory containing the dbt project", default=".")
    
    # dbt_ls command
    ls_parser = subparsers.add_parser("ls", help="List dbt resources")
    ls_parser.add_argument("--models", help="Specific models to list")
    ls_parser.add_argument("--selector", help="Named selector to use")
    ls_parser.add_argument("--exclude", help="Models to exclude")
    ls_parser.add_argument("--resource-type", help="Type of resource to list")
    ls_parser.add_argument("--project-dir", help="Directory containing the dbt project", default=".")
    ls_parser.add_argument("--output-format", help="Output format", choices=["json", "name", "path", "selector"], default="json")
    
    # dbt_compile command
    compile_parser = subparsers.add_parser("compile", help="Compile dbt models")
    compile_parser.add_argument("--models", help="Specific models to compile")
    compile_parser.add_argument("--selector", help="Named selector to use")
    compile_parser.add_argument("--exclude", help="Models to exclude")
    compile_parser.add_argument("--project-dir", help="Directory containing the dbt project", default=".")
    
    # dbt_debug command
    debug_parser = subparsers.add_parser("debug", help="Debug dbt project")
    debug_parser.add_argument("--project-dir", help="Directory containing the dbt project", default=".")
    
    # dbt_deps command
    deps_parser = subparsers.add_parser("deps", help="Install dbt package dependencies")
    deps_parser.add_argument("--project-dir", help="Directory containing the dbt project", default=".")
    
    # dbt_seed command
    seed_parser = subparsers.add_parser("seed", help="Load CSV files as seed data")
    seed_parser.add_argument("--selector", help="Named selector to use")
    seed_parser.add_argument("--exclude", help="Seeds to exclude")
    seed_parser.add_argument("--project-dir", help="Directory containing the dbt project", default=".")
    
    # dbt_show command
    show_parser = subparsers.add_parser("show", help="Preview model results")
    show_parser.add_argument("--models", help="Specific model to show", required=True)
    show_parser.add_argument("--project-dir", help="Directory containing the dbt project", default=".")
    show_parser.add_argument("--limit", help="Limit the number of rows returned", type=int)
    
    # dbt_build command
    build_parser = subparsers.add_parser("build", help="Run build command")
    build_parser.add_argument("--models", help="Specific models to build")
    build_parser.add_argument("--selector", help="Named selector to use")
    build_parser.add_argument("--exclude", help="Models to exclude")
    build_parser.add_argument("--project-dir", help="Directory containing the dbt project", default=".")
    build_parser.add_argument("--full-refresh", help="Perform a full refresh", action="store_true")
    
    # configure command
    configure_parser = subparsers.add_parser("configure", help="Configure dbt path")
    configure_parser.add_argument("path", help="Path to dbt executable")
    
    return parser.parse_args()


# Define tool functions directly
async def run_dbt_run(models=None, selector=None, exclude=None, project_dir=".", full_refresh=False):
    """Run dbt models."""
    command = ["run"]
    
    if models:
        command.extend(["-s", models])
    
    if selector:
        command.extend(["--selector", selector])
    
    if exclude:
        command.extend(["--exclude", exclude])
    
    if full_refresh:
        command.append("--full-refresh")
    
    from src.command import execute_dbt_command
    result = await execute_dbt_command(command, project_dir)
    
    if not result["success"]:
        error_msg = f"Error executing dbt run: {result['error']}"
        if 'output' in result and result['output']:
            error_msg += f"\nOutput: {result['output']}"
        return error_msg
    
    return json.dumps(result["output"]) if isinstance(result["output"], (dict, list)) else str(result["output"])

async def run_dbt_test(models=None, selector=None, exclude=None, project_dir="."):
    """Run dbt tests."""
    command = ["test"]
    
    if models:
        command.extend(["-s", models])
    
    if selector:
        command.extend(["--selector", selector])
    
    if exclude:
        command.extend(["--exclude", exclude])
    
    from src.command import execute_dbt_command
    result = await execute_dbt_command(command, project_dir)
    
    if not result["success"]:
        error_msg = f"Error executing dbt test: {result['error']}"
        if result["output"]:
            error_msg += f"\nOutput: {result['output']}"
        return error_msg
    
    return json.dumps(result["output"]) if isinstance(result["output"], (dict, list)) else str(result["output"])

async def run_dbt_ls(models=None, selector=None, exclude=None, resource_type=None, project_dir=".", output_format="json"):
    """List dbt resources."""
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
    
    from src.command import execute_dbt_command, parse_dbt_list_output
    import re
    
    result = await execute_dbt_command(command, project_dir)
    
    if not result["success"]:
        error_msg = f"Error executing dbt ls: {result['error']}"
        if "output" in result and result["output"]:
            error_msg += f"\nOutput: {result['output']}"
        return error_msg
    
    # For json format, parse the output and return as JSON
    if output_format == "json":
        # Return raw output if it's an empty string or None
        if not result["output"]:
            return "[]"
        
        # If the output is already a list, return it directly
        if isinstance(result["output"], list):
            return json.dumps(result["output"])
        
        # Parse the output
        parsed = parse_dbt_list_output(result["output"])
        return json.dumps(parsed, indent=2)
    
    # For other formats, return the raw output
    return str(result["output"])

async def run_dbt_compile(models=None, selector=None, exclude=None, project_dir="."):
    """Compile dbt models."""
    command = ["compile"]
    
    if models:
        command.extend(["-s", models])
    
    if selector:
        command.extend(["--selector", selector])
    
    if exclude:
        command.extend(["--exclude", exclude])
    
    from src.command import execute_dbt_command
    result = await execute_dbt_command(command, project_dir)
    
    if not result["success"]:
        error_msg = f"Error executing dbt compile: {result['error']}"
        if result["output"]:
            error_msg += f"\nOutput: {result['output']}"
        return error_msg
    
    return json.dumps(result["output"]) if isinstance(result["output"], (dict, list)) else str(result["output"])

async def run_dbt_debug(project_dir="."):
    """Debug dbt project."""
    command = ["debug"]
    
    from src.command import execute_dbt_command
    result = await execute_dbt_command(command, project_dir)
    
    if not result["success"]:
        error_msg = f"Error executing dbt debug: {result['error']}"
        if result["output"]:
            error_msg += f"\nOutput: {result['output']}"
        return error_msg
    
    return json.dumps(result["output"]) if isinstance(result["output"], (dict, list)) else str(result["output"])

async def run_dbt_deps(project_dir="."):
    """Install dbt package dependencies."""
    command = ["deps"]
    
    from src.command import execute_dbt_command
    result = await execute_dbt_command(command, project_dir)
    
    if not result["success"]:
        error_msg = f"Error executing dbt deps: {result['error']}"
        if result["output"]:
            error_msg += f"\nOutput: {result['output']}"
        return error_msg
    
    return json.dumps(result["output"]) if isinstance(result["output"], (dict, list)) else str(result["output"])

async def run_dbt_seed(selector=None, exclude=None, project_dir="."):
    """Load CSV files as seed data."""
    command = ["seed"]
    
    if selector:
        command.extend(["--selector", selector])
    
    if exclude:
        command.extend(["--exclude", exclude])
    
    from src.command import execute_dbt_command
    result = await execute_dbt_command(command, project_dir)
    
    if not result["success"]:
        error_msg = f"Error executing dbt seed: {result['error']}"
        if result["output"]:
            error_msg += f"\nOutput: {result['output']}"
        return error_msg
    
    return json.dumps(result["output"]) if isinstance(result["output"], (dict, list)) else str(result["output"])

async def run_dbt_show(models, project_dir=".", limit=None):
    """Preview model results."""
    command = ["show", "-s", models]
    
    if limit:
        command.extend(["--limit", str(limit)])
    
    from src.command import execute_dbt_command
    result = await execute_dbt_command(command, project_dir)
    
    if not result["success"]:
        error_msg = f"Error executing dbt show: {result['error']}"
        if result["output"]:
            error_msg += f"\nOutput: {result['output']}"
        return error_msg
    
    return json.dumps(result["output"]) if isinstance(result["output"], (dict, list)) else str(result["output"])

async def run_dbt_build(models=None, selector=None, exclude=None, project_dir=".", full_refresh=False):
    """Run build command."""
    command = ["build"]
    
    if models:
        command.extend(["-s", models])
    
    if selector:
        command.extend(["--selector", selector])
    
    if exclude:
        command.extend(["--exclude", exclude])
    
    if full_refresh:
        command.append("--full-refresh")
    
    from src.command import execute_dbt_command
    result = await execute_dbt_command(command, project_dir)
    
    if not result["success"]:
        error_msg = f"Error executing dbt build: {result['error']}"
        if result["output"]:
            error_msg += f"\nOutput: {result['output']}"
        return error_msg
    
    return json.dumps(result["output"]) if isinstance(result["output"], (dict, list)) else str(result["output"])

async def run_configure_dbt_path(path):
    """Configure dbt path."""
    import os
    from src.config import set_config
    
    if not os.path.isfile(path):
        return f"Error: File not found at {path}"
    
    set_config("dbt_path", path)
    return f"dbt path configured to: {path}"

async def main_async() -> None:
    """Main entry point for the CLI."""
    args = parse_args()
    
    # Set environment variables from arguments
    os.environ["DBT_PATH"] = args.dbt_path
    os.environ["ENV_FILE"] = args.env_file
    os.environ["LOG_LEVEL"] = args.log_level
    
    # Initialize configuration
    initialize_config()
    
    # Map commands to functions
    command_map = {
        "run": run_dbt_run,
        "test": run_dbt_test,
        "ls": run_dbt_ls,
        "compile": run_dbt_compile,
        "debug": run_dbt_debug,
        "deps": run_dbt_deps,
        "seed": run_dbt_seed,
        "show": run_dbt_show,
        "build": run_dbt_build,
        "configure": run_configure_dbt_path
    }
    
    if args.command not in command_map:
        print(f"Command '{args.command}' not found. Use --help for usage information.")
        sys.exit(1)
    
    # Prepare arguments for the function
    func_args = {}
    
    if args.command == "run":
        func_args = {
            "models": args.models,
            "selector": args.selector,
            "exclude": args.exclude,
            "project_dir": args.project_dir,
            "full_refresh": args.full_refresh
        }
    elif args.command == "test":
        func_args = {
            "models": args.models,
            "selector": args.selector,
            "exclude": args.exclude,
            "project_dir": args.project_dir
        }
    elif args.command == "ls":
        func_args = {
            "models": args.models,
            "selector": args.selector,
            "exclude": args.exclude,
            "resource_type": args.resource_type,
            "project_dir": args.project_dir,
            "output_format": args.output_format
        }
    elif args.command == "compile":
        func_args = {
            "models": args.models,
            "selector": args.selector,
            "exclude": args.exclude,
            "project_dir": args.project_dir
        }
    elif args.command == "debug":
        func_args = {
            "project_dir": args.project_dir
        }
    elif args.command == "deps":
        func_args = {
            "project_dir": args.project_dir
        }
    elif args.command == "seed":
        func_args = {
            "selector": args.selector,
            "exclude": args.exclude,
            "project_dir": args.project_dir
        }
    elif args.command == "show":
        func_args = {
            "models": args.models,
            "project_dir": args.project_dir,
            "limit": args.limit
        }
    elif args.command == "build":
        func_args = {
            "models": args.models,
            "selector": args.selector,
            "exclude": args.exclude,
            "project_dir": args.project_dir,
            "full_refresh": args.full_refresh
        }
    elif args.command == "configure":
        func_args = {
            "path": args.path
        }
    
    # Execute the function
    result = await command_map[args.command](**{k: v for k, v in func_args.items() if v is not None})
    
    # Print the result
    if args.format == "json":
        try:
            # If result is already a JSON string, parse it first
            if isinstance(result, str) and (result.startswith("{") or result.startswith("[")):
                parsed = json.loads(result)
                print(json.dumps(parsed, indent=2))
            else:
                print(json.dumps({"output": result}, indent=2))
        except json.JSONDecodeError:
            print(json.dumps({"output": result}, indent=2))
    else:
        print(result)


def main_entry() -> None:
    """Entry point for setuptools."""
    asyncio.run(main_async())


if __name__ == "__main__":
    asyncio.run(main_async())