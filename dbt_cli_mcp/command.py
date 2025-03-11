"""
Command execution utilities for the DBT CLI MCP Server.

This module handles executing dbt CLI commands, processing their output,
and provides mocking capabilities for testing.
"""

import os
import json
import logging
import subprocess
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional, Union

import dotenv

from dbt_cli_mcp.config import get_config

# Logger for this module
logger = logging.getLogger(__name__)


def load_environment(project_dir: str) -> Dict[str, str]:
    """
    Load environment variables from .env file in the project directory.
    
    Args:
        project_dir: Directory containing the dbt project
        
    Returns:
        Dictionary of environment variables
    """
    env_file = Path(project_dir) / get_config("env_file")
    env_vars = os.environ.copy()
    
    if env_file.exists():
        logger.debug(f"Loading environment from {env_file}")
        # Load variables from .env file
        dotenv.load_dotenv(dotenv_path=env_file)
        env_vars.update({k: v for k, v in os.environ.items()})
    else:
        logger.debug(f"Environment file not found: {env_file}")
        
    return env_vars


async def execute_dbt_command(
    command: List[str],
    project_dir: str = ".",
    mock_mode: Optional[bool] = None,
    mock_response: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Execute a dbt command and return the result.
    
    Args:
        command: List of command arguments (without the dbt executable)
        project_dir: Directory containing the dbt project
        mock_mode: Override config mock_mode setting
        mock_response: Mock response to return (for testing)
        
    Returns:
        Dictionary containing command result:
        {
            "success": bool,
            "output": str or dict,
            "error": str or None,
            "returncode": int
        }
    """
    # Use mock_mode from config if not explicitly provided
    if mock_mode is None:
        mock_mode = get_config("mock_mode", False)
    
    # Return mock response if in mock mode and a response is provided
    if mock_mode and mock_response is not None:
        logger.debug(f"Returning mock response for command: dbt {' '.join(command)}")
        return mock_response
    
    # Get dbt path from config
    dbt_path = get_config("dbt_path", "dbt")
    full_command = [dbt_path] + command
    
    # Load environment variables
    env_vars = load_environment(project_dir)
    
    logger.debug(f"Executing command: {' '.join(full_command)} in {project_dir}")
    
    try:
        # Execute the command
        process = await asyncio.create_subprocess_exec(
            *full_command,
            cwd=project_dir,
            env=env_vars,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            text=True
        )
        
        stdout, stderr = await process.communicate()
        success = process.returncode == 0
        
        # Try to parse JSON from stdout if available
        output = stdout
        try:
            output = json.loads(stdout)
        except json.JSONDecodeError:
            # Not JSON, keep as string
            pass
            
        result = {
            "success": success,
            "output": output,
            "error": stderr if not success else None,
            "returncode": process.returncode
        }
        
        if not success:
            logger.warning(f"Command failed with exit code {process.returncode}: {stderr}")
        
        return result
    except Exception as e:
        logger.error(f"Error executing command: {e}")
        return {
            "success": False,
            "output": None,
            "error": str(e),
            "returncode": -1
        }


def parse_dbt_list_output(output: Union[str, Dict, List]) -> List[Dict[str, Any]]:
    """
    Parse the output from dbt list command.
    
    Args:
        output: Output from dbt list command (string or parsed JSON)
        
    Returns:
        List of resources
    """
    # If already parsed as JSON dictionary with nodes
    if isinstance(output, dict) and "nodes" in output:
        return [
            {"name": name, **details}
            for name, details in output["nodes"].items()
        ]
    
    # If already parsed as JSON list
    if isinstance(output, list):
        return output
    
    # If string, try to parse as JSON
    if isinstance(output, str):
        try:
            parsed = json.loads(output)
            if isinstance(parsed, dict) and "nodes" in parsed:
                return [
                    {"name": name, **details}
                    for name, details in parsed["nodes"].items()
                ]
            elif isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            # Not JSON, parse text format (simplified)
            models = []
            for line in output.splitlines():
                if line.strip():
                    models.append({"name": line.strip()})
            return models
    
    # Fallback: return empty list
    return []


async def load_mock_response(command_name: str) -> Optional[Dict[str, Any]]:
    """
    Load a mock response for a command from the mock_responses directory.
    
    Args:
        command_name: Name of the command (e.g., 'run', 'test')
        
    Returns:
        Mock response or None if not found
    """
    # Get the directory of this file
    current_dir = Path(__file__).parent.parent
    mock_file = current_dir / "tests" / "mock_responses" / f"{command_name}.json"
    
    if mock_file.exists():
        try:
            with open(mock_file, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading mock response: {e}")
            
    return None