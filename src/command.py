"""
Command execution utilities for the DBT CLI MCP Server.

This module handles executing dbt CLI commands and processing their output.
"""

import os
import json
import logging
import subprocess
import asyncio
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Union

import dotenv

from src.config import get_config

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
    
    # Ensure HOME is set if not already defined
    if "HOME" not in env_vars:
        env_vars["HOME"] = str(Path.home())
        logger.debug(f"Setting HOME environment variable to {env_vars['HOME']}")
    
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
    project_dir: str = "."
) -> Dict[str, Any]:
    """
    Execute a dbt command and return the result.
    
    Args:
        command: List of command arguments (without the dbt executable)
        project_dir: Directory containing the dbt project
        
    Returns:
        Dictionary containing command result:
        {
            "success": bool,
            "output": str or dict,
            "error": str or None,
            "returncode": int
        }
    """
    # Get dbt path from config
    dbt_path = get_config("dbt_path", "dbt")
    full_command = [dbt_path] + command
    
    # Load environment variables
    env_vars = load_environment(project_dir)
    
    # Explicitly set HOME environment variable in os.environ
    os.environ["HOME"] = str(Path.home())
    logger.debug(f"Explicitly setting HOME environment variable in os.environ to {os.environ['HOME']}")
    
    # Set DBT_PROFILES_DIR to the absolute path of the project directory if not already set
    if "DBT_PROFILES_DIR" not in os.environ:
        # For testing compatibility, use the value from the .env file if available
        if "DBT_PROFILES_DIR" in env_vars:
            os.environ["DBT_PROFILES_DIR"] = env_vars["DBT_PROFILES_DIR"]
        else:
            # Convert project_dir to an absolute path
            abs_project_dir = str(Path(project_dir).resolve())
            os.environ["DBT_PROFILES_DIR"] = abs_project_dir
            logger.debug(f"Setting DBT_PROFILES_DIR in os.environ to {abs_project_dir} (from {project_dir})")
    
    # Update env_vars with the current os.environ
    env_vars.update(os.environ)
    
    logger.debug(f"Executing command: {' '.join(full_command)} in {project_dir}")
    
    try:
        # Execute the command
        process = await asyncio.create_subprocess_exec(
            *full_command,
            cwd=project_dir,
            env=env_vars,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        # Communicate with the process
        stdout_bytes, stderr_bytes = await process.communicate()
        stdout = stdout_bytes.decode('utf-8') if stdout_bytes else ""
        stderr = stderr_bytes.decode('utf-8') if stderr_bytes else ""
        success = process.returncode == 0
        
        # Try to parse JSON from stdout if available
        output = stdout
        
        # Check if this is dbt Cloud CLI output format with embedded JSON in log lines
        if stdout.strip().startswith('[') and '"name":' in stdout:
            try:
                # Parse the entire output as JSON array
                json_array = json.loads(stdout)
                
                # If it's an array of log objects with name field (dbt Cloud CLI format)
                if isinstance(json_array, list) and all(isinstance(item, dict) and "name" in item for item in json_array):
                    logger.debug(f"Detected dbt Cloud CLI output format with {len(json_array)} items")
                    output = json_array
            except json.JSONDecodeError:
                # Not valid JSON array, keep as string
                logger.debug("Failed to parse stdout as JSON array, keeping as string")
                pass
        else:
            # Try standard JSON parsing
            try:
                output = json.loads(stdout)
            except json.JSONDecodeError:
                # Not JSON, keep as string
                logger.debug("Failed to parse stdout as standard JSON, keeping as string")
                pass
            
        result = {
            "success": success,
            "output": output,
            "error": stderr if not success else None,
            "returncode": process.returncode
        }
        
        if not success:
            logger.warning(f"Command failed with exit code {process.returncode}: {stderr}")
            
            # Log full environment for debugging
            logger.debug(f"Full environment variables: {env_vars}")
            logger.debug(f"Current directory: {project_dir}")
            logger.debug(f"Full command: {' '.join(full_command)}")
        
        return result
    except Exception as e:
        import traceback
        stack_trace = traceback.format_exc()
        logger.error(f"Error executing command: {e}\nStack trace: {stack_trace}")
        return {
            "success": False,
            "output": None,
            "error": f"{str(e)}\nStack trace: {stack_trace}",
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
    logger.debug(f"Parsing dbt list output with type: {type(output)}")
    
    # If already parsed as JSON dictionary with nodes
    if isinstance(output, dict) and "nodes" in output:
        return [
            {"name": name, **details}
            for name, details in output["nodes"].items()
        ]
    
    # Handle dbt Cloud CLI output format - an array of objects with name property containing embedded JSON
    if isinstance(output, list) and all(isinstance(item, dict) and "name" in item for item in output):
        logger.debug(f"Found dbt Cloud CLI output format with {len(output)} items")
        extracted_models = []
        
        for item in output:
            name_value = item["name"]
            
            # Skip log messages that don't contain model data
            if any(log_msg in name_value for log_msg in [
                "Sending project", "Created invocation", "Waiting for",
                "Streaming", "Running dbt", "Invocation has finished"
            ]):
                continue
            
            # Extract model data from timestamped JSON lines (e.g., "00:59:06 {json}")
            timestamp_prefix_match = re.match(r'^(\d\d:\d\d:\d\d)\s+(.+)$', name_value)
            if timestamp_prefix_match:
                json_string = timestamp_prefix_match.group(2)
                try:
                    model_data = json.loads(json_string)
                    if isinstance(model_data, dict):
                        # Only add entries that have both name and resource_type
                        if "name" in model_data and "resource_type" in model_data:
                            extracted_models.append(model_data)
                except json.JSONDecodeError:
                    # Not valid JSON, skip this line
                    logger.debug(f"Failed to parse JSON from: {json_string[:30]}...")
                    continue
        
        # If we found model data, return it
        if extracted_models:
            logger.debug(f"Successfully extracted {len(extracted_models)} models from dbt Cloud CLI output")
            return extracted_models
        
        # If no model data found, return empty list
        logger.warning("No valid model data found in dbt Cloud CLI output")
        return []
    
    # If already parsed as regular JSON list
    if isinstance(output, list):
        # For test compatibility
        if all(isinstance(item, dict) and "name" in item for item in output):
            return output
        # For empty lists or other list types, return as is
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
                line = line.strip()
                if not line:
                    continue
                    
                # Check for dbt Cloud CLI format with timestamps (e.g., "00:59:06 {json}")
                timestamp_match = re.match(r'^(\d\d:\d\d:\d\d)\s+(.+)$', line)
                if timestamp_match:
                    json_part = timestamp_match.group(2)
                    try:
                        model_data = json.loads(json_part)
                        if isinstance(model_data, dict) and "name" in model_data and "resource_type" in model_data:
                            models.append(model_data)
                        continue
                    except json.JSONDecodeError:
                        pass
                
                # Fall back to simple name-only format
                models.append({"name": line})
            return models
    
    # Fallback: return empty list
    logger.warning("Could not parse dbt list output in any recognized format")
    return []