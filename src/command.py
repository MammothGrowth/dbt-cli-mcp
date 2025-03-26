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
from typing import List, Dict, Any, Optional, Union, Callable

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
    project_dir: str = ".",
    profiles_dir: Optional[str] = None
) -> Dict[str, Any]:
    """
    Execute a dbt command and return the result.
    
    Args:
        command: List of command arguments (without the dbt executable)
        project_dir: Directory containing the dbt project
        profiles_dir: Directory containing the profiles.yml file (defaults to project_dir if not specified)
        
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
    
    # Set DBT_PROFILES_DIR based on profiles_dir or project_dir
    if profiles_dir is not None:
        # Use the explicitly provided profiles_dir
        abs_profiles_dir = str(Path(profiles_dir).resolve())
        os.environ["DBT_PROFILES_DIR"] = abs_profiles_dir
        logger.debug(f"Setting DBT_PROFILES_DIR in os.environ to {abs_profiles_dir} (from profiles_dir)")
    else:
        # Check if there's a value from the .env file
        if "DBT_PROFILES_DIR" in env_vars:
            os.environ["DBT_PROFILES_DIR"] = env_vars["DBT_PROFILES_DIR"]
            logger.debug(f"Setting DBT_PROFILES_DIR from env_vars to {env_vars['DBT_PROFILES_DIR']}")
        else:
            # Default to project_dir
            abs_project_dir = str(Path(project_dir).resolve())
            os.environ["DBT_PROFILES_DIR"] = abs_project_dir
            logger.debug(f"Setting DBT_PROFILES_DIR in os.environ to {abs_project_dir} (from project_dir)")
    
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
        
        # Special case for 'show' command: detect "does not match any enabled nodes" as an error
        # Only check if --quiet is not in the command, as --quiet suppresses this output
        if success and command[0] == "show" and "--quiet" not in command and "does not match any enabled nodes" in stdout:
            success = False
            
        # For commands that failed, combine stdout and stderr for comprehensive output
        if not success and stderr:
            # If there's output from both stdout and stderr, combine them
            if stdout:
                output = f"{stdout}\n\nSTDERR:\n{stderr}"
            else:
                output = stderr
        else:
            # For successful commands, use stdout
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
            
            # Check if the name value is a JSON string
            if name_value.startswith('{') and '"name":' in name_value and '"resource_type":' in name_value:
                try:
                    # Parse the JSON string directly
                    model_data = json.loads(name_value)
                    if isinstance(model_data, dict) and "name" in model_data and "resource_type" in model_data:
                        extracted_models.append(model_data)
                        continue
                except json.JSONDecodeError:
                    logger.debug(f"Failed to parse JSON from: {name_value[:30]}...")
            
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
                    
                # Check if the line is a JSON string
                if line.startswith('{') and '"name":' in line and '"resource_type":' in line:
                    try:
                        model_data = json.loads(line)
                        if isinstance(model_data, dict) and "name" in model_data and "resource_type" in model_data:
                            models.append(model_data)
                            continue
                    except json.JSONDecodeError:
                        pass
                
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
    logger.info(f"Processing command result for {command_name}")
    logger.info(f"Result success: {result['success']}, returncode: {result.get('returncode')}")
    
    # Log the output type and a sample
    if "output" in result:
        if isinstance(result["output"], str):
            logger.info(f"Output type: str, first 100 chars: {result['output'][:100]}")
        elif isinstance(result["output"], (dict, list)):
            logger.info(f"Output type: {type(result['output'])}, sample: {json.dumps(result['output'])[:100]}")
        else:
            logger.info(f"Output type: {type(result['output'])}")
    
    # For errors, simply return the raw command output if available
    if not result["success"]:
        logger.warning(f"Command {command_name} failed with returncode {result.get('returncode')}")
        
        # If we have command output, return it directly
        if "output" in result and result["output"]:
            logger.info(f"Returning error output: {str(result['output'])[:100]}...")
            return str(result["output"])
        
        # If no command output, return the error message
        if result["error"]:
            logger.info(f"Returning error message: {str(result['error'])[:100]}...")
            return str(result["error"])
            
        # If neither output nor error is available, return a generic message
        logger.info("No output or error available, returning generic message")
        return f"Command failed with exit code {result.get('returncode', 'unknown')}"
    
    # Format successful output
    if output_formatter:
        logger.info(f"Using custom formatter for {command_name}")
        formatted_result = output_formatter(result["output"])
        logger.info(f"Formatted result type: {type(formatted_result)}, first 100 chars: {str(formatted_result)[:100]}")
        return formatted_result
    
    # Default output formatting
    logger.info(f"Using default formatting for {command_name}")
    if isinstance(result["output"], (dict, list)):
        json_result = json.dumps(result["output"])
        logger.info(f"JSON result length: {len(json_result)}, first 100 chars: {json_result[:100]}")
        return json_result
    else:
        str_result = str(result["output"])
        logger.info(f"String result length: {len(str_result)}, first 100 chars: {str_result[:100]}")
        return str_result