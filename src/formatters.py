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
    
    # Parse the output
    parsed = parse_dbt_list_output(output)
    
    # Filter out any empty or non-model entries
    filtered_parsed = [item for item in parsed if isinstance(item, dict) and
                      item.get("resource_type") in ["model", "seed", "test", "source", "snapshot"]]
    
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


def show_formatter(output: Any) -> str:
    """
    Formatter for dbt show command output.
    
    Args:
        output: The command output
        
    Returns:
        Formatted output string
    """
    # If output is already a dict or list, just return it as JSON
    if isinstance(output, (dict, list)):
        return json.dumps(output)
    
    # Try to convert tabular output to JSON if possible
    try:
        # Simple conversion of tabular data to JSON
        lines = str(output).strip().split("\n")
        if len(lines) > 2:  # Need at least header and one data row
            # Extract header row (assuming it's the first row)
            header = lines[0].strip().split("|")
            header = [h.strip() for h in header if h.strip()]
            
            # Extract data rows (skip header and separator row)
            data_rows = []
            for line in lines[2:]:
                if line.strip() and "|" in line:
                    values = line.strip().split("|")
                    values = [v.strip() for v in values if v.strip()]
                    if len(values) == len(header):
                        row_dict = dict(zip(header, values))
                        data_rows.append(row_dict)
            
            return json.dumps(data_rows)
    except Exception as e:
        logger.warning(f"Failed to convert tabular output to JSON: {e}")
    
    # Default to string output if conversion fails
    return str(output)