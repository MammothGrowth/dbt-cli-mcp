#!/usr/bin/env python3
"""
Main entry point for the DBT CLI MCP Server.

This module initializes the FastMCP server, registers all tools,
and handles server lifecycle.
"""

import os
import sys
import logging
import argparse
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from src.config import initialize as initialize_config, get_config
from src.tools import register_tools

# Initialize logger
logger = logging.getLogger("src")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="DBT CLI MCP Server")
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
        "--mock-mode",
        help="Enable mock mode for testing",
        action="store_true",
        default=os.environ.get("MOCK_MODE", "false").lower() == "true"
    )
    
    return parser.parse_args()


def setup_logging(log_level):
    """Set up logging configuration."""
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO
        
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stderr)
        ]
    )


def main():
    """Main entry point for the server."""
    # Parse command line arguments
    args = parse_args()
    
    # Set up logging
    setup_logging(args.log_level)
    
    # Set environment variables from arguments
    os.environ["DBT_PATH"] = args.dbt_path
    os.environ["ENV_FILE"] = args.env_file
    os.environ["LOG_LEVEL"] = args.log_level
    os.environ["MOCK_MODE"] = str(args.mock_mode).lower()
    
    # Initialize configuration
    initialize_config()
    
    # Create FastMCP server
    mcp = FastMCP("dbt-cli", log_level="ERROR")
    
    # Register tools
    register_tools(mcp)
    
    # Log server information
    logger.info(f"Starting DBT CLI MCP Server")
    logger.info(f"dbt path: {get_config('dbt_path')}")
    logger.info(f"Environment file: {get_config('env_file')}")
    logger.info(f"Mock mode: {get_config('mock_mode')}")
    
    # Run the server
    mcp.run()


if __name__ == "__main__":
    main()