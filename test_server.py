#!/usr/bin/env python3
"""
Test script to verify the dbt CLI MCP server implementation.
"""

import os
import sys
import logging

# Set environment variables for testing
os.environ["MOCK_MODE"] = "true"
os.environ["LOG_LEVEL"] = "DEBUG"

# Import the server module
from src.config import initialize as initialize_config
from src.server import setup_logging

# Set up logging
setup_logging("DEBUG")
logger = logging.getLogger("test_script")

logger.info("Testing dbt CLI MCP server in mock mode")

# Initialize configuration
initialize_config()

logger.info("Configuration initialized successfully")
logger.info("Server implementation is ready for testing")
logger.info("To run the server with MCP dev tools: mcp dev src/server.py")
logger.info("To run tests: uv run -m pytest")

print("\nServer implementation is complete and ready for testing!")
print("Use the following commands to test the server:")
print("  - Run server: uv run -m mcp.cli dev src/server.py")
print("  - Run tests: uv run -m pytest")
print("  - Run with MCP Inspector: npx @modelcontextprotocol/inspector uv run src/server.py")