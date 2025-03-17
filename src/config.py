"""
Configuration management for the DBT CLI MCP Server.

This module handles loading and managing configuration settings for the server,
including environment variables, default values, and runtime configuration.
"""

import os
import logging
from pathlib import Path
from typing import Dict, Any, Optional

# Default configuration values
DEFAULT_CONFIG = {
    "dbt_path": "dbt",  # Default to dbt in PATH
    "env_file": ".env",
    "log_level": "INFO",
}

# Current configuration (initialized with defaults)
config = DEFAULT_CONFIG.copy()

# Logger for this module
logger = logging.getLogger(__name__)


def load_from_env() -> None:
    """
    Load configuration from environment variables.
    
    Environment variables take precedence over default values.
    """
    env_mapping = {
        "DBT_PATH": "dbt_path",
        "ENV_FILE": "env_file",
        "LOG_LEVEL": "log_level",
    }
    
    for env_var, config_key in env_mapping.items():
        if env_var in os.environ:
            value = os.environ[env_var]
            
            # Convert string boolean values
            if value.lower() in ("true", "false") and config_key == "mock_mode":
                value = value.lower() == "true"
                
            config[config_key] = value
            logger.debug(f"Loaded config from environment: {config_key}={value}")


def get_config(key: str, default: Any = None) -> Any:
    """
    Get a configuration value.
    
    Args:
        key: The configuration key
        default: Default value if key is not found
        
    Returns:
        The configuration value or default
    """
    return config.get(key, default)


def set_config(key: str, value: Any) -> None:
    """
    Set a configuration value.
    
    Args:
        key: The configuration key
        value: The value to set
    """
    config[key] = value
    logger.debug(f"Updated config: {key}={value}")


def validate_config() -> bool:
    """
    Validate the current configuration.
    
    Returns:
        True if configuration is valid, False otherwise
    """
    dbt_path = config["dbt_path"]
    
    # If dbt_path is a full path, check if it exists
    if os.path.isabs(dbt_path) and not os.path.isfile(dbt_path):
        logger.warning(f"dbt executable not found at {dbt_path}")
        return False
        
    return True


def initialize() -> None:
    """
    Initialize the configuration.
    
    This loads configuration from environment variables and validates it.
    """
    load_from_env()
    
    if not validate_config():
        logger.warning("Configuration validation failed")