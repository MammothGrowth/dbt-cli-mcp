"""
Tests for the config module.
"""

import os
import pytest
from unittest.mock import patch

from src.config import (
    DEFAULT_CONFIG,
    config,
    load_from_env,
    get_config,
    set_config,
    validate_config,
    initialize
)


@pytest.fixture
def reset_config():
    """Reset the config to defaults after each test."""
    # Save original config
    original_config = config.copy()
    
    # Reset to defaults before test
    config.clear()
    config.update(DEFAULT_CONFIG.copy())
    
    yield
    
    # Reset to original after test
    config.clear()
    config.update(original_config)


@pytest.fixture
def mock_env():
    """Set up and tear down environment variables for testing."""
    # Save original environment
    original_env = os.environ.copy()
    
    # Clear relevant environment variables
    for var in ["DBT_PATH", "ENV_FILE", "LOG_LEVEL", "MOCK_MODE"]:
        if var in os.environ:
            del os.environ[var]
    
    yield
    
    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


def test_default_config():
    """Test that default config has expected values."""
    assert "dbt_path" in DEFAULT_CONFIG
    assert "env_file" in DEFAULT_CONFIG
    assert "log_level" in DEFAULT_CONFIG
    assert "mock_mode" in DEFAULT_CONFIG
    
    assert DEFAULT_CONFIG["dbt_path"] == "dbt"
    assert DEFAULT_CONFIG["env_file"] == ".env"
    assert DEFAULT_CONFIG["log_level"] == "INFO"
    assert DEFAULT_CONFIG["mock_mode"] is False


def test_load_from_env(reset_config, mock_env):
    """Test loading configuration from environment variables."""
    # Set environment variables
    os.environ["DBT_PATH"] = "/custom/path/to/dbt"
    os.environ["ENV_FILE"] = "custom.env"
    os.environ["LOG_LEVEL"] = "DEBUG"
    os.environ["MOCK_MODE"] = "true"
    
    # Load from environment
    load_from_env()
    
    # Check that config was updated
    assert config["dbt_path"] == "/custom/path/to/dbt"
    assert config["env_file"] == "custom.env"
    assert config["log_level"] == "DEBUG"
    assert config["mock_mode"] is True
    
    # Test boolean conversion
    os.environ["MOCK_MODE"] = "false"
    load_from_env()
    assert config["mock_mode"] is False


def test_get_config(reset_config):
    """Test getting configuration values."""
    # Set a test value
    config["test_key"] = "test_value"
    
    # Test getting existing key
    assert get_config("test_key") == "test_value"
    
    # Test getting non-existent key with default
    assert get_config("non_existent", "default") == "default"
    
    # Test getting non-existent key without default
    assert get_config("non_existent") is None


def test_set_config(reset_config):
    """Test setting configuration values."""
    # Set a new value
    set_config("new_key", "new_value")
    assert config["new_key"] == "new_value"
    
    # Update an existing value
    set_config("new_key", "updated_value")
    assert config["new_key"] == "updated_value"


def test_validate_config(reset_config):
    """Test configuration validation."""
    # Test with mock mode enabled (should always be valid)
    config["mock_mode"] = True
    assert validate_config() is True
    
    # Test with mock mode disabled and dbt_path as command in PATH
    config["mock_mode"] = False
    config["dbt_path"] = "dbt"  # Assuming dbt is not an absolute path
    assert validate_config() is True
    
    # Test with mock mode disabled and dbt_path as absolute path that doesn't exist
    with patch("os.path.isabs") as mock_isabs, patch("os.path.isfile") as mock_isfile:
        mock_isabs.return_value = True
        mock_isfile.return_value = False
        
        config["dbt_path"] = "/non/existent/path/to/dbt"
        assert validate_config() is False
        
        # Test with mock mode disabled and dbt_path as absolute path that exists
        mock_isfile.return_value = True
        assert validate_config() is True


def test_initialize(reset_config, mock_env):
    """Test configuration initialization."""
    # Set environment variables
    os.environ["DBT_PATH"] = "/custom/path/to/dbt"
    os.environ["MOCK_MODE"] = "true"
    
    # Mock validate_config to always return True
    with patch("src.config.validate_config") as mock_validate:
        mock_validate.return_value = True
        
        # Initialize config
        initialize()
        
        # Check that environment variables were loaded
        assert config["dbt_path"] == "/custom/path/to/dbt"
        assert config["mock_mode"] is True
        
        # Check that validate_config was called
        mock_validate.assert_called_once()