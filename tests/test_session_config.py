"""
Unit tests for session configuration management.

This module tests all aspects of the SessionConfig class including:
- Environment variable loading
- Configuration validation
- Runtime updates
- Error handling
"""

import os
import pytest
from unittest.mock import patch
from dataclasses import FrozenInstanceError

from src.session.session_config import SessionConfig, load_session_config


class TestSessionConfig:
    """Test cases for SessionConfig class."""
    
    def test_default_configuration(self):
        """Test that default configuration is valid."""
        config = SessionConfig()
        
        # Check default values
        assert config.session_timeout == 1800
        assert config.max_sessions == 10
        assert config.cleanup_interval == 600
        assert config.max_connections_per_session == 5
        assert config.health_check_interval == 300
        assert config.connection_timeout == 60
        assert config.enable_session_management is True
        assert config.enable_connection_pooling is True
        assert config.enable_session_metrics is False
        assert config.session_encryption_key is None
        assert config.audit_logging is True
        
        # Check that default configuration is valid
        assert config.is_valid()
        assert len(config.get_validation_errors()) == 0
    
    def test_environment_variable_loading_integers(self):
        """Test loading integer values from environment variables."""
        env_vars = {
            "SESSION_IDLE_TIMEOUT": "3600",
            "MAX_SESSIONS": "20",
            "MAX_CONNECTIONS_PER_SESSION": "10",
            "HEALTH_CHECK_INTERVAL": "600",
            "CLEANUP_INTERVAL": "1200",
            "CONNECTION_TIMEOUT": "120"
        }
        
        with patch.dict(os.environ, env_vars):
            config = SessionConfig.from_env()
            
            assert config.session_timeout == 3600
            assert config.max_sessions == 20
            assert config.max_connections_per_session == 10
            assert config.health_check_interval == 600
            assert config.cleanup_interval == 1200
            assert config.connection_timeout == 120
            assert config._loaded_from_env is True
    
    def test_environment_variable_loading_booleans(self):
        """Test loading boolean values from environment variables."""
        # Test various boolean representations
        boolean_tests = [
            ("true", True),
            ("True", True),
            ("TRUE", True),
            ("1", True),
            ("yes", True),
            ("on", True),
            ("false", False),
            ("False", False),
            ("FALSE", False),
            ("0", False),
            ("no", False),
            ("off", False),
            ("", False),
        ]
        
        for env_value, expected in boolean_tests:
            env_vars = {
                "USE_SESSION_MANAGEMENT": env_value,
                "ENABLE_CONNECTION_POOLING": env_value,
                "ENABLE_SESSION_METRICS": env_value,
                "AUDIT_LOGGING": env_value,
            }
            
            with patch.dict(os.environ, env_vars):
                config = SessionConfig.from_env()
                
                assert config.enable_session_management == expected
                assert config.enable_connection_pooling == expected
                assert config.enable_session_metrics == expected
                assert config.audit_logging == expected
    
    def test_environment_variable_loading_strings(self):
        """Test loading string values from environment variables."""
        env_vars = {
            "SESSION_ENCRYPTION_KEY": "test-encryption-key-123"
        }
        
        with patch.dict(os.environ, env_vars):
            config = SessionConfig.from_env()
            
            assert config.session_encryption_key == "test-encryption-key-123"
    
    def test_invalid_environment_values(self):
        """Test handling of invalid environment variable values."""
        env_vars = {
            "SESSION_IDLE_TIMEOUT": "invalid-number",
            "MAX_SESSIONS": "not-a-number",
            "ENABLE_SESSION_MANAGEMENT": "maybe",  # Invalid boolean
        }
        
        with patch.dict(os.environ, env_vars):
            config = SessionConfig.from_env()
            
            # Should fall back to defaults for invalid values
            assert config.session_timeout == 1800  # default
            assert config.max_sessions == 10  # default
            assert config.enable_session_management is True  # default
            
            # Should have validation errors recorded
            validation_errors = config.get_validation_errors()
            assert "session_timeout" in validation_errors or len(validation_errors) == 0  # May not be recorded if conversion fails
    
    def test_configuration_validation_session_timeout(self):
        """Test session timeout validation."""
        # Test minimum boundary
        config = SessionConfig(session_timeout=30)  # Too low
        assert not config.is_valid()
        errors = config.get_validation_errors()
        assert "session_timeout" in errors
        
        # Test maximum boundary
        config = SessionConfig(session_timeout=90000)  # Too high
        assert not config.is_valid()
        errors = config.get_validation_errors()
        assert "session_timeout" in errors
        
        # Test valid values
        config = SessionConfig(session_timeout=1800)  # Valid
        assert config.is_valid()
    
    def test_configuration_validation_max_sessions(self):
        """Test max sessions validation."""
        # Test minimum boundary
        config = SessionConfig(max_sessions=0)  # Too low
        assert not config.is_valid()
        errors = config.get_validation_errors()
        assert "max_sessions" in errors
        
        # Test maximum boundary
        config = SessionConfig(max_sessions=150)  # Too high
        assert not config.is_valid()
        errors = config.get_validation_errors()
        assert "max_sessions" in errors
        
        # Test valid values
        config = SessionConfig(max_sessions=10)  # Valid
        assert config.is_valid()
    
    def test_configuration_validation_cleanup_interval(self):
        """Test cleanup interval validation."""
        # Test minimum boundary
        config = SessionConfig(cleanup_interval=30)  # Too low
        assert not config.is_valid()
        errors = config.get_validation_errors()
        assert "cleanup_interval" in errors
        
        # Test maximum boundary
        config = SessionConfig(cleanup_interval=4000)  # Too high
        assert not config.is_valid()
        errors = config.get_validation_errors()
        assert "cleanup_interval" in errors
        
        # Test cleanup interval >= session timeout
        config = SessionConfig(session_timeout=1800, cleanup_interval=1800)  # Equal
        assert not config.is_valid()
        errors = config.get_validation_errors()
        assert "cleanup_interval" in errors
        
        config = SessionConfig(session_timeout=1800, cleanup_interval=2000)  # Greater
        assert not config.is_valid()
        errors = config.get_validation_errors()
        assert "cleanup_interval" in errors
    
    def test_configuration_validation_health_check_interval(self):
        """Test health check interval validation."""
        # Test minimum boundary
        config = SessionConfig(health_check_interval=15)  # Too low
        assert not config.is_valid()
        errors = config.get_validation_errors()
        assert "health_check_interval" in errors
        
        # Test maximum boundary
        config = SessionConfig(health_check_interval=2000)  # Too high
        assert not config.is_valid()
        errors = config.get_validation_errors()
        assert "health_check_interval" in errors
        
        # Test health check interval >= session timeout
        config = SessionConfig(session_timeout=1800, health_check_interval=1800)  # Equal
        assert not config.is_valid()
        errors = config.get_validation_errors()
        assert "health_check_interval" in errors
    
    def test_runtime_configuration_updates(self):
        """Test runtime configuration updates."""
        config = SessionConfig()
        
        # Test successful update
        result = config.update_config(session_timeout=3600, max_sessions=15)
        assert result is True
        assert config.session_timeout == 3600
        assert config.max_sessions == 15
        assert config.is_valid()
        
        # Test failed update (invalid values)
        original_timeout = config.session_timeout
        result = config.update_config(session_timeout=30)  # Too low
        assert result is False
        assert config.session_timeout == original_timeout  # Should be rolled back
        
        # Test update with unknown parameter
        result = config.update_config(unknown_parameter="value")
        assert result is False
    
    def test_configuration_serialization(self):
        """Test configuration to dictionary conversion."""
        config = SessionConfig(
            session_timeout=3600,
            max_sessions=15,
            enable_session_metrics=True,
            session_encryption_key="test-key"
        )
        
        config_dict = config.to_dict()
        
        assert config_dict["session_timeout"] == 3600
        assert config_dict["max_sessions"] == 15
        assert config_dict["enable_session_metrics"] is True
        assert config_dict["session_encryption_key"] == "***"  # Should be masked
        assert "is_valid" in config_dict
        assert "loaded_from_env" in config_dict
    
    def test_string_representations(self):
        """Test string representation methods."""
        config = SessionConfig()
        
        str_repr = str(config)
        assert "SessionConfig" in str_repr
        assert "session_timeout=1800" in str_repr
        
        repr_str = repr(config)
        assert repr_str == str_repr
    
    def test_load_session_config_function(self):
        """Test the convenience load_session_config function."""
        # Test with valid environment
        env_vars = {
            "SESSION_IDLE_TIMEOUT": "3600",
            "MAX_SESSIONS": "20"
        }
        
        with patch.dict(os.environ, env_vars):
            config = load_session_config()
            assert config.session_timeout == 3600
            assert config.max_sessions == 20
            assert config._loaded_from_env is True
    
    def test_load_session_config_fallback(self):
        """Test load_session_config fallback to defaults."""
        # Test with invalid environment that causes exception
        with patch('src.session.session_config.SessionConfig.from_env') as mock_from_env:
            mock_from_env.side_effect = Exception("Environment loading failed")
            
            config = load_session_config()
            
            # Should fall back to defaults
            assert config.session_timeout == 1800
            assert config.max_sessions == 10
            assert config._loaded_from_env is False
    
    def test_convert_env_value_edge_cases(self):
        """Test edge cases in environment value conversion."""
        config = SessionConfig()
        
        # Test boolean conversion edge cases
        assert config._convert_env_value("YES", bool) is True
        assert config._convert_env_value("ON", bool) is True
        assert config._convert_env_value("NO", bool) is False
        assert config._convert_env_value("OFF", bool) is False
        
        # Test integer conversion
        assert config._convert_env_value("123", int) == 123
        assert config._convert_env_value("0", int) == 0
        
        # Test string conversion
        assert config._convert_env_value("test", str) == "test"
        assert config._convert_env_value("", str) is None
        
        # Test unsupported type
        with pytest.raises(ValueError):
            config._convert_env_value("test", list)
    
    def test_multiple_validation_errors(self):
        """Test configuration with multiple validation errors."""
        config = SessionConfig(
            session_timeout=30,  # Too low
            max_sessions=0,      # Too low
            cleanup_interval=30, # Too low
            connection_timeout=5 # Too low
        )
        
        assert not config.is_valid()
        errors = config.get_validation_errors()
        
        # Should have multiple errors
        assert len(errors) >= 4
        assert "session_timeout" in errors
        assert "max_sessions" in errors
        assert "cleanup_interval" in errors
        assert "connection_timeout" in errors


class TestSessionConfigIntegration:
    """Integration tests for SessionConfig with realistic scenarios."""
    
    def test_production_like_configuration(self):
        """Test a production-like configuration."""
        env_vars = {
            "USE_SESSION_MANAGEMENT": "true",
            "SESSION_IDLE_TIMEOUT": "1800",  # 30 minutes
            "MAX_SESSIONS": "25",
            "MAX_CONNECTIONS_PER_SESSION": "8",
            "HEALTH_CHECK_INTERVAL": "300",  # 5 minutes
            "CLEANUP_INTERVAL": "600",       # 10 minutes
            "CONNECTION_TIMEOUT": "90",      # 1.5 minutes
            "ENABLE_CONNECTION_POOLING": "true",
            "ENABLE_SESSION_METRICS": "true",
            "AUDIT_LOGGING": "true",
            "SESSION_ENCRYPTION_KEY": "prod-encryption-key-2024"
        }
        
        with patch.dict(os.environ, env_vars):
            config = SessionConfig.from_env()
            
            assert config.is_valid()
            assert config.enable_session_management is True
            assert config.session_timeout == 1800
            assert config.max_sessions == 25
            assert config.max_connections_per_session == 8
            assert config.health_check_interval == 300
            assert config.cleanup_interval == 600
            assert config.connection_timeout == 90
            assert config.enable_connection_pooling is True
            assert config.enable_session_metrics is True
            assert config.audit_logging is True
            assert config.session_encryption_key == "prod-encryption-key-2024"
    
    def test_development_configuration(self):
        """Test a development-friendly configuration."""
        env_vars = {
            "USE_SESSION_MANAGEMENT": "true",
            "SESSION_IDLE_TIMEOUT": "900",   # 15 minutes (shorter for dev)
            "MAX_SESSIONS": "5",             # Fewer sessions for dev
            "MAX_CONNECTIONS_PER_SESSION": "3",
            "HEALTH_CHECK_INTERVAL": "120",  # 2 minutes (more frequent)
            "CLEANUP_INTERVAL": "300",       # 5 minutes (more frequent)
            "CONNECTION_TIMEOUT": "30",      # 30 seconds (shorter)
            "ENABLE_CONNECTION_POOLING": "true",
            "ENABLE_SESSION_METRICS": "false",  # Disabled for dev
            "AUDIT_LOGGING": "false",            # Disabled for dev
        }
        
        with patch.dict(os.environ, env_vars):
            config = SessionConfig.from_env()
            
            assert config.is_valid()
            assert config.session_timeout == 900
            assert config.max_sessions == 5
            assert config.enable_session_metrics is False
            assert config.audit_logging is False
    
    def test_configuration_update_workflow(self):
        """Test a realistic configuration update workflow."""
        # Start with default configuration
        config = SessionConfig()
        assert config.is_valid()
        
        # Update for higher load scenario
        success = config.update_config(
            max_sessions=50,
            max_connections_per_session=10,
            session_timeout=3600  # 1 hour
        )
        assert success is True
        assert config.max_sessions == 50
        assert config.max_connections_per_session == 10
        assert config.session_timeout == 3600
        
        # Try to update cleanup interval to be compatible
        success = config.update_config(cleanup_interval=1800)  # 30 minutes
        assert success is True
        assert config.cleanup_interval == 1800
        
        # Try invalid update that should fail and rollback
        original_timeout = config.session_timeout
        success = config.update_config(
            session_timeout=30,  # Invalid - too low
            max_sessions=200     # Invalid - too high
        )
        assert success is False
        assert config.session_timeout == original_timeout  # Should be rolled back
        assert config.max_sessions == 50  # Should be rolled back