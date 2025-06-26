"""
Session configuration management for the DBT CLI MCP Server.

This module provides comprehensive configuration management for session parameters,
including environment variable loading, validation, and runtime updates.
"""

import os
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Union
from pathlib import Path

# Logger for this module
logger = logging.getLogger(__name__)


@dataclass
class SessionConfig:
    """
    Configuration class for session management parameters.
    
    This class manages all configuration parameters for session management,
    connection pooling, and feature flags. It supports loading from environment
    variables with validation and runtime updates.
    """
    
    # Session management
    session_timeout: int = 1800  # 30 minutes in seconds
    max_sessions: int = 10
    cleanup_interval: int = 600  # 10 minutes in seconds
    
    # Connection management
    max_connections_per_session: int = 5
    health_check_interval: int = 300  # 5 minutes in seconds
    connection_timeout: int = 300  # 5 minutes in seconds
    
    # Feature flags
    enable_session_management: bool = True
    enable_connection_pooling: bool = True
    enable_session_metrics: bool = False
    
    # Security
    session_encryption_key: Optional[str] = None
    audit_logging: bool = True
    
    # Internal tracking
    _loaded_from_env: bool = field(default=False, init=False)
    _validation_errors: Dict[str, str] = field(default_factory=dict, init=False)
    
    def __post_init__(self):
        """Post-initialization validation and setup."""
        self._validate_configuration()
        logger.debug("SessionConfig initialized with validation")
    
    @classmethod
    def from_env(cls) -> 'SessionConfig':
        """
        Create SessionConfig instance from environment variables.
        
        Environment variable mapping:
        - USE_SESSION_MANAGEMENT → enable_session_management
        - SESSION_IDLE_TIMEOUT → session_timeout
        - MAX_SESSIONS → max_sessions
        - MAX_CONNECTIONS_PER_SESSION → max_connections_per_session
        - HEALTH_CHECK_INTERVAL → health_check_interval
        - CLEANUP_INTERVAL → cleanup_interval
        - CONNECTION_TIMEOUT → connection_timeout
        - ENABLE_CONNECTION_POOLING → enable_connection_pooling
        - ENABLE_SESSION_METRICS → enable_session_metrics
        - SESSION_ENCRYPTION_KEY → session_encryption_key
        - AUDIT_LOGGING → audit_logging
        
        Returns:
            SessionConfig instance with values loaded from environment
        """
        config = cls()
        config._load_from_environment()
        config._loaded_from_env = True
        return config
    
    def _load_from_environment(self) -> None:
        """Load configuration values from environment variables."""
        env_mapping = {
            "USE_SESSION_MANAGEMENT": ("enable_session_management", bool),
            "SESSION_IDLE_TIMEOUT": ("session_timeout", int),
            "MAX_SESSIONS": ("max_sessions", int),
            "MAX_CONNECTIONS_PER_SESSION": ("max_connections_per_session", int),
            "HEALTH_CHECK_INTERVAL": ("health_check_interval", int),
            "CLEANUP_INTERVAL": ("cleanup_interval", int),
            "CONNECTION_TIMEOUT": ("connection_timeout", int),
            "ENABLE_CONNECTION_POOLING": ("enable_connection_pooling", bool),
            "ENABLE_SESSION_METRICS": ("enable_session_metrics", bool),
            "SESSION_ENCRYPTION_KEY": ("session_encryption_key", str),
            "AUDIT_LOGGING": ("audit_logging", bool),
        }
        
        for env_var, (config_key, value_type) in env_mapping.items():
            if env_var in os.environ:
                try:
                    raw_value = os.environ[env_var]
                    converted_value = self._convert_env_value(raw_value, value_type)
                    setattr(self, config_key, converted_value)
                    logger.debug(f"Loaded config from environment: {config_key}={converted_value}")
                except (ValueError, TypeError) as e:
                    logger.warning(f"Failed to load {env_var}: {e}")
                    self._validation_errors[config_key] = f"Invalid environment value for {env_var}: {e}"
    
    def _convert_env_value(self, value: str, target_type: type) -> Union[str, int, bool, None]:
        """
        Convert environment variable string to target type.
        
        Args:
            value: Raw string value from environment
            target_type: Target type to convert to
            
        Returns:
            Converted value
            
        Raises:
            ValueError: If conversion fails
        """
        if target_type == bool:
            return value.lower() in ("true", "1", "yes", "on")
        elif target_type == int:
            return int(value)
        elif target_type == str:
            return value if value else None
        else:
            raise ValueError(f"Unsupported type: {target_type}")
    
    def _validate_configuration(self) -> None:
        """
        Validate all configuration parameters.
        
        This method checks that all configuration values are within acceptable
        ranges and combinations make sense.
        """
        self._validation_errors.clear()
        
        # Validate session timeout (minimum 60 seconds, maximum 24 hours)
        if not (60 <= self.session_timeout <= 86400):
            self._validation_errors["session_timeout"] = "Session timeout must be between 60 seconds and 24 hours"
        
        # Validate max sessions (minimum 1, maximum 100)
        if not (1 <= self.max_sessions <= 100):
            self._validation_errors["max_sessions"] = "Max sessions must be between 1 and 100"
        
        # Validate cleanup interval (minimum 60 seconds, maximum 1 hour)
        if not (60 <= self.cleanup_interval <= 3600):
            self._validation_errors["cleanup_interval"] = "Cleanup interval must be between 60 seconds and 1 hour"
        
        # Validate max connections per session (minimum 1, maximum 20)
        if not (1 <= self.max_connections_per_session <= 20):
            self._validation_errors["max_connections_per_session"] = "Max connections per session must be between 1 and 20"
        
        # Validate health check interval (minimum 30 seconds, maximum 30 minutes)
        if not (30 <= self.health_check_interval <= 1800):
            self._validation_errors["health_check_interval"] = "Health check interval must be between 30 seconds and 30 minutes"
        
        # Validate connection timeout (minimum 10 seconds, maximum 10 minutes)
        if not (10 <= self.connection_timeout <= 600):
            self._validation_errors["connection_timeout"] = "Connection timeout must be between 10 seconds and 10 minutes"
        
        # Validate cleanup interval is not greater than session timeout
        if self.cleanup_interval >= self.session_timeout:
            self._validation_errors["cleanup_interval"] = "Cleanup interval must be less than session timeout"
        
        # Validate health check interval is not greater than session timeout
        if self.health_check_interval >= self.session_timeout:
            self._validation_errors["health_check_interval"] = "Health check interval must be less than session timeout"
        
        # Log validation results
        if self._validation_errors:
            logger.warning(f"Configuration validation failed: {self._validation_errors}")
        else:
            logger.debug("Configuration validation passed")
    
    def is_valid(self) -> bool:
        """
        Check if the current configuration is valid.
        
        Returns:
            True if configuration is valid, False otherwise
        """
        return len(self._validation_errors) == 0
    
    def get_validation_errors(self) -> Dict[str, str]:
        """
        Get current validation errors.
        
        Returns:
            Dictionary of validation errors keyed by parameter name
        """
        return self._validation_errors.copy()
    
    def update_config(self, **kwargs) -> bool:
        """
        Update configuration parameters at runtime.
        
        Args:
            **kwargs: Configuration parameters to update
            
        Returns:
            True if update was successful and valid, False otherwise
        """
        # Store original values for rollback
        original_values = {}
        
        try:
            # Update values
            for key, value in kwargs.items():
                if hasattr(self, key):
                    original_values[key] = getattr(self, key)
                    setattr(self, key, value)
                    logger.debug(f"Updated config: {key}={value}")
                else:
                    logger.warning(f"Unknown configuration parameter: {key}")
                    return False
            
            # Validate updated configuration
            self._validate_configuration()
            
            if not self.is_valid():
                # Rollback changes if validation failed
                for key, value in original_values.items():
                    setattr(self, key, value)
                logger.warning("Configuration update failed validation, rolled back changes")
                return False
            
            logger.info(f"Configuration updated successfully: {list(kwargs.keys())}")
            return True
            
        except Exception as e:
            # Rollback changes on any error
            for key, value in original_values.items():
                setattr(self, key, value)
            logger.error(f"Configuration update failed: {e}")
            return False
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary.
        
        Returns:
            Dictionary representation of configuration
        """
        return {
            "session_timeout": self.session_timeout,
            "max_sessions": self.max_sessions,
            "cleanup_interval": self.cleanup_interval,
            "max_connections_per_session": self.max_connections_per_session,
            "health_check_interval": self.health_check_interval,
            "connection_timeout": self.connection_timeout,
            "enable_session_management": self.enable_session_management,
            "enable_connection_pooling": self.enable_connection_pooling,
            "enable_session_metrics": self.enable_session_metrics,
            "session_encryption_key": "***" if self.session_encryption_key else None,
            "audit_logging": self.audit_logging,
            "loaded_from_env": self._loaded_from_env,
            "is_valid": self.is_valid(),
        }
    
    def __str__(self) -> str:
        """String representation of configuration."""
        config_dict = self.to_dict()
        return f"SessionConfig({', '.join(f'{k}={v}' for k, v in config_dict.items())})"
    
    def __repr__(self) -> str:
        """Detailed string representation of configuration."""
        return self.__str__()


def load_session_config() -> SessionConfig:
    """
    Convenience function to load session configuration.
    
    This function attempts to load configuration from environment variables
    first, falling back to defaults if environment loading fails.
    
    Returns:
        SessionConfig instance
    """
    try:
        config = SessionConfig.from_env()
        if config.is_valid():
            logger.info("Session configuration loaded from environment")
            return config
        else:
            logger.warning("Environment configuration invalid, using defaults")
            return SessionConfig()
    except Exception as e:
        logger.error(f"Failed to load configuration from environment: {e}")
        logger.info("Using default session configuration")
        return SessionConfig()