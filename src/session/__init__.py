"""
Session management package for the DBT CLI MCP Server.

This package provides session management capabilities including:
- Session configuration management
- Session lifecycle management
- Connection pooling
- Session security
"""

from .session_config import SessionConfig

__all__ = ["SessionConfig"]