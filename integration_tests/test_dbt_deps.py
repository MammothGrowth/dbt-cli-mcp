#!/usr/bin/env python3
"""
Integration test for the dbt_deps tool that installs package dependencies.
"""
import os
import sys
import json
from pathlib import Path

# Add parent directory to python path to import from common.py
sys.path.append(str(Path(__file__).parent))
from common import run_cli_command, verify_output

# Path to the jaffle_shop project
JAFFLE_SHOP_PATH = Path(__file__).parent.parent / "dbt_integration_tests/jaffle_shop_duckdb"

def test_dbt_deps():
    """Test the dbt_deps tool by installing package dependencies"""
    print("Testing dbt_deps tool...")
    
    try:
        # Call the dbt_deps tool to install package dependencies
        print("Running dbt_deps...")
        deps_result = run_cli_command("deps", {
            "project_dir": str(JAFFLE_SHOP_PATH)
        })
        
        # Print the deps result for debugging
        print(f"Deps result: {deps_result[:200]}...")
        
        # Check for success indicators in the output
        # Note: The actual output may vary depending on whether packages are defined
        # and if they're already installed, so we're being flexible with our checks
        success_indicators = [
            "Installing",
            "Installed",
            "Up to date",
            "Nothing to do"
        ]
        
        # We don't need all indicators to be present, just check if any of them are
        found_indicators = [indicator for indicator in success_indicators if indicator in deps_result]
        
        # If no packages are defined, the command might still succeed without any of these indicators
        # So we'll also check if there are any error messages
        error_indicators = [
            "Error",
            "Failed",
            "Exception"
        ]
        
        found_errors = [indicator for indicator in error_indicators if indicator in deps_result]
        
        # Use assertion instead of returning True/False
        assert not found_errors, f"Found error indicators: {found_errors}\nDeps output: {deps_result}"
        
        # If we found success indicators or no errors, consider it a success
        print(f"✅ Found success indicators: {found_indicators}" if found_indicators else "✅ No errors found")
        print("✅ dbt_deps integration test passed!")
    
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    try:
        test_dbt_deps()
        sys.exit(0)
    except Exception:
        sys.exit(1)