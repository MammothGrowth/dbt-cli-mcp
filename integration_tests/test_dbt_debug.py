#!/usr/bin/env python3
"""
Integration test for the dbt_debug tool that validates project setup.
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

def test_dbt_debug():
    """Test the dbt_debug tool by validating the project setup"""
    print("Testing dbt_debug tool...")
    
    try:
        # Call the dbt_debug tool to validate the project setup
        print("Running dbt_debug...")
        debug_result = run_cli_command("debug", {
            "project_dir": str(JAFFLE_SHOP_PATH)
        })
        
        # Print the debug result for debugging
        print(f"Debug result: {debug_result[:200]}...")
        
        # Check for success indicators in the output
        success_indicators = [
            "All checks passed",
            "Configuration:          OK",
            "Connection:             OK"
        ]
        
        # We don't need all indicators to be present, just check if any of them are
        found_indicators = [indicator for indicator in success_indicators if indicator in debug_result]
        
        # Use assertion instead of returning True/False
        assert found_indicators, f"No success indicators found in debug output\nDebug output: {debug_result}"
        
        print(f"✅ Found success indicators: {found_indicators}")
        print("✅ dbt_debug integration test passed!")
    
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    try:
        test_dbt_debug()
        sys.exit(0)
    except Exception:
        sys.exit(1)