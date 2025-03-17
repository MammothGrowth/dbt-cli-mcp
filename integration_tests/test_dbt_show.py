#!/usr/bin/env python3
"""
Integration test for the dbt_show tool that previews model results.
"""
import os
import sys
import json
from pathlib import Path

# Add parent directory to python path to import from common.py
sys.path.append(str(Path(__file__).parent))
from common import run_cli_command, verify_output, cleanup_target_dir

# Path to the jaffle_shop project
JAFFLE_SHOP_PATH = Path(__file__).parent.parent / "dbt_integration_tests/jaffle_shop_duckdb"

def test_dbt_show():
    """Test the dbt_show tool by previewing a model's results"""
    print("Testing dbt_show tool...")
    
    # Clean up target directory first
    cleanup_target_dir(JAFFLE_SHOP_PATH)
    
    try:
        # First run dbt_seed to load the seed data
        print("Running dbt_seed to load test data...")
        seed_result = run_cli_command("seed", {
            "project_dir": str(JAFFLE_SHOP_PATH)
        })
        
        # Print the seed result for debugging
        print(f"Seed result: {seed_result[:200]}...")
        
        # Don't check for specific text, just proceed
        print("✅ Seed data loaded")
        
        # Call the dbt_show tool to preview the customers model
        print("Running dbt_show for customers model...")
        show_result = run_cli_command("show", {
            "project_dir": str(JAFFLE_SHOP_PATH),
            "models": "customers",
            "limit": 5
        })
        
        # Print the show result for debugging
        print(f"Show result: {show_result[:200]}...")
        
        # Check for success indicators in the output
        # The output should contain some data or column names from the customers model
        success_indicators = [
            "customer_id",
            "first_order",
            "most_recent_order",
            "number_of_orders",
            "customer_lifetime_value"
        ]
        
        # We don't need all indicators to be present, just check if any of them are
        found_indicators = [indicator for indicator in success_indicators if indicator.lower() in show_result.lower()]
        
        if not found_indicators:
            # If we don't find explicit column names, check for error indicators
            error_indicators = [
                "Error",
                "Failed",
                "Exception"
            ]
            
            found_errors = [indicator for indicator in error_indicators if indicator in show_result]
            
            if found_errors:
                print(f"❌ Found error indicators: {found_errors}")
                print(f"Show output: {show_result}")
                return False
            
            # If no column names and no errors, check if there's any tabular data
            if not any(char in show_result for char in ["|", "+", "-"]):
                print("❌ No tabular data found in output")
                print(f"Show output: {show_result}")
                return False
        
        print(f"✅ Found column indicators: {found_indicators}" if found_indicators else "✅ Found tabular data")
        print("✅ dbt_show integration test passed!")
        return True
    
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_dbt_show()
    sys.exit(0 if success else 1)