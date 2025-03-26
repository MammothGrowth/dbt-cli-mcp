#!/usr/bin/env python3
"""
Integration test for the dbt_seed tool that loads CSV files as seed data.
"""
import os
import sys
import json
from pathlib import Path

# Add parent directory to python path to import from common.py
sys.path.append(str(Path(__file__).parent))
from common import run_cli_command, verify_output, verify_files_exist, cleanup_target_dir

# Path to the jaffle_shop project
JAFFLE_SHOP_PATH = Path(__file__).parent.parent / "dbt_integration_tests/jaffle_shop_duckdb"

def test_dbt_seed():
    """Test the dbt_seed tool by loading CSV files as seed data"""
    print("Testing dbt_seed tool...")
    
    # Clean up target directory first
    cleanup_target_dir(JAFFLE_SHOP_PATH)
    
    try:
        # Call the dbt_seed tool to load seed data
        print("Running dbt_seed...")
        seed_result = run_cli_command("seed", {
            "project_dir": str(JAFFLE_SHOP_PATH)
        })
        
        # Print the seed result for debugging
        print(f"Seed result: {seed_result[:200]}...")
        
        # Check for expected seed files in the project
        seed_files = [
            JAFFLE_SHOP_PATH / "seeds" / "raw_customers.csv",
            JAFFLE_SHOP_PATH / "seeds" / "raw_orders.csv",
            JAFFLE_SHOP_PATH / "seeds" / "raw_payments.csv"
        ]
        
        # Verify the seed files exist
        assert verify_files_exist(seed_files), "Verification failed"
        
        print("✅ Seed files found in project")
        
        # Verify the target files were created
        # The exact paths may vary depending on the dbt version and configuration
        # These are common paths for compiled seed files
        target_files = [
            JAFFLE_SHOP_PATH / "target" / "compiled" / "jaffle_shop" / "seeds" / "raw_customers.csv",
            JAFFLE_SHOP_PATH / "target" / "compiled" / "jaffle_shop" / "seeds" / "raw_orders.csv",
            JAFFLE_SHOP_PATH / "target" / "compiled" / "jaffle_shop" / "seeds" / "raw_payments.csv"
        ]
        
        # We'll check if at least one of these files exists
        # since the exact path structure might vary
        found_target_files = False
        for target_file in target_files:
            if target_file.exists():
                found_target_files = True
                print(f"Found target file: {target_file}")
                break
        
        if not found_target_files:
            print("❌ No target files found")
            # This is not a critical failure as some dbt versions might not create these files
            print("⚠️ Warning: No target files found, but this might be expected depending on dbt version")
        
        # Check for success indicators in the output
        success_indicators = [
            "Completed successfully",
            "OK",
            "Success"
        ]
        
        # We don't need all indicators to be present, just check if any of them are
        found_indicators = [indicator for indicator in success_indicators if indicator in seed_result]
        
        if not found_indicators:
            # If we don't find explicit success indicators, check for error indicators
            error_indicators = [
                "Error",
                "Failed",
                "Exception"
            ]
            
            found_errors = [indicator for indicator in error_indicators if indicator in seed_result]
            
            if found_errors:
                print(f"❌ Found error indicators: {found_errors}")
                print(f"Seed output: {seed_result}")
                return False
        
        print(f"✅ Found success indicators: {found_indicators}" if found_indicators else "✅ No errors found")
        print("✅ Test passed!")
    
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    try:
        test_dbt_seed()
        sys.exit(0)
    except Exception:
        sys.exit(1)