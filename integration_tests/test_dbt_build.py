#!/usr/bin/env python3
"""
Integration test for the dbt_build tool that runs seeds, tests, snapshots, and models.
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

def test_dbt_build():
    """Test the dbt_build tool by running a comprehensive build process"""
    print("Testing dbt_build tool...")
    
    # Clean up target directory first
    cleanup_target_dir(JAFFLE_SHOP_PATH)
    
    try:
        # Call the dbt_build tool to run a comprehensive build
        print("Running dbt_build...")
        build_result = run_cli_command("build", {
            "project_dir": str(JAFFLE_SHOP_PATH)
        })
        
        # Print the build result for debugging
        print(f"Build result: {build_result[:200]}...")
        
        # Verify the target files were created for models
        model_files = [
            JAFFLE_SHOP_PATH / "target" / "compiled" / "jaffle_shop" / "models" / "customers.sql",
            JAFFLE_SHOP_PATH / "target" / "compiled" / "jaffle_shop" / "models" / "orders.sql",
            JAFFLE_SHOP_PATH / "target" / "compiled" / "jaffle_shop" / "models" / "staging" / "stg_customers.sql",
            JAFFLE_SHOP_PATH / "target" / "compiled" / "jaffle_shop" / "models" / "staging" / "stg_orders.sql",
            JAFFLE_SHOP_PATH / "target" / "compiled" / "jaffle_shop" / "models" / "staging" / "stg_payments.sql"
        ]
        
        # Verify the target files were created for seeds
        seed_files = [
            JAFFLE_SHOP_PATH / "target" / "compiled" / "jaffle_shop" / "seeds" / "raw_customers.csv",
            JAFFLE_SHOP_PATH / "target" / "compiled" / "jaffle_shop" / "seeds" / "raw_orders.csv",
            JAFFLE_SHOP_PATH / "target" / "compiled" / "jaffle_shop" / "seeds" / "raw_payments.csv"
        ]
        
        # Combine all files to check
        all_files = model_files + seed_files
        
        # We'll check if at least some of these files exist
        # since the exact path structure might vary
        found_files = []
        for file_path in all_files:
            if file_path.exists():
                found_files.append(file_path)
        
        # Use assertion instead of returning True/False
        assert found_files, "No target files found"
        
        print(f"✅ Found {len(found_files)} target files")
        for file_path in found_files[:3]:  # Print first 3 files for brevity
            print(f"  - {file_path}")
        
        # Check for success indicators in the output
        success_indicators = [
            "Completed successfully",
            "OK",
            "Success"
        ]
        
        # We don't need all indicators to be present, just check if any of them are
        found_indicators = [indicator for indicator in success_indicators if indicator in build_result]
        
        if not found_indicators:
            # If we don't find explicit success indicators, check for error indicators
            error_indicators = [
                "Error",
                "Failed",
                "Exception"
            ]
            
            found_errors = [indicator for indicator in error_indicators if indicator in build_result]
            
            # Use assertion instead of returning False
            assert not found_errors, f"Found error indicators: {found_errors}\nBuild output: {build_result}"
        
        print(f"✅ Found success indicators: {found_indicators}" if found_indicators else "✅ No errors found")
        print("✅ dbt_build integration test passed!")
    
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    try:
        test_dbt_build()
        sys.exit(0)
    except Exception:
        sys.exit(1)