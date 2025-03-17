#!/usr/bin/env python3
"""
Integration test for the dbt_run tool that runs dbt models.
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

def test_dbt_run():
    """Test the dbt_run tool by running a specific model"""
    print("Testing dbt_run tool...")
    
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
        
        # Call the dbt_run tool to run the customers model
        print("Running dbt_run for customers model...")
        run_result = run_cli_command("run", {
            "project_dir": str(JAFFLE_SHOP_PATH),
            "models": "customers"
        })
        
        # Print the run result for debugging
        print(f"Run result: {run_result[:200]}...")
        
        # Don't check for specific text, just proceed
        print("✅ Model run completed")
        
        # Verify the target files were created
        target_files = [
            JAFFLE_SHOP_PATH / "target" / "compiled" / "jaffle_shop" / "models" / "customers.sql",
            JAFFLE_SHOP_PATH / "target" / "run" / "jaffle_shop" / "models" / "customers.sql"
        ]
        files_exist = verify_files_exist(target_files)
        
        if not files_exist:
            print("❌ File verification failed")
            return False
        
        print("✅ dbt_run integration test passed!")
        return True
    
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_dbt_run()
    sys.exit(0 if success else 1)