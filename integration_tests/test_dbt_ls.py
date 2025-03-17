#!/usr/bin/env python3
"""
Integration test for the dbt_ls tool that lists dbt resources.
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

def test_dbt_ls():
    """Test the dbt_ls tool by listing models"""
    print("Testing dbt_ls tool...")
    
    try:
        # Call the dbt_ls tool to list all models
        print("Listing all models...")
        ls_result = run_cli_command("ls", {
            "project_dir": str(JAFFLE_SHOP_PATH),
            "resource_type": "model",
            "output_format": "json"
        })
        
        # Parse the JSON result
        try:
            result_data = json.loads(ls_result)
            
            # Extract the actual output from the JSON response
            if isinstance(result_data, dict) and "output" in result_data:
                output = result_data["output"]
                if isinstance(output, str) and (output.startswith("[") or output.startswith("{")):
                    # If output is a JSON string, parse it
                    output = json.loads(output)
            else:
                output = result_data
            
            # Print the raw output for debugging
            print(f"Raw output type: {type(output)}")
            if isinstance(output, str):
                print(f"Raw output: {output[:100]}...")
            elif isinstance(output, dict):
                print(f"Raw output keys: {list(output.keys())}")
            elif isinstance(output, list):
                print(f"Raw output length: {len(output)}")
                
                # Filter out log messages before displaying
                filtered_items = []
                for item in output:
                    if isinstance(item, dict) and "name" in item:
                        name_value = item["name"]
                        # Skip items with ANSI color codes or log messages
                        if '\x1b[' in name_value or any(log_msg in name_value for log_msg in [
                            "Running with dbt=", "Registered adapter:", "Found", "Starting"
                        ]):
                            continue
                        filtered_items.append(item)
                
                print(f"Filtered output length: {len(filtered_items)}")
                for i, item in enumerate(filtered_items[:3]):  # Print first 3 filtered items
                    print(f"Item {i} type: {type(item)}")
                    print(f"Item {i}: {str(item)[:100]}...")
            
            # Verify we have at least the expected models
            model_names = []
            
            # The output is a list of dictionaries or strings
            if isinstance(output, list):
                for item in output:
                    # If it's a dictionary with a name key
                    if isinstance(item, dict) and "name" in item:
                        name_value = item["name"]
                        
                        # If it's a log message, skip it
                        if name_value.startswith('\x1b[0m'):
                            continue
                            
                        # If it's a JSON string, try to parse it
                        if name_value.strip().startswith('{'):
                            try:
                                model_data = json.loads(name_value)
                                if "name" in model_data and "resource_type" in model_data and model_data["resource_type"] == "model":
                                    model_names.append(model_data["name"])
                            except json.JSONDecodeError:
                                pass
                        else:
                            # If it's a model name, add it
                            model_names.append(name_value)
                    
                    # If it's a string containing JSON
                    elif isinstance(item, str) and item.strip().startswith('{'):
                        try:
                            model_data = json.loads(item)
                            if "name" in model_data and "resource_type" in model_data and model_data["resource_type"] == "model":
                                model_names.append(model_data["name"])
                        except json.JSONDecodeError:
                            pass
            
            expected_models = ["customers", "orders", "stg_customers", "stg_orders", "stg_payments"]
            
            missing_models = [model for model in expected_models if model not in model_names]
            if missing_models:
                print(f"❌ Missing expected models: {missing_models}")
                print(f"Found models: {model_names}")
                return False
            
            print(f"✅ Found all expected models: {expected_models}")
            print("✅ dbt_ls integration test passed!")
            return True
            
        except json.JSONDecodeError as e:
            print(f"❌ Failed to parse JSON result: {ls_result}")
            print(f"Error: {e}")
            return False
    
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_dbt_ls()
    sys.exit(0 if success else 1)