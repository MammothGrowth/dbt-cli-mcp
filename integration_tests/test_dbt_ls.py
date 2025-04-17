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
            "profiles_dir": str(JAFFLE_SHOP_PATH),  # Explicitly set profiles_dir to the same as project_dir
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
            print("✅ Test passed!")
            print("DEBUG: test_dbt_ls returning True")
            return True

        except json.JSONDecodeError as e:
            print(f"❌ Failed to parse JSON result: {ls_result}")
            print(f"Error: {e}")
            print("DEBUG: test_dbt_ls returning False due to JSONDecodeError")
            return False

    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        print("DEBUG: test_dbt_ls raising exception")
        raise
def test_dbt_ls_with_profiles_dir():
    """Test the dbt_ls tool with explicit profiles_dir parameter"""
    print("Testing dbt_ls tool with explicit profiles_dir parameter...")

    try:
        # Call the dbt_ls tool with explicit profiles_dir
        print("Listing all models with explicit profiles_dir...")
        ls_result = run_cli_command("ls", {
            "project_dir": str(JAFFLE_SHOP_PATH),
            "profiles_dir": str(JAFFLE_SHOP_PATH),  # Explicitly set profiles_dir
            "resource_type": "model",
            "output_format": "json"
        })

        # Parse the JSON result (similar to test_dbt_ls)
        try:
            result_data = json.loads(ls_result)

            # Extract the actual output from the JSON response
            if isinstance(result_data, dict) and "output" in result_data:
                output = result_data["output"]
                if isinstance(output, str) and (output.startswith("[") or output.startswith("{")):
                    output = json.loads(output)
            else:
                output = result_data

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
            print("✅ Test passed!")
            print("DEBUG: test_dbt_ls_with_profiles_dir returning True")
            return True

        except json.JSONDecodeError as e:
            print(f"❌ Failed to parse JSON result: {ls_result}")
            print(f"Error: {e}")
            print("DEBUG: test_dbt_ls_with_profiles_dir returning False due to JSONDecodeError")
            return False

    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        print("DEBUG: test_dbt_ls_with_profiles_dir raising exception")
        raise

def test_dbt_ls_verbose():
    """Test the dbt_ls tool with verbose flag"""
    print("Testing dbt_ls tool with verbose flag...")

    try:
        # First test with default (simplified) output
        print("Listing models with simplified output (default)...")
        simplified_result = run_cli_command("ls", {
            "project_dir": str(JAFFLE_SHOP_PATH),
            "profiles_dir": str(JAFFLE_SHOP_PATH),
            "resource_type": "model",
            "output_format": "json"
        })

        # Then test with verbose output
        print("Listing models with verbose output...")
        verbose_result = run_cli_command("ls", {
            "project_dir": str(JAFFLE_SHOP_PATH),
            "profiles_dir": str(JAFFLE_SHOP_PATH),
            "resource_type": "model",
            "output_format": "json",
            "verbose": True
        })

        # Parse both results
        try:
            simplified_data = json.loads(simplified_result)
            verbose_data = json.loads(verbose_result)

            # Extract the actual output from the JSON responses
            if isinstance(simplified_data, dict) and "output" in simplified_data:
                simplified_output = simplified_data["output"]
                if isinstance(simplified_output, str) and (simplified_output.startswith("[") or simplified_output.startswith("{")):
                    simplified_output = json.loads(simplified_output)
            else:
                simplified_output = simplified_data

            if isinstance(verbose_data, dict) and "output" in verbose_data:
                verbose_output = verbose_data["output"]
                if isinstance(verbose_output, str) and (verbose_output.startswith("[") or verbose_output.startswith("{")):
                    verbose_output = json.loads(verbose_output)
            else:
                verbose_output = verbose_data

            # Verify both outputs contain the expected models
            simplified_models = []
            verbose_models = []

            # Debug output
            print(f"DEBUG: Simplified output type: {type(simplified_output)}")
            if isinstance(simplified_output, list):
                print(f"DEBUG: Simplified output length: {len(simplified_output)}")
                if simplified_output and len(simplified_output) > 0:
                    print(f"DEBUG: First simplified item type: {type(simplified_output[0])}")
                    print(f"DEBUG: First simplified item: {simplified_output[0]}")

            print(f"DEBUG: Verbose output type: {type(verbose_output)}")
            if isinstance(verbose_output, list):
                print(f"DEBUG: Verbose output length: {len(verbose_output)}")
                if verbose_output and len(verbose_output) > 0:
                    print(f"DEBUG: First verbose item type: {type(verbose_output[0])}")
                    print(f"DEBUG: First verbose item: {verbose_output[0]}")

            # Process simplified output
            if isinstance(simplified_output, list):
                for item in simplified_output:
                    # Handle dictionary items (properly formatted model data)
                    if isinstance(item, dict) and "name" in item and "resource_type" in item:
                        simplified_models.append(item["name"])

                        # Verify simplified output only has the required fields
                        if set(item.keys()) != {"name", "resource_type", "depends_on"}:
                            print(f"❌ Simplified output has unexpected fields: {set(item.keys())}")
                            return False

                        # Verify depends_on only has nodes
                        if "depends_on" in item and set(item["depends_on"].keys()) != {"nodes"}:
                            print(f"❌ Simplified output depends_on has unexpected fields: {set(item['depends_on'].keys())}")
                            return False
                    # Handle string items (could be model names or log messages)
                    elif isinstance(item, str):
                        # Skip log messages and only add actual model names
                        if not item.startswith('\x1b[') and not any(log_msg in item for log_msg in [
                            "Running with dbt=", "Registered adapter:", "Found", "Starting"
                        ]):
                            simplified_models.append(item)

            # Process verbose output
            if isinstance(verbose_output, list):
                for item in verbose_output:
                    # Handle dictionary items (properly formatted model data)
                    if isinstance(item, dict) and "name" in item and "resource_type" in item:
                        verbose_models.append(item["name"])

                        # Verify verbose output has more fields than simplified
                        if len(item.keys()) <= 3:
                            print(f"❌ Verbose output doesn't have enough fields: {set(item.keys())}")
                            return False

                        # Check for fields that should be in verbose but not simplified
                        for field in ["package_name", "original_file_path", "unique_id", "config"]:
                            if field not in item:
                                print(f"❌ Verbose output missing expected field: {field}")
                                return False
                    # Handle string items (could be model names or log messages)
                    elif isinstance(item, str):
                        # Skip log messages and only add actual model names
                        if not item.startswith('\x1b[') and not any(log_msg in item for log_msg in [
                            "Running with dbt=", "Registered adapter:", "Found", "Starting"
                        ]):
                            verbose_models.append(item)

            # Filter out any log messages from the model lists
            simplified_models = [model for model in simplified_models if model in ["customers", "orders", "stg_customers", "stg_orders", "stg_payments"]]
            verbose_models = [model for model in verbose_models if model in ["customers", "orders", "stg_customers", "stg_orders", "stg_payments"]]

            # Sort the model lists for consistent comparison
            simplified_models.sort()
            verbose_models.sort()

            # Verify both outputs have the same models
            expected_models = ["customers", "orders", "stg_customers", "stg_orders", "stg_payments"]
            expected_models.sort()

            missing_simplified = [model for model in expected_models if model not in simplified_models]
            missing_verbose = [model for model in expected_models if model not in verbose_models]

            if missing_simplified:
                print(f"❌ Simplified output missing expected models: {missing_simplified}")
                print(f"Found models: {simplified_models}")
                return False

            if missing_verbose:
                print(f"❌ Verbose output missing expected models: {missing_verbose}")
                print(f"Found models: {verbose_models}")
                return False

            # Debug output for final model lists
            print(f"DEBUG: Final simplified_models: {simplified_models}")
            print(f"DEBUG: Final verbose_models: {verbose_models}")
            print(f"DEBUG: Models equal? {simplified_models == verbose_models}")

            if simplified_models != verbose_models:
                print(f"❌ Simplified and verbose outputs have different models")
                print(f"Simplified: {simplified_models}")
                print(f"Verbose: {verbose_models}")
                return False

            print(f"✅ Found all expected models in both outputs: {expected_models}")
            print("✅ Test passed!")
            return True

        except json.JSONDecodeError as e:
            print(f"❌ Failed to parse JSON results")
            print(f"Error: {e}")
            return False

    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    success = True
    try:
        print("DEBUG: Starting test_dbt_ls")
        test_ls_result = test_dbt_ls()
        print(f"DEBUG: test_dbt_ls result: {test_ls_result}")
        success = test_ls_result and success

        print("DEBUG: Starting test_dbt_ls_with_profiles_dir")
        profiles_result = test_dbt_ls_with_profiles_dir()
        print(f"DEBUG: test_dbt_ls_with_profiles_dir result: {profiles_result}")
        success = profiles_result and success

        print("DEBUG: Starting test_dbt_ls_verbose")
        verbose_result = test_dbt_ls_verbose()
        print(f"DEBUG: test_dbt_ls_verbose result: {verbose_result}")
        success = verbose_result and success

        print(f"DEBUG: Final success value: {success}")
        exit_code = 0 if success else 1
        print(f"DEBUG: Exiting with code: {exit_code}")
        sys.exit(exit_code)
    except Exception as e:
        print(f"DEBUG: Exception occurred: {e}")
        sys.exit(1)