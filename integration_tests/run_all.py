#!/usr/bin/env python3
"""
Run all dbt integration tests and report results.
"""
import os
import sys
import subprocess
from pathlib import Path

def run_all_tests():
    """Run all integration tests and report results"""
    test_files = [
        f for f in os.listdir(Path(__file__).parent)
        if f.startswith("test_") and f.endswith(".py")
    ]

    results = {}

    for test_file in test_files:
        test_name = test_file[:-3]  # Remove .py extension
        print(f"\n==== Running {test_name} ====")

        # Run the test script as a subprocess
        cmd = ["uv", "run", str(Path(__file__).parent / test_file)]
        print(f"DEBUG: Running command: {' '.join(cmd)}")
        process = subprocess.run(cmd, capture_output=True, text=True)

        success = process.returncode == 0
        print(f"DEBUG: Process return code: {process.returncode}, success: {success}")
        results[test_name] = success

        print(f"---- {test_name} Output ----")
        print(process.stdout)

        if process.stderr:
            print(f"---- {test_name} Errors ----")
            print(process.stderr)

    # Print summary
    print("\n==== Test Summary ====")
    passed = sum(1 for r in results.values() if r)
    total = len(results)
    print(f"Passed: {passed}/{total}")

    for test_name, success in results.items():
        status = "✅" if success else "❌"
        print(f"{status} {test_name}")

    # Return overall success/failure
    return all(results.values())

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)