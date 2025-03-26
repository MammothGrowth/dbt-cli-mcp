#!/usr/bin/env python3
"""
Script to fix the main functions in integration test files.
"""
import os
import re
from pathlib import Path

def fix_main_function(file_path):
    """Fix the main function in a test file to use the correct test function name."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Extract the test function name from the file
    test_func_match = re.search(r'def (test_dbt_\w+)\(', content)
    if not test_func_match:
        print(f"Could not find test function in {file_path}")
        return
    
    test_func_name = test_func_match.group(1)
    
    # Replace the main function to use the correct test function
    content = re.sub(
        r'if __name__ == "__main__":\s+try:\s+test_dbt_\w+\(\)\s+sys\.exit\(0\)\s+except Exception:\s+sys\.exit\(1\)',
        f'if __name__ == "__main__":\n    try:\n        {test_func_name}()\n        sys.exit(0)\n    except Exception:\n        sys.exit(1)',
        content
    )
    
    with open(file_path, 'w') as f:
        f.write(content)
    
    print(f"Fixed main function in {file_path} to use {test_func_name}")

def main():
    """Fix all integration test files."""
    integration_tests_dir = Path('integration_tests')
    
    for file_path in integration_tests_dir.glob('test_*.py'):
        if file_path.name not in ['test_dbt_run.py', 'test_dbt_build.py', 'test_dbt_debug.py', 'test_dbt_deps.py']:
            print(f"Processing {file_path}...")
            fix_main_function(file_path)

if __name__ == "__main__":
    main()