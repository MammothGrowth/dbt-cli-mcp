#!/usr/bin/env python3
"""
Script to fix the return statements in integration test files.
"""
import os
import re
from pathlib import Path

def fix_return_statements(file_path):
    """Fix return statements in a test file to use assertions instead."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Replace return False with assertions
    content = re.sub(
        r'if not (.*?):\s+print\("❌.*?"\)\s+(?:print\(f".*?"\)\s+)?return False',
        r'assert \1, "Verification failed"',
        content
    )
    
    # Replace return True with nothing
    content = re.sub(
        r'print\("✅.*?"\)\s+return True',
        r'print("✅ Test passed!")',
        content
    )
    
    with open(file_path, 'w') as f:
        f.write(content)
    
    print(f"Fixed return statements in {file_path}")

def main():
    """Fix all integration test files."""
    integration_tests_dir = Path('integration_tests')
    
    for file_path in integration_tests_dir.glob('test_*.py'):
        if file_path.name not in ['test_dbt_run.py', 'test_dbt_build.py', 'test_dbt_debug.py', 'test_dbt_deps.py']:
            print(f"Processing {file_path}...")
            fix_return_statements(file_path)

if __name__ == "__main__":
    main()