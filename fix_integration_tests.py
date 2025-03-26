#!/usr/bin/env python3
"""
Script to fix the integration tests to use assertions instead of returning True/False.
"""
import os
import re
from pathlib import Path

def fix_test_file(file_path):
    """Fix a test file to use assertions instead of returning True/False."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Replace the return True/False pattern with assertions
    content = re.sub(
        r'if not (.*?):\s+print\("❌.*?"\)\s+return False\s+\s+print\("✅.*?"\)\s+return True',
        r'assert \1, "Verification failed"\n\n        print("✅ Test passed!")',
        content
    )
    
    # Replace the exception handling to re-raise
    content = re.sub(
        r'except Exception as e:\s+print\(f"❌.*?"\)\s+import traceback\s+traceback\.print_exc\(\)\s+return False',
        r'except Exception as e:\n        print(f"❌ Test failed with exception: {e}")\n        import traceback\n        traceback.print_exc()\n        raise',
        content
    )
    
    # Replace the main function
    content = re.sub(
        r'if __name__ == "__main__":\s+success = .*?\(\)\s+sys\.exit\(0 if success else 1\)',
        r'if __name__ == "__main__":\n    try:\n        test_dbt_run()\n        sys.exit(0)\n    except Exception:\n        sys.exit(1)',
        content
    )
    
    with open(file_path, 'w') as f:
        f.write(content)
    
    print(f"Fixed {file_path}")

def main():
    """Fix all integration test files."""
    integration_tests_dir = Path('integration_tests')
    
    for file_path in integration_tests_dir.glob('test_*.py'):
        if file_path.name != 'test_dbt_run.py':  # Skip the one we already fixed
            print(f"Processing {file_path}...")
            fix_test_file(file_path)

if __name__ == "__main__":
    main()