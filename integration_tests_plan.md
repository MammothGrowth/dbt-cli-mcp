# Plan for Replacing Unit Tests with Integration Tests

## Current State Analysis

1. We have unit tests in the `tests/` directory that test individual functions with mocks.
2. The codebase includes mocking functionality in `src/tools.py` and `src/command.py`.
3. We have a real dbt project in `dbt_integration_tests/jaffle_shop_duckdb` that we can use for integration testing.

## Revised Approach: CLI Interface

Instead of using a server-based approach, we'll create a command-line interface (CLI) for the tools. This will:

1. Allow us to execute tools directly from the command line
2. Simplify the testing process by avoiding server management
3. Provide a more user-friendly way to interact with the tools

### 1. Remove Unit Tests and Mocking Functionality

- Delete the `tests/` directory and its contents
- Remove mock-related code from `src/command.py`:
  - Remove the `load_mock_response` function
  - Remove mock-related parameters from the `execute_dbt_command` function
  - Remove mock handling logic in the tool functions

- Remove mock-related code from `src/tools.py`:
  - Remove mock-mode checks and parameter handling in each tool function
  - Remove the `set_mock_mode` tool entirely

- Remove mock-related code from `src/config.py`:
  - Remove `mock_mode` from the default configuration
  
- Update `pyproject.toml`:
  - Remove test-related configurations

### 2. Create CLI Interface

Create a new file `src/cli.py` that:
- Provides a command-line interface to all dbt tools
- Allows executing a single command without starting a server
- Formats output in a user-friendly way

### 3. Create Integration Test Structure

Create a new directory `integration_tests/` with the following structure:
```
integration_tests/
├── run_all.py                # Script to run all integration tests
├── common.py                 # Common utilities for tests
├── test_dbt_run.py           # Test dbt run tool
├── test_dbt_test.py          # Test dbt test tool
├── test_dbt_ls.py            # Test dbt ls tool
├── test_dbt_compile.py       # Test dbt compile tool
├── test_dbt_debug.py         # Test dbt debug tool
├── test_dbt_deps.py          # Test dbt deps tool
├── test_dbt_seed.py          # Test dbt seed tool
├── test_dbt_show.py          # Test dbt show tool
└── test_dbt_build.py         # Test dbt build tool
```

### 4. Implementation Details

#### CLI Interface (src/cli.py)

The CLI interface will use argparse to create a command-line tool that exposes all the dbt tools:

```python
#!/usr/bin/env python3
"""
Command-line interface for dbt tools.
"""

import os
import sys
import json
import asyncio
import argparse
from pathlib import Path
from typing import Dict, Any, Optional, List

from src.tools import (
    dbt_run, dbt_test, dbt_ls, dbt_compile, dbt_debug,
    dbt_deps, dbt_seed, dbt_show, dbt_build,
    configure_dbt_path
)
from src.config import initialize as initialize_config


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="DBT CLI MCP Command Line Interface")
    
    # Global options
    parser.add_argument(
        "--dbt-path",
        help="Path to dbt executable",
        default=os.environ.get("DBT_PATH", "dbt")
    )
    parser.add_argument(
        "--env-file",
        help="Path to environment file",
        default=os.environ.get("ENV_FILE", ".env")
    )
    parser.add_argument(
        "--log-level",
        help="Logging level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default=os.environ.get("LOG_LEVEL", "INFO")
    )
    parser.add_argument(
        "--format",
        help="Output format",
        choices=["text", "json"],
        default="text"
    )
    
    # Set up subparsers for each command
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # dbt_run command
    run_parser = subparsers.add_parser("run", help="Run dbt models")
    run_parser.add_argument("--models", help="Specific models to run")
    run_parser.add_argument("--selector", help="Named selector to use")
    run_parser.add_argument("--exclude", help="Models to exclude")
    run_parser.add_argument("--project-dir", help="Directory containing the dbt project", default=".")
    run_parser.add_argument("--full-refresh", help="Perform a full refresh", action="store_true")
    
    # dbt_test command
    test_parser = subparsers.add_parser("test", help="Run dbt tests")
    test_parser.add_argument("--models", help="Specific models to test")
    test_parser.add_argument("--selector", help="Named selector to use")
    test_parser.add_argument("--exclude", help="Models to exclude")
    test_parser.add_argument("--project-dir", help="Directory containing the dbt project", default=".")
    
    # dbt_ls command
    ls_parser = subparsers.add_parser("ls", help="List dbt resources")
    ls_parser.add_argument("--models", help="Specific models to list")
    ls_parser.add_argument("--selector", help="Named selector to use")
    ls_parser.add_argument("--exclude", help="Models to exclude")
    ls_parser.add_argument("--resource-type", help="Type of resource to list")
    ls_parser.add_argument("--project-dir", help="Directory containing the dbt project", default=".")
    ls_parser.add_argument("--output-format", help="Output format", choices=["json", "name", "path", "selector"], default="json")
    
    # dbt_compile command
    compile_parser = subparsers.add_parser("compile", help="Compile dbt models")
    compile_parser.add_argument("--models", help="Specific models to compile")
    compile_parser.add_argument("--selector", help="Named selector to use")
    compile_parser.add_argument("--exclude", help="Models to exclude")
    compile_parser.add_argument("--project-dir", help="Directory containing the dbt project", default=".")
    
    # dbt_debug command
    debug_parser = subparsers.add_parser("debug", help="Debug dbt project")
    debug_parser.add_argument("--project-dir", help="Directory containing the dbt project", default=".")
    
    # dbt_deps command
    deps_parser = subparsers.add_parser("deps", help="Install dbt package dependencies")
    deps_parser.add_argument("--project-dir", help="Directory containing the dbt project", default=".")
    
    # dbt_seed command
    seed_parser = subparsers.add_parser("seed", help="Load CSV files as seed data")
    seed_parser.add_argument("--selector", help="Named selector to use")
    seed_parser.add_argument("--exclude", help="Seeds to exclude")
    seed_parser.add_argument("--project-dir", help="Directory containing the dbt project", default=".")
    
    # dbt_show command
    show_parser = subparsers.add_parser("show", help="Preview model results")
    show_parser.add_argument("--models", help="Specific model to show", required=True)
    show_parser.add_argument("--project-dir", help="Directory containing the dbt project", default=".")
    show_parser.add_argument("--limit", help="Limit the number of rows returned", type=int)
    
    # dbt_build command
    build_parser = subparsers.add_parser("build", help="Run build command")
    build_parser.add_argument("--models", help="Specific models to build")
    build_parser.add_argument("--selector", help="Named selector to use")
    build_parser.add_argument("--exclude", help="Models to exclude")
    build_parser.add_argument("--project-dir", help="Directory containing the dbt project", default=".")
    build_parser.add_argument("--full-refresh", help="Perform a full refresh", action="store_true")
    
    # configure command
    configure_parser = subparsers.add_parser("configure", help="Configure dbt path")
    configure_parser.add_argument("path", help="Path to dbt executable")
    
    return parser.parse_args()


async def main() -> None:
    """Main entry point for the CLI."""
    args = parse_args()
    
    # Set environment variables from arguments
    os.environ["DBT_PATH"] = args.dbt_path
    os.environ["ENV_FILE"] = args.env_file
    os.environ["LOG_LEVEL"] = args.log_level
    
    # Initialize configuration
    initialize_config()
    
    # Execute the requested command
    if args.command == "run":
        result = await dbt_run(
            models=args.models,
            selector=args.selector,
            exclude=args.exclude,
            project_dir=args.project_dir,
            full_refresh=args.full_refresh
        )
    elif args.command == "test":
        result = await dbt_test(
            models=args.models,
            selector=args.selector,
            exclude=args.exclude,
            project_dir=args.project_dir
        )
    elif args.command == "ls":
        result = await dbt_ls(
            models=args.models,
            selector=args.selector,
            exclude=args.exclude,
            resource_type=args.resource_type,
            project_dir=args.project_dir,
            output_format=args.output_format
        )
    elif args.command == "compile":
        result = await dbt_compile(
            models=args.models,
            selector=args.selector,
            exclude=args.exclude,
            project_dir=args.project_dir
        )
    elif args.command == "debug":
        result = await dbt_debug(
            project_dir=args.project_dir
        )
    elif args.command == "deps":
        result = await dbt_deps(
            project_dir=args.project_dir
        )
    elif args.command == "seed":
        result = await dbt_seed(
            selector=args.selector,
            exclude=args.exclude,
            project_dir=args.project_dir
        )
    elif args.command == "show":
        result = await dbt_show(
            models=args.models,
            project_dir=args.project_dir,
            limit=args.limit
        )
    elif args.command == "build":
        result = await dbt_build(
            models=args.models,
            selector=args.selector,
            exclude=args.exclude,
            project_dir=args.project_dir,
            full_refresh=args.full_refresh
        )
    elif args.command == "configure":
        result = await configure_dbt_path(
            path=args.path
        )
    else:
        print("No command specified. Use --help for usage information.")
        sys.exit(1)
    
    # Print the result
    if args.format == "json":
        try:
            # If result is already a JSON string, parse it first
            if isinstance(result, str) and (result.startswith("{") or result.startswith("[")):
                parsed = json.loads(result)
                print(json.dumps(parsed, indent=2))
            else:
                print(json.dumps({"output": result}, indent=2))
        except json.JSONDecodeError:
            print(json.dumps({"output": result}, indent=2))
    else:
        print(result)


if __name__ == "__main__":
    asyncio.run(main())
```

#### Common Utilities (integration_tests/common.py)

```python
import os
import sys
import json
import asyncio
import subprocess
from pathlib import Path
from typing import Dict, List, Any, Optional

def run_cli_command(command: str, args: Dict[str, Any]) -> str:
    """Run a CLI command and return the output"""
    cmd = [sys.executable, "-m", "src.cli", command]
    
    # Add arguments
    for key, value in args.items():
        if isinstance(value, bool):
            if value:
                cmd.append(f"--{key.replace('_', '-')}")
        elif value is not None:
            cmd.append(f"--{key.replace('_', '-')}")
            cmd.append(str(value))
    
    # Add format as JSON
    cmd.append("--format")
    cmd.append("json")
    
    # Run the command
    process = subprocess.run(cmd, capture_output=True, text=True)
    
    if process.returncode != 0:
        raise Exception(f"Command failed with error: {process.stderr}")
    
    return process.stdout

def verify_output(output: str, expected_patterns: List[str]) -> bool:
    """Verify that the output contains the expected patterns"""
    for pattern in expected_patterns:
        if pattern not in output:
            print(f"Pattern '{pattern}' not found in output")
            return False
    
    return True

def verify_files_exist(file_paths: List[Path]) -> bool:
    """Verify that all the given files exist"""
    for file_path in file_paths:
        if not file_path.exists():
            print(f"File {file_path} does not exist")
            return False
    
    return True

def cleanup_target_dir(project_dir: Path) -> None:
    """Clean up the target directory before running tests"""
    target_dir = project_dir / "target"
    if target_dir.exists():
        import shutil
        shutil.rmtree(target_dir)
```

#### Sample Integration Test (test_dbt_run.py)

```python
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
        
        if not verify_output(seed_result, ["Successfully loaded"]):
            print("❌ Failed to load seed data")
            return False
        
        # Call the dbt_run tool to run the customers model
        print("Running dbt_run for customers model...")
        run_result = run_cli_command("run", {
            "project_dir": str(JAFFLE_SHOP_PATH),
            "models": "customers"
        })
        
        # Verify the tool execution was successful
        success = verify_output(run_result, [
            "Completed successfully",
            "customers"
        ])
        
        if not success:
            print("❌ Output verification failed")
            print(f"Output: {run_result}")
            return False
        
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
```

#### run_all.py

```python
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
        cmd = [sys.executable, str(Path(__file__).parent / test_file)]
        process = subprocess.run(cmd, capture_output=True, text=True)
        
        success = process.returncode == 0
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
```

### 5. Update pyproject.toml

Update the pyproject.toml to include the CLI functionality:

```toml
[project.scripts]
dbt-mcp = "src.cli:main_entry"
```

### 6. Update Documentation

Update the README.md to include CLI usage instructions:

```markdown
## Command Line Interface

This project provides a command-line interface (CLI) for the dbt tools:

```bash
# Run dbt models
python -m src.cli run --models customers --project-dir /path/to/project

# List dbt resources
python -m src.cli ls --resource-type model --output-format json

# Run dbt tests
python -m src.cli test --project-dir /path/to/project
```

You can also use the `--help` flag to see available options:

```bash
python -m src.cli --help
python -m src.cli run --help
```
```

## Integration Tests

The integration tests use this CLI to verify functionality with the jaffle_shop_duckdb project:

```
integration_tests/
├── run_all.py                # Script to run all tests
├── common.py                 # Common utilities
├── test_dbt_run.py           # Test dbt run tool
├── test_dbt_test.py          # Test dbt test tool
└── ...
```

To run all tests:

```bash
python integration_tests/run_all.py
```

To run a specific test:

```bash
python integration_tests/test_dbt_run.py
```
```

## Advantages of This Approach

1. **Simplified Testing**: Using the CLI directly avoids the complexity of managing a server process
2. **Improved User Experience**: Provides a convenient command-line interface for users
3. **Real-world Testing**: Tests against an actual dbt project provide more reliable validation
4. **Simplified Code**: Removing mocking functionality makes the codebase cleaner

## Next Steps

1. Remove unit tests and mocking functionality
2. Create the CLI interface
3. Implement the integration tests
4. Update documentation

## Jaffle Shop Project Structure

The integration tests will use the jaffle_shop_duckdb project, which has the following structure:

- **Models**: customers.sql, orders.sql, and staging models
- **Seeds**: raw_customers.csv, raw_orders.csv, raw_payments.csv
- **Configuration**: profiles.yml, dbt_project.yml

This project is ideal for testing as it includes multiple models, references between models, and seed data for initial loading.