"""
Common utilities for integration tests.
"""

import os
import sys
import json
import subprocess
from pathlib import Path
from typing import Dict, List, Any, Optional

def run_cli_command(command: str, args: Dict[str, Any]) -> str:
    """Run a CLI command and return the output"""
    cmd = ["uv", "run", "-m", "src.cli", "--format", "json", command]
    
    # Add arguments
    for key, value in args.items():
        if isinstance(value, bool):
            if value:
                cmd.append(f"--{key.replace('_', '-')}")
        elif value is not None:
            cmd.append(f"--{key.replace('_', '-')}")
            cmd.append(str(value))
    
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