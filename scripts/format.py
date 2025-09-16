#!/usr/bin/env python3

"""
Format script for Mooncake project
Based on DuckDB's format.py but adapted for Mooncake
"""

import os
import sys
import subprocess
import argparse
import concurrent.futures
from pathlib import Path

# Check for required tools
def check_tool(tool, version_cmd, required_text, install_msg):
    try:
        ver = subprocess.check_output(version_cmd, text=True, stderr=subprocess.STDOUT)
        if required_text and required_text not in ver:
            print(f'{install_msg}\nCurrent version: {ver}')
            sys.exit(1)
    except Exception as e:
        print(f'{install_msg}\nError: {e}')
        sys.exit(1)

# Check clang-format
check_tool('clang-format', ['clang-format', '--version'], None,
           'Please install clang-format: apt-get install clang-format')

# File extensions to format
CPP_EXTENSIONS = ['.cpp', '.hpp', '.c', '.h', '.cc', '.hh']
CMAKE_EXTENSIONS = ['CMakeLists.txt', '.cmake']
PYTHON_EXTENSIONS = ['.py']

# Directories to format
FORMATTED_DIRECTORIES = [
    'mooncake-store',
    'mooncake-transfer-engine', 
    'mooncake-integration',
    'mooncake-p2p-store',
    'scripts',
    'benchmarks',
    'tests'
]

# Ignored paths
IGNORED_PATHS = [
    'build',
    'third_party',
    '.git',
    '__pycache__',
    'cmake-build-debug',
    'cmake-build-release',
    '.vscode',
    '.idea',
    'generated',
    '_generated'
]

def should_format_file(filepath):
    """Check if a file should be formatted"""
    path = Path(filepath)
    
    # Check if in ignored path
    for ignored in IGNORED_PATHS:
        if ignored in path.parts:
            return False
    
    # Check extension
    suffix = path.suffix
    name = path.name
    
    if suffix in CPP_EXTENSIONS:
        return True
    if suffix in PYTHON_EXTENSIONS:
        return True
    if name in CMAKE_EXTENSIONS or suffix in CMAKE_EXTENSIONS:
        return True
    
    return False

def find_files_to_format(directories=None):
    """Find all files that need formatting"""
    files = []
    
    if directories is None:
        directories = FORMATTED_DIRECTORIES
    
    for dir_name in directories:
        if not os.path.exists(dir_name):
            continue
            
        for root, dirs, filenames in os.walk(dir_name):
            # Remove ignored directories from search
            dirs[:] = [d for d in dirs if d not in IGNORED_PATHS]
            
            for filename in filenames:
                filepath = os.path.join(root, filename)
                if should_format_file(filepath):
                    files.append(filepath)
    
    return files

def format_cpp_file(filepath, check_only=False):
    """Format a C++ file using clang-format"""
    if check_only:
        # Check if formatting is needed
        result = subprocess.run(
            ['clang-format', '--dry-run', '-Werror', filepath],
            capture_output=True,
            text=True
        )
        return result.returncode == 0, filepath
    else:
        # Format in place
        subprocess.run(['clang-format', '-i', filepath], check=True)
        return True, filepath

def format_python_file(filepath, check_only=False):
    """Format a Python file using black (if available)"""
    try:
        if check_only:
            result = subprocess.run(
                ['black', '--check', '--quiet', filepath],
                capture_output=True,
                text=True
            )
            return result.returncode == 0, filepath
        else:
            subprocess.run(['black', '--quiet', filepath], check=True)
            return True, filepath
    except FileNotFoundError:
        # Black not installed, skip Python formatting
        return True, filepath

def format_cmake_file(filepath, check_only=False):
    """Format a CMake file using cmake-format (if available)"""
    try:
        if check_only:
            result = subprocess.run(
                ['cmake-format', '--check', filepath],
                capture_output=True,
                text=True
            )
            return result.returncode == 0, filepath
        else:
            # Read, format, and write back
            result = subprocess.run(
                ['cmake-format', filepath],
                capture_output=True,
                text=True,
                check=True
            )
            with open(filepath, 'w') as f:
                f.write(result.stdout)
            return True, filepath
    except FileNotFoundError:
        # cmake-format not installed, skip CMake formatting
        return True, filepath

def format_file(filepath, check_only=False):
    """Format a single file based on its type"""
    path = Path(filepath)
    
    if path.suffix in CPP_EXTENSIONS:
        return format_cpp_file(filepath, check_only)
    elif path.suffix in PYTHON_EXTENSIONS:
        return format_python_file(filepath, check_only)
    elif path.name in CMAKE_EXTENSIONS or path.suffix in CMAKE_EXTENSIONS:
        return format_cmake_file(filepath, check_only)
    
    return True, filepath

def main():
    parser = argparse.ArgumentParser(description='Format Mooncake source code')
    parser.add_argument('--all', action='store_true', help='Format all files')
    parser.add_argument('--check', action='store_true', help='Check formatting without modifying')
    parser.add_argument('--fix', action='store_true', help='Fix formatting issues')
    parser.add_argument('--noconfirm', action='store_true', help='Don\'t ask for confirmation')
    parser.add_argument('--silent', action='store_true', help='Silent mode')
    parser.add_argument('--dir', help='Format specific directory')
    parser.add_argument('files', nargs='*', help='Specific files to format')
    
    args = parser.parse_args()
    
    # Determine files to format
    if args.files:
        files = args.files
    elif args.dir:
        files = find_files_to_format([args.dir])
    elif args.all:
        files = find_files_to_format()
    else:
        print("Please specify --all, --dir, or provide files to format")
        sys.exit(1)
    
    if not files:
        print("No files to format")
        return
    
    # Determine mode
    check_only = args.check and not args.fix
    
    if not args.silent:
        print(f"{'Checking' if check_only else 'Formatting'} {len(files)} files...")
    
    # Format files in parallel
    failed_files = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(format_file, f, check_only) for f in files]
        
        for future in concurrent.futures.as_completed(futures):
            success, filepath = future.result()
            if not success:
                failed_files.append(filepath)
    
    # Report results
    if check_only:
        if failed_files:
            print(f"\n{len(failed_files)} files need formatting:")
            for f in failed_files:
                print(f"  {f}")
            sys.exit(1)
        else:
            if not args.silent:
                print("All files are properly formatted")
    else:
        if not args.silent:
            print(f"Formatted {len(files)} files")
    
    return 0

if __name__ == '__main__':
    sys.exit(main())