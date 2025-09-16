#!/usr/bin/env python3

"""
Run clang-tidy on the Mooncake project
Based on LLVM's run-clang-tidy.py
"""

import argparse
import json
import multiprocessing
import os
import subprocess
import sys
from pathlib import Path
import concurrent.futures

def find_compilation_database(build_path):
    """Find the compilation database"""
    compile_db = os.path.join(build_path, 'compile_commands.json')
    if not os.path.exists(compile_db):
        print(f"Error: Could not find compile_commands.json in {build_path}")
        sys.exit(1)
    return compile_db

def get_sources_from_compilation_database(compilation_db_path):
    """Extract source files from compilation database"""
    with open(compilation_db_path, 'r') as f:
        compilation_db = json.load(f)
    
    sources = []
    for entry in compilation_db:
        file_path = entry['file']
        # Only include our source files, not third-party
        if any(dir in file_path for dir in ['mooncake-store', 'mooncake-transfer-engine', 'mooncake-integration']):
            if not any(ignore in file_path for ignore in ['third_party', 'build', 'generated']):
                sources.append(file_path)
    
    return sources

def run_tidy(file_path, build_path, fix=False, checks=None, quiet=False):
    """Run clang-tidy on a single file"""
    cmd = ['clang-tidy', '-p', build_path]
    
    if fix:
        cmd.append('-fix')
    
    if checks:
        cmd.extend(['-checks', checks])
    
    cmd.append(file_path)
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if not quiet and (result.stdout or result.stderr):
            return file_path, result.stdout + result.stderr
        elif result.returncode != 0:
            return file_path, result.stderr
        
        return file_path, None
    except subprocess.TimeoutExpired:
        return file_path, f"Timeout processing {file_path}"
    except Exception as e:
        return file_path, f"Error processing {file_path}: {e}"

def main():
    parser = argparse.ArgumentParser(description='Run clang-tidy on Mooncake')
    parser.add_argument('-p', '--build-path', default='build',
                        help='Path to build directory (default: build)')
    parser.add_argument('-fix', action='store_true',
                        help='Apply fixes')
    parser.add_argument('-checks', type=str,
                        help='Checks to run (overrides .clang-tidy)')
    parser.add_argument('-j', '--jobs', type=int, default=multiprocessing.cpu_count(),
                        help='Number of parallel jobs')
    parser.add_argument('-quiet', action='store_true',
                        help='Suppress output except for errors')
    parser.add_argument('files', nargs='*',
                        help='Specific files to check')
    
    args = parser.parse_args()
    
    # Find compilation database
    compile_db_path = find_compilation_database(args.build_path)
    
    # Get files to check
    if args.files:
        files = args.files
    else:
        files = get_sources_from_compilation_database(compile_db_path)
    
    if not files:
        print("No files to check")
        return 0
    
    if not args.quiet:
        print(f"Running clang-tidy on {len(files)} files with {args.jobs} jobs...")
    
    # Run clang-tidy in parallel
    issues_found = False
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as executor:
        futures = [
            executor.submit(run_tidy, f, args.build_path, args.fix, args.checks, args.quiet)
            for f in files
        ]
        
        for future in concurrent.futures.as_completed(futures):
            file_path, output = future.result()
            if output:
                issues_found = True
                if not args.quiet:
                    print(f"\n{file_path}:")
                print(output)
    
    if args.fix and not args.quiet:
        print(f"\nApplied fixes to files")
    elif issues_found:
        if not args.quiet:
            print(f"\nIssues found. Run with -fix to apply fixes.")
        return 1
    elif not args.quiet:
        print("No issues found")
    
    return 0

if __name__ == '__main__':
    sys.exit(main())