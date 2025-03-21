#!/usr/bin/env python3
"""
Automated experiment script with improved result parsing and progress tracking
"""

import os
import subprocess
import time
import datetime
import re
import csv
import statistics
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from tqdm import tqdm
from typing import List, Tuple, Optional

# ====================== Configuration ======================
# tcp: 1.8875GB/s
# rdma: 12.25GB/s
BENCHMARK_SCRIPT = "./run_benchmark.sh"
ENGINES = ["mooncake"]
VALUE_SIZES = [524288, 1048576, 8388608,16777200]
THREAD_COUNTS = [2,4,8]
# VALUE_SIZES = [524288]
# THREAD_COUNTS = [2,4,8]
NUM_OPS = 2000
RESULTS_DIR = "./results"
RESULTS_FILE = f"{RESULTS_DIR}/benchmark_results.csv"
SUMMARY_FILE = f"{RESULTS_DIR}/benchmark_summary.csv"
PLOTS_DIR = f"{RESULTS_DIR}/plots"
# ====================== Helper Functions ======================

def log(message: str, pbar: Optional[tqdm] = None) -> None:
    """Log messages with timestamp and tqdm integration"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"[{timestamp}] {message}"
    if pbar:
        pbar.write(msg)
    else:
        print(msg)

def format_size(size_bytes: int) -> str:
    """Convert bytes to human-readable format"""
    for unit in ['B', 'KB', 'MB']:
        if size_bytes < 1024:
            return f"{size_bytes:.0f}{unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f}GB"

def parse_benchmark_log(log_content: str) -> dict:
    """Improved log parser using state machine"""
    results = {'prefill': {}, 'decode': {}}
    current_section = None
    
    for line in log_content.split('\n'):
        if "Prefill Results" in line:
            current_section = 'prefill'
        elif "Decode Results" in line:
            current_section = 'decode'
        
        if current_section and 'Throughput:' in line:
            results[current_section]['throughput'] = float(re.search(r"Throughput:\s+([\d.]+)", line).group(1))
    
    return results

def write_csv_row(row: List, file_path: str) -> None:
    """Write single row to CSV file"""
    with open(file_path, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(row)

def plot_throughput_comparison(csv_path=RESULTS_FILE):
    """
    Generate a single PNG with two vertically stacked plots:
    - Top: Prefill throughput comparison
    - Bottom: Decode throughput comparison
    
    Args:
        csv_path (str): Path to the results CSV file
        
    Returns:
        str: Path to the combined plot image
    """
    # Create plots directory if it doesn't exist
    Path(PLOTS_DIR).mkdir(parents=True, exist_ok=True)
    
    # Load data
    df = pd.read_csv(csv_path)
    
    # Define custom colors for engines
    engine_colors = {
        'redis': '#E74C3C',  # Red for Redis
        'mooncake': '#3498DB'  # Blue for Mooncake
    }
    
    # Create a figure with 2 vertically stacked subplots
    fig, axes = plt.subplots(2, 1, figsize=(10, 10))
    
    # Create more readable x-axis labels by combining value size and thread count
    # Convert bytes to appropriate units (KB, MB) for better readability
    df['size_label'] = df['value_size'].apply(
        lambda x: f"{x} Bytes" if x < 1024 else 
                 (f"{x/1024:.0f} KB" if x < 1024*1024 else 
                  f"{x/(1024*1024):.1f} MB"))
    df['x_label'] = df['size_label'] + " - " + df['threads'].astype(str) + " T"
    # Plot prefill throughput on the top subplot
    for engine in df['engine'].unique():
        subset = df[df['engine'] == engine]
        axes[0].plot(
            subset['x_label'], 
            subset['prefill_tput'], 
            marker='o', 
            label=engine,
            color=engine_colors[engine],
            linewidth=2
        )
    
    axes[0].set_title("Prefill Throughput Comparison", fontsize=14)
    axes[0].set_ylabel("Throughput (GB/s)", fontsize=12)
    axes[0].legend()
    axes[0].grid(axis="y")
    
    # Plot decode throughput on the bottom subplot
    for engine in df['engine'].unique():
        subset = df[df['engine'] == engine]
        axes[1].plot(
            subset['x_label'], 
            subset['decode_tput'], 
            marker='o', 
            label=engine,
            color=engine_colors[engine],
            linewidth=2
        )
    
    axes[1].set_title("Decode Throughput Comparison", fontsize=14)
    axes[1].set_ylabel("Throughput (ops/ms)", fontsize=12)
    axes[1].legend()
    axes[1].grid(axis="y")
    
    # Rotate x-axis labels for better readability
    for ax in axes:
        plt.setp(ax.get_xticklabels(), rotation=45)
    
    # Add a main title to the figure
    fig.suptitle("Throughput Comparison: Prefill vs Decode", fontsize=16)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save the combined figure
    combined_plot_path = f"{PLOTS_DIR}/combined_throughput_comparison.png"
    plt.savefig(combined_plot_path, bbox_inches='tight')
    
    # Close the figure
    plt.close(fig)
    
    # Return the path to the plot
    return combined_plot_path

# ====================== Main Execution ======================

def main():
    # Initialize results directory
    Path(RESULTS_DIR).mkdir(parents=True, exist_ok=True)
    
    # Initialize CSV file
    with open(RESULTS_FILE, 'w') as f:
        f.write("engine,value_size,threads,ops,prefill_tput,decode_tput,timestamp\n")
    
    # Calculate total experiments
    total_exps = len(ENGINES) * len(VALUE_SIZES) * len(THREAD_COUNTS)
    
    # Create progress bar
    with tqdm(total=total_exps, desc="Running benchmarks", unit="exp") as pbar:
        start_time = time.time()
        results = []
        
        for engine in ENGINES:
            for value_size in VALUE_SIZES:
                for threads in THREAD_COUNTS:
                    try:
                        # Run benchmark
                        cmd = [
                            BENCHMARK_SCRIPT,
                            f"--engine={engine}",
                            f"--value-size={value_size}",
                            f"--num-ops={NUM_OPS}",
                            f"--num-threads={threads}"
                        ]
                        
                        result = subprocess.run(
                            cmd,
                            check=True,
                            capture_output=True,
                            text=True,
                            timeout=300  # 5 minute timeout
                        )
                        
                        # Parse results
                        parsed = parse_benchmark_log(result.stdout)
                        timestamp = datetime.datetime.now().isoformat()
                        
                        # Prepare data record
                        record = {
                            'engine': engine,
                            'value_size': value_size,
                            'threads': threads,
                            'ops': NUM_OPS,
                            'prefill_tput': parsed['prefill'].get('throughput'),
                            'decode_tput': parsed['decode'].get('throughput'),
                            'timestamp': timestamp
                        }
                        
                        # Write to CSV
                        csv_row = [
                            engine,
                            value_size,
                            threads,
                            NUM_OPS,
                            record['prefill_tput'],
                            record['decode_tput'],
                            timestamp
                        ]
                        write_csv_row(csv_row, RESULTS_FILE)
                        
                        # Print the results
                        print(f"Results: {record}")
                        
                        # Update progress
                        pbar.update(1)
                        log(f"Completed: {engine} {format_size(value_size)} {threads} threads", pbar)
                        
                    except subprocess.CalledProcessError as e:
                        log(f"Error running {engine}/{format_size(value_size)}/{threads}: {e.stderr}", pbar)
                    except Exception as e:
                        log(f"Unexpected error: {str(e)}", pbar)
                    
                    time.sleep(1)  # Cooldown period

        log(f"\nBenchmark completed in {time.time() - start_time:.2f} seconds", pbar)
        log(f"Results saved to {RESULTS_FILE}", pbar)
        
        # Generate and save plots
        try:
            combined_plot = plot_throughput_comparison()
            log(f"Combined plot generated and saved to {combined_plot}", pbar)
        except Exception as e:
            log(f"Error generating plots: {str(e)}", pbar)

if __name__ == "__main__":
    main()
