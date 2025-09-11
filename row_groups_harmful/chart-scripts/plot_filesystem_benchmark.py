#!/usr/bin/env python3
"""
Plot filesystem benchmark results from the evaluate_filesystem experiment.

This script reads results/filesystem_benchmark.csv and creates a plot showing
random read bandwidth vs read size, with maximum bandwidth as a horizontal line.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import argparse
from pathlib import Path

def plot_filesystem_benchmark(csv_path, output_path=None, log_scale=False):
    """
    Plot filesystem benchmark results from CSV file.
    
    Args:
        csv_path: Path to the CSV file
        output_path: Optional path to save the plot
        log_scale: Whether to use log scale for both axes
    """
    # Read the CSV file
    df = pd.read_csv(csv_path)
    
    # Create the plot
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Group by filesystem and plot each as a separate series
    filesystems = df['filesystem_name'].unique()
    colors = plt.cm.Set1(np.linspace(0, 1, len(filesystems)))
    
    for i, filesystem in enumerate(filesystems):
        filesystem_data = df[df['filesystem_name'] == filesystem]
        
        # Sort by read size for proper line connection
        filesystem_data = filesystem_data.sort_values('read_size')
        
        # Plot random read bandwidth line
        ax.plot(
            filesystem_data['read_size'],
            filesystem_data['random_bandwidth_mbps'],
            marker='o',
            linewidth=2,
            markersize=6,
            label=f'{filesystem} (random reads)',
            color=colors[i],
            solid_capstyle='round'
        )
        
        # Plot max bandwidth as horizontal dotted line
        # Use the maximum random read bandwidth
        max_bandwidth = filesystem_data['random_bandwidth_mbps'].max()
        
        ax.axhline(
            y=max_bandwidth,
            color=colors[i],
            linestyle='--',
            linewidth=2,
            label=f'{filesystem} (max bandwidth)',
            alpha=0.8
        )
    
    # Set log scale for x-axis (read size)
    ax.set_xscale('log', base=2)
    
    # Set y-axis scale
    if log_scale:
        ax.set_yscale('log')
        ax.set_ylabel('Bandwidth (MB/s, log scale)')
    else:
        ax.set_ylabel('Bandwidth (MB/s)')
    
    # Set labels and title
    ax.set_xlabel('Read Size (bytes)')
    ax.set_title('Filesystem Benchmark: Random Read Performance vs Maximum Bandwidth')
    
    # Format x-axis ticks to show powers of 2
    x_ticks = sorted(df['read_size'].unique())
    ax.set_xticks(x_ticks)
    
    # Format x-axis labels with human-readable sizes
    def format_bytes(size):
        if size >= 1024 * 1024:
            return f'{size // (1024 * 1024)}MB'
        elif size >= 1024:
            return f'{size // 1024}KB'
        else:
            return f'{size}B'
    
    ax.set_xticklabels([format_bytes(x) for x in x_ticks], rotation=45)
    
    # Add grid
    ax.grid(True, alpha=0.3)
    
    # Add legend
    ax.legend(loc='best')
    
    # Tight layout to prevent label cutoff
    plt.tight_layout()
    
    # Save or show the plot
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"Plot saved to: {output_path}")
    else:
        plt.show()

def main():
    parser = argparse.ArgumentParser(description="Plot filesystem benchmark results from evaluate_filesystem experiment")
    parser.add_argument(
        "--input", "-i",
        default="results/filesystem_benchmark.csv",
        help="Input CSV file path (default: results/filesystem_benchmark.csv)"
    )
    parser.add_argument(
        "--output", "-o",
        help="Output plot file path (if not specified, shows plot interactively)"
    )
    parser.add_argument(
        "--log-scale", "-l",
        action="store_true",
        help="Use log scale for y-axis"
    )
    
    args = parser.parse_args()
    
    # Check if input file exists
    if not Path(args.input).exists():
        print(f"Error: Input file '{args.input}' not found")
        return 1
    
    # Plot the results
    plot_filesystem_benchmark(args.input, args.output, args.log_scale)
    
    return 0

if __name__ == "__main__":
    exit(main())