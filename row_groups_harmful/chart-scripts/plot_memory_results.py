#!/usr/bin/env python3
"""
Plot memory usage results from the write_memory_experiment.

This script reads results/write_memory.csv and creates a plot showing
memory usage vs row group size, with separate lines for each dataset.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import argparse
from pathlib import Path

def plot_memory_results(csv_path, output_path=None, log_scale=False):
    """
    Plot memory usage results from CSV file.
    
    Args:
        csv_path: Path to the CSV file
        output_path: Optional path to save the plot
        log_scale: Whether to use log scale for y-axis
    """
    # Read the CSV file
    df = pd.read_csv(csv_path)
    
    # Create the plot
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Group by dataset and plot each as a separate line
    datasets = df['dataset'].unique()
    colors = plt.cm.Set1(np.linspace(0, 1, len(datasets)))
    
    for i, dataset in enumerate(datasets):
        dataset_data = df[df['dataset'] == dataset]
        
        # Sort by row group size for proper line connection
        dataset_data = dataset_data.sort_values('requested_row_group_size')
        
        ax.plot(
            dataset_data['requested_row_group_size'],
            dataset_data['peak_rss_kb'],
            marker='o',
            linewidth=2,
            markersize=6,
            label=dataset,
            color=colors[i]
        )
        
        # Add data point annotations for key values
        for _, row in dataset_data.iterrows():
            if row['peak_rss_kb'] > 0:  # Only annotate non-zero values
                ax.annotate(
                    f"{row['peak_rss_kb']:,}",
                    (row['requested_row_group_size'], row['peak_rss_kb']),
                    xytext=(5, 5),
                    textcoords='offset points',
                    fontsize=8,
                    alpha=0.7
                )
    
    # Set log scale for x-axis (row group size)
    ax.set_xscale('log', base=2)
    
    # Set y-axis scale
    if log_scale:
        ax.set_yscale('log')
        ax.set_ylabel('Peak RSS above initial (KB, log scale)')
    else:
        ax.set_ylabel('Peak RSS above initial (KB)')
    
    # Set labels and title
    ax.set_xlabel('Requested Row Group Size')
    ax.set_title('Memory Usage vs Row Group Size by Dataset')
    
    # Format x-axis ticks to show powers of 2
    x_ticks = sorted(df['requested_row_group_size'].unique())
    ax.set_xticks(x_ticks)
    ax.set_xticklabels([f'{x:,}' for x in x_ticks], rotation=45)
    
    # Add grid
    ax.grid(True, alpha=0.3)
    
    # Add legend
    ax.legend()
    
    # Tight layout to prevent label cutoff
    plt.tight_layout()
    
    # Save or show the plot
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"Plot saved to: {output_path}")
    else:
        plt.show()

def main():
    parser = argparse.ArgumentParser(description="Plot memory usage results from write_memory experiment")
    parser.add_argument(
        "--input", "-i",
        default="results/write_memory.csv",
        help="Input CSV file path (default: results/write_memory.csv)"
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
    plot_memory_results(args.input, args.output, args.log_scale)
    
    return 0

if __name__ == "__main__":
    exit(main())