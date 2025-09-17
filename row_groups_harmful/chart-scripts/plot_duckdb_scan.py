#!/usr/bin/env python3
"""
Plot DuckDB scan benchmark results (TPC-H or FineWeb)
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import argparse
from pathlib import Path

def detect_experiment_type(df):
    """Detect if this is TPC-H or FineWeb data based on query names."""
    sample_queries = set(df['query'].unique()[:3])  # Check first few queries
    
    # TPC-H queries start with Q followed by numbers
    tpch_queries = {'Q1', 'Q3', 'Q6', 'Q12', 'S1', 'S2'}
    # FineWeb queries contain 'elephant'
    fineweb_queries = {'elephant_exact', 'elephant_case_insensitive', 'elephant_with_data'}
    
    if sample_queries & tpch_queries:
        return 'tpch'
    elif sample_queries & fineweb_queries:
        return 'fineweb'
    else:
        return 'unknown'

def plot_benchmark_results(csv_path, output_path=None, experiment_type=None, log_y=False):
    """Plot benchmark results showing query performance vs row group size."""
    
    # Read the CSV file
    df = pd.read_csv(csv_path)
    
    # Auto-detect experiment type if not provided
    if experiment_type is None:
        experiment_type = detect_experiment_type(df)
    
    # Set titles and labels based on experiment type
    if experiment_type == 'tpch':
        title = 'TPC-H Query Performance vs Parquet Row Group Size'
        ylabel = 'Normalized Query Time (1.0 = best per query)'
    elif experiment_type == 'fineweb':
        title = 'FineWeb Text Search Performance vs Parquet Row Group Size'
        ylabel = 'Normalized Search Time (1.0 = best per query)'
    else:
        title = 'DuckDB Scan Performance vs Parquet Row Group Size'
        ylabel = 'Normalized Time (1.0 = best per query)'
    
    # Create the plot
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Get unique queries and create colors based on experiment type
    queries = df['query'].unique()
    
    # Define color schemes for different experiment types
    if experiment_type == 'tpch':
        # Use shades of blue for TPC-H queries
        base_colors = plt.cm.Blues(np.linspace(0.4, 0.9, len(queries)))
    elif experiment_type == 'fineweb':
        # Use shades of green for FineWeb queries
        base_colors = plt.cm.Greens(np.linspace(0.4, 0.9, len(queries)))
    else:
        # Fallback to original colormap
        base_colors = plt.cm.Set1(range(len(queries)))
    
    colors = base_colors
    
    for i, query in enumerate(queries):
        query_data = df[df['query'] == query].sort_values('row_group_size')
        
        # Normalize times: 1.0 = best (minimum) time for this query
        min_time = query_data['avg_time_seconds'].min()
        normalized_times = query_data['avg_time_seconds'] / min_time
        
        # Apply ceiling at 5.0
        capped_times = normalized_times.copy()
        capped_times[capped_times > 5.0] = 5.0
        
        # Plot normal points (< 5.0)
        normal_mask = normalized_times <= 5.0
        if normal_mask.any():
            ax.plot(
                query_data['row_group_size'][normal_mask],
                capped_times[normal_mask],
                marker='o',
                linewidth=2,
                markersize=8,
                label=query,
                color=colors[i]
            )
        
        # Plot capped points (>= 5.0) with different marker
        capped_mask = normalized_times > 5.0
        if capped_mask.any():
            ax.plot(
                query_data['row_group_size'][capped_mask],
                capped_times[capped_mask],
                marker='^',  # Triangle marker for capped values
                linewidth=2,
                markersize=10,
                color=colors[i],
                linestyle='--',
                alpha=0.7,
                label=f'{query} (≥5.0)' if not normal_mask.any() else None
            )
        
        # Connect all points with a line
        ax.plot(
            query_data['row_group_size'],
            capped_times,
            color=colors[i],
            linewidth=1,
            alpha=0.5
        )
    
    # Set log scale for x-axis (row group size)
    ax.set_xscale('log', base=2)
    
    # Set log scale for y-axis if requested
    if log_y:
        ax.set_yscale('log')
    
    # Set labels and title
    ax.set_xlabel('Row Group Size (number of rows)')
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    
    # Format x-axis ticks
    row_group_sizes = sorted(df['row_group_size'].unique())
    ax.set_xticks(row_group_sizes)
    
    # Format x-axis labels
    def format_size(size):
        if size >= 1048576:
            return f'{size // 1048576}M'
        elif size >= 1024:
            return f'{size // 1024}K'
        else:
            return str(size)
    
    ax.set_xticklabels([format_size(x) for x in row_group_sizes], rotation=45)
    
    # Add grid
    ax.grid(True, alpha=0.3)
    
    # Set y-axis limits with some padding above 5.0
    ax.set_ylim(0.8, 5.2)
    
    # Add horizontal line at y=5 to show the ceiling
    ax.axhline(y=5.0, color='red', linestyle=':', alpha=0.5, linewidth=1)
    ax.text(ax.get_xlim()[1], 5.0, ' ≥5.0', verticalalignment='center', 
            horizontalalignment='left', color='red', fontsize=9)
    
    # Add legend
    ax.legend(loc='best')
    
    # Tight layout
    plt.tight_layout()
    
    # Save or show the plot
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"Plot saved to: {output_path}")
    else:
        plt.show()

def plot_combined_results(csv_paths, output_path=None, log_y=False):
    """Plot results from multiple CSV files on the same chart."""
    fig, ax = plt.subplots(figsize=(14, 10))
    
    all_row_group_sizes = set()
    experiment_labels = []
    
    for i, csv_path in enumerate(csv_paths):
        df = pd.read_csv(csv_path)
        experiment_type = detect_experiment_type(df)
        
        # Create experiment label
        if experiment_type == 'tpch':
            exp_label = 'TPC-H'
        elif experiment_type == 'fineweb':
            exp_label = 'FineWeb'
        else:
            exp_label = f'Exp{i+1}'
        
        experiment_labels.append(exp_label)
        
        # Get unique queries and create colors based on experiment type
        queries = df['query'].unique()
        
        # Define color schemes for different experiment types
        if experiment_type == 'tpch':
            # Use shades of blue for TPC-H queries
            colors = plt.cm.Blues(np.linspace(0.4, 0.9, len(queries)))
        elif experiment_type == 'fineweb':
            # Use shades of green for FineWeb queries
            colors = plt.cm.Greens(np.linspace(0.4, 0.9, len(queries)))
        else:
            # Fallback to original colormap with offset
            color_offset = i * len(queries)
            colors = plt.cm.Set1(range(color_offset, color_offset + len(queries)))
        
        for j, query in enumerate(queries):
            query_data = df[df['query'] == query].sort_values('row_group_size')
            all_row_group_sizes.update(query_data['row_group_size'])
            
            # Normalize times: 1.0 = best (minimum) time for this query
            min_time = query_data['avg_time_seconds'].min()
            normalized_times = query_data['avg_time_seconds'] / min_time
            
            # Apply ceiling at 5.0
            capped_times = normalized_times.copy()
            capped_times[capped_times > 5.0] = 5.0
            
            # Plot normal points (< 5.0)
            normal_mask = normalized_times <= 5.0
            capped_mask = normalized_times > 5.0
            line_style = '-' if i == 0 else '--'
            
            if normal_mask.any():
                ax.plot(
                    query_data['row_group_size'][normal_mask],
                    capped_times[normal_mask],
                    marker='o',
                    linewidth=2,
                    markersize=6,
                    label=f'{exp_label}: {query}',
                    color=colors[j],
                    linestyle=line_style
                )
            
            # Plot capped points (>= 5.0) with different marker
            if capped_mask.any():
                ax.plot(
                    query_data['row_group_size'][capped_mask],
                    capped_times[capped_mask],
                    marker='^',  # Triangle marker for capped values
                    linewidth=2,
                    markersize=8,
                    color=colors[j],
                    linestyle=line_style,
                    alpha=0.7,
                    label=f'{exp_label}: {query} (≥5.0)' if not normal_mask.any() else None
                )
            
            # Connect all points with a line
            ax.plot(
                query_data['row_group_size'],
                capped_times,
                color=colors[j],
                linewidth=1,
                linestyle=line_style,
                alpha=0.3
            )
    
    # Set log scale for x-axis
    ax.set_xscale('log', base=2)
    
    # Set log scale for y-axis if requested
    if log_y:
        ax.set_yscale('log')
    
    # Set labels and title
    ax.set_xlabel('Row Group Size (number of rows)')
    ax.set_ylabel('Normalized Time (1.0 = best per query)')
    ax.set_title(f'DuckDB Scan Performance Comparison: {" vs ".join(experiment_labels)}')
    
    # Format x-axis ticks
    row_group_sizes = sorted(all_row_group_sizes)
    ax.set_xticks(row_group_sizes)
    
    def format_size(size):
        if size >= 1048576:
            return f'{size // 1048576}M'
        elif size >= 1024:
            return f'{size // 1024}K'
        else:
            return str(size)
    
    ax.set_xticklabels([format_size(x) for x in row_group_sizes], rotation=45)
    
    # Add grid
    ax.grid(True, alpha=0.3)
    
    # Set y-axis limits with some padding above 5.0
    ax.set_ylim(0.8, 5.2)
    
    # Add horizontal line at y=5 to show the ceiling
    ax.axhline(y=5.0, color='red', linestyle=':', alpha=0.5, linewidth=1)
    ax.text(ax.get_xlim()[1], 5.0, ' ≥5.0', verticalalignment='center', 
            horizontalalignment='left', color='red', fontsize=9)
    
    # Add legend
    ax.legend(loc='best', fontsize='small')
    
    # Tight layout
    plt.tight_layout()
    
    # Save or show the plot
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"Combined plot saved to: {output_path}")
    else:
        plt.show()

def main():
    parser = argparse.ArgumentParser(description="Plot DuckDB scan benchmark results")
    parser.add_argument(
        "--input", "-i",
        nargs='+',
        required=True,
        help="Input CSV file path(s). Multiple files will be plotted together."
    )
    parser.add_argument(
        "--output", "-o",
        help="Output plot file path (if not specified, shows plot interactively)"
    )
    parser.add_argument(
        "--experiment-type",
        choices=['tpch', 'fineweb', 'auto'],
        default='auto',
        help="Experiment type (default: auto-detect)"
    )
    parser.add_argument(
        "--log-y",
        action='store_true',
        help="Use logarithmic scale for y-axis (time)"
    )
    
    args = parser.parse_args()
    
    # Check if input files exist
    csv_paths = []
    for input_path in args.input:
        if not Path(input_path).exists():
            print(f"Error: Input file '{input_path}' not found")
            return 1
        csv_paths.append(input_path)
    
    # Plot the results
    if len(csv_paths) == 1:
        experiment_type = args.experiment_type if args.experiment_type != 'auto' else None
        plot_benchmark_results(csv_paths[0], args.output, experiment_type, args.log_y)
    else:
        plot_combined_results(csv_paths, args.output, args.log_y)
    
    return 0

if __name__ == "__main__":
    exit(main())