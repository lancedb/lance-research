#!/usr/bin/env python3
"""
Plot I/O parallelism data from row group experiment.

This script creates a visualization showing the temporal order of disk reads
with byte ranges on the x-axis and time encoded as color.
"""

import argparse
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import numpy as np
from pathlib import Path


def load_io_data(csv_path):
    """Load and process I/O tracking data."""
    df = pd.read_csv(csv_path)

    # Filter out requests without range information (full file reads)
    df = df.dropna(subset=["range_start", "range_end"])

    # Convert timestamps to relative time (seconds from start)
    df["timestamp_ns"] = pd.to_numeric(df["timestamp_ns"])
    start_time = df["timestamp_ns"].min()
    df["relative_time"] = (df["timestamp_ns"] - start_time) / 1e9  # Convert to seconds

    # Convert duration to milliseconds
    df["duration_ns"] = pd.to_numeric(df["duration_ns"])
    df["duration_ms"] = df["duration_ns"] / 1e6  # Convert to milliseconds

    # Convert range columns to numeric
    df["range_start"] = pd.to_numeric(df["range_start"])
    df["range_end"] = pd.to_numeric(df["range_end"])

    # Calculate range size
    df["range_size"] = df["range_end"] - df["range_start"]

    # Calculate throughput in MiBps (Mebibytes per second)
    # range_size is in bytes, duration_ms is in milliseconds
    # Convert to MiBps: (bytes / (1024*1024)) / (ms / 1000)
    df["throughput_mibps"] = (df["range_size"] / (1024 * 1024)) / (df["duration_ms"] / 1000.0)

    return df


def create_io_timeline_plot(df, output_path, height_pixels=20):
    """Create a timeline visualization of I/O requests."""

    if df.empty:
        print("No I/O requests with range data found")
        return

    # Set up the figure
    fig, ax = plt.subplots(figsize=(16, 8))

    # Get file size range
    min_byte = df["range_start"].min()
    max_byte = df["range_end"].max()
    file_size = max_byte - min_byte

    print(
        f"File size range: {min_byte:,} to {max_byte:,} bytes ({file_size:,} bytes total)"
    )
    print(f"Number of I/O requests: {len(df)}")
    print(
        f"Time range: {df['relative_time'].min():.6f}s to {df['relative_time'].max():.6f}s"
    )
    print(
        f"Throughput range: {df['throughput_mibps'].min():.2f} to {df['throughput_mibps'].max():.2f} MiBps"
    )

    # Create color map based on throughput
    throughput_range = df["throughput_mibps"].max() - df["throughput_mibps"].min()
    if throughput_range == 0:
        throughput_range = 1  # Avoid division by zero

    # Calculate time range for layout purposes
    time_range = df["relative_time"].max() - df["relative_time"].min()
    if time_range == 0:
        time_range = 1  # Avoid division by zero

    # Use a colormap that shows throughput progression
    colormap = plt.cm.viridis

    # Plot each I/O request as a colored rectangle
    # Y-axis represents the time when the read occurred
    for idx, row in df.iterrows():
        start_byte = row["range_start"]
        end_byte = row["range_end"]
        time = row["relative_time"]
        throughput = row["throughput_mibps"]

        # Normalize throughput to [0, 1] for colormap
        throughput_normalized = (throughput - df["throughput_mibps"].min()) / throughput_range
        color = colormap(throughput_normalized)

        # Create rectangle for this I/O request
        # Y-position represents the time when this read started
        # Height represents the duration of the read
        y_position = time
        height = row["duration_ns"] / 1e9  # Convert duration from nanoseconds to seconds

        rect = patches.Rectangle(
            (start_byte, y_position),  # (x, y) position
            end_byte - start_byte,  # width (byte range)
            height,  # height based on duration of the read
            linewidth=0.5,
            edgecolor="black",
            facecolor=color,
            alpha=0.8,
        )
        ax.add_patch(rect)

    # Set up the plot
    ax.set_xlim(min_byte, max_byte)

    # Calculate the end time of the latest read (start time + duration)
    df["end_time"] = df["relative_time"] + (df["duration_ns"] / 1e9)
    max_end_time = df["end_time"].max()

    ax.set_ylim(
        df["relative_time"].min() - time_range * 0.05,
        max_end_time + time_range * 0.05,
    )

    # Format x-axis to show byte positions in a readable format
    ax.set_xlabel("Byte Position in File")
    ax.set_ylabel("Time (seconds from start)")
    ax.set_title(
        "I/O Request Timeline: Byte Ranges by Read Time\n(Color: Throughput from Low → High)"
    )

    # Add colorbar to show throughput mapping
    sm = plt.cm.ScalarMappable(
        cmap=colormap,
        norm=plt.Normalize(vmin=df["throughput_mibps"].min(), vmax=df["throughput_mibps"].max()),
    )
    sm.set_array([])
    cbar = plt.colorbar(sm, ax=ax)
    cbar.set_label("Throughput (MiBps)")

    # Format x-axis with readable byte labels
    def format_bytes(x, p):
        if x >= 1e6:
            return f"{x / 1e6:.1f}MB"
        elif x >= 1e3:
            return f"{x / 1e3:.1f}KB"
        else:
            return f"{int(x)}B"

    ax.xaxis.set_major_formatter(plt.FuncFormatter(format_bytes))

    # Add grid for readability
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    # Save the plot
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    print(f"Plot saved to: {output_path}")

    plt.show()


def main():
    parser = argparse.ArgumentParser(description="Plot I/O parallelism timeline")
    parser.add_argument("input_csv", help="Path to the I/O tracking CSV file")
    parser.add_argument(
        "-o",
        "--output",
        default="charts/io_parallelism_timeline.png",
        help="Output path for the plot",
    )
    parser.add_argument(
        "--height",
        type=int,
        default=20,
        help="Height of each I/O request bar in pixels",
    )

    args = parser.parse_args()

    # Load the data
    df = load_io_data(args.input_csv)

    # Create the plot
    create_io_timeline_plot(df, args.output, args.height)


if __name__ == "__main__":
    main()
