#!/usr/bin/env python3
"""Plot latency, recall, throughput, and build cost by index segment count."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


EXPERIMENT_DIR = Path(__file__).resolve().parent
DEFAULT_RESULTS = EXPERIMENT_DIR / "multisegment_benchmark" / "results.csv"
COLOR = "#168aad"
ACCENT = "#dc2f02"
INDEX_COLORS = {
    "IVF_PQ": "#168aad",
    "IVF_HNSW_PQ": "#dc2f02",
    "IVF_HNSW_SQ": "#6f2dbd",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", nargs="?", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Directory for generated PNG files (defaults to a 'plots' folder beside results.csv).",
    )
    return parser.parse_args()


def summarize(results: pd.DataFrame) -> pd.DataFrame:
    return (
        results.groupby(["index_type", "segment_count"], sort=True)
        .agg(
            latency_mean_ms=("ann_query_ms", "mean"),
            latency_p50_ms=("ann_query_ms", lambda values: np.percentile(values, 50)),
            latency_p95_ms=("ann_query_ms", lambda values: np.percentile(values, 95)),
            latency_p99_ms=("ann_query_ms", lambda values: np.percentile(values, 99)),
            recall_mean=("recall_at_k", "mean"),
            recall_p05=("recall_at_k", lambda values: np.percentile(values, 5)),
            recall_p95=("recall_at_k", lambda values: np.percentile(values, 95)),
            build_wall_seconds=("build_wall_seconds", "first"),
            build_cpu_seconds=("build_cpu_seconds", "first"),
            segment_build_seconds_sum=("segment_build_seconds_sum", "first"),
            build_vectors_per_second=("build_vectors_per_second", "first"),
            rows=("rows", "first"),
            total_ivf_partitions=("total_ivf_partitions", "first"),
        )
        .reset_index()
    )


def index_groups(summary: pd.DataFrame):
    for index_type, group in summary.groupby("index_type", sort=True):
        yield index_type, group.sort_values("segment_count")


def finish(figure, axis, output: Path) -> None:
    axis.grid(alpha=0.22)
    axis.legend()
    figure.tight_layout()
    figure.savefig(output, dpi=160)
    plt.close(figure)


def save_latency_distribution(results: pd.DataFrame, output: Path) -> None:
    configurations = list(
        results[["index_type", "segment_count"]]
        .drop_duplicates()
        .sort_values(["index_type", "segment_count"])
        .itertuples(index=False, name=None)
    )
    samples = [
        results.loc[
            (results["index_type"] == index_type)
            & (results["segment_count"] == count),
            "ann_query_ms",
        ].to_numpy()
        for index_type, count in configurations
    ]
    labels = [f"{index_type}\n{count} seg" for index_type, count in configurations]
    figure, axis = plt.subplots(figsize=(13, 6))
    boxes = axis.boxplot(
        samples, tick_labels=labels, showfliers=False, patch_artist=True
    )
    for box, (index_type, _) in zip(boxes["boxes"], configurations):
        box.set_facecolor(INDEX_COLORS.get(index_type, COLOR))
        box.set_alpha(0.45)
    axis.set_title("ANN latency distributions by index type and segment count")
    axis.set_xlabel("Index type and physical segments")
    axis.set_ylabel("ANN query latency (ms)")
    axis.grid(axis="y", alpha=0.22)
    figure.tight_layout()
    figure.savefig(output, dpi=160)
    plt.close(figure)


def save_recall(summary: pd.DataFrame, output: Path, recall_k: int) -> None:
    figure, axis = plt.subplots(figsize=(10, 6))
    for index_type, group in index_groups(summary):
        axis.plot(
            group["segment_count"],
            group["recall_mean"],
            linewidth=2,
            marker="o",
            color=INDEX_COLORS.get(index_type),
            label=index_type,
        )
        axis.fill_between(
            group["segment_count"],
            group["recall_p05"],
            group["recall_p95"],
            color=INDEX_COLORS.get(index_type),
            alpha=0.12,
        )
    axis.set_title(f"Recall@{recall_k} by index type and segment count")
    axis.set_xlabel("Number of index segments")
    axis.set_ylabel(f"Recall@{recall_k}")
    axis.set_xticks(sorted(summary["segment_count"].unique()))
    minimum = min(float(summary["recall_p05"].min()), 1.0)
    axis.set_ylim(max(0, minimum - 0.05), 1.01)
    finish(figure, axis, output)


def save_throughput(summary: pd.DataFrame, output: Path) -> None:
    figure, axis = plt.subplots(figsize=(10, 6))
    for index_type, group in index_groups(summary):
        axis.plot(
            group["segment_count"],
            1000 / group["latency_mean_ms"],
            linewidth=2,
            marker="o",
            color=INDEX_COLORS.get(index_type),
            label=index_type,
        )
    axis.set_title("Sequential ANN throughput by index type and segment count")
    axis.set_xlabel("Number of index segments")
    axis.set_ylabel("Queries per second")
    axis.set_xticks(sorted(summary["segment_count"].unique()))
    finish(figure, axis, output)


def save_build_cost(summary: pd.DataFrame, output: Path) -> None:
    figure, axis = plt.subplots(figsize=(10, 6))
    for index_type, group in index_groups(summary):
        axis.plot(
            group["segment_count"],
            group["build_wall_seconds"],
            linewidth=2,
            marker="o",
            color=INDEX_COLORS.get(index_type),
            label=index_type,
        )
    axis.set_title("Concurrent build wall time by index type and segment count")
    axis.set_xlabel("Number of index segments")
    axis.set_ylabel("Build time (seconds)")
    axis.set_xticks(sorted(summary["segment_count"].unique()))
    finish(figure, axis, output)


def save_build_throughput(summary: pd.DataFrame, output: Path) -> None:
    figure, axis = plt.subplots(figsize=(10, 6))
    for index_type, group in index_groups(summary):
        axis.plot(
            group["segment_count"],
            group["build_vectors_per_second"],
            linewidth=2,
            marker="o",
            color=INDEX_COLORS.get(index_type),
            label=index_type,
        )
    axis.set_title("Scale-out build throughput by index type")
    axis.set_xlabel("Number of index segments / available workers")
    axis.set_ylabel("Vectors indexed per wall-clock second")
    axis.set_xticks(sorted(summary["segment_count"].unique()))
    finish(figure, axis, output)


def save_tradeoff(summary: pd.DataFrame, output: Path, recall_k: int) -> None:
    figure, axis = plt.subplots(figsize=(10, 6))
    for index_type, group in index_groups(summary):
        axis.plot(
            group["latency_p95_ms"],
            group["recall_mean"],
            color=INDEX_COLORS.get(index_type),
            linewidth=1.5,
            marker="o",
            label=index_type,
        )
        for row in group.itertuples():
            axis.annotate(
                f"{int(row.segment_count)}",
                (row.latency_p95_ms, row.recall_mean),
                xytext=(5, 5),
                textcoords="offset points",
            )
    axis.set_title("Recall/latency tradeoff")
    axis.set_xlabel("p95 ANN latency (ms)")
    axis.set_ylabel(f"Mean recall@{recall_k}")
    finish(figure, axis, output)


def main() -> None:
    args = parse_args()
    results = pd.read_csv(args.results)
    required = {
        "index_type",
        "segment_count",
        "ann_query_ms",
        "recall_at_k",
        "recall_k",
        "build_wall_seconds",
        "build_cpu_seconds",
        "segment_build_seconds_sum",
        "build_vectors_per_second",
        "rows",
        "total_ivf_partitions",
    }
    missing = required - set(results.columns)
    if missing:
        raise ValueError(f"Missing result columns: {', '.join(sorted(missing))}")
    output_dir = args.output_dir or args.results.parent / "plots"
    output_dir.mkdir(parents=True, exist_ok=True)
    summary = summarize(results)
    recall_k = int(results["recall_k"].iloc[0])
    outputs = {
        "latency_distribution": output_dir / "latency_distribution.png",
        "recall": output_dir / "recall_vs_segments.png",
        "throughput": output_dir / "query_throughput_vs_segments.png",
        "build": output_dir / "index_build_time_vs_segments.png",
        "build_throughput": output_dir / "index_build_throughput.png",
        "tradeoff": output_dir / "recall_latency_tradeoff.png",
    }
    save_latency_distribution(results, outputs["latency_distribution"])
    save_recall(summary, outputs["recall"], recall_k)
    save_throughput(summary, outputs["throughput"])
    save_build_cost(summary, outputs["build"])
    save_build_throughput(summary, outputs["build_throughput"])
    save_tradeoff(summary, outputs["tradeoff"], recall_k)
    for output in outputs.values():
        print(f"Wrote {output}")


if __name__ == "__main__":
    main()
