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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", nargs="?", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Defaults to the directory containing results.csv.",
    )
    return parser.parse_args()


def summarize(results: pd.DataFrame) -> pd.DataFrame:
    return (
        results.groupby("segment_count", sort=True)
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
            total_ivf_partitions=("total_ivf_partitions", "first"),
        )
        .reset_index()
    )


def finish(figure, axis, output: Path) -> None:
    axis.grid(alpha=0.22)
    axis.legend()
    figure.tight_layout()
    figure.savefig(output, dpi=160)
    plt.close(figure)


def save_latency(summary: pd.DataFrame, output: Path) -> None:
    figure, axis = plt.subplots(figsize=(10, 6))
    for column, label, color, marker in (
        ("latency_p50_ms", "p50", COLOR, "o"),
        ("latency_p95_ms", "p95", ACCENT, "s"),
        ("latency_p99_ms", "p99", "#6f2dbd", "^"),
    ):
        axis.plot(
            summary["segment_count"],
            summary[column],
            linewidth=2,
            marker=marker,
            color=color,
            label=label,
        )
    axis.set_title("ANN latency vs. physical index segments")
    axis.set_xlabel("Number of index segments")
    axis.set_ylabel("ANN query latency (ms)")
    axis.set_xticks(summary["segment_count"])
    finish(figure, axis, output)


def save_latency_distribution(results: pd.DataFrame, output: Path) -> None:
    segment_counts = sorted(results["segment_count"].unique())
    samples = [
        results.loc[results["segment_count"] == count, "ann_query_ms"].to_numpy()
        for count in segment_counts
    ]
    figure, axis = plt.subplots(figsize=(10, 6))
    boxes = axis.boxplot(samples, tick_labels=segment_counts, showfliers=False, patch_artist=True)
    for box in boxes["boxes"]:
        box.set_facecolor(COLOR)
        box.set_alpha(0.45)
    axis.set_title("ANN latency distribution by segment count")
    axis.set_xlabel("Number of index segments")
    axis.set_ylabel("ANN query latency (ms)")
    axis.grid(axis="y", alpha=0.22)
    figure.tight_layout()
    figure.savefig(output, dpi=160)
    plt.close(figure)


def save_recall(summary: pd.DataFrame, output: Path, recall_k: int) -> None:
    figure, axis = plt.subplots(figsize=(10, 6))
    lower = summary["recall_mean"] - summary["recall_p05"]
    upper = summary["recall_p95"] - summary["recall_mean"]
    axis.errorbar(
        summary["segment_count"],
        summary["recall_mean"],
        yerr=np.vstack([lower.clip(lower=0), upper.clip(lower=0)]),
        linewidth=2,
        marker="o",
        capsize=5,
        color=COLOR,
        label=f"mean recall@{recall_k} (p05–p95)",
    )
    axis.set_title(f"Recall@{recall_k} vs. physical index segments")
    axis.set_xlabel("Number of index segments")
    axis.set_ylabel(f"Recall@{recall_k}")
    axis.set_xticks(summary["segment_count"])
    minimum = min(float(summary["recall_p05"].min()), 1.0)
    axis.set_ylim(max(0, minimum - 0.05), 1.01)
    finish(figure, axis, output)


def save_throughput(summary: pd.DataFrame, output: Path) -> None:
    figure, axis = plt.subplots(figsize=(10, 6))
    sequential_qps = 1000 / summary["latency_mean_ms"]
    axis.plot(
        summary["segment_count"],
        sequential_qps,
        linewidth=2,
        marker="o",
        color=COLOR,
        label="sequential QPS from mean latency",
    )
    axis.set_title("Sequential ANN throughput vs. physical index segments")
    axis.set_xlabel("Number of index segments")
    axis.set_ylabel("Queries per second")
    axis.set_xticks(summary["segment_count"])
    finish(figure, axis, output)


def save_build_cost(summary: pd.DataFrame, output: Path) -> None:
    figure, axis = plt.subplots(figsize=(10, 6))
    axis.plot(
        summary["segment_count"],
        summary["build_wall_seconds"],
        linewidth=2,
        marker="o",
        color=COLOR,
        label="wall time (segments built sequentially)",
    )
    axis.plot(
        summary["segment_count"],
        summary["build_cpu_seconds"],
        linewidth=2,
        marker="s",
        color=ACCENT,
        label="process CPU time",
    )
    axis.set_title("Total index-build cost vs. physical index segments")
    axis.set_xlabel("Number of index segments")
    axis.set_ylabel("Build time (seconds)")
    axis.set_xticks(summary["segment_count"])
    finish(figure, axis, output)


def save_tradeoff(summary: pd.DataFrame, output: Path, recall_k: int) -> None:
    figure, axis = plt.subplots(figsize=(10, 6))
    axis.plot(
        summary["latency_p95_ms"],
        summary["recall_mean"],
        color=COLOR,
        linewidth=1.5,
        alpha=0.65,
    )
    axis.scatter(
        summary["latency_p95_ms"],
        summary["recall_mean"],
        color=COLOR,
        s=65,
        label="segment configurations",
    )
    for row in summary.itertuples():
        axis.annotate(
            f"{int(row.segment_count)} segment{'s' if row.segment_count != 1 else ''}",
            (row.latency_p95_ms, row.recall_mean),
            xytext=(6, 6),
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
        "segment_count",
        "ann_query_ms",
        "recall_at_k",
        "recall_k",
        "build_wall_seconds",
        "build_cpu_seconds",
        "total_ivf_partitions",
    }
    missing = required - set(results.columns)
    if missing:
        raise ValueError(f"Missing result columns: {', '.join(sorted(missing))}")
    output_dir = args.output_dir or args.results.parent
    output_dir.mkdir(parents=True, exist_ok=True)
    summary = summarize(results)
    recall_k = int(results["recall_k"].iloc[0])
    outputs = {
        "latency": output_dir / "latency_vs_segments.png",
        "latency_distribution": output_dir / "latency_distribution.png",
        "recall": output_dir / "recall_vs_segments.png",
        "throughput": output_dir / "query_throughput_vs_segments.png",
        "build": output_dir / "index_build_time_vs_segments.png",
        "tradeoff": output_dir / "recall_latency_tradeoff.png",
    }
    save_latency(summary, outputs["latency"])
    save_latency_distribution(results, outputs["latency_distribution"])
    save_recall(summary, outputs["recall"], recall_k)
    save_throughput(summary, outputs["throughput"])
    save_build_cost(summary, outputs["build"])
    save_tradeoff(summary, outputs["tradeoff"], recall_k)
    for output in outputs.values():
        print(f"Wrote {output}")


if __name__ == "__main__":
    main()
