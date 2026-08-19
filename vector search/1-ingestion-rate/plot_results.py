#!/usr/bin/env python3
"""Generate performance, ingestion, freshness, and recall plots."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
# Each index type gets its own hue; spfresh/reindex are the saturated/muted
# shade of that hue so the maintenance-strategy comparison stays legible.
COLORS = {
    "ivf_pq_spfresh": "#168aad",
    "ivf_pq_reindex": "#dc2f02",
    "ivf_rq_spfresh": "#1baf7a",
    "ivf_rq_reindex": "#eda100",
    "ivf_hnsw_spfresh": "#4a3aa7",
    "ivf_hnsw_reindex": "#e87ba4",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "results",
        nargs="?",
        type=Path,
        default=SCRIPT_DIR / "spfresh_benchmark" / "results.csv",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Directory for generated PNG files (defaults to a 'plots' folder beside results.csv).",
    )
    parser.add_argument(
        "--rolling-window",
        type=int,
        default=5,
        help="Queries in rolling averages used to smooth latency and recall.",
    )
    parser.add_argument(
        "--amortization-window",
        type=int,
        default=30,
        help="Queries used to amortize periodic work and calculate ingestion rates.",
    )
    return parser.parse_args()


def approach_groups(results: pd.DataFrame):
    for approach, group in results.groupby("approach", sort=True):
        yield approach, group.sort_values("query_number")


def add_rebuild_markers(axis, results: pd.DataFrame) -> None:
    rebuilds = results.loc[
        results["maintenance_action"] == "reindex", "query_number"
    ].drop_duplicates()
    for index, query_number in enumerate(rebuilds):
        axis.axvline(
            query_number,
            color="#6c757d",
            linestyle="--",
            linewidth=1,
            alpha=0.65,
            label="manual rebuild" if index == 0 else None,
        )


def add_raw_and_rolling_series(
    axis,
    results: pd.DataFrame,
    value_column: str,
    rolling_window: int,
) -> None:
    for approach, group in approach_groups(results):
        color = COLORS.get(approach)
        axis.plot(
            group["query_number"],
            group[value_column],
            color=color,
            alpha=0.25,
            linewidth=1,
        )
        smoothed = group[value_column].rolling(
            rolling_window, center=True, min_periods=1
        ).mean()
        axis.plot(
            group["query_number"],
            smoothed,
            color=color,
            linewidth=2,
            label=f"{approach} ({rolling_window}-query average)",
        )


def finish_plot(figure, axis, output: Path) -> None:
    axis.grid(alpha=0.2)
    axis.legend()
    figure.tight_layout()
    figure.savefig(output, dpi=160)
    plt.close(figure)


def save_recall_plot(
    results: pd.DataFrame,
    output: Path,
    rolling_window: int,
    recall_k: int | None,
) -> None:
    figure, axis = plt.subplots(figsize=(11, 6))
    add_raw_and_rolling_series(axis, results, "recall_at_k", rolling_window)
    add_rebuild_markers(axis, results)
    metric = f"Recall@{recall_k}" if recall_k is not None else "Recall@k"
    axis.set_title(f"{metric} as unindexed data accumulates")
    axis.set_xlabel("Query number")
    axis.set_ylabel(metric)
    lower_bound = max(0.0, float(results["recall_at_k"].min()) - 0.03)
    axis.set_ylim(lower_bound, 1.01)
    finish_plot(figure, axis, output)


def save_amortized_cost_plot(
    results: pd.DataFrame, output: Path, window: int
) -> None:
    figure, axis = plt.subplots(figsize=(11, 6))
    for approach, group in approach_groups(results):
        round_cost_ms = (
            (group["mutation_seconds"] + group["maintenance_seconds"]) * 1000
            + group["ann_query_ms"]
        )
        amortized = round_cost_ms.rolling(window, min_periods=1).mean()
        axis.plot(
            group["query_number"],
            amortized,
            color=COLORS.get(approach),
            linewidth=2,
            label=f"{approach} ({window}-query trailing average)",
        )
    add_rebuild_markers(axis, results)
    axis.set_title("Amortized system cost per query round")
    axis.set_xlabel("Query number")
    axis.set_ylabel("Mutation + maintenance + ANN query time (ms/query)")
    finish_plot(figure, axis, output)


def rolling_rate(
    numerator: pd.Series, denominator_seconds: pd.Series, window: int
) -> pd.Series:
    numerator_sum = numerator.rolling(window, min_periods=1).sum()
    denominator_sum = denominator_seconds.rolling(window, min_periods=1).sum()
    return numerator_sum.div(denominator_sum).where(denominator_sum > 0, 0.0)


def save_ingestion_rates_plot(
    results: pd.DataFrame, output: Path, window: int
) -> None:
    figure, (storage_axis, index_axis) = plt.subplots(
        2, 1, figsize=(11, 9), sharex=True
    )
    for approach, group in approach_groups(results):
        changed = group["appended_rows"] + group["updated_rows"]
        storage_rate = rolling_rate(changed, group["mutation_seconds"], window)
        incorporation_rate = rolling_rate(
            group["maintenance_rows_covered"],
            group["maintenance_seconds"],
            window,
        )
        if (
            window == 30
            and "index_incorporation_30q_vectors_per_second" in group.columns
        ):
            incorporation_rate = group[
                "index_incorporation_30q_vectors_per_second"
            ]
        color = COLORS.get(approach)
        storage_axis.plot(
            group["query_number"],
            storage_rate,
            color=color,
            linewidth=2,
            label=f"{approach} ({window}-query rate)",
        )
        index_axis.plot(
            group["query_number"],
            incorporation_rate,
            color=color,
            linewidth=2,
            label=f"{approach} ({window}-query rate)",
        )

    add_rebuild_markers(storage_axis, results)
    add_rebuild_markers(index_axis, results)
    storage_axis.set_title("Storage mutation throughput")
    storage_axis.set_ylabel("Changed vectors / mutation second")
    index_axis.set_title("Effective index-incorporation throughput")
    index_axis.set_xlabel("Query number")
    index_axis.set_ylabel("New changes indexed / maintenance second")
    for axis in (storage_axis, index_axis):
        axis.grid(alpha=0.2)
        axis.legend()
    figure.suptitle(f"Trailing {window}-query ingestion rates", fontsize=16)
    figure.tight_layout()
    figure.savefig(output, dpi=160)
    plt.close(figure)


def save_cumulative_indexing_time_plot(
    results: pd.DataFrame, output: Path
) -> None:
    """Plot cumulative maintenance time against experiment ingestion progress."""
    figure, axis = plt.subplots(figsize=(11, 6))
    for approach, group in approach_groups(results):
        changed = group["appended_rows"] + group["updated_rows"]
        total_changed = changed.sum()
        ingestion_percent = changed.cumsum() / total_changed * 100
        cumulative_indexing_seconds = group["maintenance_seconds"].cumsum()

        # Include the experiment origin so the staircase begins at 0% / 0s.
        x = pd.concat(
            [pd.Series([0.0]), ingestion_percent.reset_index(drop=True)],
            ignore_index=True,
        )
        y = pd.concat(
            [pd.Series([0.0]), cumulative_indexing_seconds.reset_index(drop=True)],
            ignore_index=True,
        )
        axis.step(
            x,
            y,
            where="post",
            color=COLORS.get(approach),
            linewidth=2,
            label=approach,
        )

    axis.set_title("Cumulative index-maintenance time vs. rows ingested")
    axis.set_xlabel("Rows ingested (% of experiment total)")
    axis.set_ylabel("Cumulative indexing time (seconds)")
    axis.set_xlim(0, 100)
    finish_plot(figure, axis, output)


def save_partitions_plot(results: pd.DataFrame, output: Path) -> None:
    figure, axis = plt.subplots(figsize=(11, 6))
    for approach, group in approach_groups(results):
        axis.plot(
            group["query_number"],
            group["index_partitions"],
            color=COLORS.get(approach),
            linewidth=2,
            drawstyle="steps-post",
            label=approach,
        )
    add_rebuild_markers(axis, results)
    axis.set_title("IVF partition count over time")
    axis.set_xlabel("Query number")
    axis.set_ylabel("IVF partitions")
    finish_plot(figure, axis, output)


def read_recall_k(results_path: Path) -> int | None:
    metadata_path = results_path.with_name("metadata.json")
    if not metadata_path.exists():
        return None
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    return metadata.get("arguments", {}).get("recall_k")


def main() -> None:
    args = parse_args()
    if args.rolling_window <= 0 or args.amortization_window <= 0:
        raise ValueError("Plot windows must be positive")

    results = pd.read_csv(args.results).sort_values(["approach", "query_number"])
    required = {
        "approach",
        "query_number",
        "appended_rows",
        "updated_rows",
        "mutation_seconds",
        "maintenance_action",
        "maintenance_rows_covered",
        "maintenance_seconds",
        "ann_query_ms",
        "recall_at_k",
        "indexed_rows",
        "rows_after",
        "index_partitions",
    }
    missing = required - set(results.columns)
    if missing:
        raise ValueError(f"Missing result columns: {', '.join(sorted(missing))}")

    output_dir = args.output_dir or args.results.parent / "plots"
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "recall": output_dir / "recall_at_k.png",
        "amortized": output_dir / "amortized_system_cost.png",
        "ingestion": output_dir / "ingestion_rates.png",
        "cumulative_indexing": output_dir / "cumulative_indexing_time.png",
        "partitions": output_dir / "ivf_partitions.png",
    }

    save_recall_plot(
        results,
        outputs["recall"],
        args.rolling_window,
        read_recall_k(args.results),
    )
    save_amortized_cost_plot(
        results, outputs["amortized"], args.amortization_window
    )
    save_ingestion_rates_plot(
        results, outputs["ingestion"], args.amortization_window
    )
    save_cumulative_indexing_time_plot(
        results, outputs["cumulative_indexing"]
    )
    save_partitions_plot(results, outputs["partitions"])

    for output in outputs.values():
        print(f"Wrote {output}")


if __name__ == "__main__":
    main()
