#!/usr/bin/env python3
"""Plot the local-NVMe versus AWS-S3 shuffled-read comparison."""

from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import tempfile
from pathlib import Path

os.environ.setdefault(
    "MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "sift-shuffle-matplotlib")
)

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


SCRIPT_DIR = Path(__file__).resolve().parent
BACKEND_LABELS = {"local_nvme": "Local NVMe", "aws_s3": "AWS S3"}
BACKEND_COLORS = {"local_nvme": "#168aad", "aws_s3": "#dc2f02"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "results",
        nargs="?",
        type=Path,
        default=SCRIPT_DIR / "results" / "shuffled_read_results.csv",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="PNG directory; defaults beside the results CSV.",
    )
    return parser.parse_args()


def read_trials(path: Path) -> list[dict[str, str | float]]:
    if not path.is_file():
        raise SystemExit(f"Results CSV not found: {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        raw_rows = list(csv.DictReader(handle))
    if not raw_rows:
        raise SystemExit(f"Results CSV contains no trials: {path}")
    if "backend" not in raw_rows[0]:
        raise SystemExit(
            "Results predate the NVMe/S3 comparison and have no backend column. "
            "Run attempt_1.py again."
        )
    numeric = (
        "trial",
        "rows",
        "value_bytes",
        "take_size",
        "seed",
        "wall_seconds",
        "values_per_second",
        "payload_mb_per_second",
    )
    rows: list[dict[str, str | float]] = []
    for raw in raw_rows:
        row: dict[str, str | float] = {"backend": raw["backend"]}
        row.update({name: float(raw[name]) for name in numeric})
        rows.append(row)
    return rows


def group_trials(
    rows: list[dict[str, str | float]],
) -> dict[str, list[dict[str, str | float]]]:
    groups: dict[str, list[dict[str, str | float]]] = {}
    for row in rows:
        groups.setdefault(str(row["backend"]), []).append(row)
    for group in groups.values():
        group.sort(key=lambda row: float(row["trial"]))
    return groups


def style_axis(axis: plt.Axes) -> None:
    axis.grid(axis="y", alpha=0.25)
    axis.spines[["top", "right"]].set_visible(False)


def plot_read_comparison(
    groups: dict[str, list[dict[str, str | float]]], output: Path
) -> None:
    figure, (value_axis, payload_axis) = plt.subplots(2, 1, figsize=(10, 8), sharex=True)
    all_trials: set[int] = set()
    for backend, rows in groups.items():
        trials = [int(float(row["trial"])) for row in rows]
        all_trials.update(trials)
        label = BACKEND_LABELS.get(backend, backend)
        color = BACKEND_COLORS.get(backend)
        value_axis.plot(
            trials,
            [float(row["values_per_second"]) for row in rows],
            marker="o",
            linewidth=2,
            color=color,
            label=label,
        )
        payload_axis.plot(
            trials,
            [float(row["payload_mb_per_second"]) for row in rows],
            marker="o",
            linewidth=2,
            color=color,
            label=label,
        )
    value_axis.set_title("SIFT1M one-phase shuffled-read throughput")
    value_axis.set_ylabel("Values / second")
    payload_axis.set_xlabel("Trial")
    payload_axis.set_ylabel("Payload MB / second")
    payload_axis.set_xticks(sorted(all_trials))
    for axis in (value_axis, payload_axis):
        style_axis(axis)
        axis.legend()
    figure.tight_layout()
    figure.savefig(output, dpi=180)
    plt.close(figure)


def median_for(
    rows: list[dict[str, str | float]], metric: str
) -> float:
    return statistics.median(float(row[metric]) for row in rows)


def plot_median_comparison(
    groups: dict[str, list[dict[str, str | float]]], output: Path
) -> None:
    backends = list(groups)
    labels = [BACKEND_LABELS.get(backend, backend) for backend in backends]
    colors = [BACKEND_COLORS.get(backend, "#6c757d") for backend in backends]
    figure, (value_axis, payload_axis) = plt.subplots(1, 2, figsize=(11, 5))
    value_bars = value_axis.bar(
        labels,
        [median_for(groups[backend], "values_per_second") for backend in backends],
        color=colors,
    )
    value_axis.bar_label(value_bars, fmt="{:,.0f}", padding=3)
    value_axis.set_title("Median value throughput")
    value_axis.set_ylabel("Values / second")
    payload_bars = payload_axis.bar(
        labels,
        [median_for(groups[backend], "payload_mb_per_second") for backend in backends],
        color=colors,
    )
    payload_axis.bar_label(payload_bars, fmt="{:,.1f}", padding=3)
    payload_axis.set_title("Median payload throughput")
    payload_axis.set_ylabel("MB / second")
    for axis in (value_axis, payload_axis):
        style_axis(axis)
    figure.suptitle("SIFT1M shuffled read: local NVMe vs. AWS S3", fontsize=15)
    figure.tight_layout()
    figure.savefig(output, dpi=180)
    plt.close(figure)


def plot_load_vs_read(
    groups: dict[str, list[dict[str, str | float]]],
    loads: dict[str, dict[str, object]],
    output: Path,
) -> None:
    backends = [backend for backend in groups if backend in loads]
    if not backends:
        return
    categories = ["Load", "Shuffled read\n(median)"]
    x_positions = range(len(categories))
    width = 0.8 / len(backends)
    figure, (value_axis, payload_axis) = plt.subplots(1, 2, figsize=(12, 5))
    for index, backend in enumerate(backends):
        offset = (index - (len(backends) - 1) / 2) * width
        x = [position + offset for position in x_positions]
        label = BACKEND_LABELS.get(backend, backend)
        color = BACKEND_COLORS.get(backend)
        values = [
            float(loads[backend]["values_per_second"]),
            median_for(groups[backend], "values_per_second"),
        ]
        payload = [
            float(loads[backend]["payload_mb_per_second"]),
            median_for(groups[backend], "payload_mb_per_second"),
        ]
        value_axis.bar(x, values, width=width, color=color, label=label)
        payload_axis.bar(x, payload, width=width, color=color, label=label)
    for axis, title, ylabel in (
        (value_axis, "Value throughput", "Values / second"),
        (payload_axis, "Payload throughput", "MB / second"),
    ):
        axis.set_xticks(list(x_positions), categories)
        axis.set_title(title)
        axis.set_ylabel(ylabel)
        axis.legend()
        style_axis(axis)
    figure.suptitle("SIFT1M LanceDB load and shuffled-read throughput", fontsize=15)
    figure.tight_layout()
    figure.savefig(output, dpi=180)
    plt.close(figure)


def plot_sequential_vs_shuffled(
    shuffled: dict[str, list[dict[str, str | float]]],
    sequential: dict[str, list[dict[str, str | float]]],
    output: Path,
) -> None:
    backends = [backend for backend in shuffled if backend in sequential]
    if not backends:
        raise SystemExit("No backend appears in both shuffled and sequential results")
    figure, axes = plt.subplots(
        len(backends), 2, figsize=(11, 4.5 * len(backends)), squeeze=False
    )
    pattern_labels = ["Sequential read", "Shuffled read"]
    pattern_colors = ["#52b788", "#dc2f02"]
    for row_index, backend in enumerate(backends):
        for column_index, (metric, ylabel) in enumerate(
            (
                ("values_per_second", "Values / second"),
                ("payload_mb_per_second", "Payload MB / second"),
            )
        ):
            axis = axes[row_index][column_index]
            sequential_median = median_for(sequential[backend], metric)
            shuffled_median = median_for(shuffled[backend], metric)
            bars = axis.bar(
                pattern_labels,
                [sequential_median, shuffled_median],
                color=pattern_colors,
            )
            format_string = "{:,.0f}" if metric == "values_per_second" else "{:,.1f}"
            axis.bar_label(bars, fmt=format_string, padding=3)
            slowdown = sequential_median / shuffled_median
            axis.set_title(
                f"{BACKEND_LABELS.get(backend, backend)} — {slowdown:,.1f}× shuffle penalty"
            )
            axis.set_ylabel(ylabel)
            style_axis(axis)
    figure.suptitle("SIFT1M sequential versus shuffled-read throughput", fontsize=15)
    figure.tight_layout()
    figure.savefig(output, dpi=180)
    plt.close(figure)


def main() -> None:
    args = parse_args()
    results_path = args.results.expanduser().resolve()
    output_dir = (
        args.output_dir.expanduser().resolve() if args.output_dir else results_path.parent
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    groups = group_trials(read_trials(results_path))

    outputs = [
        output_dir / "shuffled_read_trials_by_backend.png",
        output_dir / "shuffled_read_backend_comparison.png",
    ]
    plot_read_comparison(groups, outputs[0])
    plot_median_comparison(groups, outputs[1])

    loads_path = results_path.parent / "load_results.json"
    if loads_path.is_file():
        loads = json.loads(loads_path.read_text(encoding="utf-8"))
        load_output = output_dir / "load_vs_shuffled_read_by_backend.png"
        plot_load_vs_read(groups, loads, load_output)
        outputs.append(load_output)
    else:
        print(f"Skipped load comparison; not found: {loads_path}")

    sequential_path = results_path.parent / "sequential_read_results.csv"
    if sequential_path.is_file():
        sequential_groups = group_trials(read_trials(sequential_path))
        pattern_output = output_dir / "sequential_vs_shuffled_by_backend.png"
        plot_sequential_vs_shuffled(groups, sequential_groups, pattern_output)
        outputs.append(pattern_output)
    else:
        print(
            "Skipped sequential-vs-shuffled comparison; not found: "
            f"{sequential_path}"
        )
    for output in outputs:
        print(f"Wrote {output}")


if __name__ == "__main__":
    main()
