#!/usr/bin/env python3
"""Plot the local-NVMe versus AWS-S3 shuffled-read comparison."""

from __future__ import annotations

import argparse
import csv
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
        default=SCRIPT_DIR / "plots",
        help="PNG directory (default: plots/ beside this script).",
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
            "Run shuffled_read.py again."
        )
    numeric = (
        "trial",
        "rows",
        "value_bytes",
        "take_size",
        "seed",
        "wall_seconds",
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
    figure, axis = plt.subplots(figsize=(10, 5))
    all_trials: set[int] = set()
    for backend, rows in groups.items():
        trials = [int(float(row["trial"])) for row in rows]
        all_trials.update(trials)
        label = BACKEND_LABELS.get(backend, backend)
        color = BACKEND_COLORS.get(backend)
        axis.plot(
            trials,
            [float(row["payload_mb_per_second"]) for row in rows],
            marker="o",
            linewidth=2,
            color=color,
            label=label,
        )
    axis.set_title("SIFT1M one-phase shuffled-read throughput")
    axis.set_xlabel("Trial")
    axis.set_ylabel("Payload MB / second")
    axis.set_xticks(sorted(all_trials))
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
    figure, axis = plt.subplots(figsize=(6, 5))
    bars = axis.bar(
        labels,
        [median_for(groups[backend], "payload_mb_per_second") for backend in backends],
        color=colors,
    )
    axis.bar_label(bars, fmt="{:,.1f}", padding=3)
    axis.set_title("SIFT1M shuffled read: local NVMe vs. AWS S3")
    axis.set_ylabel("Median payload MB / second")
    style_axis(axis)
    figure.tight_layout()
    figure.savefig(output, dpi=180)
    plt.close(figure)


def main() -> None:
    args = parse_args()
    results_path = args.results.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    groups = group_trials(read_trials(results_path))

    outputs = [
        output_dir / "shuffled_read_trials_by_backend.png",
        output_dir / "shuffled_read_backend_comparison.png",
    ]
    plot_read_comparison(groups, outputs[0])
    plot_median_comparison(groups, outputs[1])

    for output in outputs:
        print(f"Wrote {output}")


if __name__ == "__main__":
    main()
