#!/usr/bin/env python3
"""Plot the rows/columns/overlay backfill comparison."""

from __future__ import annotations

import argparse
import csv
import os
import statistics
import tempfile
from pathlib import Path

os.environ.setdefault(
    "MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "backfill-matplotlib")
)

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


SCRIPT_DIR = Path(__file__).resolve().parent
APPROACH_ORDER = ("rows", "columns", "overlay")
APPROACH_LABELS = {"rows": "Rows (merge_insert)", "columns": "Columns (add_columns)", "overlay": "Overlay (side table)"}
APPROACH_COLORS = {"rows": "#2a78d6", "columns": "#eb6834", "overlay": "#1baf7a"}
NUMERIC_FIELDS = (
    "selectivity",
    "width_floats",
    "width_bytes",
    "trial",
    "base_rows",
    "selected_rows",
    "wall_seconds",
    "bytes_written",
    "selected_rows_per_second",
    "bytes_per_second",
    "seed",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "results",
        nargs="?",
        type=Path,
        default=SCRIPT_DIR / "results" / "backfill_results.csv",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="PNG directory; defaults beside the results CSV.",
    )
    return parser.parse_args()


def read_rows(path: Path) -> list[dict[str, object]]:
    if not path.is_file():
        raise SystemExit(f"Results CSV not found: {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        raw_rows = list(csv.DictReader(handle))
    if not raw_rows:
        raise SystemExit(f"Results CSV contains no trials: {path}")
    rows: list[dict[str, object]] = []
    for raw in raw_rows:
        row: dict[str, object] = {"approach": raw["approach"]}
        row.update({name: float(raw[name]) for name in NUMERIC_FIELDS})
        rows.append(row)
    return rows


def style_axis(axis: plt.Axes) -> None:
    axis.grid(axis="y", alpha=0.25)
    axis.spines[["top", "right"]].set_visible(False)


def group_points(
    rows: list[dict[str, object]], width_bytes: float, approach: str
) -> tuple[list[float], list[float], list[float], list[float]]:
    """Return (selectivities, medians, lo_err, hi_err) for one (width, approach)."""
    by_selectivity: dict[float, list[dict[str, object]]] = {}
    for row in rows:
        if row["width_bytes"] == width_bytes and row["approach"] == approach:
            by_selectivity.setdefault(row["selectivity"], []).append(row)
    selectivities = sorted(by_selectivity)
    return selectivities, by_selectivity


def median_series(
    rows: list[dict[str, object]], width_bytes: float, approach: str, metric: str
) -> tuple[list[float], list[float], list[float], list[float]]:
    selectivities, grouped = group_points(rows, width_bytes, approach)
    medians: list[float] = []
    lo_err: list[float] = []
    hi_err: list[float] = []
    for selectivity in selectivities:
        values = [point[metric] for point in grouped[selectivity]]
        med = statistics.median(values)
        medians.append(med)
        lo_err.append(med - min(values))
        hi_err.append(max(values) - med)
    return selectivities, medians, lo_err, hi_err


def widths_present(rows: list[dict[str, object]]) -> list[float]:
    return sorted({row["width_bytes"] for row in rows})


def approaches_present(rows: list[dict[str, object]]) -> list[str]:
    present = {row["approach"] for row in rows}
    return [approach for approach in APPROACH_ORDER if approach in present]


def plot_metric_vs_selectivity(
    rows: list[dict[str, object]],
    metric: str,
    ylabel: str,
    title: str,
    output: Path,
    log_y: bool = True,
) -> None:
    widths = widths_present(rows)
    approaches = approaches_present(rows)
    figure, axes = plt.subplots(
        1, len(widths), figsize=(5.5 * len(widths), 5), sharey=True, squeeze=False
    )
    axes = axes[0]
    for axis, width_bytes in zip(axes, widths):
        for approach in approaches:
            selectivities, medians, lo_err, hi_err = median_series(
                rows, width_bytes, approach, metric
            )
            if not selectivities:
                continue
            axis.errorbar(
                selectivities,
                medians,
                yerr=[lo_err, hi_err],
                marker="o",
                linewidth=2,
                capsize=3,
                color=APPROACH_COLORS.get(approach),
                label=APPROACH_LABELS.get(approach, approach),
            )
        axis.set_title(f"{int(width_bytes)}-byte feature")
        axis.set_xlabel("Selectivity (fraction of rows backfilled)")
        axis.set_xscale("log")
        if log_y:
            axis.set_yscale("log")
        style_axis(axis)
    axes[0].set_ylabel(ylabel)
    axes[0].legend()
    figure.suptitle(title, fontsize=15)
    figure.tight_layout()
    figure.savefig(output, dpi=180)
    plt.close(figure)


def plot_bytes_per_row(rows: list[dict[str, object]], output: Path) -> None:
    for row in rows:
        row["bytes_per_selected_row"] = (
            row["bytes_written"] / row["selected_rows"] if row["selected_rows"] else 0.0
        )
    plot_metric_vs_selectivity(
        rows,
        "bytes_per_selected_row",
        "Bytes written / backfilled row",
        "Marginal write cost per backfilled row",
        output,
    )


def plot_lowest_selectivity_summary(rows: list[dict[str, object]], output: Path) -> None:
    widths = widths_present(rows)
    approaches = approaches_present(rows)
    lowest_selectivity = min(row["selectivity"] for row in rows)
    figure, axis = plt.subplots(figsize=(2.6 * len(widths) + 2, 5.5))
    bar_width = 0.8 / len(approaches)
    x_positions = range(len(widths))
    for index, approach in enumerate(approaches):
        offset = (index - (len(approaches) - 1) / 2) * bar_width
        heights = []
        for width_bytes in widths:
            matching = [
                row["bytes_written"]
                for row in rows
                if row["approach"] == approach
                and row["width_bytes"] == width_bytes
                and row["selectivity"] == lowest_selectivity
            ]
            heights.append(statistics.median(matching) if matching else 0.0)
        bars = axis.bar(
            [position + offset for position in x_positions],
            heights,
            width=bar_width,
            color=APPROACH_COLORS.get(approach),
            label=APPROACH_LABELS.get(approach, approach),
        )
        axis.bar_label(bars, fmt=lambda v: f"{v:,.0f}", padding=3, rotation=90, fontsize=8)
    axis.set_xticks(list(x_positions), [f"{int(w)} B" for w in widths])
    axis.set_xlabel("Feature width")
    axis.set_ylabel("Bytes written")
    axis.set_yscale("log")
    axis.set_ylim(top=axis.get_ylim()[1] * 4)
    axis.set_title(
        f"Bytes written at selectivity={lowest_selectivity:g} "
        "(realistic sparse-feature backfill)"
    )
    axis.legend()
    style_axis(axis)
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
    rows = read_rows(results_path)

    outputs = [
        output_dir / "bytes_written_vs_selectivity.png",
        output_dir / "duration_vs_selectivity.png",
        output_dir / "bytes_per_row_vs_selectivity.png",
        output_dir / "lowest_selectivity_summary.png",
    ]
    plot_metric_vs_selectivity(
        rows,
        "bytes_written",
        "Bytes written (log scale)",
        "Backfill bytes written vs. selectivity, by feature width",
        outputs[0],
    )
    plot_metric_vs_selectivity(
        rows,
        "wall_seconds",
        "Wall-clock seconds (log scale)",
        "Backfill duration vs. selectivity, by feature width",
        outputs[1],
    )
    plot_bytes_per_row(rows, outputs[2])
    plot_lowest_selectivity_summary(rows, outputs[3])

    for output in outputs:
        print(f"Wrote {output}")


if __name__ == "__main__":
    main()
