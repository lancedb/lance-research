#!/usr/bin/env python3
"""Compare row-, column-, and overlay-based backfill strategies in LanceDB.

Feature engineering workflows repeatedly add new derived columns to an
existing dataset: a table of base rows (here, SIFT1M vectors, standing in
for any pre-existing dataset) gets a new feature backfilled onto some
fraction of its rows. This script benchmarks three ways to write that
backfill using pylance/lancedb directly against the same base dataset:

* ``rows``   -- the only strategy most current table formats support for
  updating existing rows: a keyed MERGE (``Table.merge_insert`` with
  ``when_matched_update_all``). Because Lance stores data column-major in
  per-fragment files, updating even one column forces the *entire* matched
  row -- every existing column plus the new one -- to be rewritten. Since
  ``when_matched_update_all`` deletes and reinserts matched rows, once
  selected rows are scattered across a fragment (true for almost any
  selectivity below 100%, given enough fragments), nearly the whole
  fragment's data files must be rewritten regardless of how few rows
  actually changed.
* ``columns`` -- a Lance-native column backfill (``LanceDataset.add_columns``
  with a batch UDF). This appends a brand-new data file holding *only* the
  new column's values (real values for selected rows, nulls elsewhere) and
  never touches the bytes of existing columns. Cost scales with dataset
  size and column width, not with how scattered the selected rows are.
* ``overlay`` -- writes the backfilled values for *only* the selected rows
  into an entirely separate, small side table (id + feature), never
  touching the base dataset at write time. This is the cheapest and most
  failure-tolerant option: a partial or failed feature computation only
  ever affects its own disposable side table, and cost scales with
  selectivity alone. Reconciling the overlay back into the base table (or
  joining it at read time) is a separate, later operation not measured
  here.

Parameters swept: selectivity (fraction of rows backfilled), data width
(float32 elements per feature value), and approach (rows/columns/overlay).
Measured: wall-clock duration and bytes written to disk.

Quick smoke test:
    python attempt_1.py --rows 5000 --selectivities 0.1 1.0 \
        --widths 1 32 --trials 1 --work-dir /tmp/lance-backfill-smoke
"""

from __future__ import annotations

import argparse
import csv
import json
import platform
import shutil
import statistics
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

try:
    import lance
    import lancedb
    import numpy as np
    import pyarrow as pa
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependencies. Install them with:\n"
        "  python -m pip install -r requirements.txt\n"
        f"Original error: {exc}"
    ) from exc


EXPERIMENT_DIR = Path(__file__).resolve().parent
DEFAULT_SIFT_BASE = (
    EXPERIMENT_DIR.parents[1] / "vector search" / "sift" / "sift_base.fvecs"
)
BASE_TABLE_NAME = "backfill_base"
ID_COLUMN = "id"
VECTOR_COLUMN = "vector"
FEATURE_COLUMN = "feature"
VECTOR_DIMENSION = 128
APPROACHES = ("rows", "columns", "overlay")
RESULT_FIELDS = (
    "approach",
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


@dataclass(frozen=True)
class BackfillResult:
    approach: str
    selectivity: float
    width_floats: int
    width_bytes: int
    trial: int
    base_rows: int
    selected_rows: int
    wall_seconds: float
    bytes_written: int
    selected_rows_per_second: float
    bytes_per_second: float
    seed: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--sift-base", type=Path, default=DEFAULT_SIFT_BASE)
    parser.add_argument(
        "--rows",
        type=int,
        default=100_000,
        help="Rows of SIFT1M base vectors to load as the pre-existing dataset.",
    )
    parser.add_argument("--write-batch-rows", type=int, default=8192)
    parser.add_argument(
        "--selectivities",
        type=float,
        nargs="+",
        default=[0.01, 0.05, 0.25, 1.0],
        help="Fractions of rows to backfill a value into (0, 1].",
    )
    parser.add_argument(
        "--widths",
        type=int,
        nargs="+",
        default=[1, 32, 256],
        help="Feature width in float32 elements (4/128/1024 bytes by default).",
    )
    parser.add_argument(
        "--approaches",
        choices=APPROACHES,
        nargs="+",
        default=list(APPROACHES),
    )
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--seed", type=int, default=20250817)
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=EXPERIMENT_DIR / "work_lancedb",
        help="Scratch directory for the base dataset and per-trial copies.",
    )
    parser.add_argument("--output-dir", type=Path, default=EXPERIMENT_DIR / "results")
    parser.add_argument(
        "--overwrite", action="store_true", help="Rebuild the base dataset."
    )
    parser.add_argument(
        "--keep-work",
        action="store_true",
        help="Do not delete the work directory when the run finishes.",
    )
    args = parser.parse_args()
    if args.rows <= 0 or args.write_batch_rows <= 0 or args.trials <= 0:
        parser.error("--rows, --write-batch-rows, and --trials must be positive")
    for selectivity in args.selectivities:
        if not (0 < selectivity <= 1):
            parser.error("--selectivities values must be in (0, 1]")
    for width in args.widths:
        if width <= 0:
            parser.error("--widths values must be positive")
    return args


def load_sift_vectors(path: Path) -> np.ndarray:
    if not path.is_file():
        raise FileNotFoundError(f"SIFT1M base vectors not found at {path}")
    record_bytes = 4 * (VECTOR_DIMENSION + 1)
    if path.stat().st_size % record_bytes:
        raise ValueError(f"{path} is not a valid {VECTOR_DIMENSION}-D fvecs file")
    rows = path.stat().st_size // record_bytes
    records = np.memmap(
        path, dtype=np.float32, mode="r", shape=(rows, VECTOR_DIMENSION + 1)
    )
    if not np.all(np.asarray(records[:, 0]).view(np.int32) == VECTOR_DIMENSION):
        raise ValueError(f"Not every record in {path} has dimension {VECTOR_DIMENSION}")
    return records[:, 1:]


def base_schema() -> pa.Schema:
    return pa.schema(
        [(ID_COLUMN, pa.uint64()), (VECTOR_COLUMN, pa.list_(pa.float32(), VECTOR_DIMENSION))]
    )


def base_batches(vectors: np.ndarray, rows: int, batch_rows: int) -> Iterator[pa.RecordBatch]:
    schema = base_schema()
    for start in range(0, rows, batch_rows):
        stop = min(start + batch_rows, rows)
        contiguous = np.ascontiguousarray(vectors[start:stop], dtype=np.float32)
        vector_array = pa.FixedSizeListArray.from_arrays(
            pa.array(contiguous.reshape(-1), type=pa.float32()), VECTOR_DIMENSION
        )
        yield pa.RecordBatch.from_arrays(
            [pa.array(np.arange(start, stop, dtype=np.uint64)), vector_array],
            schema=schema,
        )


def directory_bytes(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def feature_type(width_floats: int) -> pa.DataType:
    return pa.list_(pa.float32(), width_floats)


def feature_array(values: np.ndarray, width_floats: int) -> pa.Array:
    flat = pa.array(np.ascontiguousarray(values, dtype=np.float32).reshape(-1), type=pa.float32())
    return pa.FixedSizeListArray.from_arrays(flat, width_floats)


def build_base_dataset(
    vectors: np.ndarray, rows: int, batch_rows: int, base_dir: Path
) -> int:
    if rows > len(vectors):
        raise ValueError(f"Requested {rows:,} rows, but SIFT contains {len(vectors):,}.")
    base_dir.parent.mkdir(parents=True, exist_ok=True)
    reader = pa.RecordBatchReader.from_batches(
        base_schema(), base_batches(vectors, rows, batch_rows)
    )
    db = lancedb.connect(base_dir)
    table = db.create_table(BASE_TABLE_NAME, data=reader, mode="overwrite")
    actual_rows = table.count_rows()
    if actual_rows != rows:
        raise RuntimeError(f"wrote {actual_rows:,} rows; expected {rows:,}")
    return actual_rows


def select_row_ids(base_rows: int, selectivity: float, seed_key: tuple) -> np.ndarray:
    rng = np.random.default_rng(list(seed_key))
    count = max(1, round(base_rows * selectivity))
    return np.sort(rng.choice(base_rows, size=count, replace=False)).astype(np.uint64)


def run_rows_trial(
    base_dir: Path,
    scratch_dir: Path,
    width_floats: int,
    selected_ids: np.ndarray,
    values: np.ndarray,
) -> tuple[float, int]:
    trial_dir = scratch_dir / "rows_trial"
    shutil.rmtree(trial_dir, ignore_errors=True)
    shutil.copytree(base_dir, trial_dir)
    try:
        db = lancedb.connect(trial_dir)
        table = db.open_table(BASE_TABLE_NAME)
        pre_bytes = directory_bytes(trial_dir)
        source = pa.table(
            {
                ID_COLUMN: pa.array(selected_ids, type=pa.uint64()),
                FEATURE_COLUMN: feature_array(values, width_floats),
            }
        )
        started = time.perf_counter_ns()
        # Metadata-only schema evolution: the column must exist before a
        # keyed MERGE can update it. This step writes no row data.
        table.add_columns(pa.field(FEATURE_COLUMN, feature_type(width_floats)))
        table.merge_insert(ID_COLUMN).when_matched_update_all().execute(source)
        elapsed = (time.perf_counter_ns() - started) / 1e9
        bytes_written = directory_bytes(trial_dir) - pre_bytes
        return elapsed, bytes_written
    finally:
        shutil.rmtree(trial_dir, ignore_errors=True)


def run_columns_trial(
    base_dir: Path,
    scratch_dir: Path,
    base_rows: int,
    width_floats: int,
    selected_ids: np.ndarray,
    values: np.ndarray,
    write_batch_rows: int,
) -> tuple[float, int]:
    trial_dir = scratch_dir / "columns_trial"
    shutil.rmtree(trial_dir, ignore_errors=True)
    shutil.copytree(base_dir, trial_dir)
    try:
        db = lancedb.connect(trial_dir)
        table = db.open_table(BASE_TABLE_NAME)
        pre_bytes = directory_bytes(trial_dir)

        value_lookup = np.zeros((base_rows, width_floats), dtype=np.float32)
        value_lookup[selected_ids] = values
        mask_lookup = np.zeros(base_rows, dtype=bool)
        mask_lookup[selected_ids] = True
        output_schema = pa.schema([pa.field(FEATURE_COLUMN, feature_type(width_floats))])

        @lance.batch_udf(output_schema=output_schema)
        def compute_feature(batch: pa.RecordBatch) -> pa.RecordBatch:
            ids = batch[ID_COLUMN].to_numpy()
            batch_values = value_lookup[ids]
            batch_mask = mask_lookup[ids]
            rows = [
                row.tolist() if keep else None
                for row, keep in zip(batch_values, batch_mask)
            ]
            array = pa.array(rows, type=feature_type(width_floats))
            return pa.RecordBatch.from_arrays([array], names=[FEATURE_COLUMN])

        started = time.perf_counter_ns()
        table.to_lance().add_columns(
            compute_feature, read_columns=[ID_COLUMN], batch_size=write_batch_rows
        )
        elapsed = (time.perf_counter_ns() - started) / 1e9
        bytes_written = directory_bytes(trial_dir) - pre_bytes
        return elapsed, bytes_written
    finally:
        shutil.rmtree(trial_dir, ignore_errors=True)


def run_overlay_trial(
    scratch_dir: Path,
    width_floats: int,
    selected_ids: np.ndarray,
    values: np.ndarray,
) -> tuple[float, int]:
    overlay_dir = scratch_dir / "overlay_trial"
    shutil.rmtree(overlay_dir, ignore_errors=True)
    try:
        source = pa.table(
            {
                ID_COLUMN: pa.array(selected_ids, type=pa.uint64()),
                FEATURE_COLUMN: feature_array(values, width_floats),
            }
        )
        started = time.perf_counter_ns()
        db = lancedb.connect(overlay_dir)
        db.create_table("overlay", data=source, mode="create")
        elapsed = (time.perf_counter_ns() - started) / 1e9
        bytes_written = directory_bytes(overlay_dir)
        return elapsed, bytes_written
    finally:
        shutil.rmtree(overlay_dir, ignore_errors=True)


def run_all_trials(args: argparse.Namespace, base_dir: Path, base_rows: int) -> list[BackfillResult]:
    scratch_dir = args.work_dir / "scratch"
    scratch_dir.mkdir(parents=True, exist_ok=True)
    results: list[BackfillResult] = []
    for trial in range(args.trials):
        for selectivity in args.selectivities:
            id_seed = (args.seed, trial, round(selectivity * 1_000_000))
            selected_ids = select_row_ids(base_rows, selectivity, id_seed)
            for width in args.widths:
                value_seed = id_seed + (width,)
                rng = np.random.default_rng(list(value_seed))
                values = rng.standard_normal((len(selected_ids), width)).astype(np.float32)
                for approach in args.approaches:
                    if approach == "rows":
                        elapsed, bytes_written = run_rows_trial(
                            base_dir, scratch_dir, width, selected_ids, values
                        )
                    elif approach == "columns":
                        elapsed, bytes_written = run_columns_trial(
                            base_dir,
                            scratch_dir,
                            base_rows,
                            width,
                            selected_ids,
                            values,
                            args.write_batch_rows,
                        )
                    else:
                        elapsed, bytes_written = run_overlay_trial(
                            scratch_dir, width, selected_ids, values
                        )
                    result = BackfillResult(
                        approach=approach,
                        selectivity=selectivity,
                        width_floats=width,
                        width_bytes=width * 4,
                        trial=trial + 1,
                        base_rows=base_rows,
                        selected_rows=len(selected_ids),
                        wall_seconds=elapsed,
                        bytes_written=bytes_written,
                        selected_rows_per_second=len(selected_ids) / elapsed if elapsed > 0 else float("inf"),
                        bytes_per_second=bytes_written / elapsed if elapsed > 0 else float("inf"),
                        seed=args.seed,
                    )
                    results.append(result)
                    print(
                        f"[{approach:7s}] selectivity={selectivity:<6g} "
                        f"width_bytes={result.width_bytes:<5d} trial={result.trial} "
                        f"-> {result.wall_seconds:.4f}s, {result.bytes_written:,} bytes",
                        flush=True,
                    )
    return results


def summarize(results: list[BackfillResult]) -> dict[str, object]:
    summary: dict[str, object] = {}
    keys = sorted(
        {(r.approach, r.selectivity, r.width_bytes) for r in results}
    )
    for approach, selectivity, width_bytes in keys:
        matching = [
            r
            for r in results
            if r.approach == approach and r.selectivity == selectivity and r.width_bytes == width_bytes
        ]
        durations = [r.wall_seconds for r in matching]
        byte_counts = [r.bytes_written for r in matching]
        summary[f"{approach}|selectivity={selectivity}|width_bytes={width_bytes}"] = {
            "trials": len(matching),
            "median_wall_seconds": statistics.median(durations),
            "median_bytes_written": statistics.median(byte_counts),
        }
    return summary


def write_outputs(
    args: argparse.Namespace, base_rows: int, results: list[BackfillResult]
) -> None:
    args.output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = args.output_dir / "backfill_results.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=RESULT_FIELDS)
        writer.writeheader()
        writer.writerows(asdict(result) for result in results)

    metadata = {
        "experiment": "Backfill approaches comparison: rows vs columns vs overlay",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "dataset": {
            "name": "SIFT1M",
            "subset": "base",
            "source_path": str(args.sift_base),
            "rows": base_rows,
            "dimensions": VECTOR_DIMENSION,
            "value_type": "float32",
        },
        "parameters": {
            "selectivities": args.selectivities,
            "widths_floats": args.widths,
            "widths_bytes": [w * 4 for w in args.widths],
            "approaches": args.approaches,
            "trials": args.trials,
            "seed": args.seed,
            "write_batch_rows": args.write_batch_rows,
        },
        "method": {
            "rows": (
                "lancedb Table.add_columns() adds a null feature column "
                "(metadata-only), then Table.merge_insert(id)."
                "when_matched_update_all() upserts the selected rows, forcing "
                "a full rewrite of every matched row's existing columns."
            ),
            "columns": (
                "LanceDataset.add_columns() with a batch UDF appends only "
                "the new feature column's data files; existing columns are "
                "untouched."
            ),
            "overlay": (
                "A standalone side table holding only (id, feature) for the "
                "selected rows is written; the base dataset is never opened "
                "for writing."
            ),
        },
        "measurement": {
            "bytes_written": (
                "Delta in on-disk directory size across the operation "
                "(overlay: absolute size of the new side table)."
            ),
            "wall_seconds": "perf_counter wall-clock time of the write operation only.",
        },
        "summary": summarize(results),
        "software": {
            "python": platform.python_version(),
            "lancedb": getattr(lancedb, "__version__", "unknown"),
            "pylance": getattr(lance, "__version__", "unknown"),
            "pyarrow": pa.__version__,
            "numpy": np.__version__,
        },
        "system": {"platform": platform.platform(), "machine": platform.machine()},
    }
    metadata_path = args.output_dir / "metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    for path in (csv_path, metadata_path):
        print(f"Wrote {path}")


def main() -> None:
    args = parse_args()
    args.sift_base = args.sift_base.expanduser().resolve()
    args.work_dir = args.work_dir.expanduser().resolve()
    args.output_dir = args.output_dir.expanduser().resolve()

    base_dir = args.work_dir / "base"
    if args.overwrite or not base_dir.exists():
        vectors = load_sift_vectors(args.sift_base)
        base_rows = build_base_dataset(vectors, args.rows, args.write_batch_rows, base_dir)
        del vectors
    else:
        db = lancedb.connect(base_dir)
        base_rows = db.open_table(BASE_TABLE_NAME).count_rows()
        if base_rows != args.rows:
            raise SystemExit(
                f"Existing base dataset has {base_rows:,} rows, but --rows "
                f"asked for {args.rows:,}. Pass --overwrite to rebuild."
            )
    print(f"Base dataset: {base_rows:,} rows at {base_dir}")

    results = run_all_trials(args, base_dir, base_rows)
    write_outputs(args, base_rows, results)

    if not args.keep_work:
        shutil.rmtree(args.work_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
