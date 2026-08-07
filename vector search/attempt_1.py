"""Benchmark Lance SPFresh index maintenance on SIFT1M.

This is an ingestion/index-maintenance benchmark, not an ANN accuracy benchmark.
It creates one indexed baseline, copies it for a fair A/B comparison, and then
applies the same sequence of inserts and vector upserts to two approaches:

* ``spfresh``: incrementally maintain the existing IVF-PQ index with
  ``dataset.optimize.optimize_indices`` (Lance's SPFresh path).
* ``reindex``: replace and retrain the complete IVF-PQ index after every batch.

Quick smoke test:
    python test_search.py --initial-rows 10000 --steps 2 \
        --append-rows 1000 --update-rows 1000 --num-partitions 16 \
        --work-dir /tmp/lance-spfresh-smoke --overwrite
"""
   
from __future__ import annotations
import argparse
import csv
import json
import os
import shutil
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

# Avoid materializing IVF centroids merely to collect partition statistics.
os.environ.setdefault("LANCE_INCLUDE_VECTOR_CENTROIDS", "false")

import lance
import numpy as np
import pyarrow as pa


SIFT_BASE = Path("sift/sift_base.fvecs")
VECTOR_DIMENSION = 128
INDEX_NAME = "vector_idx"
VECTOR_COLUMN = "vector"
EXTRAPOLATION_ROWS = 1_000_000_000


@dataclass
class StepResult:
    approach: str
    step: int
    rows_before: int
    appended_rows: int
    updated_rows: int
    rows_after: int
    mutation_seconds: float
    maintenance_seconds: float
    total_seconds: float
    maintenance_vectors_per_second: float
    end_to_end_vectors_per_second: float
    extrapolated_1b_maintenance_hours: float
    indexed_rows: int
    unindexed_rows: int
    index_segments: int
    index_partitions: int
    partition_size_min: int
    partition_size_max: int
    partition_size_cv: float
    fragments: int

#not interesting. Step 0. validating sanity checks, basic setups
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sift-base", type=Path, default=SIFT_BASE)
    parser.add_argument("--work-dir", type=Path, default=Path("spfresh_benchmark"))
    parser.add_argument("--initial-rows", type=int, default=800_000)
    parser.add_argument("--steps", type=int, default=5)
    parser.add_argument("--append-rows", type=int, default=20_000)
    parser.add_argument("--update-rows", type=int, default=20_000)
    parser.add_argument("--initial-write-batch-rows", type=int, default=100_000)
    parser.add_argument("--num-partitions", type=int, default=256)
    parser.add_argument("--num-sub-vectors", type=int, default=16)
    parser.add_argument(
        "--spfresh-merge-indices",
        type=int,
        default=1,
        help="Delta index segments to merge per SPFresh maintenance operation.",
    )
    parser.add_argument(
        "--approach",
        choices=("both", "spfresh", "reindex"),
        default="both",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Remove the exact --work-dir before starting.",
    )
    return parser.parse_args()

def validate_args(args: argparse.Namespace, available_rows: int) -> None:
    positive = {
        "initial_rows": args.initial_rows,
        "steps": args.steps,
        "initial_write_batch_rows": args.initial_write_batch_rows,
        "num_partitions": args.num_partitions,
        "num_sub_vectors": args.num_sub_vectors,
    }
    for name, value in positive.items():
        if value <= 0:
            raise ValueError(f"--{name.replace('_', '-')} must be positive")
    if args.append_rows < 0 or args.update_rows < 0:
        raise ValueError("--append-rows and --update-rows cannot be negative")
    if args.append_rows + args.update_rows == 0:
        raise ValueError("At least one of --append-rows or --update-rows must be nonzero")
    if args.update_rows > args.initial_rows:
        raise ValueError("--update-rows cannot exceed --initial-rows")
    if VECTOR_DIMENSION % args.num_sub_vectors != 0:
        raise ValueError("SIFT's 128 dimensions must divide evenly into subvectors")

    required = args.initial_rows + args.steps * (
        args.append_rows + args.update_rows
    )
    if required > available_rows:
        raise ValueError(
            f"Workload needs {required:,} distinct source vectors, but "
            f"{args.sift_base} contains {available_rows:,}. Reduce the initial "
            "size, step count, append rows, or update rows."
        )

def load_sift_vectors(path: Path) -> np.ndarray:
    """Memory-map and validate an fvecs file without loading all of it into RAM."""
    if not path.exists():
        raise FileNotFoundError(f"Could not find SIFT vectors at {path}")

    record_bytes = 4 * (VECTOR_DIMENSION + 1)
    file_bytes = path.stat().st_size
    if file_bytes % record_bytes:
        raise ValueError(f"{path} is not a valid {VECTOR_DIMENSION}-D fvecs file")

    rows = file_bytes // record_bytes
    records = np.memmap(
        path, dtype=np.float32, mode="r", shape=(rows, VECTOR_DIMENSION + 1)
    )
    dimensions = np.asarray(records[:, 0]).view(np.int32)
    if not np.all(dimensions == VECTOR_DIMENSION):
        raise ValueError(f"Not every record in {path} has dimension {VECTOR_DIMENSION}")
    return records[:, 1:]

def vector_table(vectors: np.ndarray, ids: np.ndarray) -> pa.Table:
    """Create Lance's id + fixed-size-list vector schema."""
    contiguous = np.ascontiguousarray(vectors, dtype=np.float32)
    values = pa.array(contiguous.reshape(-1), type=pa.float32())
    vector_array = pa.FixedSizeListArray.from_arrays(values, VECTOR_DIMENSION)
    return pa.table(
        {
            "id": pa.array(np.asarray(ids, dtype=np.int64)),
            VECTOR_COLUMN: vector_array,
        }
    )

def reset_work_dir(path: Path, overwrite: bool) -> None:
    if path.exists():
        if not overwrite:
            raise FileExistsError(
                f"{path} already exists; choose another --work-dir or pass --overwrite"
            )
        shutil.rmtree(path)
    path.mkdir(parents=True)

#Step 1 of experiment. Set up data and stuff. Make 2 copies
def create_baseline(
    vectors: np.ndarray, args: argparse.Namespace, baseline_path: Path
) -> tuple[float, float]:
    """Write and index the common baseline; return write and index seconds."""
    print(f"Writing {args.initial_rows:,} baseline vectors in bounded batches...")
    write_start = time.perf_counter()
    dataset = None
    for start in range(0, args.initial_rows, args.initial_write_batch_rows):
        stop = min(start + args.initial_write_batch_rows, args.initial_rows)
        table = vector_table(vectors[start:stop], np.arange(start, stop))
        if dataset is None:
            dataset = lance.write_dataset(
                table,
                baseline_path,
                max_rows_per_group=8192,
                max_rows_per_file=1024 * 1024,
            )
        else:
            dataset.insert(table)
    write_seconds = time.perf_counter() - write_start

    assert dataset is not None
    print(
        f"Building the common {args.num_partitions}-partition IVF-PQ baseline index..."
    )
    index_start = time.perf_counter()
    dataset.create_index(
        VECTOR_COLUMN,
        index_type="IVF_PQ",
        name=INDEX_NAME,
        metric="L2",
        num_partitions=args.num_partitions,
        num_sub_vectors=args.num_sub_vectors,
    )
    index_seconds = time.perf_counter() - index_start
    return write_seconds, index_seconds

def selected_approaches(value: str) -> list[str]:
    return ["spfresh", "reindex"] if value == "both" else [value]

def copy_baseline(
    baseline_path: Path, work_dir: Path, approaches: Iterable[str]
) -> dict[str, Path]:
    paths = {}
    for approach in approaches:
        destination = work_dir / approach
        shutil.copytree(baseline_path, destination)
        paths[approach] = destination
    return paths

#step 2 of experiment. mutate data in successive rounds, evaluate 2 copies by diff approaches
def apply_mutations(
    dataset_path: Path, append_table: pa.Table | None, update_table: pa.Table | None
) -> float:
    """Time storage mutations separately from index maintenance."""
    start = time.perf_counter()
    dataset = lance.dataset(dataset_path)
    if append_table is not None:
        dataset.insert(append_table)
    if update_table is not None:
        # An update is represented as an upsert: Lance deletes the matched old row
        # and appends its replacement, which exercises index deletion + insertion.
        dataset = lance.dataset(dataset_path)
        (
            dataset.merge_insert("id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute(update_table)
        )
    return time.perf_counter() - start

def maintain_index(
    dataset_path: Path, approach: str, args: argparse.Namespace
) -> float:
    dataset = lance.dataset(dataset_path)
    start = time.perf_counter()
    if approach == "spfresh":
        dataset.optimize.optimize_indices(
            index_names=[INDEX_NAME],
            num_indices_to_merge=args.spfresh_merge_indices,
        )
    else:
        dataset.create_index(
            VECTOR_COLUMN,
            index_type="IVF_PQ",
            name=INDEX_NAME,
            metric="L2",
            replace=True,
            num_partitions=args.num_partitions,
            num_sub_vectors=args.num_sub_vectors,
        )
    return time.perf_counter() - start

def run_step(
    approach: str,
    step: int,
    dataset_path: Path,
    append_table: pa.Table | None,
    update_table: pa.Table | None,
    args: argparse.Namespace,
) -> StepResult:
    before = lance.dataset(dataset_path).count_rows()
    mutation_seconds = apply_mutations(dataset_path, append_table, update_table)
    maintenance_seconds = maintain_index(dataset_path, approach, args)
    total_seconds = mutation_seconds + maintenance_seconds
    changed = args.append_rows + args.update_rows
    state = index_state(lance.dataset(dataset_path))
    maintenance_rate = changed / maintenance_seconds

    result = StepResult(
        approach=approach,
        step=step,
        rows_before=before,
        appended_rows=args.append_rows,
        updated_rows=args.update_rows,
        rows_after=int(state["rows"]),
        mutation_seconds=mutation_seconds,
        maintenance_seconds=maintenance_seconds,
        total_seconds=total_seconds,
        maintenance_vectors_per_second=maintenance_rate,
        end_to_end_vectors_per_second=changed / total_seconds,
        extrapolated_1b_maintenance_hours=(
            EXTRAPOLATION_ROWS / maintenance_rate / 3600
        ),
        indexed_rows=int(state["indexed_rows"]),
        unindexed_rows=int(state["unindexed_rows"]),
        index_segments=int(state["segments"]),
        index_partitions=int(state["partitions"]),
        partition_size_min=int(state["partition_min"]),
        partition_size_max=int(state["partition_max"]),
        partition_size_cv=float(state["partition_cv"]),
        fragments=int(state["fragments"]),
    )
    print(
        f"{approach:7s} step={step} changed={changed:,} "
        f"mutate={mutation_seconds:.3f}s maintain={maintenance_seconds:.3f}s "
        f"maintenance_rate={maintenance_rate:,.0f} vec/s "
        f"indexed={result.indexed_rows:,}/{result.rows_after:,} "
        f"segments={result.index_segments} partitions={result.index_partitions}"
    )
    return result

#step 3 of experiment: measure and report
def index_state(dataset: lance.LanceDataset) -> dict[str, int | float]:
    description = next(
        item for item in dataset.describe_indices() if item.name == INDEX_NAME
    )
    stats = dataset.index_statistics(INDEX_NAME)
    segments = stats.get("indices", [])
    partition_sizes = [
        int(partition["size"])
        for segment in segments
        for partition in segment.get("partitions", [])
    ]
    indexed_rows = int(description.num_rows_indexed)
    live_rows = dataset.count_rows()
    sizes = np.asarray(partition_sizes, dtype=np.float64)
    return {
        "rows": live_rows,
        "indexed_rows": indexed_rows,
        "unindexed_rows": max(0, live_rows - indexed_rows),
        # ``IndexDescription.segments`` is available in newer pylance builds,
        # while the stable statistics dictionary exposes the count directly.
        "segments": int(stats.get("num_segments", len(segments))),
        "partitions": len(partition_sizes),
        "partition_min": int(sizes.min()) if sizes.size else 0,
        "partition_max": int(sizes.max()) if sizes.size else 0,
        "partition_cv": float(sizes.std() / sizes.mean()) if sizes.size else 0.0,
        "fragments": len(dataset.get_fragments()),
    }

def write_results(path: Path, results: list[StepResult]) -> None:
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(asdict(results[0]).keys()))
        writer.writeheader()
        writer.writerows(asdict(result) for result in results)

def print_summary(results: list[StepResult]) -> None:
    print("\nAggregate maintenance throughput (all update steps):")
    for approach in sorted({result.approach for result in results}):
        selected = [result for result in results if result.approach == approach]
        changed = sum(item.appended_rows + item.updated_rows for item in selected)
        maintenance_seconds = sum(item.maintenance_seconds for item in selected)
        total_seconds = sum(item.total_seconds for item in selected)
        print(
            f"  {approach:7s}: maintenance={changed / maintenance_seconds:,.0f} vec/s, "
            f"end-to-end={changed / total_seconds:,.0f} vec/s, "
            f"linear 1B maintenance={EXTRAPOLATION_ROWS / (changed / maintenance_seconds) / 3600:,.2f}h"
        )




def main() -> None:
    args = parse_args()
    vectors = load_sift_vectors(args.sift_base)
    validate_args(args, len(vectors))
    reset_work_dir(args.work_dir, args.overwrite)

    baseline_path = args.work_dir / "baseline"
    write_seconds, initial_index_seconds = create_baseline(vectors, args, baseline_path)
    initial_index_rate = args.initial_rows / initial_index_seconds
    print(
        f"Baseline write: {write_seconds:.3f}s; index build: {initial_index_seconds:.3f}s "
        f"({initial_index_rate:,.0f} vec/s; naive linear 1B build "
        f"{EXTRAPOLATION_ROWS / initial_index_rate / 3600:,.2f}h)"
    )

    approaches = selected_approaches(args.approach)
    dataset_paths = copy_baseline(baseline_path, args.work_dir, approaches)
    metadata = {
        "lance_version": lance.__version__,
        "source": str(args.sift_base),
        "source_rows": len(vectors),
        "vector_dimension": VECTOR_DIMENSION,
        "vector_dtype": "float32",
        "initial_write_seconds": write_seconds,
        "initial_index_seconds": initial_index_seconds,
        "initial_index_vectors_per_second": initial_index_rate,
        "arguments": {
            key: str(value) if isinstance(value, Path) else value
            for key, value in vars(args).items()
        },
        "warning": "1B timing is a linear extrapolation from SIFT1M, not a 1B run.",
    }
    (args.work_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
    )

    results: list[StepResult] = []
    append_source_start = args.initial_rows
    update_source_start = args.initial_rows + args.steps * args.append_rows

    for step in range(1, args.steps + 1):
        append_offset = append_source_start + (step - 1) * args.append_rows
        update_offset = update_source_start + (step - 1) * args.update_rows

        append_table = None
        if args.append_rows:
            append_table = vector_table(
                vectors[append_offset : append_offset + args.append_rows],
                np.arange(append_offset, append_offset + args.append_rows),
            )

        update_table = None
        if args.update_rows:
            # Rotate through baseline IDs so every source row has a unique key in
            # each batch. Reusing an ID in a later step is a valid later update.
            update_ids = (
                np.arange(args.update_rows, dtype=np.int64)
                + (step - 1) * args.update_rows
            ) % args.initial_rows
            update_table = vector_table(
                vectors[update_offset : update_offset + args.update_rows], update_ids
            )

        # Alternate execution order to reduce systematic warm-cache/order bias.
        step_order = approaches if step % 2 else list(reversed(approaches))
        for approach in step_order:
            results.append(
                run_step(
                    approach,
                    step,
                    dataset_paths[approach],
                    append_table,
                    update_table,
                    args,
                )
            )
        write_results(args.work_dir / "results.csv", results)

    print_summary(results)
    print(f"\nDetailed results: {args.work_dir / 'results.csv'}")
    print(f"Run metadata:     {args.work_dir / 'metadata.json'}")


if __name__ == "__main__":
    main()
