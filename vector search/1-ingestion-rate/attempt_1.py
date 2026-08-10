"""Benchmark Lance SPFresh index maintenance on SIFT1M.

This is primarily an ingestion/index-maintenance benchmark, with one ANN query
and recall@k check after each mutation round. It creates one indexed baseline,
copies it for a fair A/B comparison, and then applies the same sequence of
inserts and vector upserts to two approaches:

* ``spfresh``: incrementally maintain the existing IVF-PQ index with
  ``dataset.optimize.optimize_indices`` (Lance's SPFresh path).
* ``reindex``: periodically replace and retrain the complete IVF-PQ index.
  Both approaches run maintenance every 30 queries and after the final query,
  so they use the same schedule and finish fully indexed.

Quick smoke test:
    python attempt_1.py --initial-rows 10000 --queries 3 \
        --append-rows 1000 --update-rows 250 --maintenance-every 2 \
        --work-dir /tmp/lance-spfresh-smoke
"""
   
from __future__ import annotations
import argparse
import csv
import json
import math
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


EXPERIMENT_DIR = Path(__file__).resolve().parent
SIFT_BASE = EXPERIMENT_DIR.parent / "sift" / "sift_base.fvecs"
DEFAULT_WORK_DIR = EXPERIMENT_DIR / "spfresh_benchmark"
VECTOR_DIMENSION = 128
INDEX_NAME = "vector_idx"
VECTOR_COLUMN = "vector"
EXTRAPOLATION_ROWS = 1_000_000_000
OUTPUT_DECIMAL_PLACES = 3
INDEX_INCORPORATION_RATE_WINDOW = 30


@dataclass
class StepResult:
    approach: str
    query_number: int
    rows_before: int
    appended_rows: int
    updated_rows: int
    rows_after: int
    mutation_seconds: float
    maintenance_action: str
    maintenance_rows_covered: int
    maintenance_seconds: float
    index_incorporation_30q_vectors_per_second: float
    total_seconds: float
    maintenance_vectors_per_second: float | None
    end_to_end_vectors_per_second: float
    extrapolated_1b_maintenance_hours: float | None
    recall_at_k: float
    exact_query_ms: float
    ann_query_ms: float
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
    parser.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR)
    parser.add_argument("--initial-rows", type=int, default=800_000)
    parser.add_argument(
        "--queries",
        type=int,
        default=150,
        help="Number of mutate-then-query rounds to run.",
    )
    parser.add_argument("--append-rows", type=int, default=1_000)
    parser.add_argument("--update-rows", type=int, default=250)
    parser.add_argument("--initial-write-batch-rows", type=int, default=100_000)
    parser.add_argument(
        "--target-partition-size",
        type=int,
        default=4096,
        help=(
            "Target rows per IVF partition used to derive num_partitions as "
            "min(num_rows / target_partition_size, sqrt(num_rows))."
        ),
    )
    parser.add_argument("--num-sub-vectors", type=int, default=16)
    parser.add_argument(
        "--maintenance-every",
        "--reindex-every",
        dest="maintenance_every",
        type=int,
        default=30,
        help=(
            "Run both SPFresh optimization and manual rebuilding every N queries. "
            "Both also run after the final query when it is off-cycle."
        ),
    )
    parser.add_argument(
        "--spfresh-merge-indices",
        type=int,
        default=1,
        help="Delta index segments to merge per SPFresh maintenance operation.",
    )
    parser.add_argument("--recall-k", type=int, default=10)
    parser.add_argument("--recall-nprobes", type=int, default=16)
    parser.add_argument("--recall-refine-factor", type=int, default=5)
    parser.add_argument(
        "--approach",
        choices=("both", "spfresh", "reindex"),
        default="both",
    )
    return parser.parse_args()

def validate_args(args: argparse.Namespace, available_rows: int) -> None:
    positive = {
        "initial_rows": args.initial_rows,
        "queries": args.queries,
        "initial_write_batch_rows": args.initial_write_batch_rows,
        "target_partition_size": args.target_partition_size,
        "num_sub_vectors": args.num_sub_vectors,
        "maintenance_every": args.maintenance_every,
        "recall_k": args.recall_k,
        "recall_nprobes": args.recall_nprobes,
        "recall_refine_factor": args.recall_refine_factor,
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
    if args.queries > args.initial_rows:
        raise ValueError("--queries cannot exceed --initial-rows")
    if VECTOR_DIMENSION % args.num_sub_vectors != 0:
        raise ValueError("SIFT's 128 dimensions must divide evenly into subvectors")

    required = args.initial_rows + args.queries * (
        args.append_rows + args.update_rows
    )
    if required > available_rows:
        raise ValueError(
            f"Workload needs {required:,} distinct source vectors, but "
            f"{args.sift_base} contains {available_rows:,}. Reduce the initial "
            "size, query count, append rows, or update rows."
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


def recommended_num_partitions(num_rows: int, target_partition_size: int) -> int:
    """Apply Lance's production partition-count recommendation."""
    return max(
        1,
        int(min(num_rows / target_partition_size, math.sqrt(num_rows))),
    )


def reset_work_dir(path: Path) -> None:
    if path.exists():
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
    num_partitions = recommended_num_partitions(
        args.initial_rows, args.target_partition_size
    )
    print(
        f"Building the common {num_partitions:,}-partition IVF-PQ baseline index "
        f"for {args.initial_rows:,} rows..."
    )
    index_start = time.perf_counter()
    dataset.create_index(
        VECTOR_COLUMN,
        index_type="IVF_PQ",
        name=INDEX_NAME,
        metric="L2",
        num_partitions=num_partitions,
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

def maintenance_is_due(query_number: int, args: argparse.Namespace) -> bool:
    """Maintain both approaches on one cadence and force a fresh final state."""
    return (
        query_number % args.maintenance_every == 0
        or query_number == args.queries
    )


def maintain_index(
    dataset_path: Path,
    approach: str,
    query_number: int,
    args: argparse.Namespace,
) -> tuple[str, float]:
    if not maintenance_is_due(query_number, args):
        return "skipped", 0.0

    dataset = lance.dataset(dataset_path)
    start = time.perf_counter()
    if approach == "spfresh":
        dataset.optimize.optimize_indices(
            index_names=[INDEX_NAME],
            num_indices_to_merge=args.spfresh_merge_indices,
        )
    else:
        num_partitions = recommended_num_partitions(
            dataset.count_rows(), args.target_partition_size
        )
        dataset.create_index(
            VECTOR_COLUMN,
            index_type="IVF_PQ",
            name=INDEX_NAME,
            metric="L2",
            replace=True,
            num_partitions=num_partitions,
            num_sub_vectors=args.num_sub_vectors,
        )
    return approach, time.perf_counter() - start


def maintenance_rows_covered(
    query_number: int,
    changed_per_query: int,
    args: argparse.Namespace,
) -> int:
    """Return mutation volume incorporated by this maintenance operation."""
    if not maintenance_is_due(query_number, args):
        return 0

    previous_maintenance_query = (
        (query_number - 1) // args.maintenance_every
    ) * args.maintenance_every
    return (query_number - previous_maintenance_query) * changed_per_query


def measure_query(
    dataset: lance.LanceDataset,
    query: np.ndarray,
    args: argparse.Namespace,
) -> tuple[float, float, float]:
    """Measure one exact query, one ANN query, and recall@k."""
    exact_start = time.perf_counter()
    exact_ids = set(
        dataset.to_table(
            nearest={
                "column": VECTOR_COLUMN,
                "q": query,
                "k": args.recall_k,
                "use_index": False,
            }
        )["id"].to_pylist()
    )
    exact_ms = (time.perf_counter() - exact_start) * 1000

    ann_start = time.perf_counter()
    ann_ids = set(
        dataset.to_table(
            nearest={
                "column": VECTOR_COLUMN,
                "q": query,
                "k": args.recall_k,
                "nprobes": args.recall_nprobes,
                "refine_factor": args.recall_refine_factor,
            }
        )["id"].to_pylist()
    )
    ann_ms = (time.perf_counter() - ann_start) * 1000
    recall = len(exact_ids & ann_ids) / len(exact_ids)
    return recall, exact_ms, ann_ms


def run_query_round(
    approach: str,
    query_number: int,
    dataset_path: Path,
    append_table: pa.Table | None,
    update_table: pa.Table | None,
    query: np.ndarray,
    args: argparse.Namespace,
) -> StepResult:
    before = lance.dataset(dataset_path).count_rows()
    mutation_seconds = apply_mutations(dataset_path, append_table, update_table)
    maintenance_action, maintenance_seconds = maintain_index(
        dataset_path, approach, query_number, args
    )
    total_seconds = mutation_seconds + maintenance_seconds
    changed = args.append_rows + args.update_rows
    covered = maintenance_rows_covered(query_number, changed, args)
    dataset = lance.dataset(dataset_path)
    state = index_state(dataset)
    recall_at_k, exact_query_ms, ann_query_ms = measure_query(dataset, query, args)
    maintenance_rate = (
        covered / maintenance_seconds if maintenance_seconds > 0 else None
    )

    result = StepResult(
        approach=approach,
        query_number=query_number,
        rows_before=before,
        appended_rows=args.append_rows,
        updated_rows=args.update_rows,
        rows_after=int(state["rows"]),
        mutation_seconds=mutation_seconds,
        maintenance_action=maintenance_action,
        maintenance_rows_covered=covered,
        maintenance_seconds=maintenance_seconds,
        index_incorporation_30q_vectors_per_second=0.0,
        total_seconds=total_seconds,
        maintenance_vectors_per_second=maintenance_rate,
        end_to_end_vectors_per_second=changed / total_seconds,
        extrapolated_1b_maintenance_hours=(
            EXTRAPOLATION_ROWS / maintenance_rate / 3600
            if maintenance_rate is not None
            else None
        ),
        recall_at_k=recall_at_k,
        exact_query_ms=exact_query_ms,
        ann_query_ms=ann_query_ms,
        indexed_rows=int(state["indexed_rows"]),
        unindexed_rows=int(state["unindexed_rows"]),
        index_segments=int(state["segments"]),
        index_partitions=int(state["partitions"]),
        partition_size_min=int(state["partition_min"]),
        partition_size_max=int(state["partition_max"]),
        partition_size_cv=float(state["partition_cv"]),
        fragments=int(state["fragments"]),
    )
    maintenance_summary = (
        f"{maintenance_seconds:.3f}s "
        f"for {covered:,} accumulated changes "
        f"({maintenance_rate:,.0f} vec/s)"
        if maintenance_rate is not None
        else "skipped"
    )
    print(
        f"{approach:7s} query={query_number:3d} changed={changed:,} "
        f"mutate={mutation_seconds:.3f}s maintain={maintenance_summary} "
        f"ann={ann_query_ms:.3f}ms recall@{args.recall_k}={recall_at_k:.3f} "
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

def round_output_floats(value):
    """Round floats for readable output without reducing calculation precision."""
    if isinstance(value, float):
        return round(value, OUTPUT_DECIMAL_PLACES)
    if isinstance(value, dict):
        return {key: round_output_floats(item) for key, item in value.items()}
    if isinstance(value, list):
        return [round_output_floats(item) for item in value]
    return value


def write_results(path: Path, results: list[StepResult]) -> None:
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(asdict(results[0]).keys()))
        writer.writeheader()
        writer.writerows(round_output_floats(asdict(result)) for result in results)


def update_index_incorporation_rates(results: list[StepResult]) -> None:
    """Populate the same trailing 30-query rate used by ingestion_rates.png."""
    for approach in {result.approach for result in results}:
        selected = sorted(
            (result for result in results if result.approach == approach),
            key=lambda result: result.query_number,
        )
        for index, result in enumerate(selected):
            window = selected[
                max(0, index - INDEX_INCORPORATION_RATE_WINDOW + 1) : index + 1
            ]
            covered = sum(item.maintenance_rows_covered for item in window)
            seconds = sum(item.maintenance_seconds for item in window)
            result.index_incorporation_30q_vectors_per_second = (
                covered / seconds if seconds > 0 else 0.0
            )


def write_readable_results(path: Path, results: list[StepResult]) -> None:
    """Write a compact fixed-width view of the most useful result columns."""
    columns = [
        ("query", "query_number", ">"),
        ("approach", "approach", "<"),
        ("rows", "rows_after", ">"),
        ("unindexed", "unindexed_rows", ">"),
        ("action", "maintenance_action", "<"),
        ("mutate_s", "mutation_seconds", ">"),
        ("maintain_s", "maintenance_seconds", ">"),
        (
            "index_30q_vec_s",
            "index_incorporation_30q_vectors_per_second",
            ">",
        ),
        ("ann_ms", "ann_query_ms", ">"),
        ("exact_ms", "exact_query_ms", ">"),
        ("recall", "recall_at_k", ">"),
        ("partitions", "index_partitions", ">"),
        ("segments", "index_segments", ">"),
    ]
    rows = []
    for result in results:
        values = round_output_floats(asdict(result))
        rows.append([str(values[key]) for _, key, _ in columns])

    widths = [
        max(len(heading), *(len(row[index]) for row in rows))
        for index, (heading, _, _) in enumerate(columns)
    ]
    header = "  ".join(
        f"{heading:{alignment}{widths[index]}}"
        for index, (heading, _, alignment) in enumerate(columns)
    )
    separator = "  ".join("-" * width for width in widths)
    lines = [header, separator]
    for row in rows:
        lines.append(
            "  ".join(
                f"{value:{columns[index][2]}{widths[index]}}"
                for index, value in enumerate(row)
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def print_summary(results: list[StepResult]) -> None:
    print("\nAggregate results (all query rounds):")
    for approach in sorted({result.approach for result in results}):
        selected = [result for result in results if result.approach == approach]
        changed = sum(item.appended_rows + item.updated_rows for item in selected)
        covered = sum(item.maintenance_rows_covered for item in selected)
        maintenance_seconds = sum(item.maintenance_seconds for item in selected)
        total_seconds = sum(item.total_seconds for item in selected)
        maintenance_rate = covered / maintenance_seconds
        ann_latencies = np.asarray(
            [item.ann_query_ms for item in selected], dtype=np.float64
        )
        print(
            f"  {approach:7s}: maintenance={maintenance_rate:,.0f} vec/s, "
            f"end-to-end={changed / total_seconds:,.0f} vec/s, "
            f"ANN mean={ann_latencies.mean():.3f}ms, "
            f"p95={np.percentile(ann_latencies, 95):.3f}ms, "
            f"linear 1B maintenance={EXTRAPOLATION_ROWS / maintenance_rate / 3600:,.2f}h"
        )



def main() -> None:
    args = parse_args()
    vectors = load_sift_vectors(args.sift_base)
    validate_args(args, len(vectors))
    reset_work_dir(args.work_dir)

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
        "partition_policy": (
            "max(1, int(min(num_rows / target_partition_size, sqrt(num_rows))))"
        ),
        "arguments": {
            key: str(value) if isinstance(value, Path) else value
            for key, value in vars(args).items()
        },
        "warning": "1B timing is a linear extrapolation from SIFT1M, not a 1B run.",
    }
    (args.work_dir / "metadata.json").write_text(
        json.dumps(round_output_floats(metadata), indent=2) + "\n", encoding="utf-8"
    )

    results: list[StepResult] = []
    query_indices = np.linspace(
        0,
        args.initial_rows - 1,
        num=args.queries,
        dtype=np.int64,
    )
    query_vectors = np.ascontiguousarray(vectors[query_indices], dtype=np.float32)
    append_source_start = args.initial_rows
    update_source_start = args.initial_rows + args.queries * args.append_rows

    for query_number in range(1, args.queries + 1):
        append_offset = append_source_start + (query_number - 1) * args.append_rows
        update_offset = update_source_start + (query_number - 1) * args.update_rows

        append_table = None
        if args.append_rows:
            append_table = vector_table(
                vectors[append_offset : append_offset + args.append_rows],
                np.arange(append_offset, append_offset + args.append_rows),
            )

        update_table = None
        if args.update_rows:
            # Rotate through baseline IDs so every source row has a unique key in
            # each batch. Reusing an ID in a later query is a valid later update.
            update_ids = (
                np.arange(args.update_rows, dtype=np.int64)
                + (query_number - 1) * args.update_rows
            ) % args.initial_rows
            update_table = vector_table(
                vectors[update_offset : update_offset + args.update_rows], update_ids
            )

        # Alternate execution order to reduce systematic warm-cache/order bias.
        query_order = (
            approaches if query_number % 2 else list(reversed(approaches))
        )
        for approach in query_order:
            results.append(
                run_query_round(
                    approach,
                    query_number,
                    dataset_paths[approach],
                    append_table,
                    update_table,
                    query_vectors[query_number - 1],
                    args,
                )
            )
        update_index_incorporation_rates(results)
        write_results(args.work_dir / "results.csv", results)
        write_readable_results(args.work_dir / "results_readable.txt", results)

    print_summary(results)
    print(f"\nDetailed results: {args.work_dir / 'results.csv'}")
    print(f"Readable results: {args.work_dir / 'results_readable.txt'}")
    print(f"Run metadata:     {args.work_dir / 'metadata.json'}")


if __name__ == "__main__":
    main()
