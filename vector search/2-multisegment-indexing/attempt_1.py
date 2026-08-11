#!/usr/bin/env python3
"""Benchmark one logical Lance IVF-PQ index with 1 through 5 segments.

The dataset and query workload stay fixed.  For each requested segment count,
the script divides the dataset's immutable fragments into disjoint groups,
builds one independently trained IVF-PQ segment per group with Lance's
distributed-index API, commits those physical segments as one logical index,
and runs the same query/repetition matrix.

The bundled SIFT1M data is a practical local-scale default.  A genuine 5B run
requires a 5B-vector fvecs file plus compatible query and ground-truth files.

Quick smoke test:
    python attempt_1.py --rows 10000 --segment-counts 1,2 \
        --data-fragments 4 --queries 3 --warmup-queries 1 --repetitions 2 \
        --ground-truth-mode exact --work-dir /tmp/lance-multisegment-smoke
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import platform
import shutil
import time
from dataclasses import asdict, dataclass
from pathlib import Path

# Avoid materializing IVF centroids while collecting index statistics.
os.environ.setdefault("LANCE_INCLUDE_VECTOR_CENTROIDS", "false")

import lance
import numpy as np
import pyarrow as pa


EXPERIMENT_DIR = Path(__file__).resolve().parent
SIFT_DIR = EXPERIMENT_DIR.parent / "sift"
DEFAULT_BASE = SIFT_DIR / "sift_base.fvecs"
DEFAULT_QUERIES = SIFT_DIR / "sift_query.fvecs"
DEFAULT_GROUND_TRUTH = SIFT_DIR / "sift_groundtruth.ivecs"
DEFAULT_WORK_DIR = EXPERIMENT_DIR / "multisegment_benchmark"
VECTOR_COLUMN = "vector"
ID_COLUMN = "id"
INDEX_NAME = "vector_idx"
OUTPUT_DECIMAL_PLACES = 3


@dataclass
class QueryResult:
    segment_count: int
    actual_segment_count: int
    query_number: int
    repetition: int
    rows: int
    data_fragments: int
    rows_per_segment_min: int
    rows_per_segment_max: int
    total_ivf_partitions: int
    build_wall_seconds: float
    build_cpu_seconds: float
    recall_k: int
    recall_at_k: float
    precision_at_k: float
    ann_query_ms: float
    query_throughput_qps: float
    nprobes: int
    refine_factor: int


def parse_segment_counts(value: str) -> list[int]:
    try:
        counts = [int(item.strip()) for item in value.split(",") if item.strip()]
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "segment counts must be comma-separated integers"
        ) from error
    if not counts or any(count <= 0 for count in counts):
        raise argparse.ArgumentTypeError("segment counts must be positive")
    if len(counts) != len(set(counts)):
        raise argparse.ArgumentTypeError("segment counts cannot contain duplicates")
    return counts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-vectors", type=Path, default=DEFAULT_BASE)
    parser.add_argument("--query-vectors", type=Path, default=DEFAULT_QUERIES)
    parser.add_argument("--ground-truth", type=Path, default=DEFAULT_GROUND_TRUTH)
    parser.add_argument(
        "--ground-truth-mode",
        choices=("auto", "file", "exact"),
        default="auto",
        help=(
            "auto uses the ivecs file only for a full input dataset; file always "
            "uses it; exact computes brute-force neighbors before indexing."
        ),
    )
    parser.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR)
    parser.add_argument(
        "--rows",
        type=int,
        default=1_000_000,
        help="Fixed dataset scale. The bundled SIFT input supports up to 1M rows.",
    )
    parser.add_argument(
        "--segment-counts",
        type=parse_segment_counts,
        default=parse_segment_counts("1,2,3,4,5"),
    )
    parser.add_argument(
        "--data-fragments",
        type=int,
        default=60,
        help="Target number of similarly sized data fragments to group into segments.",
    )
    parser.add_argument("--write-batch-rows", type=int, default=100_000)
    parser.add_argument("--target-partition-size", type=int, default=4096)
    parser.add_argument("--num-sub-vectors", type=int, default=16)
    parser.add_argument("--queries", type=int, default=100)
    parser.add_argument("--warmup-queries", type=int, default=10)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--recall-k", type=int, default=10)
    parser.add_argument("--nprobes", type=int, default=16)
    parser.add_argument("--refine-factor", type=int, default=5)
    return parser.parse_args()


def read_vector_file(path: Path, scalar_type: np.dtype) -> np.ndarray:
    """Memory-map an fvecs/ivecs file and return its payload columns."""
    if not path.exists():
        raise FileNotFoundError(path)
    header = np.memmap(path, dtype=np.int32, mode="r", shape=(1,))
    width = int(header[0])
    if width <= 0:
        raise ValueError(f"Invalid vector width in {path}")
    record_bytes = np.dtype(scalar_type).itemsize * (width + 1)
    if path.stat().st_size % record_bytes:
        raise ValueError(f"Invalid vector file size for {path}")
    row_count = path.stat().st_size // record_bytes
    records = np.memmap(path, dtype=scalar_type, mode="r", shape=(row_count, width + 1))
    dimensions = np.asarray(records[:, 0]).view(np.int32)
    if not np.all(dimensions == width):
        raise ValueError(f"Inconsistent vector widths in {path}")
    return records[:, 1:]


def validate_args(
    args: argparse.Namespace,
    base_vectors: np.ndarray,
    query_vectors: np.ndarray,
) -> None:
    positive = {
        "rows": args.rows,
        "data_fragments": args.data_fragments,
        "write_batch_rows": args.write_batch_rows,
        "target_partition_size": args.target_partition_size,
        "num_sub_vectors": args.num_sub_vectors,
        "queries": args.queries,
        "repetitions": args.repetitions,
        "recall_k": args.recall_k,
        "nprobes": args.nprobes,
        "refine_factor": args.refine_factor,
    }
    for name, value in positive.items():
        if value <= 0:
            raise ValueError(f"--{name.replace('_', '-')} must be positive")
    if args.warmup_queries < 0:
        raise ValueError("--warmup-queries cannot be negative")
    if args.rows > len(base_vectors):
        raise ValueError(
            f"Requested {args.rows:,} rows but {args.base_vectors} has only "
            f"{len(base_vectors):,}. A 5B run needs an external 5B-vector file."
        )
    required_queries = max(args.queries, args.warmup_queries)
    if required_queries > len(query_vectors):
        raise ValueError(
            f"Need {required_queries:,} query vectors but only have "
            f"{len(query_vectors):,}"
        )
    if args.data_fragments < max(args.segment_counts):
        raise ValueError("--data-fragments must be at least the largest segment count")
    if base_vectors.shape[1] != query_vectors.shape[1]:
        raise ValueError("Base and query vector dimensions differ")
    if base_vectors.shape[1] % args.num_sub_vectors:
        raise ValueError("Vector dimension must divide evenly into --num-sub-vectors")


def vector_record_batch(vectors: np.ndarray, start_id: int) -> pa.RecordBatch:
    contiguous = np.ascontiguousarray(vectors, dtype=np.float32)
    values = pa.array(contiguous.reshape(-1), type=pa.float32())
    vector_array = pa.FixedSizeListArray.from_arrays(values, contiguous.shape[1])
    return pa.record_batch(
        [
            pa.array(np.arange(start_id, start_id + len(contiguous)), type=pa.int64()),
            vector_array,
        ],
        names=[ID_COLUMN, VECTOR_COLUMN],
    )


def write_dataset(
    vectors: np.ndarray, dataset_path: Path, args: argparse.Namespace
) -> tuple[lance.LanceDataset, float]:
    dimension = vectors.shape[1]
    schema = pa.schema(
        [
            pa.field(ID_COLUMN, pa.int64()),
            pa.field(VECTOR_COLUMN, pa.list_(pa.float32(), dimension)),
        ]
    )

    def batches():
        for start in range(0, args.rows, args.write_batch_rows):
            stop = min(start + args.write_batch_rows, args.rows)
            yield vector_record_batch(vectors[start:stop], start)

    rows_per_file = math.ceil(args.rows / args.data_fragments)
    print(
        f"Writing {args.rows:,} rows to {dataset_path} in about "
        f"{args.data_fragments} data fragments..."
    )
    started = time.perf_counter()
    reader = pa.RecordBatchReader.from_batches(schema, batches())
    dataset = lance.write_dataset(
        reader,
        dataset_path,
        max_rows_per_file=rows_per_file,
        max_rows_per_group=min(8192, rows_per_file),
    )
    return dataset, time.perf_counter() - started


def fragment_groups(
    dataset: lance.LanceDataset, segment_count: int
) -> tuple[list[list[int]], list[int]]:
    fragments = dataset.get_fragments()
    if len(fragments) < segment_count:
        raise ValueError(
            f"Dataset has {len(fragments)} fragments, fewer than "
            f"the requested {segment_count} segments"
        )
    groups = [[] for _ in range(segment_count)]
    row_counts = [0 for _ in range(segment_count)]
    # Greedy assignment balances rows even if the final fragment is smaller.
    for fragment in sorted(fragments, key=lambda item: item.count_rows(), reverse=True):
        target = min(range(segment_count), key=row_counts.__getitem__)
        groups[target].append(fragment.fragment_id)
        row_counts[target] += fragment.count_rows()
    return groups, row_counts


def drop_current_index(dataset_path: Path) -> lance.LanceDataset:
    dataset = lance.dataset(dataset_path)
    for description in dataset.describe_indices():
        if description.name == INDEX_NAME:
            dataset.drop_index(INDEX_NAME)
            dataset = lance.dataset(dataset_path)
            break
    return dataset


def build_segmented_index(
    dataset_path: Path, segment_count: int, args: argparse.Namespace
) -> dict[str, int | float]:
    dataset = drop_current_index(dataset_path)
    groups, group_rows = fragment_groups(dataset, segment_count)
    built_segments = []
    print(f"\nBuilding {segment_count} independently trained IVF-PQ segment(s)...")
    wall_started = time.perf_counter()
    cpu_started = time.process_time()
    for number, fragment_ids in enumerate(groups, start=1):
        print(
            f"  segment {number}/{segment_count}: "
            f"{len(fragment_ids)} fragments, {group_rows[number - 1]:,} rows"
        )
        built_segments.append(
            dataset.create_index_uncommitted(
                VECTOR_COLUMN,
                index_type="IVF_PQ",
                name=INDEX_NAME,
                metric="L2",
                fragment_ids=fragment_ids,
                target_partition_size=args.target_partition_size,
                num_sub_vectors=args.num_sub_vectors,
            )
        )
    dataset.commit_existing_index_segments(
        INDEX_NAME, VECTOR_COLUMN, built_segments
    )
    build_cpu_seconds = time.process_time() - cpu_started
    build_wall_seconds = time.perf_counter() - wall_started

    dataset = lance.dataset(dataset_path)
    description = next(
        item for item in dataset.describe_indices() if item.name == INDEX_NAME
    )
    statistics = dataset.index_statistics(INDEX_NAME)
    physical_segments = statistics.get("indices", [])
    total_partitions = sum(
        len(segment.get("partitions", [])) for segment in physical_segments
    )
    actual_segment_count = len(getattr(description, "segments", physical_segments))
    if actual_segment_count != segment_count:
        raise RuntimeError(
            f"Requested {segment_count} segments but Lance committed "
            f"{actual_segment_count}"
        )
    if int(description.num_rows_indexed) != args.rows:
        raise RuntimeError(
            f"Index covers {description.num_rows_indexed:,}/{args.rows:,} rows"
        )
    return {
        "actual_segment_count": actual_segment_count,
        "rows_per_segment_min": min(group_rows),
        "rows_per_segment_max": max(group_rows),
        "total_ivf_partitions": total_partitions,
        "build_wall_seconds": build_wall_seconds,
        "build_cpu_seconds": build_cpu_seconds,
    }


def exact_ground_truth(
    dataset: lance.LanceDataset,
    queries: np.ndarray,
    recall_k: int,
) -> list[set[int]]:
    print(f"Computing exact top-{recall_k} ground truth for {len(queries)} queries...")
    neighbors = []
    for number, query in enumerate(queries, start=1):
        table = dataset.to_table(
            columns=[ID_COLUMN],
            nearest={
                "column": VECTOR_COLUMN,
                "q": np.asarray(query, dtype=np.float32),
                "k": recall_k,
                "use_index": False,
            },
            disable_scoring_autoprojection=True,
        )
        neighbors.append(set(table[ID_COLUMN].to_pylist()))
        if number % 25 == 0 or number == len(queries):
            print(f"  exact queries: {number}/{len(queries)}")
    return neighbors


def load_ground_truth(
    args: argparse.Namespace,
    base_vectors: np.ndarray,
    dataset: lance.LanceDataset,
    queries: np.ndarray,
) -> tuple[list[set[int]], str]:
    use_file = args.ground_truth_mode == "file" or (
        args.ground_truth_mode == "auto"
        and args.rows == len(base_vectors)
        and args.ground_truth.exists()
    )
    if use_file:
        raw = read_vector_file(args.ground_truth, np.int32)
        if len(raw) < args.queries or raw.shape[1] < args.recall_k:
            raise ValueError("Ground-truth file lacks enough queries or neighbors")
        return [set(map(int, row[: args.recall_k])) for row in raw[: args.queries]], "file"
    if args.rows > 10_000_000:
        raise ValueError(
            "Exact ground truth above 10M rows is disabled. Supply a compatible "
            "ivecs file with --ground-truth-mode file."
        )
    return exact_ground_truth(dataset, queries, args.recall_k), "exact"


def run_queries(
    dataset_path: Path,
    segment_count: int,
    build: dict[str, int | float],
    queries: np.ndarray,
    truth: list[set[int]],
    args: argparse.Namespace,
) -> list[QueryResult]:
    dataset = lance.dataset(dataset_path)
    nearest_common = {
        "column": VECTOR_COLUMN,
        "k": args.recall_k,
        "nprobes": args.nprobes,
        "refine_factor": args.refine_factor,
    }
    for query in queries[: args.warmup_queries]:
        dataset.to_table(
            columns=[ID_COLUMN],
            nearest={**nearest_common, "q": np.asarray(query, dtype=np.float32)},
            disable_scoring_autoprojection=True,
        )

    results = []
    for repetition in range(1, args.repetitions + 1):
        # Rotate query order between repetitions to reduce ordering/cache bias.
        order = np.roll(np.arange(args.queries), repetition - 1)
        for query_index in order:
            started_ns = time.perf_counter_ns()
            table = dataset.to_table(
                columns=[ID_COLUMN],
                nearest={
                    **nearest_common,
                    "q": np.asarray(queries[query_index], dtype=np.float32),
                },
                disable_scoring_autoprojection=True,
            )
            latency_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
            returned = set(map(int, table[ID_COLUMN].to_pylist()))
            overlap = len(returned & truth[query_index])
            results.append(
                QueryResult(
                    segment_count=segment_count,
                    actual_segment_count=int(build["actual_segment_count"]),
                    query_number=int(query_index) + 1,
                    repetition=repetition,
                    rows=args.rows,
                    data_fragments=len(dataset.get_fragments()),
                    rows_per_segment_min=int(build["rows_per_segment_min"]),
                    rows_per_segment_max=int(build["rows_per_segment_max"]),
                    total_ivf_partitions=int(build["total_ivf_partitions"]),
                    build_wall_seconds=float(build["build_wall_seconds"]),
                    build_cpu_seconds=float(build["build_cpu_seconds"]),
                    recall_k=args.recall_k,
                    recall_at_k=overlap / args.recall_k,
                    precision_at_k=overlap / max(1, len(returned)),
                    ann_query_ms=latency_ms,
                    query_throughput_qps=1000 / latency_ms,
                    nprobes=args.nprobes,
                    refine_factor=args.refine_factor,
                )
            )
        print(
            f"  segments={segment_count} repetition={repetition}/"
            f"{args.repetitions} complete"
        )
    return results


def rounded(value):
    if isinstance(value, float):
        return round(value, OUTPUT_DECIMAL_PLACES)
    if isinstance(value, dict):
        return {key: rounded(item) for key, item in value.items()}
    return value


def write_results(path: Path, results: list[QueryResult]) -> None:
    if not results:
        return
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(asdict(results[0])))
        writer.writeheader()
        writer.writerows(rounded(asdict(result)) for result in results)


def summarize(results: list[QueryResult]) -> list[dict[str, int | float]]:
    summaries = []
    for segment_count in sorted({result.segment_count for result in results}):
        selected = [r for r in results if r.segment_count == segment_count]
        latencies = np.asarray([r.ann_query_ms for r in selected])
        recalls = np.asarray([r.recall_at_k for r in selected])
        first = selected[0]
        summaries.append(
            {
                "segments": segment_count,
                "rows": first.rows,
                "queries_measured": len(selected),
                "partitions": first.total_ivf_partitions,
                "build_wall_s": first.build_wall_seconds,
                "build_cpu_s": first.build_cpu_seconds,
                "latency_mean_ms": float(latencies.mean()),
                "latency_p50_ms": float(np.percentile(latencies, 50)),
                "latency_p95_ms": float(np.percentile(latencies, 95)),
                "latency_p99_ms": float(np.percentile(latencies, 99)),
                "mean_recall_at_k": float(recalls.mean()),
                "recall_p05": float(np.percentile(recalls, 5)),
                "sequential_qps_from_mean": 1000 / float(latencies.mean()),
            }
        )
    return summaries


def write_summary_csv(path: Path, summaries: list[dict[str, int | float]]) -> None:
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(summaries[0]))
        writer.writeheader()
        writer.writerows(rounded(summary) for summary in summaries)


def write_readable(path: Path, summaries: list[dict[str, int | float]]) -> None:
    columns = [
        ("segments", "segments", ">"),
        ("rows", "rows", ">"),
        ("partitions", "partitions", ">"),
        ("build_wall_s", "build_wall_s", ">"),
        ("build_cpu_s", "build_cpu_s", ">"),
        ("mean_ms", "latency_mean_ms", ">"),
        ("p50_ms", "latency_p50_ms", ">"),
        ("p95_ms", "latency_p95_ms", ">"),
        ("p99_ms", "latency_p99_ms", ">"),
        ("mean_recall", "mean_recall_at_k", ">"),
        ("qps", "sequential_qps_from_mean", ">"),
    ]
    table = []
    for summary in summaries:
        values = rounded(summary)
        table.append([str(values[key]) for _, key, _ in columns])
    widths = [
        max(len(heading), *(len(row[index]) for row in table))
        for index, (heading, _, _) in enumerate(columns)
    ]
    lines = [
        "  ".join(
            f"{heading:{alignment}{widths[index]}}"
            for index, (heading, _, alignment) in enumerate(columns)
        ),
        "  ".join("-" * width for width in widths),
    ]
    lines.extend(
        "  ".join(
            f"{value:{columns[index][2]}{widths[index]}}"
            for index, value in enumerate(row)
        )
        for row in table
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_metadata(
    path: Path,
    args: argparse.Namespace,
    ground_truth_source: str,
    data_write_seconds: float,
    actual_fragments: int,
) -> None:
    arguments = {
        key: str(value) if isinstance(value, Path) else value
        for key, value in vars(args).items()
    }
    metadata = {
        "experiment": "multi-segment vs single-segment IVF-PQ",
        "arguments": arguments,
        "ground_truth_source": ground_truth_source,
        "data_write_seconds": data_write_seconds,
        "actual_data_fragments": actual_fragments,
        "timing": (
            "ANN latency uses perf_counter_ns around Lance query execution and "
            "result materialization; build_wall_seconds uses perf_counter; "
            "build_cpu_seconds uses process_time."
        ),
        "environment": {
            "python": platform.python_version(),
            "lance": lance.__version__,
            "numpy": np.__version__,
            "pyarrow": pa.__version__,
            "platform": platform.platform(),
        },
    }
    path.write_text(json.dumps(rounded(metadata), indent=2) + "\n", encoding="utf-8")


def reset_work_dir(path: Path) -> None:
    resolved = path.resolve()
    if resolved in {Path("/").resolve(), EXPERIMENT_DIR.resolve()}:
        raise ValueError(f"Refusing to reset unsafe work directory: {resolved}")
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)


def main() -> None:
    args = parse_args()
    base_vectors = read_vector_file(args.base_vectors, np.float32)
    query_vectors = read_vector_file(args.query_vectors, np.float32)
    validate_args(args, base_vectors, query_vectors)
    queries = query_vectors[: args.queries]

    reset_work_dir(args.work_dir)
    dataset_path = args.work_dir / "data.lance"
    dataset, write_seconds = write_dataset(base_vectors, dataset_path, args)
    actual_fragments = len(dataset.get_fragments())
    if actual_fragments < max(args.segment_counts):
        raise RuntimeError(
            f"Lance wrote only {actual_fragments} fragments; need at least "
            f"{max(args.segment_counts)}"
        )
    truth, truth_source = load_ground_truth(
        args, base_vectors, dataset, queries
    )
    write_metadata(
        args.work_dir / "metadata.json",
        args,
        truth_source,
        write_seconds,
        actual_fragments,
    )

    all_results: list[QueryResult] = []
    for segment_count in args.segment_counts:
        build = build_segmented_index(dataset_path, segment_count, args)
        all_results.extend(
            run_queries(
                dataset_path,
                segment_count,
                build,
                queries,
                truth,
                args,
            )
        )
        summaries = summarize(all_results)
        write_results(args.work_dir / "results.csv", all_results)
        write_summary_csv(args.work_dir / "summary.csv", summaries)
        write_readable(args.work_dir / "results_readable.txt", summaries)

    print(f"\nDetailed samples: {args.work_dir / 'results.csv'}")
    print(f"Summary CSV:      {args.work_dir / 'summary.csv'}")
    print(f"Readable summary: {args.work_dir / 'results_readable.txt'}")
    print(f"Metadata:         {args.work_dir / 'metadata.json'}")


if __name__ == "__main__":
    main()
