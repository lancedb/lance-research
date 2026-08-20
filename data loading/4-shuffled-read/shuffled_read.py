#!/usr/bin/env python3
"""Compare one-phase SIFT1M shuffled reads on local NVMe and AWS S3.

Each SIFT1M base vector is one value: 128 float32 components (512 bytes). For
each backend, the script writes the same LanceDB table and then uses pylance
``LanceDataset.take`` to consume the same seeded row permutations.

AWS credentials are never stored by this script. LanceDB obtains them through
the standard AWS credential chain (for example, an EC2 IAM role or AWS_* env
vars) and sends signed HTTPS requests to the S3 API.
"""

from __future__ import annotations

import argparse
import csv
import gc
import json
import os
import platform
import shutil
import statistics
import subprocess
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
TABLE_NAME = "sift1m_shuffled_read"
VECTOR_COLUMN = "vector"
VECTOR_DIMENSION = 128
VALUE_BYTES = VECTOR_DIMENSION * 4
BACKEND_LABELS = {"local_nvme": "Local NVMe", "aws_s3": "AWS S3"}
RESULT_FIELDS = (
    "backend",
    "trial",
    "cache_state",
    "rows",
    "value_bytes",
    "take_size",
    "seed",
    "wall_seconds",
    "payload_mb_per_second",
)


@dataclass(frozen=True)
class StorageTarget:
    backend: str
    uri: str
    db: object
    local_path: Path | None = None

@dataclass(frozen=True)
class ReadResult:
    backend: str
    trial: int
    cache_state: str
    rows: int
    value_bytes: int
    take_size: int
    seed: int
    wall_seconds: float
    payload_mb_per_second: float

# Step 0. Not interesting: argument parsing/validation, storage-target
# discovery, and loading the shared SIFT1M source vectors.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backend",
        choices=("both", "local_nvme", "aws_s3"),
        default="both",
        help="Storage backend(s) to test (default: both).",
    )
    parser.add_argument(
        "--mode", choices=("all", "load", "benchmark"), default="all"
    )
    parser.add_argument("--sift-base", type=Path, default=DEFAULT_SIFT_BASE)
    parser.add_argument(
        "--local-db-dir",
        "--db-dir",
        dest="local_db_dir",
        type=Path,
        default=EXPERIMENT_DIR / "nvme_lancedb",
    )
    parser.add_argument(
        "--s3-uri",
        default=os.environ.get("SIFT_SHUFFLE_S3_URI"),
        help="S3 database prefix, e.g. s3://bucket/benchmarks/sift1m. "
        "Defaults to SIFT_SHUFFLE_S3_URI.",
    )
    parser.add_argument(
        "--aws-region",
        default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"),
        help="AWS region; defaults to AWS_REGION or AWS_DEFAULT_REGION.",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=EXPERIMENT_DIR / "results"
    )
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--write-batch-rows", type=int, default=8192)
    parser.add_argument(
        "--take-size",
        type=int,
        default=4096,
        help="Offsets per take() request, not a two-phase shuffle block.",
    )
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--seed", type=int, default=20250812)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--allow-non-nvme", action="store_true")
    args = parser.parse_args()
    for name in ("rows", "write_batch_rows", "take_size", "trials"):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    if "://" in str(args.local_db_dir):
        parser.error("--local-db-dir must be a local filesystem path")
    if args.backend in ("both", "aws_s3"):
        if not args.s3_uri:
            parser.error(
                "--s3-uri (or SIFT_SHUFFLE_S3_URI) is required because the "
                "default --backend=both includes AWS S3"
            )
        if not args.s3_uri.startswith("s3://"):
            parser.error("--s3-uri must start with s3://")
    return args

def selected_backends(selection: str) -> list[str]:
    return ["local_nvme", "aws_s3"] if selection == "both" else [selection]

def nvme_devices() -> list[str]:
    if platform.system() == "Darwin":
        try:
            raw = subprocess.run(
                ["system_profiler", "SPNVMeDataType", "-json"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout
            report = json.loads(raw)
            return [
                str(item.get("device_model") or item.get("_name"))
                for controller in report.get("SPNVMeDataType", [])
                for item in controller.get("_items", [])
                if item.get("device_model") or item.get("_name")
            ]
        except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
            return []
    if platform.system() == "Linux":
        try:
            raw = subprocess.run(
                ["lsblk", "-dno", "NAME,TRAN,MODEL"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout
            return [line.strip() for line in raw.splitlines() if "nvme" in line.lower()]
        except (OSError, subprocess.SubprocessError):
            return []
    return []

def connect_targets(args: argparse.Namespace, devices: list[str]) -> list[StorageTarget]:
    targets: list[StorageTarget] = []
    for backend in selected_backends(args.backend):
        if backend == "local_nvme":
            if not devices and not args.allow_non_nvme:
                raise SystemExit(
                    "Could not confirm local NVMe hardware. Pass --allow-non-nvme "
                    "only if the local target is intentionally not NVMe."
                )
            args.local_db_dir.mkdir(parents=True, exist_ok=True)
            uri = str(args.local_db_dir)
            targets.append(
                StorageTarget(backend, uri, lancedb.connect(uri), args.local_db_dir)
            )
        else:
            storage_options = {
                "new_table_data_storage_version": "stable",
                "new_table_enable_v2_manifest_paths": "true",
            }
            if args.aws_region:
                storage_options["region"] = args.aws_region
            uri = args.s3_uri.rstrip("/")
            targets.append(
                StorageTarget(
                    backend,
                    uri,
                    lancedb.connect(uri, storage_options=storage_options),
                )
            )
    return targets


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


def lance_schema() -> pa.Schema:
    return pa.schema(
        [("id", pa.uint64()), (VECTOR_COLUMN, pa.list_(pa.float32(), VECTOR_DIMENSION))]
    )


def sift_batches(
    vectors: np.ndarray, rows: int, batch_rows: int
) -> Iterator[pa.RecordBatch]:
    table_schema = lance_schema()
    for start in range(0, rows, batch_rows):
        stop = min(start + batch_rows, rows)
        contiguous = np.ascontiguousarray(vectors[start:stop], dtype=np.float32)
        vector_array = pa.FixedSizeListArray.from_arrays(
            pa.array(contiguous.reshape(-1), type=pa.float32()), VECTOR_DIMENSION
        )
        yield pa.RecordBatch.from_arrays(
            [pa.array(np.arange(start, stop, dtype=np.uint64)), vector_array],
            schema=table_schema,
        )


def table_exists(db: object, name: str) -> bool:
    try:
        db.open_table(name)  # type: ignore[attr-defined]
        return True
    except Exception as exc:
        message = str(exc).lower()
        if "not found" in message or "does not exist" in message:
            return False
        raise


# Step 1 of experiment. Write the same SIFT1M table to each selected backend.
# This is setup for the shuffled-read benchmark, not something the experiment
# measures, so it is intentionally untimed and unreported.
def load_table(
    args: argparse.Namespace, target: StorageTarget, vectors: np.ndarray
) -> None:
    if args.rows > len(vectors):
        raise ValueError(
            f"Requested {args.rows:,} vectors, but SIFT contains {len(vectors):,}."
        )
    exists = table_exists(target.db, TABLE_NAME)
    if exists and not args.overwrite:
        raise SystemExit(
            f"{BACKEND_LABELS[target.backend]} table already exists at {target.uri}. "
            "Use --mode benchmark or add --overwrite."
        )
    if target.local_path is not None:
        estimated_bytes = args.rows * (VALUE_BYTES + 8)
        free_bytes = shutil.disk_usage(target.local_path).free
        if estimated_bytes > free_bytes * 0.85:
            raise SystemExit(
                f"Local payload needs about {estimated_bytes / 1e9:.1f} GB, but "
                f"only {free_bytes / 1e9:.1f} GB is free."
            )
    reader = pa.RecordBatchReader.from_batches(
        lance_schema(), sift_batches(vectors, args.rows, args.write_batch_rows)
    )
    table = target.db.create_table(  # type: ignore[attr-defined]
        TABLE_NAME, data=reader, mode="overwrite" if exists else "create"
    )
    actual_rows = table.count_rows()
    if actual_rows != args.rows:
        raise RuntimeError(f"wrote {actual_rows:,} rows; expected {args.rows:,}")
    print(f"Loaded {actual_rows:,} rows [{target.backend}]: {target.uri}", flush=True)

# Step 2 of experiment. Read back the same seeded shuffle order on each
# backend/trial and time it.
def validate_table(table: object, expected_rows: int) -> tuple[object, int]:
    actual_rows = table.count_rows()  # type: ignore[attr-defined]
    dataset = table.to_lance()  # type: ignore[attr-defined]
    vector_type = dataset.schema.field(VECTOR_COLUMN).type
    valid_vector = (
        pa.types.is_fixed_size_list(vector_type)
        and vector_type.list_size == VECTOR_DIMENSION
        and vector_type.value_type == pa.float32()
    )
    if actual_rows != expected_rows or not valid_vector:
        raise SystemExit(
            f"Table shape mismatch: rows={actual_rows:,}, vector_type={vector_type}."
        )
    return dataset, actual_rows


def benchmark(
    args: argparse.Namespace, target: StorageTarget
) -> list[ReadResult]:
    if not table_exists(target.db, TABLE_NAME):
        raise SystemExit(
            f"Table missing on {BACKEND_LABELS[target.backend]}; run --mode load first."
        )
    table = target.db.open_table(TABLE_NAME)  # type: ignore[attr-defined]
    dataset, actual_rows = validate_table(table, args.rows)
    results: list[ReadResult] = []
    for trial in range(args.trials):
        trial_seed = args.seed + trial
        offsets = np.random.default_rng(trial_seed).permutation(actual_rows)
        values_read = 0
        gc_was_enabled = gc.isenabled()
        gc.disable()
        started = time.perf_counter_ns()
        try:
            for start in range(0, actual_rows, args.take_size):
                selected = dataset.take(
                    offsets[start : start + args.take_size], columns=[VECTOR_COLUMN]
                )
                values_read += selected.num_rows
        finally:
            elapsed = (time.perf_counter_ns() - started) / 1e9
            if gc_was_enabled:
                gc.enable()
        if values_read != actual_rows:
            raise RuntimeError(f"read {values_read:,} values; expected {actual_rows:,}")
        result = ReadResult(
            backend=target.backend,
            trial=trial + 1,
            cache_state="first_pass" if trial == 0 else "repeat_cache_unspecified",
            rows=actual_rows,
            value_bytes=VALUE_BYTES,
            take_size=args.take_size,
            seed=trial_seed,
            wall_seconds=elapsed,
            payload_mb_per_second=values_read * VALUE_BYTES / elapsed / 1e6,
        )
        results.append(result)
        print(f"Read [{target.backend}]: {json.dumps(asdict(result), sort_keys=True)}")
    return results

# Step 3 of experiment: measure and report.
def write_readable_results(path: Path, results: list[ReadResult]) -> None:
    """Write a fixed-width text view of the CSV results for quick scanning."""
    rows = []
    for result in results:
        values = asdict(result)
        rows.append(
            [
                f"{values[field]:.3f}" if isinstance(values[field], float) else str(values[field])
                for field in RESULT_FIELDS
            ]
        )
    widths = [
        max(len(heading), *(len(row[index]) for row in rows))
        for index, heading in enumerate(RESULT_FIELDS)
    ]
    header = "  ".join(
        f"{heading:<{widths[index]}}" for index, heading in enumerate(RESULT_FIELDS)
    )
    separator = "  ".join("-" * width for width in widths)
    lines = [header, separator]
    for row in rows:
        lines.append(
            "  ".join(f"{value:<{widths[index]}}" for index, value in enumerate(row))
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def summarize_reads(results: list[ReadResult]) -> dict[str, object]:
    summaries: dict[str, object] = {}
    for backend in sorted({result.backend for result in results}):
        rates = [
            result.payload_mb_per_second for result in results if result.backend == backend
        ]
        summaries[backend] = {
            "trials": len(rates),
            "mean_payload_mb_per_second": statistics.fmean(rates),
            "median_payload_mb_per_second": statistics.median(rates),
            "min_payload_mb_per_second": min(rates),
            "max_payload_mb_per_second": max(rates),
        }
    if {"local_nvme", "aws_s3"}.issubset(summaries):
        local = summaries["local_nvme"]["median_payload_mb_per_second"]  # type: ignore[index]
        s3 = summaries["aws_s3"]["median_payload_mb_per_second"]  # type: ignore[index]
        summaries["comparison"] = {
            "local_nvme_over_aws_s3_median_speedup": local / s3
        }
    return summaries


def write_outputs(
    args: argparse.Namespace,
    devices: list[str],
    targets: list[StorageTarget],
    reads: list[ReadResult],
) -> None:
    args.output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = args.output_dir / "shuffled_read_results.csv"
    readable_path = args.output_dir / "shuffled_read_results_readable.txt"
    if reads:
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=RESULT_FIELDS)
            writer.writeheader()
            writer.writerows(asdict(result) for result in reads)
        write_readable_results(readable_path, reads)

    metadata = {
        "experiment": "one-phase SIFT1M shuffled read: local NVMe vs AWS S3",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "dataset": {
            "name": "SIFT1M",
            "subset": "base",
            "source_path": str(args.sift_base),
            "source_bytes": args.sift_base.stat().st_size if args.sift_base.exists() else None,
            "rows": args.rows,
            "dimensions": VECTOR_DIMENSION,
            "value_type": "float32",
        },
        "targets": {target.backend: target.uri for target in targets},
        "aws_region": args.aws_region,
        "nvme_confirmed": bool(devices),
        "nvme_devices": devices,
        "method": (
            "Each backend stores the same table and consumes identical seeded "
            "permutations using sequential pylance LanceDataset.take() calls."
        ),
        "interpretation_note": (
            "Running this process on a laptop measures internet path plus S3. "
            "For a storage-focused comparison, run on an EC2 instance in the "
            "same region as S3 and use that instance's local NVMe condition."
        ),
        "cache_note": "OS and Lance caches are not flushed between trials.",
        "read_summary": summarize_reads(reads) if reads else None,
        "configuration": {
            "backend": args.backend,
            "rows": args.rows,
            "value_bytes": VALUE_BYTES,
            "write_batch_rows": args.write_batch_rows,
            "take_size": args.take_size,
            "trials": args.trials,
            "seed": args.seed,
        },
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
    for path in (csv_path if reads else None, readable_path if reads else None, metadata_path):
        if path is not None and path.exists():
            print(f"Wrote {path}")


def main() -> None:
    args = parse_args()
    args.sift_base = args.sift_base.expanduser().resolve()
    args.local_db_dir = args.local_db_dir.expanduser().resolve()
    args.output_dir = args.output_dir.expanduser().resolve()
    devices = nvme_devices()
    targets = connect_targets(args, devices)
    for target in targets:
        print(f"Target [{target.backend}]: {target.uri}")

    vectors = load_sift_vectors(args.sift_base) if args.mode in ("all", "load") else None
    if vectors is not None:
        for target in targets:
            load_table(args, target, vectors)
    reads: list[ReadResult] = []
    if args.mode in ("all", "benchmark"):
        for target in targets:
            reads.extend(benchmark(args, target))
    write_outputs(args, devices, targets, reads)


if __name__ == "__main__":
    main()
