import argparse
import csv
import json
import multiprocessing
import os
import time
from datetime import datetime
from itertools import product
from typing import List, NamedTuple, Optional, Tuple

import fsspec
import lance
import pyarrow.dataset as ds
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq
from lance.lance import trace_to_chrome

from common import scan_lance
from metered_fs import MeteredFSHandler


class TimeResult(NamedTuple):
    """Row format of the runtime results CSV file"""

    format: str
    columns: str
    predicate: str
    late_materialization: bool
    num_rows: int
    selectivity: float
    runtime: float


def measure_lance_time(
    path: str,
    columns: List[str],
    predicate: str,
    late_materialization: bool,
    num_iters: int = 10,
) -> TimeResult:
    ds = lance.dataset(os.path.join(path, "lance"))
    total_rows = ds.count_rows()

    times = []

    for _ in range(num_iters):
        start = time.perf_counter()
        num_res = scan_lance(
            ds,
            columns=columns,
            predicate=predicate,
            late_materialization=late_materialization,
        )
        end = time.perf_counter()
        times.append(end - start)

    return TimeResult(
        format="lance",
        columns=",".join(columns),
        predicate=predicate,
        late_materialization=late_materialization,
        num_rows=total_rows,
        selectivity=num_res / total_rows,
        runtime=min(times),
    )


class IOResult(NamedTuple):
    format: str
    columns: str
    predicate: str
    late_materialization: bool
    num_ios: int
    total_bytes: int


def measure_lance_io(
    path: str,
    columns: List[str],
    predicate: str,
    late_materialization: bool,
) -> IOResult:
    dataset = lance.dataset(os.path.join(path, "lance"))

    # Enable tracing so we can record IOs
    guard = trace_to_chrome("lance_trace.json", "debug")
    scan_lance(
        dataset,
        columns=columns,
        predicate=predicate,
        late_materialization=late_materialization,
    )
    guard.finish_tracing()

    with open("lance_trace.json") as f:
        trace = json.load(f)
    num_ios = 0
    total_bytes = 0
    for event in trace:
        # ph is the event type.  b is for begin, e is for end
        if event["name"] == "get_range" and event["ph"] == "b":
            num_ios += 1
            byte_range = event["args"]["range"]
            start, end = byte_range.split("..")
            total_bytes += int(end) - int(start)

    return IOResult(
        format="lance",
        columns=",".join(columns),
        predicate=predicate,
        late_materialization=late_materialization,
        num_ios=num_ios,
        total_bytes=total_bytes,
    )


def scan_parquet(
    ds: pq.ParquetDataset,
    alt_ds: Optional[pq.ParquetDataset],
    columns: List[str],
    predicate: str,
    late_materialization: bool,
) -> int:
    if late_materialization:
        if alt_ds is None:
            raise ValueError("alt_ds must be specified for late materialization")
        return scan_parquet_late(ds, alt_ds, columns, predicate)
    else:
        return scan_parquet_early(ds, columns, predicate)


def scan_parquet_early(
    ds: pq.ParquetDataset,
    columns: List[str],
    predicate: str,
) -> int:
    reader = ds.scanner(
        columns=columns,
        filter=predicate,
    ).to_batches()
    num_rows = 0
    for batch in reader:
        num_rows += batch.num_rows
    return num_rows


def scan_parquet_late(
    ds: pq.ParquetDataset,
    alt_ds: pq.ParquetDataset,
    columns: List[str],
    predicate: str,
) -> int:
    # First pass, record IOs just reading the filter columns.
    reader = ds.scanner(
        columns=["id"],
        filter=predicate,
    ).to_batches()

    num_rows = 0
    for batch in reader:
        num_rows += batch.num_rows

    # Second pass, do the projection with take. We use the alternate dataset
    # so that the row ids don't count towards are IO counts.
    reader = alt_ds.scanner(
        columns=["id"],
        filter=predicate,
    ).to_batches()
    for batch in reader:
        ds.take(batch["id"], columns=columns)

    return num_rows


def measure_parquet_time(
    path: str,
    columns: List[str],
    predicate: str,
    late_materialization: bool,
    num_iters: int = 10,
) -> None:
    dataset = ds.dataset(os.path.join(path, "parquet"), format="parquet")
    total_rows = dataset.count_rows()

    times = []

    for _ in range(num_iters):
        start = time.perf_counter()
        num_res = scan_parquet(
            dataset,
            None,
            columns=columns,
            predicate=predicate,
            late_materialization=late_materialization,
        )
        end = time.perf_counter()
        times.append(end - start)

    return TimeResult(
        format="parquet",
        columns=",".join(columns),
        predicate=predicate,
        late_materialization=late_materialization,
        num_rows=total_rows,
        selectivity=num_res / total_rows,
        runtime=min(times),
    )


def measure_parquet_io(
    path: str,
    columns: List[str],
    predicate: str,
    late_materialization: bool,
) -> Tuple[int, int]:
    handler = MeteredFSHandler(pa_fs.FSSpecHandler(fsspec.filesystem("file")))
    path = os.path.abspath(os.path.join(path, "parquet"))
    dataset = ds.dataset(path, format="parquet", filesystem=pa_fs.PyFileSystem(handler))
    dataset_alt = ds.dataset(path, format="parquet")

    scan_parquet(
        dataset,
        dataset_alt,
        columns=columns,
        predicate=predicate,
        late_materialization=late_materialization,
    )

    return IOResult(
        format="parquet",
        columns=",".join(columns),
        predicate=predicate,
        late_materialization=late_materialization,
        num_ios=handler.num_ios,
        total_bytes=handler.total_bytes,
    )


COLUMN_SETS = [
    # Scalar columns
    ["id"],
    # Vector columns
    ["vec"],
    # Image columns
    ["img"],
]

# TODO: Tune these to target specific selectivities [0.1, 0.25, 0.5, 0.75, 0.9]
MIN_VALUE = [
    100,
    250,
    500,
    750,
    900,
]


def run_all(path: str, num_iters: int = 10) -> None:
    out_dir = f"./data/{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
    os.makedirs(out_dir)

    with open(out_dir + "/runtime_results.csv", "w") as f:
        writer = csv.writer(f)
        for columns, min_value in product(COLUMN_SETS, MIN_VALUE):
            predicate = ds.field("id") > min_value
            for lm in [True, False]:
                res = measure_lance_time(
                    path=path,
                    columns=columns,
                    predicate=predicate,
                    late_materialization=lm,
                    num_iters=num_iters,
                )
                writer.writerow(res)

            # Note: we do not measure time with Parquet's late materialization,
            # because there is not a proper implementation.

            res = measure_parquet_time(
                path=path,
                columns=columns,
                predicate=predicate,
                late_materialization=False,
                num_iters=num_iters,
            )
            writer.writerow(res)

    with open(out_dir + "/io_results.csv", "w") as f:
        writer = csv.writer(f)
        for columns, min_value in product(COLUMN_SETS, MIN_VALUE):
            for late_materialization in [True, False]:
                res = run_in_process(
                    measure_lance_io,
                    path=path,
                    columns=columns,
                    predicate=predicate,
                    late_materialization=late_materialization,
                )
                writer.writerow(res)

                res = measure_parquet_io(
                    path=path,
                    columns=columns,
                    predicate=predicate,
                    late_materialization=late_materialization,
                )
                writer.writerow(res)


def run_in_process(func, *args, **kwargs):
    q = multiprocessing.Queue()
    p = multiprocessing.Process(
        target=run_process_inner, args=(q, func, *args), kwargs=kwargs
    )
    p.start()
    p.join()
    return q.get()


def run_process_inner(queue, func, *args, **kwargs):
    result = func(*args, **kwargs)
    queue.put(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(
        title="commands", help="sub-command help", dest="command"
    )
    run_parser = subparsers.add_parser("run", help="run all experiments")
    one_parser = subparsers.add_parser("run_one", help="run one experiment")

    # Arguments for running all experiments
    run_parser.add_argument("--path", type=str, required=True)
    run_parser.add_argument("--num_iters", type=int, default=10)

    # Arguments for running one experiment
    one_parser.add_argument("--path", type=str, required=True)
    one_parser.add_argument("--columns", type=str, required=True)
    one_parser.add_argument("--predicate", type=str, required=True)
    one_parser.add_argument("--late_materialization", action="store_true")
    one_parser.add_argument("--num_iters", type=int, default=10)

    args = parser.parse_args()

    if args.command == "run":
        run_all(args.path, args.num_iters)
