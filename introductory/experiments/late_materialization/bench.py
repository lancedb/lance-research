import argparse
import csv
import datetime
from itertools import product
import json
import os
import time
from typing import List, NamedTuple, Tuple

import lance
from lance.tracing import trace_to_chrome
import pyarrow.parquet as pq
from fsspec.implementations.local import LocalFileSystem

class TimeResult(NamedTuple):
    columns: str
    predicate: str
    late_materialization: bool
    num_rows: int
    selectivity: float
    runtime: float

def scan_lance(
    ds: lance.LanceDataset,
    columns: List[str],
    predicate: str,
    late_materialization: bool,
) -> int:
    reader = ds.scanner(
        columns=columns,
        predicate=predicate,
        late_materialization=late_materialization,
    ).to_batches()
    num_rows = 0
    for batch in reader:
        num_rows += batch.num_rows
    return num_rows

def measure_lance_time(
    path: str,
    columns: List[str],
    predicate: str,
    late_materialization: bool,
    num_iters: int=10,
) -> TimeResult:
    ds = lance.dataset(path)
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
        columns=','.join(columns),
        predicate=predicate,
        late_materialization=late_materialization,
        num_rows=total_rows,
        selectivity=num_res / total_rows,
        runtime=min(times),
    )


def measure_lance_io(
    path: str,
    columns: List[str],
    predicate: str,
    late_materialization: bool,
) -> Tuple[int, int]:
    ds = lance.dataset(path)

    # Enable tracing so we can record IOs    
    trace_to_chrome(file="lance_trace.json", level="debug")

    scan_lance(
        ds,
        columns=columns,
        filter=predicate,
        late_materialization=late_materialization,
    )

    with open("lance_trace.json") as f:
        trace = json.load(f)
    num_ios = 0
    total_bytes = 0
    for event in trace:
        if event["name"] == "get_range":
            num_ios += 1
            byte_range = event["args"]["range"]
            start, end = byte_range.split("..")
            total_bytes += int(end) - int(start)
    
    print(f"IOs: {num_ios}")
    print(f"Bytes: {total_bytes}")


def scan_parquet(
    ds: pq.ParquetDataset,
    columns: List[str],
    predicate: str,
    late_materialization: bool,
) -> int:
    if late_materialization:
        raise NotImplementedError("TODO: figure out how to implement late materialization")
    reader = ds.scanner(
        columns=columns,
        filter=predicate,
    ).to_batches()
    num_rows = 0
    for batch in reader:
        num_rows += batch.num_rows
    return num_rows


def measure_parquet_time(
    path: str,
    columns: List[str],
    predicate: str,
    late_materialization: bool,
    num_iters: int=10,
) -> None:
    ds = pq.ParquetDataset(path)
    total_rows = ds.count_rows()

    times = []
    
    for _ in range(num_iters):
        start = time.perf_counter()
        num_res = scan_parquet(
            ds,
            columns=columns,
            predicate=predicate,
            late_materialization=late_materialization,
        )
        end = time.perf_counter()
        times.append(end - start)

    return TimeResult(
        columns=','.join(columns),
        predicate=predicate,
        late_materialization=late_materialization,
        num_rows=total_rows,
        selectivity=num_res / total_rows,
        runtime=min(times),
    )


# TODO: measured IOs for parquet
# def measure_parquet_io(
#     path: str,
#     columns: List[str],
#     predicate: str,
#     late_materialization: bool,
# ) -> Tuple[int, int]:
#     ds = pq.ParquetDataset(path)

#     # Enable tracing so we can record IOs    
#     trace_to_chrome(file="parquet_trace.json", level="debug")

#     scan_lance(
#         ds,
#         columns=columns,
#         filter=predicate,
#         late_materialization=late_materialization,
#     )

#     with open("parquet_trace.json") as f:
#         trace = json.load(f)
#     num_ios = 0
#     total_bytes = 0
#     for event in trace:
#         if event["name"] == "get_range":
#             num_ios += 1
#             byte_range = event["args"]["range"]
#             start, end = byte_range.split("..")
#             total_bytes += int(end) - int(start)
    
#     print(f"IOs: {num_ios}")
#     print(f"Bytes: {total_bytes}")

COLUMN_SETS = [
    # Scalar columns
    # Vector columns
    # Image columns
]

# TODO: Tune these to target specific selectivities [0.1, 0.25, 0.5, 0.75, 0.9]
MIN_VALUE = [

]

def run_all(path: str, num_iters: int=10) -> None:
    out_dir = f"./data/{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
    os.makedirs(out_dir)

    with open(out_dir + "/runtime_results.csv", "w") as f:
        with csv.writer(f) as writer:
            for columns, min_value in product(COLUMN_SETS, MIN_VALUE):
                for lm in [True, False]:
                    res = measure_lance_time(
                        path=path,
                        columns=columns,
                        predicate=f"col > {min_value}",
                        late_materialization=lm,
                        num_iters=num_iters,
                    )
                    writer.writerow(res)
                # TODO: measure_parquet_time with late_materialization=True
                res = measure_parquet_time(
                    path=path,
                    columns=columns,
                    predicate=f"col > {min_value}",
                    late_materialization=False,
                    num_iters=num_iters,
                )
                writer.writerow(res)

    with open(out_dir + "/io_results.csv", "w") as f:
        with csv.writer(f) as writer:
            for columns, min_value in product(COLUMN_SETS, MIN_VALUE):
                for lm in [True, False]:
                    res = measure_lance_io(
                        path=path,
                        columns=columns,
                        predicate=f"col > {min_value}",
                        late_materialization=lm,
                    )
                    writer.writerow(res)





if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    run_parser = parser.add_subparsers('run', help='run all experiments', dest='command')
    one_parser = parser.add_subparsers('run_one', help='run one experiment', dest='command')

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
