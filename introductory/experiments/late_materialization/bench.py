import csv
import json
import multiprocessing
import os
from typing import List, NamedTuple

import pytest
import fsspec
import lance
import pyarrow.dataset as pa_ds
import pyarrow.fs as pa_fs
from late_materialization import scan_datafusion
from lance.lance import trace_to_chrome

from metered_fs import MeteredFSHandler

multiprocessing.set_start_method("spawn", force=True)


def lance_scan(ds, columns, predicate, late_materialization):
    reader = ds.scanner(
        columns=columns,
        filter=predicate,
        use_late_materialization=late_materialization,
        use_stats=False,
    ).to_batches()
    num_rows = 0
    for batch in reader:
        num_rows += batch.num_rows
    return num_rows


def pyarrow_scan(ds, columns, predicate):
    reader = ds.scanner(
        columns=columns,
        filter=predicate,
    ).to_batches()
    num_rows = 0
    for batch in reader:
        num_rows += batch.num_rows
    return num_rows


# Runtime benchmarks
@pytest.mark.parametrize("project", ["id", "vec", "img"])
@pytest.mark.parametrize("min_value", [10000, 25000, 50000, 75000, 90000])
@pytest.mark.parametrize("library", ["lance", "pyarrow", "datafusion"])
@pytest.mark.parametrize("late_materialization", [True, False])
def test_runtime(benchmark, project, min_value, library, late_materialization):
    columns = [project]
    if library == "lance":
        ds = lance.dataset("data/lance")
        num_rows = benchmark(
            lance_scan,
            ds,
            columns,
            predicate=f"id >= {min_value}",
            late_materialization=late_materialization,
        )
    elif library == "pyarrow":
        if late_materialization:
            pytest.skip("PyArrow does not support late materialization")
        ds = pa_ds.dataset("data/parquet", format="parquet")
        num_rows = benchmark(
            pyarrow_scan,
            ds,
            columns,
            predicate=pa_ds.field("id") >= min_value,
        )
    elif library == "datafusion":
        if columns == ["vec"]:
            # See: https://github.com/apache/arrow-datafusion/issues/8742
            pytest.skip("DataFusion does not support vector columns in projection")
        num_rows = benchmark(
            scan_datafusion,
            "data/parquet",
            columns,
            min_value,
            late_materialization=late_materialization,
            measure_io=False,
        )

    assert num_rows == 100_000 - min_value


class IOResult(NamedTuple):
    format: str
    columns: str
    predicate: str
    late_materialization: bool
    num_rows: int
    selectivity: float
    num_ios: int
    total_bytes: int


@pytest.fixture(scope="session")
def io_results():
    data = []
    yield data
    with open("io_results.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(IOResult._fields)
        for res in data:
            assert isinstance(res, IOResult)
            writer.writerow(res)


def measure_lance_io(
    path: str,
    columns: List[str],
    predicate: str,
    late_materialization: bool,
) -> IOResult:
    dataset = lance.dataset(path)

    # Enable tracing so we can record IOs
    guard = trace_to_chrome("lance_trace.json", "debug")
    num_rows = lance_scan(
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
        format="Lance",
        columns=",".join(columns),
        predicate=predicate,
        late_materialization=late_materialization,
        num_rows=num_rows,
        selectivity=num_rows / dataset.count_rows(),
        num_ios=num_ios,
        total_bytes=total_bytes,
    )


def measure_parquet_io(
    path: str,
    columns: List[str],
    predicate: str,
) -> IOResult:
    handler = MeteredFSHandler(pa_fs.FSSpecHandler(fsspec.filesystem("file")))
    path = os.path.abspath(path)
    dataset = pa_ds.dataset(
        path, format="parquet", filesystem=pa_fs.PyFileSystem(handler)
    )

    num_rows = pyarrow_scan(
        dataset,
        columns=columns,
        predicate=predicate,
    )

    return IOResult(
        format="Parquet",
        columns=",".join(columns),
        predicate=predicate,
        late_materialization=False,
        num_rows=num_rows,
        selectivity=num_rows / dataset.count_rows(),
        num_ios=handler.num_ios,
        total_bytes=handler.total_bytes,
    )


@pytest.mark.parametrize("project", ["id", "vec", "img"])
@pytest.mark.parametrize("min_value", [10000, 25000, 50000, 75000, 90000])
@pytest.mark.parametrize("library", ["lance", "pyarrow", "datafusion"])
@pytest.mark.parametrize("late_materialization", [True, False])
def test_io(io_results, project, min_value, library, late_materialization):
    columns = [project]
    if library == "lance":
        # Useful for debugging:
        # import logging
        # logger = multiprocessing.log_to_stderr()
        # logger.setLevel(logging.DEBUG)
        res = run_in_process(
            measure_lance_io,
            "data/lance",
            columns,
            predicate=f"id >= {min_value}",
            late_materialization=late_materialization,
        )
    elif library == "pyarrow":
        if late_materialization:
            pytest.skip("PyArrow does not support late materialization")
        ds = pa_ds.dataset("data/parquet", format="parquet")
        res = measure_parquet_io(
            "data/parquet",
            columns,
            predicate=pa_ds.field("id") >= min_value,
        )
    elif library == "datafusion":
        if columns == ["vec"]:
            # See: https://github.com/apache/arrow-datafusion/issues/8742
            pytest.skip("DataFusion does not support vector columns in projection")
        pytest.skip("Not yet implemented")

    io_results.append(res)


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
