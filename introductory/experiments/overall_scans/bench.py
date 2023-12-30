import os
import lance
import pyarrow.dataset as ds
import pytest


def run_parquet_scan(path):
    scan = ds.dataset(path, format="parquet").scanner()
    num_rows = 0
    for batch in scan.to_batches():
        num_rows += batch.num_rows
    return num_rows


def run_lance_scan(path):
    scan = lance.dataset(path).scanner()
    num_rows = 0
    for batch in scan.to_batches():
        num_rows += batch.num_rows
    return num_rows


@pytest.mark.benchmark(group="tpch")
@pytest.mark.parametrize("dataset_dir", os.listdir("data/tpch"))
def test_scan_tpch(dataset_dir, benchmark):
    path = os.path.join("data/tpch", dataset_dir)
    if dataset_dir.startswith("parquet"):
        num_rows = benchmark(run_parquet_scan, path)
    elif dataset_dir.startswith("lance"):
        num_rows = benchmark(run_lance_scan, path)
    else:
        raise Exception("Unknown data format")
    assert num_rows == 59986052


# TODO: parametrize by projection
@pytest.mark.benchmark(group="laion")
@pytest.mark.parametrize("dataset_dir", os.listdir("data/laion"))
def test_scan_laion(dataset_dir, benchmark):
    path = os.path.join("data/laion", dataset_dir)
    if dataset_dir.startswith("parquet"):
        num_rows = benchmark(run_parquet_scan, path)
    elif dataset_dir.startswith("lance"):
        num_rows = benchmark(run_lance_scan, path)
    else:
        raise Exception("Unknown data format")
    assert num_rows == 1870007
