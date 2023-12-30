import lance
import pyarrow.dataset as ds
import pytest


def run_parquet_scan():
    scan = ds.dataset("data/lineitem_parquet", format="parquet").scanner()
    num_rows = 0
    for batch in scan.to_batches():
        num_rows += batch.num_rows
    return num_rows


def run_lance_scan(path="data/lineitem_lance"):
    scan = lance.dataset(path).scanner()
    num_rows = 0
    for batch in scan.to_batches():
        num_rows += batch.num_rows
    return num_rows


@pytest.mark.parametrize("format", ["parquet", "lance", "lance2"])
def test_scan(format, benchmark):
    if format == "parquet":
        num_rows = benchmark(run_parquet_scan)
    elif format == "lance":
        num_rows = benchmark(run_lance_scan)
    elif format == "lance2":
        num_rows = benchmark(run_lance_scan, path="data/lineitem_lance2")
    assert num_rows == 59986052
