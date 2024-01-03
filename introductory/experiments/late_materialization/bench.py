import pytest

import lance
import pyarrow.dataset as pa_ds
from late_materialization import scan_datafusion

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
        if columns == ['vec']:
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


    


