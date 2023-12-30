import random
import shutil

import lance
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyarrow.dataset as ds

nrows = 100_000
ndims = 768

values = pc.random(nrows * ndims).cast(pa.float32())
vectors = pa.FixedSizeListArray.from_arrays(values, ndims)

tab = pa.table(
    {
        "id": pa.array(range(nrows)),
        "vec": vectors,
        "img": pa.array([random.randbytes(40 * 1024) for _ in range(nrows)]),
    }
)

file_format = ds.ParquetFileFormat()

shutil.rmtree("test_data", ignore_errors=True)

ds.write_dataset(
    tab,
    "test_data/parquet",
    format="parquet",
    file_options=file_format.make_write_options(
        write_statistics=False, write_page_index=False
    ),
    max_rows_per_group=10 * 1024,
)

lance.write_dataset(tab, "test_data/lance")
