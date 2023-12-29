import random

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
        "img": pa.array([random.randbytes(4 * 1024) for _ in range(nrows)]),
    }
)

file_format = ds.ParquetFileFormat()

ds.write_dataset(
    tab,
    "test_data/parquet",
    format="parquet",
    file_options=file_format.make_write_options(
        write_statistics=False, write_page_index=False
    ),
)

lance.write_dataset(tab, "test_data/lance")
