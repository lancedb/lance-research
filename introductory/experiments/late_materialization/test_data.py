import random
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import lance

nrows = 1000
ndims = 768

values = pc.random(nrows * ndims).cast(pa.float32())
vectors = pa.FixedSizeListArray.from_arrays(values, ndims)

tab = pa.table({
    'id': pa.array(range(nrows)),
    'vec': vectors,
    'img': pa.array([random.randbytes(4 * 1024) for _ in range(nrows)])
})

pq.write_to_dataset(tab, 'test_data/parquet')

lance.write_dataset(tab, 'test_data/lance')