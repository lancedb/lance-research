import argparse
import random
import shutil
import logging

import lance
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyarrow.dataset as ds

nrows = 100 * 1024
ndims = 768


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)

    values = pc.random(nrows * ndims).cast(pa.float32())
    vectors = pa.FixedSizeListArray.from_arrays(values, ndims)

    logging.info("Generating %i rows of data", nrows)
    tab = pa.table(
        {
            "id": pa.array(range(nrows)),
            "int": pa.array((random.randint(0, nrows) for _ in range(nrows)), pa.int64()),
            "vec": vectors,
            "img": pa.array([random.randbytes(40 * 1024) for _ in range(nrows)]),
        }
    )

    file_format = ds.ParquetFileFormat()

    shutil.rmtree("data", ignore_errors=True)

    # We write a version of Parquet without statistics so we can isolate pushdown
    # performance from late materialization.
    for with_stats in [True, False]:
        logging.info("Writing parquet with stats=%s", with_stats)
        parquet_name = "data/parquet"

        if with_stats:
            parquet_name += "_stats"

        ds.write_dataset(
            tab,
            parquet_name,
            format="parquet",
            file_options=file_format.make_write_options(
                write_statistics=with_stats,
                write_page_index=with_stats,
            ),
            max_rows_per_group=10 * 1024,
        )

    # We can disable use of statistics at read time for Lance
    logging.info("Writing Lance")
    lance.write_dataset(tab, "data/lance", max_rows_per_group=10 * 1024)
