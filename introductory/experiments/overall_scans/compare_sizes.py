"""Quick script to compare sizes of Parquet and Lance vector columns"""
import tempfile
import os

import lance
import pyarrow.parquet as pq
import pyarrow.dataset as ds


def get_size(start_path):
    total_size = 0
    for dirpath, _, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


# with tempfile.TemporaryDirectory() as tmpdir:
#     dataset = lance.dataset("data/laion/lance-100K")
#     lance.write_dataset(dataset.scanner(columns=["vector"]), tmpdir)
#     print("Lance size: {}GB".format(get_size(tmpdir) / 1024 / 1024 / 1024)) # 5.3GB

#5.3 GB

with tempfile.TemporaryDirectory() as tmpdir:
    dataset = ds.dataset("data/laion/parquet-100K", format="parquet")
    opts = ds.ParquetFileFormat().make_write_options(
        compression="none",
        use_byte_stream_split=True
    )
    ds.write_dataset(dataset.scanner(columns=["vector"]), tmpdir, format="parquet", file_options=opts)
    #2.5GB w/o byte_stream_split
    print("Parquet size: {}GB".format(get_size(tmpdir) / 1024 / 1024 / 1024)) 

    # min: -0.54296875
    # max: 0.58056640625

    path = os.path.join(tmpdir, os.listdir(tmpdir)[0])
    f = pq.ParquetFile(path)
    print(f.metadata.row_group(0).column(0))