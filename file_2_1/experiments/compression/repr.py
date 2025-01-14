import tempfile
from pathlib import Path

import pyarrow.parquet as pq

from lance.file import LanceFileReader, LanceFileWriter

print("filename,parquet_size,lance_size_2_0,lance_size_2_1")

with tempfile.TemporaryDirectory() as temp:
    tempdir = Path(temp)
    for path in Path("datafiles").iterdir():
        if "trimmed" in path.stem and path.suffix == ".parquet":
            parquet_size = path.stat().st_size
            table = pq.read_table(str(path))
            with LanceFileWriter(tempdir.joinpath("v2.lance"), version="2.0") as writer:
                writer.write_batch(table)
            old_lance_size = tempdir.joinpath("v2.lance").stat().st_size
            with LanceFileWriter(
                tempdir.joinpath("v2_1.lance"), version="2.1"
            ) as writer:
                for batch in table.to_batches(max_chunksize=32 * 1024):
                    writer.write_batch(batch)
            new_lance_size = tempdir.joinpath("v2_1.lance").stat().st_size
            print(f"{path.name},{parquet_size},{old_lance_size},{new_lance_size}")

            reader = LanceFileReader(tempdir.joinpath("v2_1.lance"))
            # print(reader.metadata())
