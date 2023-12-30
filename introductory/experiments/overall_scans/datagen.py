import abc
import argparse
import contextlib
import os
import shutil
import logging
import time

import duckdb
import lance
import pyarrow as pa
import pyarrow.dataset as ds


def get_size(start_path):
    total_size = 0
    for dirpath, _, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def size_to_str(size: int):
    if size >= 1024 * 1024:
        return f"{size // 1024 // 1024}M"
    elif size >= 1024:
        return f"{size // 1024}K"
    else:
        return f"{size}"


@contextlib.contextmanager
def time_log():
    start = time.time()
    yield
    end = time.time()
    logging.info("Finished in: %.1fs", end - start)


class DatasetGenerator(abc.ABC):
    """Generate datasets for scan benchmarks."""

    # Template method: concrete subclasses implement write_parquet and write_lance
    def generate(self, base_path, row_group_sizes):
        shutil.rmtree(base_path, ignore_errors=True)
        for row_group_size in row_group_sizes:
            size_str = size_to_str(row_group_size)

            pq_path = os.path.join(base_path, f"parquet-{size_str}")
            self.write_parquet(pq_path, row_group_size)

            lance_path = os.path.join(base_path, f"lance-{size_str}")
            self.write_lance(lance_path, row_group_size)

    @abc.abstractmethod
    def write_parquet(self, path, row_group_size):
        pass

    @abc.abstractmethod
    def write_lance(self, path, row_group_size):
        pass


class TPCHGenerator(DatasetGenerator):
    def __init__(self):
        duckdb.sql("INSTALL tpch")
        duckdb.sql("LOAD tpch")

        logging.info("Generating TPCH dataset")
        with time_log():
            duckdb.sql("CALL dbgen(sf = 10)")

    def write_parquet(self, path, row_group_size):
        if row_group_size <= 1024:
            logging.warning("Skipping Parquet generation for row_group_size <= 1K, as it causes OOM for DuckDB.")
            return

        logging.info(
            "Writing TPCH dataset to Parquet (group_size = %s)",
            size_to_str(row_group_size),
        )
        with time_log():
            # DuckDB doesn't create the directory for us
            os.makedirs(path)
            duckdb.sql(
                f"COPY lineitem TO '{path}/data.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE {row_group_size})"
            )

    def write_lance(self, path, row_group_size):
        logging.info(
            "Writing TPCH dataset to Lance (group_size = %s)",
            size_to_str(row_group_size),
        )
        with time_log():
            reader = duckdb.sql("SELECT * FROM lineitem").fetch_arrow_reader(
                row_group_size
            )
            lance.write_dataset(reader, path, max_rows_per_group=row_group_size)


class LAIONGenerator(DatasetGenerator):
    def __init__(self):
        try:
            self.data_path = os.environ["LAION_1M_DATA"]
        except KeyError:
            raise Exception("LAION_1M_DATA environment variable not set")

    def write_parquet(self, path, row_group_size):
        logging.info(
            "Writing LAION dataset to Parquet (group_size = %s)",
            size_to_str(row_group_size),
        )
        with time_log():
            pq_data = ds.dataset(self.data_path, format="parquet")
            reader = pq_data.scanner().to_reader()
            ds.write_dataset(
                reader, path, format="parquet", max_rows_per_group=row_group_size
            )

    def write_lance(self, path, row_group_size):
        logging.info(
            "Writing LAION dataset to Lance (group_size = %s)",
            size_to_str(row_group_size),
        )
        with time_log():
            pq_data = ds.dataset(self.data_path, format="parquet")
            # Need to transform 'element' name to 'item'
            lance_ds = lance.write_dataset([], path, schema=pq_data.schema)
            schema = lance_ds.schema
            reader = pq_data.scanner().to_reader()

            def scan():
                for batch in reader:
                    yield pa.Table.from_batches([batch]).cast(schema).to_batches()[0]

            lance_ds = lance.write_dataset(
                scan(),
                path,
                mode="append",
                schema=schema,
                max_rows_per_group=row_group_size,
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--dataset", choices=["tpch", "laion"], required=False)
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)

    if args.dataset == "tpch" or args.dataset is None:
        TPCHGenerator().generate("data/tpch", [1024, 100 * 1024])

    if args.dataset == "laion" or args.dataset is None:
        LAIONGenerator().generate("data/laion", [1024, 100 * 1024])
