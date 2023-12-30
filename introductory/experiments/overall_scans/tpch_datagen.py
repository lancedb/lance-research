import os
import shutil

import duckdb
import lance

duckdb.sql("INSTALL tpch")
duckdb.sql("LOAD tpch")

duckdb.sql("CALL dbgen(sf = 10)")

query = "SELECT * FROM lineitem"

shutil.rmtree("data", ignore_errors=True)

if not os.path.exists("data/lineitem_parquet"):
    os.mkdir("data/lineitem_parquet")
duckdb.sql(f"COPY ({query}) TO 'data/lineitem_parquet/data.parquet' (FORMAT PARQUET)")

reader = duckdb.sql(query).fetch_arrow_reader(10 * 1024)
lance.write_dataset(reader, "data/lineitem_lance")

reader = duckdb.sql(query).fetch_arrow_reader(10 * 1024)
lance.write_dataset(reader, "data/lineitem_lance2", max_rows_per_group=100 * 1024)


def get_size(start_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


# Get total size of the lineitem_lance directory
lance_size_gb = get_size("data/lineitem_lance") / 1024 / 1024 / 1024
lance2_size_gb = get_size("data/lineitem_lance2") / 1024 / 1024 / 1024
parquet_size_gb = get_size("data/lineitem_parquet") / 1024 / 1024 / 1024


print(f"Parquet size: {parquet_size_gb} GB")
print(f"Lance size: {lance_size_gb} GB")
print(f"Lance2 size: {lance2_size_gb} GB")
