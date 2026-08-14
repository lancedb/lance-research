#!/usr/bin/env python3
"""
Download the Alpaca dataset and store it in a LanceDB table.

Requires:
    LANCEDB_URI  URI of the LanceDB database (e.g. s3://my-bucket/data)

Run once before training:
    LANCEDB_URI=s3://my-bucket/data python prepare_data.py
"""

import os
import sys

# Use lancedb from the submodule (requires the Rust extension to be built:
#   cd lancedb/python && maturin develop --release)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lancedb", "python", "python"))

import pyarrow as pa
import lancedb
from datasets import load_dataset

TABLE_NAME = "alpaca"
# Must divide evenly into world_size.
# 64 works for 1, 2, 4, 8, 16, or 32 GPUs.
NUM_SPLITS = 64


def main():
    lancedb_uri = os.environ.get("LANCEDB_URI")
    if not lancedb_uri:
        print("Error: LANCEDB_URI environment variable is not set.")
        sys.exit(1)

    print("Downloading Alpaca dataset...")
    dataset = load_dataset("tatsu-lab/alpaca", split="train")
    n_orig = len(dataset)

    # StreamingDataset requires num_rows % num_splits == 0
    n = (n_orig // NUM_SPLITS) * NUM_SPLITS
    dataset = dataset.select(range(n))
    print(f"Dataset: {n_orig} examples -> {n} (truncated to nearest multiple of {NUM_SPLITS})")

    table_data = pa.table({
        "instruction": pa.array(dataset["instruction"], type=pa.string()),
        "input": pa.array(dataset["input"], type=pa.string()),
        "output": pa.array(dataset["output"], type=pa.string()),
    })

    db = lancedb.connect(lancedb_uri)

    if TABLE_NAME in db.table_names():
        print(f"Dropping existing '{TABLE_NAME}' table...")
        db.drop_table(TABLE_NAME)

    table = db.create_table(TABLE_NAME, data=table_data)
    print(f"Stored {len(table)} rows in '{lancedb_uri}/{TABLE_NAME}'")
    print(f"Schema: {table.schema}")


if __name__ == "__main__":
    main()
