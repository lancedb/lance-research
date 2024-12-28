import shutil
import time

import lance
import numpy as np
import os
import pyarrow as pa
from tqdm import tqdm

# 32M rows * 32 values per row ~ 1GB
NUM_ROWS = 32_000_000
ROWS_PER_FILE = 1_000_000
NUM_FILES = NUM_ROWS // ROWS_PER_FILE
DATASET_PATH_BASE = "/tmp/random_depth"
DURATION_S = 10
TAKE_SIZE = 100
DISK_NAME = "nvme0n1"


def tablegen(batch_size: int):
    items = np.random.rand(32 * batch_size)
    items = pa.array(items, pa.float64())
    nulls = np.random.choice([True, False], 32 * batch_size, p=[0.9, 0.1])
    nulls = pa.array(nulls, pa.bool_())

    values = pa.Array.from_buffers(
        pa.list_(items.type, 32),
        batch_size,
        [nulls.buffers()[1]],
        children=[items],
    )
    values2 = pa.Array.from_buffers(
        pa.list_(pa.list_(items.type, 16), 2),
        batch_size,
        [nulls.buffers()[1]],
        children=[
            pa.Array.from_buffers(
                pa.list_(items.type, 16),
                batch_size * 2,
                [nulls.buffers()[1]],
                children=[items],
            )
        ],
    )
    values3 = pa.Array.from_buffers(
        pa.list_(pa.list_(pa.list_(items.type, 8), 2), 2),
        batch_size,
        [nulls.buffers()[1]],
        children=[
            pa.Array.from_buffers(
                pa.list_(pa.list_(items.type, 8), 2),
                batch_size * 2,
                [nulls.buffers()[1]],
                children=[
                    pa.Array.from_buffers(
                        pa.list_(items.type, 8),
                        batch_size * 4,
                        [nulls.buffers()[1]],
                        children=[items],
                    )
                ],
            )
        ],
    )
    values4 = pa.Array.from_buffers(
        pa.list_(pa.list_(pa.list_(pa.list_(items.type, 4), 2), 2), 2),
        batch_size,
        [nulls.buffers()[1]],
        children=[
            pa.Array.from_buffers(
                pa.list_(pa.list_(pa.list_(items.type, 4), 2), 2),
                batch_size * 2,
                [nulls.buffers()[1]],
                children=[
                    pa.Array.from_buffers(
                        pa.list_(pa.list_(items.type, 4), 2),
                        batch_size * 4,
                        [nulls.buffers()[1]],
                        children=[
                            pa.Array.from_buffers(
                                pa.list_(items.type, 4),
                                batch_size * 8,
                                [nulls.buffers()[1]],
                                children=[items],
                            )
                        ],
                    )
                ],
            )
        ],
    )
    return pa.record_batch(
        {
            "values": values,
            "values2": values2,
            "values3": values3,
            "values4": values4,
        }
    )


def datagen(file_version: str) -> lance.LanceDataset:
    dataset_path = f"{DATASET_PATH_BASE}/{file_version.replace('.', '_')}.lance"

    def batch_gen():
        print("Generating test data")
        for i in tqdm(range(NUM_FILES)):
            yield tablegen(ROWS_PER_FILE)

    try:
        ds = lance.dataset(dataset_path)
        if ds.count_rows() < NUM_ROWS:
            shutil.rmtree(dataset_path)
            raise Exception("Dataset is too small")
        return ds
    except:
        sample_batch = tablegen(1)
        return lance.write_dataset(
            batch_gen(),
            dataset_path,
            schema=sample_batch.schema,
            data_storage_version=file_version,
        )


def get_disk_read_count():
    return lance.iops_counter()


def drop_caches():
    assert os.system('sudo sh -c "sync; echo 1 > /proc/sys/vm/drop_caches"') == 0


def run_experiment(dataset: lance.LanceDataset):
    indices = np.arange(NUM_ROWS)
    np.random.shuffle(indices)

    counts = []
    for col in ds.schema.names:
        drop_caches()
        start = time.time()
        offset = 0
        rows_read = 0
        pbar = tqdm(total=DURATION_S)
        elapsed = 0
        starting_read_count = get_disk_read_count()
        while elapsed < DURATION_S:
            batch_indices = indices[offset : offset + TAKE_SIZE]
            batch = dataset.take(batch_indices, columns=[col])
            offset += TAKE_SIZE
            if offset + TAKE_SIZE >= NUM_ROWS:
                offset = 0
            rows_read += batch.num_rows
            elapsed = min(DURATION_S, time.time() - start)
            pbar.update(elapsed - pbar.n)
        pbar.close()
        ending_read_count = get_disk_read_count()
        num_reads = ending_read_count - starting_read_count
        print(f"Column {col} read {rows_read} rows using {num_reads} reads")
        counts.append(rows_read)
    print(counts)


if __name__ == "__main__":
    ds = datagen("2.0")
    # print(ds.schema)
    # ds.take([5000], columns=["values4"])
    # drop_caches()
    # num_reads = get_disk_read_count()
    # ds.take(
    #     [100000, 200000, 300000, 400000, 500000, 600000, 700000], columns=["values4"]
    # )
    # print(f"Reads: {get_disk_read_count() - num_reads}")

    run_experiment(ds)
    ds = datagen("2.1")
    run_experiment(ds)
