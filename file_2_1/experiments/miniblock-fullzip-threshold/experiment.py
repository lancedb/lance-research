import shutil
import time

import lance
import numpy as np
import os
import pyarrow as pa
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

NUM_ROWS = 16_000_000
ROWS_PER_FILE = 1_000_000
NUM_FILES = NUM_ROWS // ROWS_PER_FILE
DATASET_PATH_BASE = "/tmp/mini_vs_zip"
DURATION_S = 10
TAKE_SIZE = 100
NUM_THREADS = 8


def tablegen(batch_size: int, style: str):
    buffer = np.random.bytes(batch_size * 1024)
    nulls = np.random.choice([True, False], batch_size, p=[0.9, 0.1])
    nulls = pa.array(nulls, pa.bool_())
    nulls_buffer = nulls.buffers()[1]
    pa_buffer = pa.py_buffer(buffer)
    arrays = []
    fields = []
    arrays.append(
        pa.Array.from_buffers(pa.float64(), batch_size, [nulls_buffer, pa_buffer])
    )
    fields.append(
        pa.field(
            "double",
            pa.float64(),
            metadata={b"lance-encoding:structural-encoding": style.encode("utf-8")},
        )
    )
    for size in [4, 8, 16, 32, 64, 128, 256, 512, 1024]:
        dtype = pa.binary(size)
        arr = pa.Array.from_buffers(dtype, batch_size, [nulls_buffer, pa_buffer])
        arrays.append(arr)
        fields.append(
            pa.field(
                f"fixed_{size}",
                dtype,
                metadata={b"lance-encoding:structural-encoding": style.encode("utf-8")},
            )
        )

    schema = pa.schema(fields)

    return pa.record_batch(arrays, schema=schema)


def datagen(style) -> lance.LanceDataset:
    dataset_path = f"{DATASET_PATH_BASE}/{style}.lance"

    def batch_gen():
        print("Generating test data")
        for i in tqdm(range(NUM_FILES)):
            yield tablegen(ROWS_PER_FILE, style)

    try:
        ds = lance.dataset(dataset_path)
        if ds.count_rows() < NUM_ROWS:
            shutil.rmtree(dataset_path)
            raise Exception("Dataset is too small")
        return ds
    except:
        sample_batch = tablegen(1, style)
        return lance.write_dataset(
            batch_gen(),
            dataset_path,
            schema=sample_batch.schema,
            data_storage_version="2.1",
        )


def drop_caches():
    assert os.system('sudo sh -c "sync; echo 1 > /proc/sys/vm/drop_caches"') == 0


def run_experiment_random_access(dataset: lance.LanceDataset):
    indices = np.arange(NUM_ROWS)
    np.random.shuffle(indices)

    counts = []
    for col in ds.schema.names:
        drop_caches()
        start = time.time()
        with ThreadPoolExecutor() as executor:

            def thread_task(thread_index):
                offset = 0 * NUM_ROWS // NUM_THREADS
                rows_read = 0
                elapsed = 0
                while elapsed < DURATION_S:
                    batch_indices = indices[offset : offset + TAKE_SIZE]
                    batch = dataset.take(batch_indices, columns=[col])
                    offset += TAKE_SIZE
                    if offset + TAKE_SIZE >= NUM_ROWS:
                        offset = 0
                    rows_read += batch.num_rows
                    elapsed = min(DURATION_S, time.time() - start)
                return rows_read

            starting_read_count = lance.bytes_read_counter()
            starting_iops_count = lance.iops_counter()
            futures = [
                executor.submit(thread_task, thread_index)
                for thread_index in range(NUM_THREADS)
            ]
            rows_read = sum([f.result() for f in futures])
            ending_read_count = lance.bytes_read_counter()
            ending_iops_count = lance.iops_counter()
            num_reads = ending_read_count - starting_read_count
            num_iops = ending_iops_count - starting_iops_count
            print(
                f"Column {col} read {rows_read} rows using {num_reads} bytes and {num_iops} iops"
            )
            counts.append(rows_read)
    print(counts)


def run_experiment_full_scan(dataset: lance.LanceDataset):
    counts = []
    for col in ds.schema.names:
        start = time.time()
        starting_read_count = lance.bytes_read_counter()
        starting_iops_count = lance.iops_counter()
        rows_read = 0
        in_mem_bytes_read = 0
        while time.time() - start < DURATION_S:
            tbl = dataset.to_table(columns=[col], batch_size=32 * 1024)
            rows_read += tbl.num_rows
            in_mem_bytes_read += tbl.get_total_buffer_size()
        ending_read_count = lance.bytes_read_counter()
        ending_iops_count = lance.iops_counter()
        num_reads = ending_read_count - starting_read_count
        num_iops = ending_iops_count - starting_iops_count
        gbps = in_mem_bytes_read / 1e9 / DURATION_S
        print(
            f"Column {col} read {rows_read} rows using {num_reads} bytes and {num_iops} iops ({gbps:.2f} GB/s)"
        )
        counts.append(rows_read)
    print(counts)


if __name__ == "__main__":
    ds = datagen("miniblock")
    # run_experiment_random_access(ds)
    run_experiment_full_scan(ds)
    ds = datagen("fullzip")
    # run_experiment_random_access(ds)
    run_experiment_full_scan(ds)
