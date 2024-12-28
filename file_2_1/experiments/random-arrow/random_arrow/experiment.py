from concurrent.futures import ThreadPoolExecutor
import os
import threading
from datetime import datetime

import numpy as np
import pyarrow as pa

NUM_ROWS = 1_000_000_000
ROWS_PER_FILE = 1_000_000
ROWS_PER_BATCH = 100
NUM_THREADS = 20
NUM_FILES = int(NUM_ROWS / ROWS_PER_FILE)


def file_path(file_index: int):
    return f"/tmp/file_{file_index}.arrow"


def create_files():
    print("Creating files")
    for file_index in range(NUM_FILES):
        if os.path.exists(file_path(file_index)):
            continue
        print(f"Creating file {file_index}")
        floats = np.random.random(ROWS_PER_FILE)
        table = pa.table({"floats": pa.array(floats, type=pa.float64())})

        with pa.ipc.RecordBatchFileWriter(
            file_path(file_index), table.schema
        ) as writer:
            writer.write_table(table)


def perform_take(indices, files):
    indices.sort()
    next_batch = []
    last_file_index = -1
    for idx in indices:
        print(f"Next batch has {len(next_batch)} rows")
        file_index = int(idx / ROWS_PER_FILE)
        row_index = idx % ROWS_PER_FILE
        if file_index != last_file_index and len(next_batch) > 0:
            print(f"Array take of {len(next_batch)} rows")
            arr = files[file_index].column(0).take(next_batch)
            print(f"Array has {len(arr)} rows")
            next_batch = []
        last_file_index = file_index
        next_batch.append(row_index)
    if len(next_batch) > 0:
        print(f"Array take of {len(next_batch)} rows")
        arr = files[file_index].column(0).take(next_batch)
        print(f"Array has {len(arr)} rows")


def run_experiment():
    num_iterations = 0

    print("Loading files")
    files = []
    for file_index in range(NUM_FILES):
        source = pa.memory_map(file_path(file_index), "rb")
        files.append(pa.ipc.open_file(source).read_all())

    indices = list(range(100_000_000))  # /*NUM_ROWS*/))
    print("Shuffling indices")
    np.random.shuffle(indices)

    os.system('sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"')

    print("Making measurements")
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:

        def worker(thread_index: int):
            print(f"Thread {thread_index} started")
            start = datetime.now()
            indices_offset = thread_index * (NUM_ROWS // NUM_THREADS)
            num_iterations = 0
            while (datetime.now() - start).total_seconds() < 10:
                batch_indices = indices[
                    indices_offset : indices_offset + ROWS_PER_BATCH
                ]
                print(
                    f"Thread {thread_index} performing take starting at {indices_offset}"
                )
                perform_take(batch_indices, files)
                indices_offset += ROWS_PER_BATCH % NUM_ROWS
                num_iterations += ROWS_PER_BATCH
            return num_iterations

        futures = []
        for thread_index in range(NUM_THREADS):
            futures.append(executor.submit(worker, thread_index))

        num_iterations = sum([f.result() for f in futures])

        print(f"Performed {num_iterations} iterations in 10 seconds")


if __name__ == "__main__":
    create_files()
    run_experiment()
