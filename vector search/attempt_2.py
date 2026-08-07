import shutil
import time
from pathlib import Path

import lance
import numpy as np
import pyarrow as pa


# ---------------- Configuration ----------------

URI = Path("benchmark.lance")
SIFT_BASE = Path("sift/sift_base.fvecs")
SIFT_NUM_VECTORS = 1_000_000
DIM = 128
INITIAL_SIZES = [100_000, 500_000]
INGEST_BATCH_SIZE = 50_000

K = 100
NPROBES_VALUES = [8, 16, 32]
REFINE_FACTORS = [1, 5, 10]

NUM_PARTITIONS = 256
NUM_SUB_VECTORS = 16
NUM_QUERIES = 100
RNG = np.random.default_rng(0)


# ---------------- Data helpers ----------------

def make_table(vectors, start_id):
    vector_values = pa.array(
        np.ascontiguousarray(vectors, dtype=np.float32).reshape(-1),
        type=pa.float32(),
    )
    vector_column = pa.FixedSizeListArray.from_arrays(vector_values, DIM)
    return pa.table({
        "id": np.arange(
            start_id, start_id + len(vectors), dtype=np.int64
        ),
        "vector": vector_column,
    })


def load_sift_vectors():
    """Load the SIFT1M base vectors used by the Lance quickstart.

    An fvecs record stores a four-byte dimension followed by 128 float32
    values.  A memory map avoids holding another 512 MB copy in RAM.
    """
    if not SIFT_BASE.exists():
        raise FileNotFoundError(
            f"Could not find {SIFT_BASE}. Download and unpack sift.tar.gz first."
        )

    expected_bytes = SIFT_NUM_VECTORS * (DIM + 1) * np.dtype(np.float32).itemsize
    actual_bytes = SIFT_BASE.stat().st_size
    if actual_bytes < expected_bytes:
        raise ValueError(
            f"{SIFT_BASE} is too small for SIFT1M "
            f"({actual_bytes:,} bytes found; expected {expected_bytes:,})."
        )

    records = np.memmap(
        SIFT_BASE,
        dtype=np.float32,
        mode="r",
        shape=(SIFT_NUM_VECTORS, DIM + 1),
    )
    return records[:, 1:]


def sample_sift_vectors(sift_vectors, initial_size):
    """Select disjoint SIFT1M vectors for the initial data and ingestion batch."""
    required = initial_size + INGEST_BATCH_SIZE
    if required > len(sift_vectors):
        raise ValueError(
            f"Experiment needs {required:,} SIFT vectors, but only "
            f"{len(sift_vectors):,} are available."
        )

    selected = RNG.choice(len(sift_vectors), size=required, replace=False)
    return sift_vectors[selected[:initial_size]], sift_vectors[selected[initial_size:]]


def reset_dataset(initial_vectors):
    if URI.exists():
        shutil.rmtree(URI)

    lance.write_dataset(
        make_table(initial_vectors, 0),
        URI,
        max_rows_per_group=8192,
    )
    return lance.dataset(URI)


# ---------------- Index maintenance strategies ----------------

def rebuild_on_demand(dataset):
    """Baseline: rebuild the IVF-PQ index over the current dataset."""
    start = time.perf_counter()

    dataset.create_index(
        "vector",
        index_type="IVF_PQ",
        name="vector_idx",
        num_partitions=NUM_PARTITIONS,
        num_sub_vectors=NUM_SUB_VECTORS,
        replace=True,
    )

    return time.perf_counter() - start


def maintain_spfresh(dataset):
    """
    Replace this with the project's SPFresh maintenance call.

    This must update the existing index incrementally rather than
    rebuilding the entire index.
    """
    raise NotImplementedError(
        "Insert the actual SPFresh API here."
    )


# ---------------- Evaluation ----------------

def exact_neighbors(dataset, query):
    table = dataset.to_table(
        nearest={
            "column": "vector",
            "q": query,
            "k": K,
            "use_index": False,
        }
    )
    return set(table["id"].to_pylist())


def ann_neighbors(dataset, query, nprobes, refine_factor):
    table = dataset.to_table(
        nearest={
            "column": "vector",
            "q": query,
            "k": K,
            "nprobes": nprobes,
            "refine_factor": refine_factor,
        }
    )
    return set(table["id"].to_pylist())


def measure_recall(dataset, queries, nprobes, refine_factor):
    recalls = []

    for query in queries:
        truth = exact_neighbors(dataset, query)
        result = ann_neighbors(
            dataset, query, nprobes, refine_factor
        )

        recalls.append(len(truth & result) / K)

    return float(np.mean(recalls))


# ---------------- One experiment ----------------

def run_experiment(
    approach,
    initial_size,
    nprobes,
    refine_factor,
    sift_vectors,
):
    # Use real SIFT1M embeddings rather than synthetic Gaussian vectors.
    # The two sets are disjoint, so the appended batch can safely supply queries.
    initial_vectors, new_vectors = sample_sift_vectors(sift_vectors, initial_size)

    dataset = reset_dataset(initial_vectors)

    # Build the initial index.
    dataset.create_index(
        "vector",
        index_type="IVF_PQ",
        name="vector_idx",
        num_partitions=NUM_PARTITIONS,
        num_sub_vectors=NUM_SUB_VECTORS,
    )

    # Ingest new data.
    dataset = lance.write_dataset(
        make_table(new_vectors, initial_size),
        URI,
        mode="append",
    )

    # Maintain the index.
    if approach == "rebuild":
        build_seconds = rebuild_on_demand(dataset)
    elif approach == "spfresh":
        build_seconds = maintain_spfresh(dataset)
    else:
        raise ValueError(approach)

    dataset = lance.dataset(URI)

    # Query vectors come from the newly ingested data.
    query_ids = RNG.choice(
        len(new_vectors),
        size=NUM_QUERIES,
        replace=False,
    )
    queries = new_vectors[query_ids]

    recall = measure_recall(
        dataset,
        queries,
        nprobes,
        refine_factor,
    )

    ingestion_rate = INGEST_BATCH_SIZE / build_seconds

    return {
        "approach": approach,
        "initial_size": initial_size,
        "nprobes": nprobes,
        "refine_factor": refine_factor,
        "recall_at_100": recall,
        "build_seconds": build_seconds,
        "estimated_vectors_per_second": ingestion_rate,
        "num_partitions": NUM_PARTITIONS,
    }


# ---------------- Benchmark sweep ----------------

def main():
    results = []
    sift_vectors = load_sift_vectors()

    for approach in ["rebuild", "spfresh"]:
        for initial_size in INITIAL_SIZES:
            for nprobes in NPROBES_VALUES:
                for refine_factor in REFINE_FACTORS:

                    result = run_experiment(
                        approach,
                        initial_size,
                        nprobes,
                        refine_factor,
                        sift_vectors,
                    )

                    results.append(result)
                    print(result)

    print("\nFINAL RESULTS")
    for result in results:
        print(result)


if __name__ == "__main__":
    main()
