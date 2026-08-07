"""Lance vector-search quickstart: exact search versus IVF_PQ ANN search.

Before running, download and unpack the SIFT dataset so that
``sift/sift_base.fvecs`` exists.  Install the dependencies with:

    python3 -m pip install pylance numpy pandas pyarrow duckdb
"""

from pathlib import Path
import time

import duckdb
import lance
import numpy as np
from lance.vector import vec_to_table


URI = "vec_data.lance"
SIFT_BASE = Path("sift/sift_base.fvecs")
NUM_VECTORS = 1_000_000
VECTOR_DIMENSION = 128
K = 10


def load_sift_vectors() -> np.ndarray:
    """Read the one-million 128-dimensional SIFT base vectors."""
    if not SIFT_BASE.exists():
        raise FileNotFoundError(
            f"Could not find {SIFT_BASE}. Download and unpack sift.tar.gz first."
        )

    # Each fvec record starts with a 4-byte dimension field, followed by floats.
    # The tutorial's dataset contains 1,000,000 records of 128 float32 values.
    with SIFT_BASE.open("rb") as file:
        raw = file.read()

    expected_bytes = NUM_VECTORS * (4 + VECTOR_DIMENSION * 4)
    if len(raw) < expected_bytes:
        raise ValueError(
            f"{SIFT_BASE} is too small for the SIFT1M base dataset "
            f"({len(raw):,} bytes found; expected {expected_bytes:,})."
        )

    # Skip each per-record dimension field, then retain the float values.
    records = np.frombuffer(raw[:expected_bytes], dtype=np.float32).reshape(
        NUM_VECTORS, VECTOR_DIMENSION + 1
    )
    return records[:, 1:]


def create_dataset() -> None:
    """Convert SIFT vectors to Lance format, as in the quickstart."""
    print("Converting SIFT1M vectors to a Lance dataset...")
    vectors = load_sift_vectors()
    table = vec_to_table(dict(enumerate(vectors)))
    lance.write_dataset(
        table,
        URI,
        max_rows_per_group=8192,
        max_rows_per_file=1024 * 1024,
    )


def main() -> None:
    create_dataset()
    sift1m = lance.dataset(URI)

    # Use random SIFT vectors as search queries, matching the tutorial.
    samples = duckdb.query(
        "SELECT vector FROM sift1m USING SAMPLE 100"
    ).to_df().vector

    # Exact (brute-force) vector search before an ANN index exists.
    start = time.perf_counter()
    exact_results = sift1m.to_table(
        nearest={"column": "vector", "q": samples.iloc[0], "k": K}
    )
    exact_seconds = time.perf_counter() - start
    print(f"\nExact search time (sec): {exact_seconds:.6f}")
    print(exact_results.to_pandas())

    # Build the IVF_PQ approximate-nearest-neighbor index from the tutorial.
    print("\nBuilding IVF_PQ ANN index...")
    sift1m.create_index(
        "vector",
        index_type="IVF_PQ",
        num_partitions=256,
        num_sub_vectors=16,
    )

    # Reopen after index creation, then measure ANN search across all 100 queries.
    sift1m = lance.dataset(URI)
    total_seconds = 0.0
    ann_results = None
    for query in samples:
        start = time.perf_counter()
        ann_results = sift1m.to_table(
            nearest={"column": "vector", "q": query, "k": K}
        )
        total_seconds += time.perf_counter() - start

    print(f"\nANN average search time (sec): {total_seconds / len(samples):.6f}")
    print(ann_results.to_pandas())


if __name__ == "__main__":
    main()
