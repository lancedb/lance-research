# Multi-segment indexing benchmark

This experiment compares three Lance vector-index families, each built with 1,
2, 3, 4, or 5 physical index segments:

- `IVF_PQ`: IVF partitions with product-quantized search
- `IVF_HNSW_PQ`: HNSW search within IVF partitions, with PQ compression
- `IVF_HNSW_SQ`: HNSW search within IVF partitions, with scalar quantization

The vector dataset, queries, IVF target partition size, `nprobes`, and
refinement factor remain fixed. PQ and HNSW construction/query parameters are
also fixed wherever they apply.

Each physical segment owns a disjoint group of Lance data fragments and trains
its own index model. Lance commits the segments under one logical index. At
query time Lance searches the segments and merges their candidates into one
global top-k result.

The primary scale-out hypothesis is about **index construction**: a monolithic
index has one large training/shuffle working set and one long build critical
path. With multiple segments, each worker handles only about `rows / segments`
rows and the segment builds can run concurrently. The benchmark therefore
measures actual concurrent build wall time, process CPU time, summed worker
elapsed time, build throughput, committed index size, and the largest row set
assigned to one worker.
The last value is a working-set proxy, not a direct resident-memory measurement.
Query latency and recall measure the cost paid for that build scalability.

## Local SIFT1M run

From the repository root:

```bash
.venv/bin/python "vector search/2-multisegment-indexing/attempt_1.py"
.venv/bin/python "vector search/2-multisegment-indexing/plot_results.py"
```

The benchmark writes to `multisegment_benchmark/`:

- `results.csv`: one row per timed query repetition
- `summary.csv`: aggregate metrics by segment count
- `results_readable.txt`: aligned human-readable summary
- `metadata.json`: arguments, timing definitions, and dependency versions
- nine PNG plots covering query latency/recall, build throughput/time/index size,
  per-worker working set, and tradeoffs

## Quick smoke test

```bash
.venv/bin/python "vector search/2-multisegment-indexing/attempt_1.py" \
  --rows 10000 --segment-counts 1,2 --data-fragments 4 \
  --index-types IVF_PQ,IVF_HNSW_PQ,IVF_HNSW_SQ \
  --queries 3 --warmup-queries 1 --repetitions 2 \
  --ground-truth-mode exact \
  --work-dir /tmp/lance-multisegment-smoke
```

## About the fixed 5B scale

The checked-in SIFT data contains 1M vectors, so the default is a runnable local
development version, not a 5B result. For the intended 5B experiment, pass a
5B-vector fvecs file, a compatible query fvecs file, and precomputed ivecs
ground truth:

```bash
python attempt_1.py \
  --rows 5000000000 \
  --base-vectors /data/base-5b.fvecs \
  --query-vectors /data/query.fvecs \
  --ground-truth /data/groundtruth.ivecs \
  --ground-truth-mode file
```

By default, the runner dispatches one concurrent local worker per segment. Use
`--build-workers 1` for sequential construction or set an explicit worker cap.
At 5B scale these same independent builds should normally run on separate
machines; a one-machine run can suffer CPU, memory, and I/O contention and will
understate distributed speedup. Query latency is measured around the native
Lance call with `perf_counter_ns`; Python launch overhead is small but not zero.
A Rust harness is the appropriate follow-up when sub-millisecond precision
matters.

Use `--index-types` to run a subset. The alias `IVF-PQ-HNSW` is accepted and
normalized to Lance's official `IVF_HNSW_PQ` name. HNSW behavior is controlled
with `--hnsw-m`, `--hnsw-ef-construction`, and `--hnsw-ef`.
