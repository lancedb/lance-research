# Multi-segment indexing benchmark

This experiment compares one logical Lance IVF-PQ index built from 1, 2, 3,
4, or 5 physical index segments. The vector dataset, queries, IVF target
partition size, PQ configuration, `nprobes`, and refinement factor remain fixed.

Each physical segment owns a disjoint group of Lance data fragments and trains
its own IVF/PQ model. Lance commits the segments under one logical index. At
query time Lance searches the segments and merges their candidates into one
global top-k result.

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
- six PNG plots covering latency, recall, throughput, build time, and tradeoffs

## Quick smoke test

```bash
.venv/bin/python "vector search/2-multisegment-indexing/attempt_1.py" \
  --rows 10000 --segment-counts 1,2 --data-fragments 4 \
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

At 5B scale the segment builds should normally be dispatched concurrently on
separate workers. This Python runner builds them sequentially so it can run on
one machine and reports both total wall time and process CPU time. Query latency
is measured around the native Lance call with `perf_counter_ns`; Python launch
overhead is small but not zero. A Rust harness is the appropriate follow-up when
sub-millisecond precision matters.
