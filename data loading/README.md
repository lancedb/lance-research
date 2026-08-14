# LanceDB Model Training Examples

This repository contains examples that test and demonstrate the Lance elastic
dataloader as a data source for LLM training workloads.

Each example is self-contained in its own subdirectory and includes a script to
load data into LanceDB and a training script that reads from it using
[`StreamingDataset`](https://github.com/westonpace/lancedb/blob/feat-elastic-dataloader/python/python/lancedb/streaming.py)
— an elastic, resumable PyTorch `IterableDataset` backed by a LanceDB table.

## Examples

| Directory | Description |
|-----------|-------------|
| [`instruct_fine_tuning/`](instruct_fine_tuning/) | Fine-tune a causal LM on the Alpaca instruction-following dataset |
| [`resnet_benchmark/`](resnet_benchmark/) | Benchmark ResNet-50 training throughput to stress-test the data loader |

## What is the Lance dataloader?

`StreamingDataset` provides two properties that standard PyTorch dataloaders
lack:

- **Elastic determinism** — for a fixed `(num_splits, shuffle_seed, epoch)`,
  the set of samples in every global training step is identical regardless of
  how many GPUs or DataLoader workers are in use. Scaling the cluster up or
  down mid-run does not change which samples appear in which step.

- **Resumability** — `state_dict()` / `load_state_dict()` capture per-split
  consumption counts so training can resume from an exact mid-epoch position,
  even when the distributed topology changes between runs.
