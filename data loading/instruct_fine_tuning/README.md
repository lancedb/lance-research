# Instruct Fine-Tuning with LanceDB

Fine-tunes a causal language model on the
[Alpaca](https://huggingface.co/datasets/tatsu-lab/alpaca) instruction-following
dataset (52k examples) using LanceDB as the training data source.

The example demonstrates:
- Storing a fine-tuning dataset in a LanceDB table
- Streaming training batches from LanceDB via `StreamingDataset`
- Elastic determinism — consistent global batches across any number of GPUs
- Mid-epoch resumability via `state_dict()` / `load_state_dict()`
- LoRA fine-tuning with PEFT to keep memory requirements low

## Prerequisites

### 1. Build the LanceDB submodule

The `lancedb/` submodule tracks the `feat-elastic-dataloader` branch, which
contains `StreamingDataset`. Its Rust extension must be compiled before running:

```bash
cd lancedb/python
maturin develop --release
cd ../..
```

Requires Rust and [maturin](https://github.com/PyO3/maturin):
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
pip install maturin
```

### 2. Install Python dependencies

```bash
pip install transformers peft accelerate datasets pyarrow
```

## Usage

### Prepare the dataset

Downloads Alpaca from HuggingFace and stores it in a LanceDB table at `./data`:

```bash
python prepare_data.py
```

### Run training

Single GPU:
```bash
python train.py
```

8 GPUs:
```bash
torchrun --nproc_per_node=8 train.py
```

## Configuration

Key constants at the top of `train.py`:

| Variable | Default | Description |
|----------|---------|-------------|
| `MODEL_NAME` | `Qwen/Qwen2.5-0.5B-Instruct` | Base model. Swap for a larger model once throughput is confirmed (e.g. `meta-llama/Llama-3.2-1B`). |
| `NUM_EPOCHS` | `100` | Number of training epochs. |
| `BATCH_SIZE` | `4` | Per-GPU micro-batch size. |
| `MAX_LENGTH` | `512` | Sequence length in tokens. |
| `NUM_SPLITS` | `64` | LanceDB table splits. Must divide evenly into `world_size × NUM_WORKERS`. |
| `NUM_WORKERS` | `0` | DataLoader workers per GPU. Increase if training is I/O-bound. |
| `LEARNING_RATE` | `2e-4` | AdamW learning rate. |

## Output

Training logs tokens/second throughput at every 50 steps and a per-epoch
summary. Checkpoints (including dataset state for resumption) are saved to
`./checkpoints/` after each epoch. The final LoRA adapter is written to
`./checkpoints/final_adapter/`.

## Resuming from a checkpoint

```python
checkpoint = torch.load("checkpoints/checkpoint_epoch_010.pt")
dataset = StreamingDataset(table, num_splits=NUM_SPLITS, ...)
dataset.load_state_dict(checkpoint["dataset_state"])
optimizer.load_state_dict(checkpoint["optimizer_state"])
```
