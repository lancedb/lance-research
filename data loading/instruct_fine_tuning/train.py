#!/usr/bin/env python3
"""
Fine-tune an LLM on the Alpaca dataset stored in LanceDB.

Prerequisites:
    1. Build lancedb from the submodule:
           cd lancedb/python && maturin develop --release && cd ../..
    2. Install dependencies:
           pip install transformers peft accelerate object-store-python
    3. Prepare data:
           LANCEDB_URI=s3://my-bucket/data python prepare_data.py

Each log line breaks down time into:
  - tok/s:     tokens processed per second over the log interval (wall-clock)
  - rows/s:    training examples processed per second over the log interval
  - gpu_ms:    time spent on forward + backward + optimizer step (GPU-side)
  - data_ms:   wall-clock time per step minus gpu_ms (CPU/IO-side)
  - u/r/c/d:   pipeline row counts: unscanned / raw / cooked / done
  - MB/s:      raw bytes fetched from storage per second
  - ld:        avg time per step waiting for LanceDB I/O (load)
  - xf:        avg time per step for any StreamingDataset transform
  - cpu%:      system CPU utilization averaged over the log interval
  - gpu%/sm%:  GPU utilization (pynvml coarse or DCGM SM-active)

When data_ms >> gpu_ms the pipeline is I/O bound.
When gpu_ms >> data_ms the GPU is the bottleneck.

Required environment variables:
    LANCEDB_URI      URI of the LanceDB database  (e.g. s3://my-bucket/data)
    CHECKPOINT_URI   URI for checkpoint storage    (e.g. s3://my-bucket/checkpoints)

Single GPU:
    python train.py

8 GPUs:
    torchrun --nproc_per_node=8 train.py
"""

import io
import os
import sys
import logging
import time
import tempfile
import threading
from urllib.parse import urlparse

try:
    import psutil as _psutil
    _psutil.cpu_percent()  # prime so first interval read is accurate
    _psutil_ok = True
except ImportError:
    _psutil_ok = False

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lancedb", "python", "python"))

import torch
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import DataLoader
from transformers import AutoTokenizer, AutoModelForCausalLM
from peft import LoraConfig, get_peft_model, TaskType
from object_store import ObjectStore
import functools
import lancedb
from lancedb.streaming import StreamingDataset


def _open_table(uri: str, table_name: str):
    """Module-level factory so it survives pickling into DataLoader workers."""
    return lancedb.connect(uri).open_table(table_name)

# ── Configuration ──────────────────────────────────────────────────────────────
# Ungated, fast to download. Swap for a larger model once throughput is confirmed:
#   "meta-llama/Llama-3.2-1B"           (requires HF token)
#   "microsoft/Phi-3-mini-4k-instruct"
MODEL_NAME = "Qwen/Qwen2.5-0.5B-Instruct"

TABLE_NAME = "alpaca"

NUM_EPOCHS = 100
# Must divide evenly into world_size.
# 64 works for 1, 2, 4, 8, 16, or 32 GPUs.
NUM_SPLITS = 64
NUM_WORKERS = 1       # DataLoader workers per GPU.
BATCH_SIZE    = int(os.environ.get("BATCH_SIZE", 64))   # Per-GPU micro-batch size.
TORCH_COMPILE  = os.environ.get("TORCH_COMPILE",  "1") not in ("0", "false", "False")
GRAD_CKPT      = os.environ.get("GRAD_CKPT",      "1") not in ("0", "false", "False")
MEMORY_DEBUG   = os.environ.get("MEMORY_DEBUG",   "0") not in ("0", "false", "False")
MAX_LENGTH = int(os.environ.get("MAX_LENGTH", 512))  # Truncate sequences to this many tokens.
SHUFFLE_SEED = 42
_BASE_LR      = 2e-4                                          # LR calibrated for batch size 4.
_BASE_BATCH   = 4
LEARNING_RATE = float(os.environ.get("LEARNING_RATE",
                      _BASE_LR * (BATCH_SIZE / _BASE_BATCH))) # Linear scaling rule.
GRAD_CLIP     = float(os.environ.get("GRAD_CLIP", 1.0))
LOG_INTERVAL  = 50    # Log throughput every N steps.

# StreamingDataset I/O tuning
READ_BATCH_SIZE  = 64   # Rows fetched per split per take_offsets call.
PREFETCH_BATCHES = 4    # Concurrent read_batch_size fetches in flight per split.

# LoRA
LORA_R = 16
LORA_ALPHA = 32
LORA_DROPOUT = 0.05
LORA_TARGET_MODULES = ["q_proj", "v_proj", "k_proj", "o_proj"]


# ── Distributed helpers ────────────────────────────────────────────────────────

def setup_distributed():
    if "RANK" not in os.environ:
        return 0, 1
    dist.init_process_group("nccl")
    rank = dist.get_rank()
    world_size = dist.get_world_size()
    torch.cuda.set_device(rank)
    return rank, world_size


def cleanup_distributed():
    if dist.is_initialized():
        dist.destroy_process_group()


# ── Checkpoint helpers ─────────────────────────────────────────────────────────

def _parse_uri(uri: str):
    """Return (ObjectStore base URL, path prefix) from a full URI."""
    parsed = urlparse(uri)
    if parsed.scheme in ("s3", "gs", "az", "abfs"):
        base = f"{parsed.scheme}://{parsed.netloc}"
        prefix = parsed.path.lstrip("/")
    else:
        base = uri
        prefix = ""
    return base, prefix


def save_checkpoint(store: ObjectStore, prefix: str, name: str, obj) -> None:
    buf = io.BytesIO()
    torch.save(obj, buf)
    buf.seek(0)
    key = f"{prefix}/{name}" if prefix else name
    store.put(key, buf)


def save_adapter(store: ObjectStore, prefix: str, model, tokenizer) -> None:
    """Save a PEFT adapter to object storage via a temporary local directory."""
    with tempfile.TemporaryDirectory() as tmp:
        model.save_pretrained(tmp)
        tokenizer.save_pretrained(tmp)
        for fname in os.listdir(tmp):
            key = f"{prefix}/final_adapter/{fname}" if prefix else f"final_adapter/{fname}"
            with open(os.path.join(tmp, fname), "rb") as f:
                store.put(key, f.read())


# ── Data helpers ───────────────────────────────────────────────────────────────

def format_example(row: dict) -> str:
    instruction = str(row.get("instruction", ""))
    inp = str(row.get("input", ""))
    output = str(row.get("output", ""))
    if inp.strip():
        return (
            f"### Instruction:\n{instruction}\n\n"
            f"### Input:\n{inp}\n\n"
            f"### Response:\n{output}"
        )
    return f"### Instruction:\n{instruction}\n\n### Response:\n{output}"


class TokenizeTransform:
    """Tokenize a full Arrow read-batch inside StreamingDataset's transform hook.

    Runs on READ_BATCH_SIZE rows at once so the tokenizer's batched C++ path
    is used.  Each returned item holds variable-length (un-padded) tensors;
    padding to the training micro-batch maximum happens in collate_fn.
    Running here means tokenization time is captured in dataset.transform_time
    and shows up in the xf column of the training log.

    Implemented as a class (not a closure) so it is picklable — required when
    NUM_WORKERS > 0 uses forkserver to send the dataset to worker processes.
    """

    def __init__(self, tokenizer):
        self.tokenizer = tokenizer

    def __call__(self, batch):
        rows = batch.to_pylist()
        texts = [format_example(row) + self.tokenizer.eos_token for row in rows]
        encoded = self.tokenizer(texts, max_length=MAX_LENGTH, truncation=True)
        result = []
        for ids, mask in zip(encoded["input_ids"], encoded["attention_mask"]):
            ids_t  = torch.tensor(ids,  dtype=torch.long)
            mask_t = torch.tensor(mask, dtype=torch.long)
            labels = ids_t.clone()
            labels[labels == self.tokenizer.pad_token_id] = -100
            result.append({"input_ids": ids_t, "attention_mask": mask_t, "labels": labels})
        return result


def collate_fn(batch):
    """Pad a micro-batch of pre-tokenized variable-length sequences and stack."""
    max_len = max(r["input_ids"].shape[0] for r in batch)
    input_ids_out      = []
    attention_mask_out = []
    labels_out         = []
    for r in batch:
        pad = max_len - r["input_ids"].shape[0]
        input_ids_out.append(torch.nn.functional.pad(r["input_ids"],      (0, pad), value=0))
        attention_mask_out.append(torch.nn.functional.pad(r["attention_mask"], (0, pad), value=0))
        labels_out.append(torch.nn.functional.pad(r["labels"],       (0, pad), value=-100))
    return {
        "input_ids":      torch.stack(input_ids_out),
        "attention_mask": torch.stack(attention_mask_out),
        "labels":         torch.stack(labels_out),
    }


# ── Memory debug helper ───────────────────────────────────────────────────────

def mem_checkpoint(label: str) -> None:
    if not MEMORY_DEBUG or not torch.cuda.is_available():
        return
    torch.cuda.synchronize()
    allocated = torch.cuda.memory_allocated() / 1024**3
    reserved  = torch.cuda.memory_reserved()  / 1024**3
    logging.info(f"[mem] {label}: {allocated:.2f} GiB allocated, {reserved:.2f} GiB reserved")


# ── GPU utilisation sampler ────────────────────────────────────────────────────

class GpuUtilSampler:
    """Background-thread GPU utilisation sampler.

    Preferred path — pydcgm DCGM_FI_PROF_SM_ACTIVE: the fraction of time
    at least one warp is active on an SM, averaged over the measurement
    window.  This is the same metric nvtop shows on Ampere/Ada GPUs and
    gives a true SM-level utilisation rather than NVML's coarser "did any
    kernel run in the last ~167 ms" window counter.

    Fallback — pynvml nvmlDeviceGetUtilizationRates().gpu sampled at high
    frequency in this thread and averaged over the log interval.  Much
    better than a single point-in-time read (which always lands mid-compute
    and reports ~100%) even though it is still the coarser NVML counter.

    If neither library is available the sampler is a no-op.
    """

    def __init__(self, device_index: int, interval_ms: int = 100):
        self._interval  = interval_ms / 1000.0
        self._lock      = threading.Lock()
        self._samples   = []
        self._stop      = threading.Event()
        self._sample_fn = None
        self._label     = "n/a"

        self._sample_fn, self._label = self._init(device_index)
        threading.Thread(target=self._run, daemon=True).start()

    def _init(self, idx):
        try:
            return self._init_dcgm(idx), "sm%"
        except Exception as exc:
            logging.debug("pydcgm unavailable (%s); trying pynvml", exc)
        try:
            return self._init_pynvml(idx), "gpu%"
        except Exception as exc:
            logging.debug("pynvml unavailable (%s); GPU util disabled", exc)
        return None, "n/a"

    def _init_dcgm(self, idx):
        for p in ("/usr/local/dcgm/bindings/python3",):
            if p not in sys.path:
                sys.path.insert(0, p)

        import pydcgm                   # type: ignore
        import dcgm_structs as _s       # type: ignore
        import dcgm_fields  as _f       # type: ignore

        handle = pydcgm.DcgmHandle(opMode=_s.DCGM_OPERATION_MODE_AUTO)
        group  = pydcgm.DcgmGroup(handle, groupName=f"train_{idx}",
                                   groupType=_s.DCGM_GROUP_DEFAULT)
        fg     = pydcgm.DcgmFieldGroup(handle, f"train_fg_{idx}",
                                        [_f.DCGM_FI_PROF_SM_ACTIVE])
        group.samples.WatchFields(fg, 10_000, 30.0, 0)
        handle.GetSystem().UpdateAllFields(waitForUpdate=True)

        FIELD = _f.DCGM_FI_PROF_SM_ACTIVE

        def sample():
            handle.GetSystem().UpdateAllFields(waitForUpdate=False)
            latest = group.samples.GetLatest(fg)
            gpu_vals = latest.values
            if idx in gpu_vals and FIELD in gpu_vals[idx]:
                v = gpu_vals[idx][FIELD].value
                if isinstance(v, (int, float)) and v >= 0:
                    return float(v) * 100.0
            return None

        if sample() is None:
            raise RuntimeError(f"DCGM_FI_PROF_SM_ACTIVE returned no data for GPU {idx}")
        return sample

    def _init_pynvml(self, idx):
        import pynvml                   # type: ignore
        pynvml.nvmlInit()
        h = pynvml.nvmlDeviceGetHandleByIndex(idx)

        def sample():
            return float(pynvml.nvmlDeviceGetUtilizationRates(h).gpu)

        return sample

    def _run(self):
        if self._sample_fn is None:
            return
        while not self._stop.wait(self._interval):
            try:
                v = self._sample_fn()
                if v is not None:
                    with self._lock:
                        self._samples.append(v)
            except Exception:
                pass

    @property
    def label(self):
        return self._label

    def read_and_reset(self):
        with self._lock:
            if not self._samples:
                return "n/a"
            avg = sum(self._samples) / len(self._samples)
            self._samples.clear()
        return f"{avg:.0f}%"

    def close(self):
        self._stop.set()


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    rank, world_size = setup_distributed()
    is_main = rank == 0

    logging.basicConfig(
        level=logging.INFO if is_main else logging.WARNING,
        format="%(asctime)s %(message)s",
        datefmt="%H:%M:%S",
    )

    # ── Environment ───────────────────────────────────────────────────────────
    lancedb_uri = os.environ.get("LANCEDB_URI")
    if not lancedb_uri:
        print("Error: LANCEDB_URI environment variable is not set.")
        sys.exit(1)

    checkpoint_uri = os.environ.get("CHECKPOINT_URI")
    if not checkpoint_uri:
        print("Error: CHECKPOINT_URI environment variable is not set.")
        sys.exit(1)

    checkpoint_store_base, checkpoint_prefix = _parse_uri(checkpoint_uri)
    checkpoint_store = ObjectStore(checkpoint_store_base)

    if is_main:
        logging.info(f"World size: {world_size}")
        logging.info(f"Model: {MODEL_NAME}")
        logging.info(f"LanceDB: {lancedb_uri}")
        logging.info(f"Checkpoints: {checkpoint_uri}")

    # ── Model + tokenizer ──────────────────────────────────────────────────────
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    model = AutoModelForCausalLM.from_pretrained(MODEL_NAME, torch_dtype=torch.bfloat16)
    mem_checkpoint("base model loaded (CPU)")

    lora_config = LoraConfig(
        r=LORA_R,
        lora_alpha=LORA_ALPHA,
        target_modules=LORA_TARGET_MODULES,
        lora_dropout=LORA_DROPOUT,
        bias="none",
        task_type=TaskType.CAUSAL_LM,
    )
    model = get_peft_model(model, lora_config)
    if GRAD_CKPT:
        model.enable_input_require_grads()
        model.gradient_checkpointing_enable()
    if is_main:
        model.print_trainable_parameters()

    device = torch.device(f"cuda:{rank}" if torch.cuda.is_available() else "cpu")
    gpu_sampler = GpuUtilSampler(rank) if device.type == "cuda" else None
    model = model.to(device)
    mem_checkpoint("model + LoRA moved to GPU")
    if world_size > 1:
        model = DDP(model, device_ids=[rank], find_unused_parameters=False)
    if TORCH_COMPILE:
        model = torch.compile(model, dynamic=True)

    optimizer = torch.optim.AdamW(model.parameters(), lr=LEARNING_RATE)
    mem_checkpoint("optimizer initialized")
    if is_main:
        logging.info(f"Batch size: {BATCH_SIZE} | lr: {LEARNING_RATE:.2e} | grad_clip: {GRAD_CLIP} | grad_ckpt: {GRAD_CKPT}")

    # ── LanceDB ───────────────────────────────────────────────────────────────
    db = lancedb.connect(lancedb_uri)
    table = db.open_table(TABLE_NAME)
    if is_main:
        logging.info(f"LanceDB table: {len(table)} rows, {NUM_SPLITS} splits")

    tokenize_transform = TokenizeTransform(tokenizer)

    # ── CUDA timing events ─────────────────────────────────────────────────────
    start_event = torch.cuda.Event(enable_timing=True)
    end_event   = torch.cuda.Event(enable_timing=True)

    # ── Training loop ─────────────────────────────────────────────────────────
    total_tokens = 0
    total_steps = 0
    training_start = time.perf_counter()

    for epoch in range(NUM_EPOCHS):
        epoch_start = time.perf_counter()

        dataset = StreamingDataset(
            table,
            num_splits=NUM_SPLITS,
            shuffle_seed=SHUFFLE_SEED,
            epoch=epoch,
            rank=rank,
            world_size=world_size,
            read_batch_size=READ_BATCH_SIZE,
            prefetch_batches=PREFETCH_BATCHES,
            transform=tokenize_transform,
            connection_factory=functools.partial(_open_table, lancedb_uri),
        )
        dataloader = DataLoader(
            dataset,
            batch_size=BATCH_SIZE,
            collate_fn=collate_fn,
            num_workers=NUM_WORKERS,
            multiprocessing_context="forkserver" if NUM_WORKERS > 0 else None,
            pin_memory=NUM_WORKERS > 0,
        )

        model.train()
        epoch_loss   = 0.0
        epoch_steps  = 0
        epoch_tokens = 0

        interval_tokens      = 0
        interval_rows        = 0
        interval_gpu_ms      = 0.0
        interval_start       = time.perf_counter()
        prev_bytes_loaded    = dataset.bytes_loaded
        prev_fetch_time      = dataset.fetch_time
        prev_transform_time  = dataset.transform_time

        for step, batch in enumerate(dataloader):
            input_ids      = batch["input_ids"].to(device, non_blocking=True)
            attention_mask = batch["attention_mask"].to(device, non_blocking=True)
            labels         = batch["labels"].to(device, non_blocking=True)

            if step == 0:
                mem_checkpoint("before first forward pass")
            start_event.record()
            outputs = model(input_ids=input_ids, attention_mask=attention_mask, labels=labels)
            loss = outputs.loss
            if step == 0:
                mem_checkpoint("after first forward pass")
            optimizer.zero_grad()
            loss.backward()
            if step == 0:
                mem_checkpoint("after first backward pass")
            torch.nn.utils.clip_grad_norm_(model.parameters(), GRAD_CLIP)
            optimizer.step()
            if step == 0:
                mem_checkpoint("after first optimizer step")
            end_event.record()
            torch.cuda.synchronize()
            gpu_ms = start_event.elapsed_time(end_event)

            batch_rows   = input_ids.shape[0]
            batch_tokens = int(attention_mask.sum())
            epoch_tokens    += batch_tokens
            total_tokens    += batch_tokens
            epoch_loss      += loss.item()
            epoch_steps     += 1
            total_steps     += 1
            interval_tokens += batch_tokens
            interval_rows   += batch_rows
            interval_gpu_ms += gpu_ms

            if is_main and total_steps % LOG_INTERVAL == 0:
                elapsed      = time.perf_counter() - interval_start
                tok_per_sec  = interval_tokens / elapsed if elapsed > 0 else 0.0
                rows_per_sec = interval_rows / elapsed if elapsed > 0 else 0.0
                avg_gpu_ms   = interval_gpu_ms / LOG_INTERVAL
                avg_data_ms  = elapsed * 1000 / LOG_INTERVAL - avg_gpu_ms

                interval_mb  = (dataset.bytes_loaded - prev_bytes_loaded) / 1e6
                mb_per_sec   = interval_mb / elapsed if elapsed > 0 else 0.0
                avg_fetch_ms = (dataset.fetch_time - prev_fetch_time) * 1000 / LOG_INTERVAL
                avg_xform_ms = (dataset.transform_time - prev_transform_time) * 1000 / LOG_INTERVAL
                unscanned    = dataset.unscanned_rows
                raw_depth    = dataset.raw_queue_depth
                cooked_depth = dataset.prefetch_queue_depth
                complete     = dataset.consumed_rows
                cpu_str      = f"{_psutil.cpu_percent():.0f}%" if _psutil_ok else "n/a"
                gpu_str      = gpu_sampler.read_and_reset() if gpu_sampler else "n/a"
                gpu_lbl      = gpu_sampler.label if gpu_sampler else "gpu%"

                logging.info(
                    f"epoch {epoch + 1:3d}/{NUM_EPOCHS} "
                    f"step {step + 1:5d} | "
                    f"loss {loss.item():.4f} | "
                    f"{tok_per_sec:,.0f} tok/s | "
                    f"{rows_per_sec:.1f} rows/s | "
                    f"gpu {avg_gpu_ms:.1f}ms | "
                    f"data {avg_data_ms:.1f}ms | "
                    f"{unscanned}/{raw_depth}/{cooked_depth}/{complete} | "
                    f"{mb_per_sec:.1f} MB/s | "
                    f"ld {avg_fetch_ms:.1f}ms | "
                    f"xf {avg_xform_ms:.1f}ms | "
                    f"cpu {cpu_str} {gpu_lbl} {gpu_str}"
                )
                interval_tokens     = 0
                interval_rows       = 0
                interval_gpu_ms     = 0.0
                interval_start      = time.perf_counter()
                prev_bytes_loaded   = dataset.bytes_loaded
                prev_fetch_time     = dataset.fetch_time
                prev_transform_time = dataset.transform_time

        epoch_time = time.perf_counter() - epoch_start
        if is_main:
            avg_loss = epoch_loss / epoch_steps if epoch_steps else 0
            logging.info(
                f"=== epoch {epoch + 1:3d} | "
                f"loss {avg_loss:.4f} | "
                f"{epoch_time:.1f}s | "
                f"{epoch_tokens:,} tokens"
            )

            checkpoint = {
                "epoch": epoch,
                "dataset_state": dataset.state_dict(),
                "optimizer_state": optimizer.state_dict(),
            }
            save_checkpoint(
                checkpoint_store, checkpoint_prefix,
                f"checkpoint_epoch_{epoch + 1:03d}.pt", checkpoint,
            )

    if is_main:
        elapsed = time.perf_counter() - training_start
        logging.info(
            f"Training complete | "
            f"{elapsed / 60:.1f} min | "
            f"{total_tokens:,} total tokens | "
            f"{total_tokens / elapsed:,.0f} avg tok/s"
        )
        base = model.module if world_size > 1 else model
        save_adapter(checkpoint_store, checkpoint_prefix, base, tokenizer)
        logging.info(f"Adapter saved to {checkpoint_uri}/final_adapter")

    if gpu_sampler:
        gpu_sampler.close()
    cleanup_distributed()


if __name__ == "__main__":
    import multiprocessing
    import resource
    multiprocessing.set_start_method("forkserver")
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(65536, hard), hard))
    main()
