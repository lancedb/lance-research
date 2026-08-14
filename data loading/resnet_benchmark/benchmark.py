#!/usr/bin/env python3
"""
Benchmark ResNet-50 training throughput with LanceDB as the data source.

This benchmark is designed to stress the data loader: ResNet-50 is fast
enough on a modern GPU that it is easily I/O bound when reading images from
cloud storage, making data pipeline efficiency the dominant variable.

Each log line breaks down time into:
  - img/s:      images processed per second (wall-clock)
  - gpu_ms:     time spent on forward + backward pass (GPU-side)
  - data_ms:    wall-clock time per step minus gpu_ms (CPU/IO-side)
  - u/r/c/d:    pipeline row counts: unscanned / raw / cooked / done
  - MB/s:       raw bytes fetched from storage per second
  - ld:         avg time per step waiting for LanceDB I/O (load)
  - xf:         avg time per step for JPEG decode + image transforms
  - cpu%:       system CPU utilization averaged over the log interval
  - vram%:      GPU VRAM in use as a percentage of total device memory

When data_ms >> gpu_ms the pipeline is I/O bound.
When gpu_ms >> data_ms the GPU is the bottleneck.
When fetch_ms dominates data_ms, S3 latency/bandwidth is the limit.
When xform_ms dominates data_ms, JPEG decode / image transforms are the limit.

Required environment variables:
    LANCEDB_URI      URI of the LanceDB database (e.g. s3://my-bucket/data)

Single GPU:
    python benchmark.py

8 GPUs:
    torchrun --nproc_per_node=8 benchmark.py
"""

import os
import sys
import logging
import time
import threading

try:
    import psutil as _psutil
    _psutil.cpu_percent()  # prime so first interval read is accurate
    _psutil_ok = True
except ImportError:
    _psutil_ok = False


sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "lancedb", "python", "python"))

import numpy as np
import torch
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import DataLoader
from torchvision import models
from torchvision.io import decode_jpeg
import torchvision.transforms.v2 as v2
import functools
import lancedb
from lancedb.streaming import StreamingDataset


def _open_table(uri: str, table_name: str):
    """Module-level factory so it survives pickling into DataLoader workers."""
    return lancedb.connect(uri).open_table(table_name)

# ── Configuration ──────────────────────────────────────────────────────────────
TABLE_NAME   = "resnet_images"
NUM_EPOCHS   = 5
NUM_SPLITS   = int(os.environ.get("NUM_SPLITS",   8))
NUM_WORKERS  = int(os.environ.get("NUM_WORKERS",  1))
BATCH_SIZE   = int(os.environ.get("BATCH_SIZE", 512))
SHUFFLE_SEED = 42
LOG_INTERVAL = 20     # log every N steps

# StreamingDataset I/O tuning
READ_BATCH_SIZE  = int(os.environ.get("READ_BATCH_SIZE",  64))  # Rows fetched per split per take_offsets call.
PREFETCH_BATCHES = int(os.environ.get("PREFETCH_BATCHES",  1))  # Concurrent read_batch_size fetches in flight per split.


# ── Transforms ─────────────────────────────────────────────────────────────────

# v2 transforms accept a stacked (N, C, H, W) uint8 batch and run the full
# pipeline — crop, flip, cast, normalize — as a single batched C++ dispatch.
# Trade-off: all N images in a read-batch share the same random crop/flip
# parameters (v2 draws params once per call for paired-data consistency).
# Fine for throughput benchmarking; for real training augmentation diversity
# is only reduced within each 64-image fetch, not across the epoch.
_batch_transform = v2.Compose([
    v2.RandomResizedCrop(224),
    v2.RandomHorizontalFlip(),
    v2.ToDtype(torch.float32, scale=True),
    v2.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
])


# ── Distributed helpers ────────────────────────────────────────────────────────

def setup_distributed():
    if "RANK" not in os.environ:
        return 0, 1
    dist.init_process_group("nccl")
    rank = dist.get_rank()
    torch.cuda.set_device(rank)
    return rank, dist.get_world_size()


def cleanup_distributed():
    if dist.is_initialized():
        dist.destroy_process_group()


# ── Transform / Collate ────────────────────────────────────────────────────────

def decode_transform(batch):
    """Decode JPEG and apply image transforms in the background prefetch thread.

    Zero-copy Arrow access: numpy views the binary column buffers directly,
    torch.from_numpy shares that memory with no heap copy.  After decoding,
    the full augmentation pipeline runs as a single batched v2 dispatch on
    the stacked (N, C, H, W) tensor — one C++ call instead of 64 per-image
    Python dispatches.
    """
    col     = batch.column("image")
    bufs    = col.buffers()
    offsets = np.frombuffer(bufs[1], dtype=np.int64)  # int64 for large_binary
    data    = np.frombuffer(bufs[2], dtype=np.uint8)

    imgs = [
        decode_jpeg(torch.from_numpy(data[offsets[i] : offsets[i + 1]]))
        for i in range(len(col))
    ]

    labels = batch.column("label").to_pylist()
    batch_t = _batch_transform(torch.stack(imgs))
    return [{"image": batch_t[i], "label": lbl} for i, lbl in enumerate(labels)]


def collate_fn(rows):
    return (
        torch.stack([row["image"] for row in rows]),
        torch.tensor([row["label"] for row in rows], dtype=torch.long),
    )


# ── Prefetcher ─────────────────────────────────────────────────────────────────

class DataPrefetcher:
    """Overlap H2D transfer with GPU computation via a secondary CUDA stream.

    Without prefetching the critical path per step is:
      GPU done → next(dataloader) → H2D (12 ms on PCIe 3.0) → GPU starts
    With prefetching:
      GPU starts batch N
        └─ side stream: H2D batch N+1 (12 ms, runs concurrently on PCIe)
      GPU done (465 ms later) → wait_stream (instant) → GPU starts N+1

    The H2D cost is absorbed into the GPU step, so data_ms → ~0.
    pin_memory=True on the DataLoader is required: non_blocking H2D only
    bypasses the CPU↔GPU copy synchronisation when the source is pinned.
    """

    def __init__(self, loader, device, dtype=None):
        self._iter   = iter(loader)
        self._device = device
        self._dtype  = dtype
        self._stream = torch.cuda.Stream(device)
        self._images = None
        self._labels = None
        self._preload()

    def _preload(self):
        try:
            images, labels = next(self._iter)
        except StopIteration:
            self._images = None
            return
        with torch.cuda.stream(self._stream):
            self._images = images.to(self._device, dtype=self._dtype, non_blocking=True)
            self._labels = labels.to(self._device, non_blocking=True)

    def __iter__(self):
        return self

    def __next__(self):
        torch.cuda.current_stream().wait_stream(self._stream)
        images = self._images
        labels = self._labels
        if images is None:
            raise StopIteration
        # Tell CUDA not to reclaim this memory until the current stream is done.
        images.record_stream(torch.cuda.current_stream())
        labels.record_stream(torch.cuda.current_stream())
        self._preload()
        return images, labels


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
        # NGC containers ship pydcgm outside the default Python path.
        for p in ("/usr/local/dcgm/bindings/python3",):
            if p not in sys.path:
                sys.path.insert(0, p)

        import pydcgm                   # type: ignore
        import dcgm_structs as _s       # type: ignore
        import dcgm_fields  as _f       # type: ignore

        handle = pydcgm.DcgmHandle(opMode=_s.DCGM_OPERATION_MODE_AUTO)
        group  = pydcgm.DcgmGroup(handle, groupName=f"bench_{idx}",
                                   groupType=_s.DCGM_GROUP_DEFAULT)
        fg     = pydcgm.DcgmFieldGroup(handle, f"bench_fg_{idx}",
                                        [_f.DCGM_FI_PROF_SM_ACTIVE])
        # 10 ms update frequency; keep 30 s of history
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
                    return float(v) * 100.0   # DCGM returns 0.0–1.0
            return None

        # Confirm we actually get data before committing to this path.
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

    lancedb_uri = os.environ.get("LANCEDB_URI")
    if not lancedb_uri:
        print("Error: LANCEDB_URI environment variable is not set.")
        sys.exit(1)

    # ── Model ──────────────────────────────────────────────────────────────────
    device     = torch.device(f"cuda:{rank}" if torch.cuda.is_available() else "cpu")
    gpu_sampler = GpuUtilSampler(rank) if device.type == "cuda" else None
    model = models.resnet50(weights=None).to(device, dtype=torch.bfloat16)
    model = torch.compile(model, dynamic=True)
    if world_size > 1:
        model = DDP(model, device_ids=[rank])

    criterion = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=0.1, momentum=0.9,
                                weight_decay=1e-4)

    if is_main:
        logging.info(f"World size: {world_size} | Device: {device}")
        logging.info(f"Batch size: {BATCH_SIZE} | Splits: {NUM_SPLITS}")

    # ── LanceDB ────────────────────────────────────────────────────────────────
    db = lancedb.connect(lancedb_uri)
    table = db.open_table(TABLE_NAME)
    if is_main:
        logging.info(f"Table: {len(table):,} rows")

    # ── CUDA timing events ─────────────────────────────────────────────────────
    start_event = torch.cuda.Event(enable_timing=True)
    end_event   = torch.cuda.Event(enable_timing=True)

    # ── Training loop ──────────────────────────────────────────────────────────
    total_images = 0
    total_steps  = 0
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
            transform=decode_transform,
            connection_factory=functools.partial(_open_table, lancedb_uri),
        )
        dataloader = DataLoader(
            dataset,
            batch_size=BATCH_SIZE,
            collate_fn=collate_fn,
            num_workers=NUM_WORKERS,
            multiprocessing_context="forkserver" if NUM_WORKERS > 0 else None,
            # pin_memory only helps when workers pre-pin batches while the GPU
            # runs the previous step.  With num_workers=0 everything is
            # synchronous in the main thread so pinning is in the critical
            # path regardless — no benefit.
            pin_memory=NUM_WORKERS > 0,
        )

        model.train()
        interval_images    = 0
        interval_gpu_ms    = 0.0
        interval_start     = time.perf_counter()
        prev_bytes_loaded  = dataset.bytes_loaded
        prev_fetch_time    = dataset.fetch_time
        prev_transform_time = dataset.transform_time

        prefetcher = DataPrefetcher(dataloader, device, dtype=torch.bfloat16)
        for step, (images, labels) in enumerate(prefetcher):

            start_event.record()
            outputs = model(images)
            loss = criterion(outputs, labels)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            end_event.record()
            torch.cuda.synchronize()

            gpu_ms = start_event.elapsed_time(end_event)
            batch_n = images.shape[0]

            interval_images += batch_n
            total_images    += batch_n
            total_steps     += 1
            interval_gpu_ms += gpu_ms

            if is_main and (total_steps % LOG_INTERVAL == 0):
                elapsed     = time.perf_counter() - interval_start
                img_per_sec = interval_images / elapsed
                avg_gpu_ms  = interval_gpu_ms / LOG_INTERVAL
                avg_data_ms = elapsed * 1000 / LOG_INTERVAL - avg_gpu_ms

                interval_mb    = (dataset.bytes_loaded - prev_bytes_loaded) / 1e6
                mb_per_sec     = interval_mb / elapsed if elapsed > 0 else 0.0
                avg_fetch_ms   = (dataset.fetch_time - prev_fetch_time) * 1000 / LOG_INTERVAL
                avg_xform_ms   = (dataset.transform_time - prev_transform_time) * 1000 / LOG_INTERVAL
                unscanned    = dataset.unscanned_rows
                raw_depth    = dataset.raw_queue_depth
                cooked_depth = dataset.prefetch_queue_depth
                complete     = dataset.consumed_rows
                cpu_str = f"{_psutil.cpu_percent():.0f}%" if _psutil_ok else "n/a"
                gpu_str = gpu_sampler.read_and_reset() if gpu_sampler else "n/a"
                gpu_lbl = gpu_sampler.label if gpu_sampler else "gpu%"

                logging.info(
                    f"epoch {epoch + 1}/{NUM_EPOCHS} "
                    f"step {step + 1:5d} | "
                    f"loss {loss.item():.4f} | "
                    f"{img_per_sec:,.0f} img/s | "
                    f"gpu {avg_gpu_ms:.1f}ms | "
                    f"data {avg_data_ms:.1f}ms | "
                    f"{unscanned}/{raw_depth}/{cooked_depth}/{complete} | "
                    f"{mb_per_sec:.1f} MB/s | "
                    f"ld {avg_fetch_ms:.1f}ms | "
                    f"xf {avg_xform_ms:.1f}ms | "
                    f"cpu {cpu_str} {gpu_lbl} {gpu_str}"
                )
                interval_images     = 0
                interval_gpu_ms     = 0.0
                interval_start      = time.perf_counter()
                prev_bytes_loaded   = dataset.bytes_loaded
                prev_fetch_time     = dataset.fetch_time
                prev_transform_time = dataset.transform_time

        epoch_time = time.perf_counter() - epoch_start
        if is_main:
            logging.info(
                f"=== epoch {epoch + 1}/{NUM_EPOCHS} | "
                f"{epoch_time:.1f}s | "
                f"{total_images / (time.perf_counter() - training_start):,.0f} avg img/s"
            )

    if gpu_sampler:
        gpu_sampler.close()
    cleanup_distributed()


if __name__ == "__main__":
    import multiprocessing
    import resource
    multiprocessing.set_start_method("forkserver")
    # torch.compile opens many files while tracing; raise the fd limit to avoid EMFILE.
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(65536, hard), hard))
    main()
