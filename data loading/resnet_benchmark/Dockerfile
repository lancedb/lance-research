# syntax=docker/dockerfile:1
# Build from the repo root:
#   docker build -f resnet_benchmark/Dockerfile .
FROM nvcr.io/nvidia/pytorch:25.11-py3

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"

RUN pip install --no-cache-dir \
    maturin \
    torchvision \
    Pillow \
    pyarrow \
    object-store-python

WORKDIR /workspace

# Copy shared lancedb submodule from repo root.
COPY lancedb/Cargo.toml lancedb/Cargo.lock lancedb/rust-toolchain.toml lancedb/
COPY lancedb/rust/ lancedb/rust/
COPY lancedb/nodejs/Cargo.toml lancedb/nodejs/
RUN mkdir -p lancedb/nodejs/src && touch lancedb/nodejs/src/lib.rs
COPY lancedb/LICENSE lancedb/
COPY lancedb/python/ lancedb/python/

RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/cargo-target \
    export CARGO_TARGET_DIR=/cargo-target && \
    cd lancedb/python && \
    maturin build --release --out /tmp/wheels && \
    pip install /tmp/wheels/*.whl && \
    rm -rf /tmp/wheels

RUN python -c "\
import shutil, glob, os, lancedb; \
so = glob.glob(os.path.join(os.path.dirname(lancedb.__file__), '_lancedb*.so'))[0]; \
shutil.copy(so, '/workspace/lancedb/python/python/lancedb/')"

# Copy benchmark scripts into a subdirectory mirroring the repo layout so that
# relative imports (../lancedb) resolve correctly inside the container.
COPY resnet_benchmark/prepare_data.py resnet_benchmark/benchmark.py resnet_benchmark/

# Prepare data:
#   docker run ... -e LANCEDB_URI=s3://... <image> python resnet_benchmark/prepare_data.py
#
# Run benchmark:
#   docker run --gpus all -e LANCEDB_URI=s3://... <image>
#
# Multi-GPU:
#   docker run --gpus all -e LANCEDB_URI=s3://... <image> \
#     torchrun --nproc_per_node=4 resnet_benchmark/benchmark.py
CMD ["python", "resnet_benchmark/benchmark.py"]
