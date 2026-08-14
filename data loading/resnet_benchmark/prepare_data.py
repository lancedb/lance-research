#!/usr/bin/env python3
"""
Generate synthetic JPEG images and store them in a LanceDB table.

Generates realistic-sized 224x224 RGB JPEG images (~30-50 KB each) with
random pixel data and integer labels, matching the ImageNet image size used
in standard ResNet benchmarks.

Required environment variables:
    LANCEDB_URI   URI of the LanceDB database (e.g. s3://my-bucket/data)

Optional environment variables:
    NUM_IMAGES    Number of images to generate (default: 128000)
    NUM_CLASSES   Number of label classes (default: 1000)
    JPEG_QUALITY  JPEG compression quality 1-95 (default: 85)

Run once before benchmarking:
    LANCEDB_URI=s3://my-bucket/data python prepare_data.py
"""

import io
import os
import sys
import random

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lancedb", "python", "python"))

import numpy as np
import pyarrow as pa
from PIL import Image
import lancedb

TABLE_NAME = "resnet_images"
NUM_IMAGES  = int(os.environ.get("NUM_IMAGES",  128_000))
NUM_CLASSES = int(os.environ.get("NUM_CLASSES", 1_000))
JPEG_QUALITY = int(os.environ.get("JPEG_QUALITY", 85))
BATCH_SIZE  = 1_000   # rows written per LanceDB append
IMAGE_SIZE  = (224, 224)


def make_jpeg(rng: np.random.Generator) -> bytes:
    pixels = rng.integers(0, 256, (*IMAGE_SIZE, 3), dtype=np.uint8)
    img = Image.fromarray(pixels, mode="RGB")
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=JPEG_QUALITY)
    return buf.getvalue()


def main():
    lancedb_uri = os.environ.get("LANCEDB_URI")
    if not lancedb_uri:
        print("Error: LANCEDB_URI environment variable is not set.")
        sys.exit(1)

    db = lancedb.connect(lancedb_uri)

    if TABLE_NAME in db.table_names():
        print(f"Dropping existing '{TABLE_NAME}' table...")
        db.drop_table(TABLE_NAME)

    rng = np.random.default_rng(42)
    table = None
    generated = 0

    print(f"Generating {NUM_IMAGES:,} synthetic {IMAGE_SIZE[0]}x{IMAGE_SIZE[1]} "
          f"JPEG images (quality={JPEG_QUALITY})...", flush=True)

    while generated < NUM_IMAGES:
        batch_n = min(BATCH_SIZE, NUM_IMAGES - generated)
        images = [make_jpeg(rng) for _ in range(batch_n)]
        labels = rng.integers(0, NUM_CLASSES, batch_n).tolist()

        batch = pa.table({
            "image": pa.array(images, type=pa.large_binary()),
            "label": pa.array(labels, type=pa.int32()),
        })

        if table is None:
            table = db.create_table(TABLE_NAME, data=batch)
        else:
            table.add(batch)

        generated += batch_n
        avg_kb = sum(len(b) for b in images) / len(images) / 1024
        print(f"  {generated:>8,} / {NUM_IMAGES:,}  avg image size: {avg_kb:.1f} KB", flush=True)

    print(f"\nDone. {len(table):,} rows in '{lancedb_uri}/{TABLE_NAME}'", flush=True)
    print(f"Schema: {table.schema}")


if __name__ == "__main__":
    main()
