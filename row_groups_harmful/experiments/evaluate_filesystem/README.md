# Filesystem Benchmark

This experiment benchmarks the random read performance of different storage systems across various read sizes to understand how buffer size affects throughput.

## Overview

The benchmark creates a large test file (default 2GB) and then performs random reads with different sizes ranging from 128 bytes to 128MB. It measures both bandwidth (MB/s) and IOPS for each read size, helping to identify the optimal buffer size for different storage systems.

## Key Features

- **Random Read Testing**: Performs non-overlapping random reads to avoid cache effects
- **Multiple Read Sizes**: Tests read sizes from 128 bytes to 128MB in powers of 2
- **Dual Implementation**: Supports both local filesystem (std::fs) and object store (S3, etc.)
- **Cache Clearing**: Attempts to clear kernel page cache between iterations for accurate measurements
- **Statistical Averaging**: Runs 5 iterations per test and reports averages
- **CSV Output**: Results can be merged with existing data for comparative analysis

## Usage

```bash
# Test local filesystem
./target/release/evaluate_filesystem \
    --path /path/to/test/directory \
    --filesystem-name my_filesystem \
    --file-size 2147483648 \
    --parallel-reads 128 \
    --output results/filesystem_benchmark.csv

# Test S3 bucket
./target/release/evaluate_filesystem \
    --path s3://my-bucket/test-prefix \
    --filesystem-name s3_test \
    --file-size 2147483648 \
    --parallel-reads 128 \
    --output results/filesystem_benchmark.csv
```

## Parameters

- `--path`: Local directory path or S3 URL (s3://bucket/prefix)
- `--filesystem-name`: Name for this test run (appears in CSV output)
- `--file-size`: Size of test file in bytes (default: 2GB)
- `--parallel-reads`: Number of parallel read threads (default: 128)
- `--output`: CSV file path for results

## Output

The benchmark generates CSV output with columns:
- `filesystem_name`: Name of the filesystem being tested
- `parallel_reads`: Number of parallel read threads used
- `read_size`: Size of each random read in bytes
- `random_bandwidth_mbps`: Average bandwidth in MB/s

## Visualization

Use the included chart script to visualize results:

```bash
python3 chart-scripts/plot_filesystem_benchmark.py \
    --input results/filesystem_benchmark.csv \
    --output charts/filesystem_performance.png
```

The chart shows bandwidth vs read size curves for each filesystem, with vertical lines indicating where each system reaches 90% of its maximum bandwidth.

## Hardware

The results in the benchmark data were collected on the following hardware:

- **nvme_980pro**: Samsung 980 Pro NVMe SSD (local filesystem testing)
- **nvme_980pro_os**: Samsung 980 Pro NVMe SSD via object store (file:// protocol)
- **s3**: Amazon S3 from AWS c5.4xlarge instance (standard networking)
- **s3_net_opt**: Amazon S3 from AWS c7gn.8xlarge instance (enhanced networking, up to 50 Gbps)

## Implementation Details

- **Non-overlapping Reads**: Generates random read positions with minimum 4KB spacing to ensure sector alignment and avoid overlaps
- **Thread-based Parallelism**: Uses OS threads with atomic counters for work distribution
- **Multipart Uploads**: For files >100MB to object store, uses multipart upload for efficiency
- **Error Handling**: Uses panic-based error handling for simplicity in benchmark code
- **Memory Efficiency**: Streams large file creation to avoid excessive memory usage