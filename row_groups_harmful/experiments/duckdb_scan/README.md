# DuckDB Row Group Scan Benchmarks

This directory contains two experiments that investigate the impact of Parquet row group size on scan performance using DuckDB:

1. **TPC-H Benchmark** (`tpch_benchmark.py`) - Analytical queries on structured data
2. **FineWeb Text Search** (`fineweb_search.py`) - Text search queries on real web data

Both experiments create datasets with varying row group sizes (from 4K to 16M rows) and measure query execution times and memory usage.

## Overview

The experiments demonstrate that row group size significantly affects scan performance. Row groups that are too small create excessive metadata overhead, while very large row groups can reduce parallelism and increase memory usage. These benchmarks help identify optimal row group sizes for different scan workloads.

## Key Features

- **TPC-H Data Generation**: Uses DuckDB's built-in TPC-H extension to generate standard benchmark data
- **Variable Row Group Sizes**: Tests row group sizes from 4,096 to 1,048,576 rows (powers of 2)
- **Multiple Query Types**: Includes representative TPC-H queries (Q1, Q3, Q6, Q12) covering different access patterns
- **Statistical Measurement**: Runs each query 3 times and reports averages
- **Parquet Format**: All datasets stored in Parquet format with precise row group control
- **Visualization**: Generates charts showing performance trends across row group sizes

## Prerequisites

```bash
# Install uv if you don't have it
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync
```

## Usage

### Basic Benchmark

```bash
# Run full benchmark with scale factor 1
uv run python tpch_benchmark.py

# Custom configuration
uv run python tpch_benchmark.py \
    --scale-factor 1 \
    --output-dir data/synthetic/tpch/custom \
    --results-file results/tpch_custom.csv
```

### Quick Test

```bash
# Run TPC-H with subset of row group sizes for testing
uv run python test_quick.py

# Test FineWeb search functionality
uv run python test_fineweb.py
```

### FineWeb Text Search

```bash
# Run full FineWeb text search benchmark
uv run python fineweb_search.py

# Custom configuration
uv run python fineweb_search.py \
    --source-file ../../data/real/fineweb/data_CC-MAIN-2024-51_000_00000.parquet \
    --output-dir data/synthetic/fineweb/custom \
    --results-file results/fineweb_custom.csv
```

### Visualization

```bash
# Generate performance chart
uv run --directory ../../ python chart-scripts/plot_duckdb_scan.py --input results/tpch_row_groups.csv --output charts/tpch_performance.png
```

## Parameters

- `--scale-factor`: TPC-H scale factor (default: 1)
- `--output-dir`: Directory for generated datasets (default: "data/synthetic/tpch")
- `--results-file`: CSV file for benchmark results (default: "results/tpch_row_groups.csv")
- `--skip-generation`: Skip data generation and only run benchmarks

## Row Group Sizes Tested

The experiment tests the following row group sizes (number of rows per row group):

- 4,096 (4K)
- 8,192 (8K)  
- 16,384 (16K)
- 32,768 (32K)
- 65,536 (64K)
- 131,072 (128K)
- 262,144 (256K)
- 524,288 (512K)
- 1,048,576 (1M)

## TPC-H Queries

The benchmark includes four representative TPC-H queries:

- **Q1**: Aggregation query with filters (lineitem table scan)
- **Q3**: Multi-table join with aggregation (customer, orders, lineitem)
- **Q6**: Simple aggregation with selective filters (lineitem table scan)
- **Q12**: Two-table join with complex aggregation (orders, lineitem)

These queries represent different access patterns and computational complexity typical in analytical workloads.

## FineWeb Text Search Queries

The FineWeb benchmark includes six text search queries on real web data:

- **elephant_exact**: Simple case-sensitive substring search (`LIKE '%elephant%'`)
- **elephant_case_insensitive**: Case-insensitive substring search (`LOWER(text) LIKE '%elephant%'`)
- **elephant_with_data**: Returns matching records with metadata (id, url, token_count)
- **elephant_stats**: Aggregation statistics on matching records (avg/min/max tokens)
- **elephant_regex**: Word boundary regex search for exact word matches
- **multiple_animals**: Multi-pattern search (elephant, tiger, lion counts)

These queries test different text processing patterns: simple scanning, case conversion, aggregation, and complex pattern matching on variable-length text data.

## Output

### CSV Results

The benchmark generates a CSV file with columns:
- `query`: Query identifier (Q1, Q3, Q6, Q12)
- `row_group_size`: Number of rows per row group
- `avg_time_seconds`: Average query execution time over 3 runs
- `min_time_seconds`: Best execution time
- `runs`: Number of successful runs

### Generated Datasets

For each row group size, the experiment creates a directory containing:
```
data/synthetic/tpch/
├── rg_4096/
│   ├── customer.parquet
│   ├── lineitem.parquet
│   ├── nation.parquet
│   ├── orders.parquet
│   ├── part.parquet
│   ├── partsupp.parquet
│   ├── region.parquet
│   └── supplier.parquet
├── rg_8192/
│   └── ...
```

## Expected Results

Based on initial testing with TPC-H scale factor 1:

- **Small row groups (4K-8K)**: Higher overhead, slower performance
- **Medium row groups (16K-64K)**: Optimal balance, fastest performance  
- **Large row groups (256K-1M)**: Diminishing returns, potential memory pressure

The optimal row group size typically falls in the 16K-64K range for most analytical queries, balancing metadata overhead with parallelism and memory efficiency.

## Implementation Details

- **Fresh Connections**: Each benchmark uses a fresh DuckDB connection to avoid caching effects
- **Warm-up Runs**: Each query is run once for warm-up before timed measurements
- **Error Handling**: Graceful handling of query failures with detailed logging
- **Memory Isolation**: Each row group test uses an isolated in-memory database
- **Parquet Control**: Direct control over row group size via DuckDB's COPY command

## Extending the Benchmark

To add more queries or modify the experiment:

1. **Add Queries**: Extend the `get_tpch_queries()` function
2. **Change Scale**: Modify the `--scale-factor` parameter
3. **Custom Row Group Sizes**: Edit the `row_group_sizes` list in `main()`
4. **Additional Metrics**: Extend the results collection in `run_query_benchmark()`

## Performance Considerations

- **Memory Usage**: Large datasets may require significant memory
- **Disk Space**: Each row group size creates a complete dataset copy
- **Runtime**: Full benchmark with all row group sizes takes significant time
- **Reproducibility**: Results may vary based on hardware and system load