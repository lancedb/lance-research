#!/usr/bin/env python3
"""
FineWeb Text Search Benchmark

This experiment loads FineWeb data from the data/real directory,
rewrites it to Parquet files with different row group sizes, and then
runs text search queries to measure performance impact.
"""

import argparse
import json
import time
from pathlib import Path
import duckdb
import pandas as pd
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_fineweb_data(conn, source_file):
    """Load FineWeb data from the source Parquet file."""
    logger.info(f"Loading FineWeb data from {source_file}")

    # Create a view from the source Parquet file
    conn.execute(f"CREATE VIEW fineweb_source AS SELECT * FROM '{source_file}'")

    # Get basic statistics
    count_result = conn.execute("SELECT COUNT(*) FROM fineweb_source").fetchone()
    logger.info(f"Loaded {count_result[0]:,} rows from FineWeb dataset")


def export_fineweb_with_row_group_size(conn, output_path, row_group_size):
    """Export FineWeb data to Parquet with specified row group size."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Exporting FineWeb data with row group size {row_group_size}")

    # Export the data with specified row group size
    conn.execute(
        f"COPY fineweb_source TO '{output_path}' (FORMAT PARQUET, ROW_GROUP_SIZE {row_group_size})"
    )

    logger.info(f"Exported FineWeb data to {output_path}")


def get_text_search_queries():
    """Return text search queries for benchmarking."""
    queries = {
        "elephant_exact": """
            SELECT COUNT(*) as match_count 
            FROM fineweb 
            WHERE text LIKE '%elephant%'
        """,
        "elephant_case_insensitive": """
            SELECT COUNT(*) as match_count 
            FROM fineweb 
            WHERE LOWER(text) LIKE '%elephant%'
        """,
        "elephant_with_data": """
            SELECT id, url, token_count, LENGTH(text) as text_length
            FROM fineweb 
            WHERE text LIKE '%elephant%'
            ORDER BY token_count DESC
            LIMIT 10
        """,
        "elephant_stats": """
            SELECT 
                COUNT(*) as total_matches,
                AVG(token_count) as avg_tokens,
                AVG(LENGTH(text)) as avg_text_length,
                MAX(token_count) as max_tokens,
                MIN(token_count) as min_tokens
            FROM fineweb 
            WHERE text LIKE '%elephant%'
        """,
        "elephant_regex": """
            SELECT COUNT(*) as match_count
            FROM fineweb 
            WHERE regexp_matches(text, '\\belephant\\b', 'i')
        """,
        "multiple_animals": """
            SELECT 
                SUM(CASE WHEN text LIKE '%elephant%' THEN 1 ELSE 0 END) as elephant_count,
                SUM(CASE WHEN text LIKE '%tiger%' THEN 1 ELSE 0 END) as tiger_count,
                SUM(CASE WHEN text LIKE '%lion%' THEN 1 ELSE 0 END) as lion_count
            FROM fineweb
        """,
    }
    return queries


def fetch_peak_memory():
    """Fetch peak memory from /tmp/duckdb.json profiling information"""
    try:
        with open("/tmp/duckdb.json", "r") as f:
            data = json.load(f)
            return data["system_peak_buffer_memory"]
    except (FileNotFoundError, KeyError, json.JSONDecodeError):
        return 0


def run_text_search_benchmark(dataset_path, row_group_size):
    """Run text search queries against the dataset and measure execution time."""
    logger.info(
        f"Running text search benchmark on dataset with row group size {row_group_size}"
    )

    # Create a fresh connection for benchmarking
    conn = duckdb.connect(":memory:")
    conn.sql("PRAGMA enable_profiling = 'json';")
    conn.sql("PRAGMA profiling_output = '/tmp/duckdb.json'")
    conn.sql(
        'PRAGMA custom_profiling_settings = \'{"SYSTEM_PEAK_BUFFER_MEMORY": "true"}\''
    )

    try:
        # Create view for the Parquet file
        conn.execute(f"CREATE VIEW fineweb AS SELECT * FROM '{dataset_path}'")

        queries = get_text_search_queries()
        results = []

        for query_name, query_sql in queries.items():
            logger.info(f"Running {query_name}")

            # Warm up run
            try:
                conn.execute(query_sql).fetchall()
            except Exception as e:
                logger.error(f"Warmup failed for {query_name}: {e}")
                continue

            # Timed runs
            times = []
            rams = []
            for run in range(3):  # Run each query 3 times
                start_time = time.time()
                try:
                    result = conn.execute(query_sql).fetchall()
                    end_time = time.time()
                    execution_time = end_time - start_time
                    times.append(execution_time)
                    rams.append(fetch_peak_memory())

                    # Log first result for verification
                    if run == 0 and result:
                        if query_name.endswith("_count") or "COUNT" in query_sql:
                            logger.info(f"{query_name} found {result[0][0]} matches")
                        elif query_name == "elephant_with_data":
                            logger.info(f"{query_name} returned {len(result)} results")

                    logger.info(f"{query_name} run {run + 1}: {execution_time:.3f}s")
                except Exception as e:
                    logger.error(f"Run {run + 1} failed for {query_name}: {e}")
                    continue

            if times:
                avg_time = sum(times) / len(times)
                avg_ram = sum(rams) / len(rams) if rams else 0
                min_time = min(times)
                min_ram = min(rams) if rams else 0
                results.append(
                    {
                        "query": query_name,
                        "row_group_size": row_group_size,
                        "avg_time_seconds": avg_time,
                        "min_time_seconds": min_time,
                        "min_ram_bytes": min_ram,
                        "avg_ram_bytes": avg_ram,
                        "runs": len(times),
                    }
                )
                logger.info(
                    f"{query_name} average: {avg_time:.3f}s, best: {min_time:.3f}s"
                )

        return results

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="FineWeb Text Search Benchmark")
    parser.add_argument(
        "--source-file",
        default="data/real/fineweb/data_CC-MAIN-2024-51_000_00000.parquet",
        help="Source FineWeb Parquet file",
    )
    parser.add_argument(
        "--output-dir",
        default="data/synthetic/fineweb",
        help="Output directory for datasets with different row group sizes",
    )
    parser.add_argument(
        "--results-file",
        default="results/fineweb_search.csv",
        help="CSV file to save benchmark results",
    )
    parser.add_argument(
        "--skip-generation",
        action="store_true",
        help="Skip data generation and only run benchmarks",
    )

    args = parser.parse_args()

    # Check if source file exists
    source_path = Path(args.source_file)
    if not source_path.exists():
        logger.error(f"Source file not found: {source_path}")
        return 1

    # Create output directories
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    Path(args.results_file).parent.mkdir(parents=True, exist_ok=True)

    # Row group sizes to test (similar to TPC-H experiment)
    row_group_sizes = [
        1024,
        2048,
        4096,
        8192,
        16384,
        32768,
        65536,
        131072,
        262144,
        524288,
        1048576,
    ]

    all_results = []

    for row_group_size in row_group_sizes:
        logger.info(f"Processing row group size {row_group_size}")

        dataset_path = Path(args.output_dir) / f"fineweb_rg_{row_group_size}.parquet"

        if not args.skip_generation:
            # Create a connection to load and export data
            conn = duckdb.connect(":memory:")

            try:
                # Load source data
                load_fineweb_data(conn, source_path)

                # Remove existing dataset file
                if dataset_path.exists():
                    dataset_path.unlink()

                # Export with specified row group size
                export_fineweb_with_row_group_size(conn, dataset_path, row_group_size)

            except Exception as e:
                logger.error(
                    f"Error generating dataset for row group size {row_group_size}: {e}"
                )
                continue

            finally:
                conn.close()

        # Run benchmarks
        if dataset_path.exists():
            try:
                results = run_text_search_benchmark(dataset_path, row_group_size)
                all_results.extend(results)
            except Exception as e:
                logger.error(
                    f"Error running benchmark for row group size {row_group_size}: {e}"
                )
        else:
            logger.warning(
                f"Dataset file {dataset_path} does not exist, skipping benchmark"
            )

    # Save results to CSV
    if all_results:
        df = pd.DataFrame(all_results)
        df.to_csv(args.results_file, index=False)
        logger.info(f"Results saved to {args.results_file}")

        # Print summary
        logger.info("Text Search Benchmark Summary:")
        for query in df["query"].unique():
            query_data = df[df["query"] == query]
            logger.info(f"\n{query}:")
            for _, row in query_data.iterrows():
                logger.info(
                    f"  Row Group {row['row_group_size']:>7}: {row['avg_time_seconds']:.3f}s"
                )
    else:
        logger.warning("No results to save")


if __name__ == "__main__":
    main()
