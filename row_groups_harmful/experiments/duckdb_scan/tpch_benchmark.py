#!/usr/bin/env python3
"""
TPC-H Row Group Size Benchmark

This experiment generates TPC-H data at scale factor 1 using DuckDB,
saves it to Parquet files with different row group sizes, and then
runs TPC-H queries to measure performance impact.
"""

import argparse
import json
import time
import shutil
from pathlib import Path
import duckdb
import pandas as pd
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def setup_tpch_data(conn, scale_factor=1):
    """Generate TPC-H data using DuckDB's built-in TPC-H extension."""
    logger.info(f"Generating TPC-H data at scale factor {scale_factor}")

    # Install and load the TPC-H extension
    conn.execute("INSTALL tpch")
    conn.execute("LOAD tpch")

    # Generate TPC-H data
    conn.execute(f"CALL dbgen(sf={scale_factor})")

    logger.info("TPC-H data generation completed")


def export_table_with_row_group_size(conn, table_name, output_dir, row_group_size):
    """Export a table to Parquet with specified row group size."""
    output_path = Path(output_dir) / f"{table_name}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Exporting {table_name} with row group size {row_group_size}")

    # Export the table with specified row group size
    conn.execute(
        f"COPY {table_name} TO '{output_path}' (FORMAT PARQUET, ROW_GROUP_SIZE {row_group_size})"
    )

    logger.info(f"Exported {table_name} to {output_path}")


def create_dataset_with_row_group_size(conn, output_dir, row_group_size):
    """Create a complete TPC-H dataset with the specified row group size."""
    logger.info(f"Creating dataset with row group size {row_group_size}")

    # TPC-H table names
    tables = [
        "customer",
        "lineitem",
        "nation",
        "orders",
        "part",
        "partsupp",
        "region",
        "supplier",
    ]

    for table in tables:
        export_table_with_row_group_size(conn, table, output_dir, row_group_size)

    logger.info(f"Dataset creation completed for row group size {row_group_size}")


def get_tpch_queries():
    """Return a subset of TPC-H queries for benchmarking."""
    queries = {
        "Q1": """
            SELECT
                l_returnflag,
                l_linestatus,
                SUM(l_quantity) AS sum_qty,
                SUM(l_extendedprice) AS sum_base_price,
                SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                AVG(l_quantity) AS avg_qty,
                AVG(l_extendedprice) AS avg_price,
                AVG(l_discount) AS avg_disc,
                COUNT(*) AS count_order
            FROM
                lineitem
            WHERE
                l_shipdate <= CAST('1998-09-02' AS date)
            GROUP BY
                l_returnflag,
                l_linestatus
            ORDER BY
                l_returnflag,
                l_linestatus;
        """,
        "Q3": """
            SELECT
                l_orderkey,
                SUM(l_extendedprice * (1 - l_discount)) AS revenue,
                o_orderdate,
                o_shippriority
            FROM
                customer,
                orders,
                lineitem
            WHERE
                c_mktsegment = 'BUILDING'
                AND c_custkey = o_custkey
                AND l_orderkey = o_orderkey
                AND o_orderdate < CAST('1995-03-15' AS date)
                AND l_shipdate > CAST('1995-03-15' AS date)
            GROUP BY
                l_orderkey,
                o_orderdate,
                o_shippriority
            ORDER BY
                revenue DESC,
                o_orderdate
            LIMIT 10;
        """,
        "Q6": """
            SELECT
                SUM(l_extendedprice * l_discount) AS revenue
            FROM
                lineitem
            WHERE
                l_shipdate >= CAST('1994-01-01' AS date)
                AND l_shipdate < CAST('1995-01-01' AS date)
                AND l_discount BETWEEN 0.05 AND 0.07
                AND l_quantity < 24;
        """,
        "Q12": """
            SELECT
                l_shipmode,
                SUM(CASE
                    WHEN o_orderpriority = '1-URGENT'
                         OR o_orderpriority = '2-HIGH'
                         THEN 1
                    ELSE 0
                END) AS high_line_count,
                SUM(CASE
                    WHEN o_orderpriority <> '1-URGENT'
                         AND o_orderpriority <> '2-HIGH'
                         THEN 1
                    ELSE 0
                END) AS low_line_count
            FROM
                orders,
                lineitem
            WHERE
                o_orderkey = l_orderkey
                AND l_shipmode IN ('MAIL', 'SHIP')
                AND l_commitdate < l_receiptdate
                AND l_shipdate < l_commitdate
                AND l_receiptdate >= CAST('1994-01-01' AS date)
                AND l_receiptdate < CAST('1995-01-01' AS date)
            GROUP BY
                l_shipmode
            ORDER BY
                l_shipmode;
        """,
        "S1": """SELECT SUM(l_quantity) FROM lineitem;""",
        "S2": """SELECT SUM(l_partkey + l_suppkey + l_linenumber), SUM(l_quantity + l_extendedprice + l_discount + l_tax) FROM lineitem;""",
    }
    return queries


def fetch_peak_memory():
    """Fetch peak memory from /tmp/duckdb.json profiling information"""
    with open("/tmp/duckdb.json", "r") as f:
        data = json.load(f)
        return data["system_peak_buffer_memory"]


def run_query_benchmark(dataset_dir, row_group_size):
    """Run TPC-H queries against the dataset and measure execution time."""
    logger.info(f"Running benchmark on dataset with row group size {row_group_size}")

    # Create a fresh connection for benchmarking
    conn = duckdb.connect(":memory:")
    conn.sql("PRAGMA enable_profiling = 'json';")
    conn.sql("PRAGMA profiling_output = '/tmp/duckdb.json'")
    conn.sql(
        'PRAGMA custom_profiling_settings = \'{"SYSTEM_PEAK_BUFFER_MEMORY": "true"}\''
    )

    try:
        # Create views for the Parquet files
        tables = [
            "customer",
            "lineitem",
            "nation",
            "orders",
            "part",
            "partsupp",
            "region",
            "supplier",
        ]

        for table in tables:
            parquet_path = Path(dataset_dir) / f"{table}.parquet"
            conn.execute(f"CREATE VIEW {table} AS SELECT * FROM '{parquet_path}'")

        queries = get_tpch_queries()
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
                    conn.execute(query_sql).fetchall()
                    end_time = time.time()
                    execution_time = end_time - start_time
                    times.append(execution_time)
                    rams.append(fetch_peak_memory())
                    logger.info(f"{query_name} run {run + 1}: {execution_time:.3f}s")
                except Exception as e:
                    logger.error(f"Run {run + 1} failed for {query_name}: {e}")
                    continue

            if times:
                avg_time = sum(times) / len(times)
                avg_ram = sum(rams) / len(rams)
                min_time = min(times)
                min_ram = min(rams)
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
    parser = argparse.ArgumentParser(description="TPC-H Row Group Size Benchmark")
    parser.add_argument(
        "--output-dir",
        default="data/synthetic/tpch",
        help="Output directory for datasets and results",
    )
    parser.add_argument(
        "--results-file",
        default="results/tpch_row_groups.csv",
        help="CSV file to save benchmark results",
    )
    parser.add_argument(
        "--scale-factor", type=int, default=1, help="TPC-H scale factor (default: 1)"
    )
    parser.add_argument(
        "--skip-generation",
        action="store_true",
        help="Skip data generation and only run benchmarks",
    )

    args = parser.parse_args()

    # Create output directories
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    Path(args.results_file).parent.mkdir(parents=True, exist_ok=True)

    # Row group sizes to test (powers of 2 from 4096 to 1Mi)
    # For testing, use a subset first
    if args.scale_factor >= 1:
        row_group_sizes = [
            4096,
            16384,
            65536,
            262144,
            1048576,
            4194304,
            16777216,
        ]
    else:
        # For smaller scale factors, test fewer row group sizes
        row_group_sizes = [4096, 16384, 65536, 262144]

    all_results = []

    for row_group_size in row_group_sizes:
        logger.info(f"Processing row group size {row_group_size}")

        # Create a separate connection for each iteration
        conn = duckdb.connect(":memory:")

        try:
            dataset_dir = Path(args.output_dir) / f"rg_{row_group_size}"

            if not args.skip_generation:
                # Generate TPC-H data if not skipping
                if row_group_size == row_group_sizes[0]:  # Only generate once
                    setup_tpch_data(conn, args.scale_factor)
                else:
                    # For subsequent iterations, we need to regenerate or reuse
                    setup_tpch_data(conn, args.scale_factor)

                # Remove existing dataset directory
                if dataset_dir.exists():
                    shutil.rmtree(dataset_dir)

                # Create dataset with this row group size
                create_dataset_with_row_group_size(conn, dataset_dir, row_group_size)

            # Run benchmarks
            if dataset_dir.exists():
                results = run_query_benchmark(dataset_dir, row_group_size)
                all_results.extend(results)
            else:
                logger.warning(
                    f"Dataset directory {dataset_dir} does not exist, skipping benchmark"
                )

        except Exception as e:
            logger.error(f"Error processing row group size {row_group_size}: {e}")

        finally:
            conn.close()

    # Save results to CSV
    if all_results:
        df = pd.DataFrame(all_results)
        df.to_csv(args.results_file, index=False)
        logger.info(f"Results saved to {args.results_file}")

        # Print summary
        logger.info("Benchmark Summary:")
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
