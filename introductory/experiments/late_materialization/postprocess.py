"""Convert benchmark data output by pytest-benchmark into a CSV file."""
import json
import csv
import argparse
import os
import typing

import lance


def get_row_count():
    """Get number of rows in dataset. This should be the same across them all."""
    return lance.dataset("data/lance").count_rows()


class TimeResult(typing.NamedTuple):
    """Row format of the runtime results CSV file"""

    library: str
    columns: str
    predicate: str
    late_materialization: bool
    selectivity: float
    runtime: float


def iter_runtime_benches(path):
    with open(path) as f:
        results = json.load(f)
    for benchmark in results["benchmarks"]:
        if benchmark["name"].startswith("test_runtime"):
            yield benchmark


def iter_runtime_bench_data(path):
    total_rows = get_row_count()

    for benchmark in iter_runtime_benches(path):
        # Example: "test_runtime[True-lance-10000-id]" -> "lance-1K"
        bench_parameters = benchmark["name"].split("[")[1][:-1]
        late_materialization, library, min_value, project = bench_parameters.split("-")
        late_materialization = late_materialization == "True"
        min_value = int(min_value)
        selectivity = (total_rows - min_value) / total_rows

        yield TimeResult(
            library=library,
            columns=project,
            predicate=f"id >= {min_value}",
            late_materialization=late_materialization,
            selectivity=selectivity,
            runtime=benchmark["stats"]["min"],
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=str)
    args = parser.parse_args()

    if args.data is None:
        # Get the latest benchmark data, if not specified
        benches_directory = os.path.join(".benchmarks", os.listdir(".benchmarks")[0])
        args.data = os.path.join(
            benches_directory, sorted(os.listdir(benches_directory))[-1]
        )
        print("Using latest benchmark data: {}".format(args.data))

    with open("runtime_results.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(TimeResult._fields)

        for benchmark in iter_runtime_bench_data(args.data):
            writer.writerow(benchmark)
