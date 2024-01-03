"""Convert benchmark data output by pytest-benchmark into a CSV file."""
import json
import csv
import argparse
import os
import typing


class Result(typing.NamedTuple):
    dataset: str
    format: str
    row_group_size: int
    dataset_size_bytes: int
    scan_time: float


def get_size(start_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def iter_dataset_sizes():
    datasets = os.listdir("data")
    for dataset in datasets:
        for name in os.listdir(os.path.join("data", dataset)):
            fmt, group_size = name.split("-")
            size = get_size(os.path.join("data", dataset, name))
            yield dict(
                dataset=dataset,
                format=fmt,
                row_group_size=group_size,
                dataset_size_bytes=size,
            )


def iter_benchmark_data(path):
    with open(path) as f:
        results = json.load(f)
    for benchmark in results["benchmarks"]:
        # Example: "test_scan_tpch[lance-1K]" -> "lance-1K"
        bench_parameter = benchmark["name"].split("[")[1][:-1]
        format, row_group_size = bench_parameter.split("-")
        yield dict(
            dataset=benchmark["group"],
            format=format,
            row_group_size=row_group_size,
            benchmark=benchmark,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=str)
    args = parser.parse_args()

    if args.data is None:
        # Get the latest benchmark data, if not specified
        benches_directory = os.path.join(".benchmarks", os.listdir(".benchmarks")[0])
        args.data = os.path.join(benches_directory, os.listdir(benches_directory)[-1])

    # We need to join to the benchmark data with (dataset, format, row_group_size)
    dataset_meta = {
        (d["dataset"], d["format"], d["row_group_size"]): d
        for d in iter_dataset_sizes()
    }

    with open("results.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["dataset", "format", "row_group_size", "dataset_size_bytes", "scan_time"]
        )
        for benchmark in iter_benchmark_data(args.data):
            meta = dataset_meta[
                (benchmark["dataset"], benchmark["format"], benchmark["row_group_size"])
            ]

            group_size = benchmark["row_group_size"]
            if group_size.endswith("K"):
                group_size = int(group_size[:-1]) * 1024
            else:
                raise NotImplementedError("Only K suffix supported")

            row = Result(
                dataset=benchmark["dataset"],
                format=benchmark["format"],
                row_group_size=group_size,
                dataset_size_bytes=meta["dataset_size_bytes"],
                scan_time=benchmark["benchmark"]["stats"]["min"],
            )
            writer.writerow(row)
