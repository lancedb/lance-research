# Note, this script expects the CWD is the same directory that contains the script.  It
# also assumes you have built the benchmark program and it is located in the rust build
# directory at target/release/main
import os
import shutil
import subprocess
import sys

TAKE_SIZES = [1, 16, 256]
COLUMNS = [
    "scalar",
    "string",
    "scalar-list",
    "string-list",
    "vector",
    "vector-list",
    "binary",
    "binary-list",
]


def get_bench_args(column_type: str, take_size: int, version: str):
    args = [
        "target/release/main",
        "--workdir",
        "file:///tmp/lances",
        "--cache-metadata",
        "--take-size",
        str(take_size),
        "--data-type",
        column_type,
        "--drop-caches",
        "--concurrency",
        "256",
        "--format",
        version,
        "--quiet",
    ]

    if column_type == "vector":
        args.append("--num-files")
        args.append("10")
    elif column_type == "binary" or column_type == "vector-list":
        args.append("--num-files")
        args.append("2")
    elif column_type == "binary-list":
        args.append("--num-files")
        args.append("1")
    elif column_type.endswith("list"):
        args.append("--num-files")
        args.append("200")

    return args


def bench_throughput(
    column_type: str,
    take_size: int,
    version: str,
):
    args = get_bench_args(column_type, take_size, version)

    result = subprocess.run(args, capture_output=True)
    if result.returncode != 0:
        print(args)
        print(result.stdout)
        print(result.stderr)
        assert False

    return float(result.stdout)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python benchmark_lance.py <version>")
        sys.exit(1)

    if sys.argv[1] == "2.0":
        version = "lance2-0"
    elif sys.argv[1] == "2.1":
        version = "lance2-1"
    else:
        print("Invalid version, must be 2.0 or 2.1")
        sys.exit(1)

    print("take_size,column,takes_per_second")
    for column in COLUMNS:
        shutil.rmtree("/tmp/lances", ignore_errors=True)
        os.mkdir("/tmp/lances")
        for take_size in TAKE_SIZES:
            takes_per_second = bench_throughput(
                column,
                take_size,
                version,
            )
            print(f"{take_size},{column},{takes_per_second}")
