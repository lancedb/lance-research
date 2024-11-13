# Note, this script expects the CWD is the same directory that contains the script.  It
# also assumes you have built the benchmark program and it is located in the rust build
# directory at target/release/main
import os
import shutil
import subprocess

COLUMNS = [
    "nested-list1",
    "nested-list2",
    "nested-list3",
    "nested-list4",
    "nested-list5",
    "nested1",
    "nested2",
    "nested3",
    "nested4",
    "nested5",
]
# FORMATS = ["lance2-0", "lance2-1"]
FORMATS = ["lance2-1"]


def get_bench_args(
    column_type: str,
    format: str,
):
    args = [
        "target/release/main",
        "--workdir",
        "file:///tmp/nesteds",
        "--take-size",
        "256",
        "--cache-metadata",
        "--data-type",
        column_type,
        "--page-size-kb",
        "8",
        "--drop-caches",
        "--concurrency",
        "256",
        "--quiet",
        "--num-files",
        "1000",
        "--format",
        format,
    ]

    return args


def bench_throughput(
    column_type: str,
    format: str,
):
    args = get_bench_args(column_type, format)

    result = subprocess.run(args, capture_output=True)
    if result.returncode != 0:
        print(args)
        print(result.stdout)
        print(result.stderr)
        assert False

    return float(result.stdout)


if __name__ == "__main__":
    print("format,nesting_type,nesting_level,takes_per_second")
    for format in FORMATS:
        for column_idx, column in enumerate(COLUMNS):
            shutil.rmtree("/tmp/nesteds", ignore_errors=True)
            os.mkdir("/tmp/nesteds")
            takes_per_second = bench_throughput(
                column,
                format,
            )
            nesting_type = "list"
            if column_idx >= 5:
                nesting_type = "validity"
            print(f"{format},{nesting_type},{(column_idx % 5)+1},{takes_per_second}")
