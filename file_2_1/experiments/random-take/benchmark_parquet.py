# Note, this script expects the CWD is the same directory that contains the script.  It
# also assumes you have built the benchmark program and it is located in the rust build
# directory at target/release/main
import os
import shutil
import subprocess

TAKE_SIZES = [1, 16, 256]
PAGE_SIZES = [8, 64, 256]
COLUMNS = [
    "scalar",
    "string",
    "vector",
    "scalar-list",
    "string-list",
    "vector-list",
    "binary",
    "binary-list",
]


def get_bench_args(
    column_type: str,
    take_size: int,
    page_size: int,
):
    args = [
        "target/release/main",
        "--workdir",
        "file:///tmp/parquets",
        "--take-size",
        str(take_size),
        "--cache-metadata",
        "--data-type",
        column_type,
        "--page-size-kb",
        str(page_size),
        "--drop-caches",
        "--concurrency",
        "128",
        "--quiet",
    ]

    if column_type == "vector":
        args.append("--num-files")
        args.append("10")
        args.append("--row-group-size")
        args.append("1000")
    elif column_type == "binary" or column_type == "vector-list":
        args.append("--num-files")
        args.append("2")
        args.append("--row-group-size")
        args.append("1000")
    elif column_type == "binary-list":
        args.append("--num-files")
        args.append("1")
        args.append("--row-group-size")
        args.append("500")
    elif column_type.endswith("list"):
        args.append("--num-files")
        args.append("200")
        args.append("--row-group-size")
        args.append("10000")
    else:
        (args.append("--row-group-size"),)
        (args.append("100000"),)

    return args


def bench_throughput(
    format: str,
    column_type: str,
    page_size: int,
    take_size: int,
):
    args = get_bench_args(column_type, take_size, page_size)

    result = subprocess.run(args, capture_output=True)
    if result.returncode != 0:
        print(args)
        print(result.stdout)
        print(result.stderr)
        assert False

    return float(result.stdout)


if __name__ == "__main__":
    print("page_size_kb,take_size,column,io_size,takes_per_second")
    for column in COLUMNS:
        shutil.rmtree("/tmp/parquets", ignore_errors=True)
        os.mkdir("/tmp/parquets")
        for take_size in TAKE_SIZES:
            if take_size == 256:
                page_sizes = PAGE_SIZES
            else:
                page_sizes = [8]
            for page_size in page_sizes:
                takes_per_second = bench_throughput(
                    format,
                    column,
                    page_size,
                    take_size,
                )
                # io_size, iops = bench_io(
                #     format, column, take_size, row_group_size, use_cache
                # )
                io_size = 0
                print(f"{page_size},{take_size},{column},{io_size},{takes_per_second}")
