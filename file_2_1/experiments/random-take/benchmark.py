# Note, this script expects the CWD is the same directory that contains the script.  It
# also assumes you have built the benchmark program and it is located in the rust build
# directory at target/release/random-take-bench
import os
import subprocess

TAKE_SIZES = [1, 4, 16, 64, 256]
PAGE_SIZES = [8, 64, 256, 1024]
FORMATS = ["parquet"]
COLUMNS = ["int", "double", "embedding"]
USE_CACHE = [True]
ROW_GROUP_SIZES = [100000]


def get_bench_args(
    column_type: str,
    take_size: int,
    page_size: int,
    row_group_size: int,
    cache_metadata: bool,
):
    args = [
        "target/release/random-take-bench",
        "--take-size",
        str(take_size),
        "--data-type",
        column_type,
        "--page-size-kb",
        str(page_size),
        "--row-group-size",
        str(row_group_size),
        "--drop-caches",
        "--quiet",
    ]

    if column_type == "embedding":
        args.append("--num-files")
        args.append("5")

    if cache_metadata:
        args.append("--cache-metadata")

    return args


def bench_throughput(
    format: str,
    column_type: str,
    page_size: int,
    take_size: int,
    row_group_size: int,
    cache_metadata: bool,
):
    args = get_bench_args(
        column_type, take_size, page_size, row_group_size, cache_metadata
    )

    result = subprocess.run(args, capture_output=True)
    if result.returncode != 0:
        print(args)
        print(result.stdout)
        print(result.stderr)
        assert False

    return float(result.stdout)


def bench_io(
    format: str,
    column_type: str,
    take_size: int,
    row_group_size: int,
    cache_metadata: bool,
):
    if format == "parquet":
        datafile = f"/tmp/input_rgs_{row_group_size}.parquet"
    else:
        datafiles = os.listdir("/tmp/bench_dataset.lance/data")
        assert len(datafiles) == 1
        datafile = f"/tmp/bench_dataset.lance/data/{str(datafiles[0])}"

    args = ["strace", "-e", "pread64", "-z", "-qqq", "-f", "-P", datafile]
    args.extend(
        get_bench_args(
            1, format, column_type, take_size, row_group_size, cache_metadata, False
        )
    )

    result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if result.returncode != 0:
        print(args)
        print(result.stdout)
        print(result.stderr)
        assert False

    lines = [line.strip() for line in result.stdout.splitlines()]
    start_idx = lines.index(b"Bench Start")
    stop_idx = lines.index(b"Bench End")
    lines = lines[start_idx + 1 : stop_idx]
    iops = len(lines)
    io_size = sum([int(line.rpartition(b"=")[2]) for line in lines])
    return io_size, iops


if __name__ == "__main__":
    print(
        "use_cache,row_group_size,page_size_kb,take_size,column,io_size,takes_per_second"
    )
    for use_cache in USE_CACHE:
        for format in FORMATS:
            for page_size in PAGE_SIZES:
                for row_group_size in ROW_GROUP_SIZES:
                    for take_size in TAKE_SIZES:
                        for column in COLUMNS:
                            takes_per_second = bench_throughput(
                                format,
                                column,
                                page_size,
                                take_size,
                                row_group_size,
                                use_cache,
                            )
                            # io_size, iops = bench_io(
                            #     format, column, take_size, row_group_size, use_cache
                            # )
                            io_size = 0
                            print(
                                f"{use_cache},{row_group_size},{page_size},{take_size},{column},{io_size},{takes_per_second}"
                            )
