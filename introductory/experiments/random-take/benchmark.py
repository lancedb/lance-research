# Note, this script expects the CWD is the same directory that contains the script.  It
# also assumes you have built the benchmark program and it is located in the rust build
# directory at target/release/random-take-bench
import os
import subprocess

TAKE_SIZES = [1, 64, 256, 512, 1024]
FORMATS = ["parquet", "lance"]
COLUMNS = ["double", "vector"]
USE_CACHE = [False, True]
ROW_GROUP_SIZES = [100000]
ITERATIONS = 1000

def get_bench_args(iterations: int, format: str, column_type: str, take_size: int, row_group_size: int, cache_metadata: bool, quiet: bool):
    args = [
        "target/release/random-take-bench",
        "--iterations",
        f"{iterations}",
        "--take-size",
        f"{take_size}",
        "--row-group-size",
        f"{row_group_size}",
    ]

    if quiet:
        args.append("--quiet")
    
    if cache_metadata:
        args.append("--cache-metadata")
    
    args.extend([
        format,
        column_type,
        "/home/pace/dev/data/laion_100m/shard_0000.parquet"
    ])

    return args

def bench_latency(format: str, column_type: str, take_size: int, row_group_size: int, cache_metadata: bool):
    if column_type == "vector" and take_size > 100:
        iterations = 100
    else:
        iterations = 1000
    if os.environ.get("QUICK") is not None:
        iterations = 5

    args = get_bench_args(iterations, format, column_type, take_size, row_group_size, cache_metadata, True)

    result = subprocess.run(args, capture_output=True)
    if result.returncode != 0:
        print(args)
        print(result.stdout)
        print(result.stderr)
        assert False

    return float(result.stdout)

def bench_io(format: str, column_type: str, take_size: int, row_group_size: int, cache_metadata: bool):
    if format == "parquet":
        datafile = f"/tmp/input_rgs_{row_group_size}.parquet"
    else:
        datafiles = os.listdir(f"/tmp/bench_dataset.lance/data")
        assert len(datafiles) == 1
        datafile = f"/tmp/bench_dataset.lance/data/{str(datafiles[0])}"

    args = [
        "strace",
        "-e",
        "pread64",
        "-z",
        "-qqq",
        "-f",
        "-P",
        datafile
    ]
    args.extend(get_bench_args(1, format, column_type, take_size, row_group_size, cache_metadata, False))

    result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if result.returncode != 0:
        print(args)
        print(result.stdout)
        print(result.stderr)
        assert False

    lines = [line.strip() for line in result.stdout.splitlines()]
    start_idx = lines.index(b"Bench Start")
    stop_idx = lines.index(b"Bench End")
    lines = lines[start_idx+1:stop_idx]
    iops = len(lines)
    io_size = sum([int(line.rpartition(b"=")[2]) for line in lines])
    return io_size, iops

if __name__ == '__main__':
    print("use_cache,format,row_group_size,take_size,column,latency,io_size,iops")
    for use_cache in USE_CACHE:
        for format in FORMATS:
            for row_group_size in ROW_GROUP_SIZES:
                for take_size in TAKE_SIZES:
                    for column in COLUMNS:
                        latency = bench_latency(format, column, take_size, row_group_size, use_cache)
                        io_size, iops = bench_io(format, column, take_size, row_group_size, use_cache)
                        print(f"{use_cache},{format},{row_group_size},{take_size},{column},{latency},{io_size},{iops}")
